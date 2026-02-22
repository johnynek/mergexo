from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import logging
import queue
import re
import threading
import time

from mergexo.agent_adapter import (
    AgentAdapter,
    AgentSession,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
)
from mergexo.config import AppConfig, RepoConfig
from mergexo.feedback_loop import (
    FeedbackEventRecord,
    append_action_token,
    compute_general_comment_token,
    compute_review_reply_token,
    compute_turn_key,
    event_key,
    extract_action_tokens,
    has_action_token,
    is_bot_login,
)
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.models import (
    GeneratedDesign,
    Issue,
    IssueFlow,
    PullRequestIssueComment,
    PullRequestSnapshot,
    PullRequestReviewComment,
    WorkResult,
)
from mergexo.observability import log_event
from mergexo.state import (
    ImplementationCandidateState,
    StateStore,
    TrackedPullRequestState,
)


LOGGER = logging.getLogger("mergexo.orchestrator")
_MAX_FEEDBACK_GIT_OP_ROUNDS = 3
_MAX_FEEDBACK_GIT_OPS_PER_ROUND = 4


@dataclass(frozen=True)
class _SlotLease:
    slot: int
    path: Path


@dataclass(frozen=True)
class _FeedbackFuture:
    issue_number: int
    future: Future[None]


@dataclass(frozen=True)
class _GitOpOutcome:
    op: str
    success: bool
    detail: str


class DirectFlowError(RuntimeError):
    """Base class for direct-flow startup failures."""


class DirectFlowBlockedError(DirectFlowError):
    """Agent reported it cannot safely proceed without more context."""


class DirectFlowValidationError(DirectFlowError):
    """Direct-flow output failed deterministic policy checks."""


class SlotPool:
    def __init__(self, manager: GitRepoManager, worker_count: int) -> None:
        self._manager = manager
        self._slots: queue.Queue[int] = queue.Queue(maxsize=worker_count)
        for slot in range(worker_count):
            self._slots.put(slot)

    def acquire(self) -> _SlotLease:
        slot = self._slots.get(block=True)
        path = self._manager.ensure_checkout(slot)
        log_event(LOGGER, "slot_acquired", slot=slot)
        return _SlotLease(slot=slot, path=path)

    def release(self, lease: _SlotLease) -> None:
        self._slots.put(lease.slot)
        log_event(LOGGER, "slot_released", slot=lease.slot)


class Phase1Orchestrator:
    def __init__(
        self,
        config: AppConfig,
        *,
        state: StateStore,
        github: GitHubGateway,
        git_manager: GitRepoManager,
        agent: AgentAdapter,
    ) -> None:
        self._config = config
        self._state = state
        self._github = github
        self._git = git_manager
        self._agent = agent
        self._slot_pool = SlotPool(git_manager, config.runtime.worker_count)
        self._running: dict[int, Future[WorkResult]] = {}
        self._running_feedback: dict[int, _FeedbackFuture] = {}
        self._running_lock = threading.Lock()

    def run(self, *, once: bool) -> None:
        self._git.ensure_layout()

        with ThreadPoolExecutor(max_workers=self._config.runtime.worker_count) as pool:
            while True:
                log_event(
                    LOGGER,
                    "poll_started",
                    once=once,
                    feedback_loop_enabled=self._config.runtime.enable_feedback_loop,
                )
                self._reap_finished()
                self._enqueue_new_work(pool)
                self._enqueue_implementation_work(pool)
                if self._config.runtime.enable_feedback_loop:
                    self._enqueue_feedback_work(pool)
                log_event(
                    LOGGER,
                    "poll_completed",
                    running_issue_count=len(self._running),
                    running_feedback_count=len(self._running_feedback),
                )

                if once:
                    self._wait_for_all(pool)
                    break

                time.sleep(self._config.runtime.poll_interval_seconds)

    def _enqueue_new_work(self, pool: ThreadPoolExecutor) -> None:
        labels = _trigger_labels(self._config.repo)
        issues = self._github.list_open_issues_with_any_labels(labels)
        log_event(LOGGER, "issues_fetched", issue_count=len(issues), label_count=len(labels))
        for issue in issues:
            flow = _resolve_issue_flow(
                issue=issue,
                design_label=self._config.repo.trigger_label,
                bugfix_label=self._config.repo.bugfix_label,
                small_job_label=self._config.repo.small_job_label,
            )
            if flow is None:
                log_event(
                    LOGGER,
                    "issue_skipped",
                    issue_number=issue.number,
                    reason="no_matching_trigger_label",
                )
                continue

            with self._running_lock:
                if not self._has_capacity_locked():
                    log_event(
                        LOGGER,
                        "issue_skipped",
                        issue_number=issue.number,
                        reason="worker_capacity_full",
                    )
                    return
                if issue.number in self._running:
                    log_event(
                        LOGGER,
                        "issue_skipped",
                        issue_number=issue.number,
                        reason="already_running",
                    )
                    continue

            if not self._state.can_enqueue(issue.number):
                log_event(
                    LOGGER,
                    "issue_skipped",
                    issue_number=issue.number,
                    reason="already_processed",
                )
                continue

            self._state.mark_running(issue.number)
            fut = pool.submit(self._process_issue, issue, flow)
            with self._running_lock:
                self._running[issue.number] = fut
            log_event(LOGGER, "issue_enqueued", issue_number=issue.number, flow=flow)

    def _enqueue_implementation_work(self, pool: ThreadPoolExecutor) -> None:
        candidates = self._state.list_implementation_candidates()
        log_event(
            LOGGER,
            "implementation_candidates_fetched",
            candidate_count=len(candidates),
        )
        for candidate in candidates:
            with self._running_lock:
                if not self._has_capacity_locked():
                    log_event(
                        LOGGER,
                        "implementation_skipped",
                        issue_number=candidate.issue_number,
                        reason="worker_capacity_full",
                    )
                    return
                if candidate.issue_number in self._running:
                    log_event(
                        LOGGER,
                        "implementation_skipped",
                        issue_number=candidate.issue_number,
                        reason="already_running",
                    )
                    continue

            self._state.mark_running(candidate.issue_number)
            fut = pool.submit(self._process_implementation_candidate, candidate)
            with self._running_lock:
                self._running[candidate.issue_number] = fut
            log_event(
                LOGGER,
                "issue_enqueued",
                issue_number=candidate.issue_number,
                flow="implementation",
            )

    def _enqueue_feedback_work(self, pool: ThreadPoolExecutor) -> None:
        tracked_prs = self._state.list_tracked_pull_requests()
        log_event(LOGGER, "feedback_scan_started", tracked_pr_count=len(tracked_prs))
        for tracked in tracked_prs:
            with self._running_lock:
                if not self._has_capacity_locked():
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="worker_capacity_full",
                    )
                    return
                if tracked.pr_number in self._running_feedback:
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="already_running",
                    )
                    continue
            fut = pool.submit(self._process_feedback_turn, tracked)
            with self._running_lock:
                self._running_feedback[tracked.pr_number] = _FeedbackFuture(
                    issue_number=tracked.issue_number,
                    future=fut,
                )

    def _has_capacity_locked(self) -> bool:
        active = len(self._running) + len(self._running_feedback)
        return active < self._config.runtime.worker_count

    def _reap_finished(self) -> None:
        finished_issue_numbers: list[int] = []
        finished_pr_numbers: list[int] = []
        with self._running_lock:
            for issue_number, fut in self._running.items():
                if fut.done():
                    finished_issue_numbers.append(issue_number)

            for pr_number, handle in self._running_feedback.items():
                if handle.future.done():
                    finished_pr_numbers.append(pr_number)

            for issue_number in finished_issue_numbers:
                fut = self._running.pop(issue_number)
                try:
                    result = fut.result()
                    self._state.mark_completed(
                        issue_number=result.issue_number,
                        branch=result.branch,
                        pr_number=result.pr_number,
                        pr_url=result.pr_url,
                    )
                    log_event(
                        LOGGER,
                        "issue_processing_completed",
                        issue_number=result.issue_number,
                        pr_number=result.pr_number,
                        branch=result.branch,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._state.mark_failed(issue_number=issue_number, error=str(exc))
                    log_event(
                        LOGGER,
                        "issue_processing_failed",
                        issue_number=issue_number,
                        error_type=type(exc).__name__,
                    )

            for pr_number in finished_pr_numbers:
                handle = self._running_feedback.pop(pr_number)
                try:
                    handle.future.result()
                    log_event(
                        LOGGER,
                        "feedback_turn_completed",
                        issue_number=handle.issue_number,
                        pr_number=pr_number,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._state.mark_pr_status(
                        pr_number=pr_number,
                        issue_number=handle.issue_number,
                        status="blocked",
                        error=str(exc),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=handle.issue_number,
                        pr_number=pr_number,
                        reason=type(exc).__name__,
                    )

    def _wait_for_all(self, pool: ThreadPoolExecutor) -> None:
        while True:
            self._reap_finished()
            with self._running_lock:
                if not self._running and not self._running_feedback:
                    return
            time.sleep(1.0)

    def _process_issue(self, issue: Issue, flow: IssueFlow) -> WorkResult:
        lease = self._slot_pool.acquire()
        try:
            log_event(
                LOGGER,
                "issue_processing_started",
                issue_number=issue.number,
                slot=lease.slot,
                flow=flow,
            )
            self._git.prepare_checkout(lease.path)
            if flow == "design_doc":
                return self._process_design_issue(issue=issue, checkout_path=lease.path)
            return self._process_direct_issue(issue=issue, flow=flow, checkout_path=lease.path)
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "issue_processing_failed",
                issue_number=issue.number,
                flow=flow,
                error_type=type(exc).__name__,
            )
            # Re-raise by design: _reap_finished() consumes Future failures and marks
            # the issue as failed in state without crashing the orchestrator loop.
            raise
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

    def _process_design_issue(self, *, issue: Issue, checkout_path: Path) -> WorkResult:
        slug = _slugify(issue.title)
        branch = _issue_branch(flow="design_doc", issue_number=issue.number, slug=slug)
        self._git.create_or_reset_branch(checkout_path, branch)

        design_relpath = f"{self._config.repo.design_docs_dir}/{issue.number}-{slug}.md"
        log_event(
            LOGGER,
            "design_turn_started",
            issue_number=issue.number,
            branch=branch,
        )
        start_result = self._agent.start_design_from_issue(
            issue=issue,
            repo_full_name=self._config.repo.full_name,
            design_doc_path=design_relpath,
            default_branch=self._config.repo.default_branch,
            cwd=checkout_path,
        )
        log_event(LOGGER, "design_turn_completed", issue_number=issue.number)
        generated = start_result.design
        if start_result.session:
            self._state.save_agent_session(
                issue_number=issue.number,
                adapter=start_result.session.adapter,
                thread_id=start_result.session.thread_id,
            )

        design_abs_path = checkout_path / design_relpath
        design_abs_path.parent.mkdir(parents=True, exist_ok=True)
        design_abs_path.write_text(
            _render_design_doc(issue=issue, design=generated),
            encoding="utf-8",
        )

        self._git.commit_all(checkout_path, f"docs: add design for issue #{issue.number}")
        self._git.push_branch(checkout_path, branch)

        pr = self._github.create_pull_request(
            title=f"Design doc for #{issue.number}: {generated.title}",
            head=branch,
            base=self._config.repo.default_branch,
            body=(
                f"Design doc for issue #{issue.number}.\n\n"
                f"Refs #{issue.number}\n\n"
                f"Source issue: {issue.html_url}"
            ),
        )

        self._github.post_issue_comment(
            issue_number=issue.number,
            body=f"Opened design PR: {pr.html_url}",
        )

        log_event(
            LOGGER,
            "issue_processing_completed",
            issue_number=issue.number,
            pr_number=pr.number,
            branch=branch,
            flow="design_doc",
        )
        return WorkResult(
            issue_number=issue.number,
            branch=branch,
            pr_number=pr.number,
            pr_url=pr.html_url,
        )

    def _process_direct_issue(
        self,
        *,
        issue: Issue,
        flow: IssueFlow,
        checkout_path: Path,
    ) -> WorkResult:
        slug = _slugify(issue.title)
        branch = _issue_branch(flow=flow, issue_number=issue.number, slug=slug)
        self._git.create_or_reset_branch(checkout_path, branch)

        if flow == "bugfix":
            start_result = self._agent.start_bugfix_from_issue(
                issue=issue,
                repo_full_name=self._config.repo.full_name,
                default_branch=self._config.repo.default_branch,
                coding_guidelines_path=self._config.repo.coding_guidelines_path,
                cwd=checkout_path,
            )
            flow_label = "bugfix"
        elif flow == "small_job":
            start_result = self._agent.start_small_job_from_issue(
                issue=issue,
                repo_full_name=self._config.repo.full_name,
                default_branch=self._config.repo.default_branch,
                coding_guidelines_path=self._config.repo.coding_guidelines_path,
                cwd=checkout_path,
            )
            flow_label = "small-job"
        else:
            raise DirectFlowValidationError(f"Unsupported direct flow: {flow}")

        if start_result.session:
            self._state.save_agent_session(
                issue_number=issue.number,
                adapter=start_result.session.adapter,
                thread_id=start_result.session.thread_id,
            )

        if start_result.blocked_reason:
            self._github.post_issue_comment(
                issue_number=issue.number,
                body=(
                    f"MergeXO {flow_label} flow was blocked for issue #{issue.number}: "
                    f"{start_result.blocked_reason}"
                ),
            )
            raise DirectFlowBlockedError(
                f"{flow_label} flow blocked: {start_result.blocked_reason}"
            )

        if flow == "bugfix":
            staged_files = self._git.list_staged_files(checkout_path)
            if not _has_regression_test_changes(staged_files):
                self._github.post_issue_comment(
                    issue_number=issue.number,
                    body=(
                        "MergeXO bugfix flow requires at least one staged regression test under "
                        "`tests/`. No PR was opened."
                    ),
                )
                raise DirectFlowValidationError(
                    "Bugfix flow requires at least one staged regression test under tests/"
                )

        commit_message = start_result.commit_message or _default_commit_message(
            flow=flow, issue_number=issue.number
        )
        self._git.commit_all(checkout_path, commit_message)
        self._git.push_branch(checkout_path, branch)

        pr = self._github.create_pull_request(
            title=start_result.pr_title,
            head=branch,
            base=self._config.repo.default_branch,
            body=(
                f"{start_result.pr_summary}\n\n"
                f"Fixes #{issue.number}\n\n"
                f"Source issue: {issue.html_url}"
            ),
        )

        self._github.post_issue_comment(
            issue_number=issue.number,
            body=f"Opened {flow_label} PR: {pr.html_url}",
        )
        log_event(
            LOGGER,
            "issue_processing_completed",
            issue_number=issue.number,
            pr_number=pr.number,
            branch=branch,
            flow=flow,
        )
        return WorkResult(
            issue_number=issue.number,
            branch=branch,
            pr_number=pr.number,
            pr_url=pr.html_url,
        )

    def _process_implementation_candidate(
        self,
        candidate: ImplementationCandidateState,
    ) -> WorkResult:
        lease = self._slot_pool.acquire()
        try:
            log_event(
                LOGGER,
                "issue_processing_started",
                issue_number=candidate.issue_number,
                slot=lease.slot,
                flow="implementation",
            )
            self._git.prepare_checkout(lease.path)

            issue = self._github.get_issue(candidate.issue_number)
            slug = _design_branch_slug(candidate.design_branch)
            if slug is None:
                raise DirectFlowValidationError(
                    "Implementation candidate is missing a valid design branch suffix"
                )

            branch = f"agent/impl/{slug}"
            self._git.create_or_reset_branch(lease.path, branch)

            design_relpath = f"{self._config.repo.design_docs_dir}/{slug}.md"
            design_abs_path = lease.path / design_relpath
            if not design_abs_path.exists():
                detail = (
                    f" Design PR: {candidate.design_pr_url}"
                    if candidate.design_pr_url is not None
                    else ""
                )
                self._github.post_issue_comment(
                    issue_number=issue.number,
                    body=(
                        "MergeXO implementation flow requires a merged design doc at "
                        f"`{design_relpath}` on `{self._config.repo.default_branch}`.{detail}"
                    ),
                )
                raise DirectFlowValidationError(
                    f"Implementation flow requires merged design doc at {design_relpath}"
                )
            design_doc_markdown = design_abs_path.read_text(encoding="utf-8")

            start_result = self._agent.start_implementation_from_design(
                issue=issue,
                repo_full_name=self._config.repo.full_name,
                default_branch=self._config.repo.default_branch,
                coding_guidelines_path=self._config.repo.coding_guidelines_path,
                design_doc_path=design_relpath,
                design_doc_markdown=design_doc_markdown,
                design_pr_number=candidate.design_pr_number,
                design_pr_url=candidate.design_pr_url,
                cwd=lease.path,
            )
            if start_result.session:
                self._state.save_agent_session(
                    issue_number=issue.number,
                    adapter=start_result.session.adapter,
                    thread_id=start_result.session.thread_id,
                )

            if start_result.blocked_reason:
                self._github.post_issue_comment(
                    issue_number=issue.number,
                    body=(
                        f"MergeXO implementation flow was blocked for issue #{issue.number}: "
                        f"{start_result.blocked_reason}"
                    ),
                )
                raise DirectFlowBlockedError(
                    f"implementation flow blocked: {start_result.blocked_reason}"
                )

            commit_message = start_result.commit_message or _default_commit_message(
                flow="small_job", issue_number=issue.number
            )
            self._git.commit_all(lease.path, commit_message)
            self._git.push_branch(lease.path, branch)

            design_doc_url = _design_doc_url(
                repo_full_name=self._config.repo.full_name,
                default_branch=self._config.repo.default_branch,
                design_doc_path=design_relpath,
            )
            design_pr_line = (
                f"Design source PR: {candidate.design_pr_url}\n\n"
                if candidate.design_pr_url
                else ""
            )
            pr = self._github.create_pull_request(
                title=start_result.pr_title,
                head=branch,
                base=self._config.repo.default_branch,
                body=(
                    f"{start_result.pr_summary}\n\n"
                    f"Fixes #{issue.number}\n\n"
                    f"Implements design doc: [{design_relpath}]({design_doc_url})\n\n"
                    f"{design_pr_line}"
                    f"Source issue: {issue.html_url}"
                ),
            )

            self._github.post_issue_comment(
                issue_number=issue.number,
                body=f"Opened implementation PR: {pr.html_url}",
            )
            log_event(
                LOGGER,
                "issue_processing_completed",
                issue_number=issue.number,
                pr_number=pr.number,
                branch=branch,
                flow="implementation",
            )
            return WorkResult(
                issue_number=issue.number,
                branch=branch,
                pr_number=pr.number,
                pr_url=pr.html_url,
            )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "issue_processing_failed",
                issue_number=candidate.issue_number,
                flow="implementation",
                error_type=type(exc).__name__,
            )
            raise
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

    def _process_feedback_turn(self, tracked: TrackedPullRequestState) -> None:
        lease = self._slot_pool.acquire()
        try:
            log_event(
                LOGGER,
                "feedback_turn_started",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                slot=lease.slot,
                branch=tracked.branch,
            )
            pr = self._github.get_pull_request(tracked.pr_number)
            if pr.merged:
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="merged",
                    last_seen_head_sha=pr.head_sha,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_merged",
                )
                return
            if pr.state.lower() != "open":
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="closed",
                    last_seen_head_sha=pr.head_sha,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_closed",
                )
                return

            review_comments = self._github.list_pull_request_review_comments(tracked.pr_number)
            issue_comments = self._github.list_pull_request_issue_comments(tracked.pr_number)
            changed_files = self._github.list_pull_request_files(tracked.pr_number)
            issue = self._github.get_issue(tracked.issue_number)

            previous_pending = self._state.list_pending_feedback_events(tracked.pr_number)
            normalized_review = self._normalize_review_events(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                comments=review_comments,
            )
            normalized_issue = self._normalize_issue_events(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                comments=issue_comments,
            )
            self._state.ingest_feedback_events([event for event, _ in normalized_review])
            self._state.ingest_feedback_events([event for event, _ in normalized_issue])

            pending_events = self._state.list_pending_feedback_events(tracked.pr_number)
            log_event(
                LOGGER,
                "feedback_events_pending",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                pending_count=len(pending_events),
            )
            if not pending_events:
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="no_pending_events",
                )
                return

            pending_event_keys = tuple(event.event_key for event in pending_events)
            pending_event_key_set = set(pending_event_keys)
            pending_review_comments = tuple(
                comment
                for normalized_event, comment in normalized_review
                if normalized_event.event_key in pending_event_key_set
            )
            pending_issue_comments = tuple(
                comment
                for normalized_event, comment in normalized_issue
                if normalized_event.event_key in pending_event_key_set
            )

            session_row = self._state.get_agent_session(tracked.issue_number)
            if session_row is None:
                self._github.post_issue_comment(
                    issue_number=tracked.pr_number,
                    body=(
                        "MergeXO feedback loop is blocked for this PR because no saved "
                        f"agent session was found for issue #{tracked.issue_number}."
                    ),
                )
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="blocked",
                    last_seen_head_sha=pr.head_sha,
                    error="missing saved agent session",
                )
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="missing_agent_session",
                )
                return
            session = AgentSession(adapter=session_row[0], thread_id=session_row[1])

            if not self._git.restore_feedback_branch(lease.path, tracked.branch, pr.head_sha):
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="head_mismatch_retry",
                )
                return

            turn_head_sha = (
                pr.head_sha if not previous_pending else (tracked.last_seen_head_sha or pr.head_sha)
            )
            self._state.mark_pr_status(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                status="awaiting_feedback",
                last_seen_head_sha=turn_head_sha,
            )
            turn_key = compute_turn_key(
                pr_number=tracked.pr_number,
                head_sha=turn_head_sha,
                pending_event_keys=pending_event_keys,
            )
            turn = FeedbackTurn(
                turn_key=turn_key,
                issue=issue,
                pull_request=pr,
                review_comments=pending_review_comments,
                issue_comments=pending_issue_comments,
                changed_files=changed_files,
            )

            feedback_outcome = self._run_feedback_agent_with_git_ops(
                tracked=tracked,
                session=session,
                turn=turn,
                checkout_path=lease.path,
                pull_request=pr,
            )
            if feedback_outcome is None:
                return
            result, pr = feedback_outcome

            if result.commit_message:
                try:
                    self._git.commit_all(lease.path, result.commit_message)
                except RuntimeError as exc:
                    if _is_no_staged_changes_error(exc):
                        error = (
                            "agent returned commit_message but no staged changes were found; "
                            "feedback turn requires repo edits before posting replies"
                        )
                        self._state.mark_pr_status(
                            pr_number=tracked.pr_number,
                            issue_number=tracked.issue_number,
                            status="blocked",
                            last_seen_head_sha=pr.head_sha,
                            error=error,
                        )
                        log_event(
                            LOGGER,
                            "feedback_turn_blocked",
                            issue_number=tracked.issue_number,
                            pr_number=tracked.pr_number,
                            reason="commit_message_without_changes",
                        )
                        return
                    raise
                else:
                    self._git.push_branch(lease.path, tracked.branch)
                    pr = self._github.get_pull_request(tracked.pr_number)
                    if pr.merged:
                        self._state.mark_pr_status(
                            pr_number=tracked.pr_number,
                            issue_number=tracked.issue_number,
                            status="merged",
                            last_seen_head_sha=pr.head_sha,
                        )
                        log_event(
                            LOGGER,
                            "feedback_turn_completed",
                            issue_number=tracked.issue_number,
                            pr_number=tracked.pr_number,
                            result="pr_merged",
                        )
                        return
                    if pr.state.lower() != "open":
                        self._state.mark_pr_status(
                            pr_number=tracked.pr_number,
                            issue_number=tracked.issue_number,
                            status="closed",
                            last_seen_head_sha=pr.head_sha,
                        )
                        log_event(
                            LOGGER,
                            "feedback_turn_completed",
                            issue_number=tracked.issue_number,
                            pr_number=tracked.pr_number,
                            result="pr_closed",
                        )
                        return

            expected_tokens: list[str] = []
            for review_reply in result.review_replies:
                token = compute_review_reply_token(
                    turn_key=turn_key,
                    review_comment_id=review_reply.review_comment_id,
                    body=review_reply.body,
                )
                expected_tokens.append(token)
                if self._token_exists_remotely(tracked.pr_number, token):
                    continue
                self._github.post_review_comment_reply(
                    pr_number=tracked.pr_number,
                    review_comment_id=review_reply.review_comment_id,
                    body=append_action_token(body=review_reply.body, token=token),
                )

            if result.general_comment:
                token = compute_general_comment_token(
                    turn_key=turn_key, body=result.general_comment
                )
                expected_tokens.append(token)
                if not self._token_exists_remotely(tracked.pr_number, token):
                    self._github.post_issue_comment(
                        issue_number=tracked.pr_number,
                        body=append_action_token(body=result.general_comment, token=token),
                    )

            remote_tokens = self._fetch_remote_action_tokens(tracked.pr_number)
            if not set(expected_tokens).issubset(remote_tokens):
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="token_reconciliation_incomplete",
                )
                return

            self._state.finalize_feedback_turn(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                processed_event_keys=pending_event_keys,
                session=result.session,
                head_sha=pr.head_sha,
            )
            log_event(
                LOGGER,
                "feedback_turn_completed",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                turn_key=turn_key,
                processed_event_count=len(pending_event_keys),
            )
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

    def _run_feedback_agent_with_git_ops(
        self,
        *,
        tracked: TrackedPullRequestState,
        session: AgentSession,
        turn: FeedbackTurn,
        checkout_path: Path,
        pull_request: PullRequestSnapshot,
    ) -> tuple[FeedbackResult, PullRequestSnapshot] | None:
        current_session = session
        current_turn = turn
        current_pr = pull_request
        round_number = 0

        while True:
            log_event(
                LOGGER,
                "feedback_agent_call_started",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                turn_key=current_turn.turn_key,
            )
            result = self._agent.respond_to_feedback(
                session=current_session,
                turn=current_turn,
                cwd=checkout_path,
            )
            log_event(
                LOGGER,
                "feedback_agent_call_completed",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                turn_key=current_turn.turn_key,
                review_reply_count=len(result.review_replies),
                has_general_comment=result.general_comment is not None,
                has_commit_message=result.commit_message is not None,
            )
            current_session = result.session
            if not result.git_ops:
                return result, current_pr

            if len(result.git_ops) > _MAX_FEEDBACK_GIT_OPS_PER_ROUND:
                error = (
                    "agent requested too many git operations in one round; "
                    f"max={_MAX_FEEDBACK_GIT_OPS_PER_ROUND}"
                )
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="blocked",
                    last_seen_head_sha=current_pr.head_sha,
                    error=error,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="too_many_git_ops_requested",
                )
                return None

            round_number += 1
            if round_number > _MAX_FEEDBACK_GIT_OP_ROUNDS:
                error = (
                    "agent exceeded maximum git-op follow-up rounds; "
                    f"max={_MAX_FEEDBACK_GIT_OP_ROUNDS}"
                )
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="blocked",
                    last_seen_head_sha=current_pr.head_sha,
                    error=error,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="git_op_round_limit_exceeded",
                )
                return None

            outcomes = self._execute_feedback_git_ops(
                tracked=tracked,
                checkout_path=checkout_path,
                requests=result.git_ops,
            )

            current_pr = self._github.get_pull_request(tracked.pr_number)
            if current_pr.merged:
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="merged",
                    last_seen_head_sha=current_pr.head_sha,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_merged",
                )
                return None
            if current_pr.state.lower() != "open":
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="closed",
                    last_seen_head_sha=current_pr.head_sha,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_closed",
                )
                return None

            current_turn = FeedbackTurn(
                turn_key=current_turn.turn_key,
                issue=current_turn.issue,
                pull_request=current_pr,
                review_comments=current_turn.review_comments,
                issue_comments=current_turn.issue_comments
                + (
                    PullRequestIssueComment(
                        comment_id=-(1000 + round_number),
                        body=_render_git_op_result_comment(
                            outcomes=outcomes, round_number=round_number
                        ),
                        user_login="mergexo-system",
                        html_url="",
                        created_at="now",
                        updated_at="now",
                    ),
                ),
                changed_files=self._github.list_pull_request_files(tracked.pr_number),
            )

    def _execute_feedback_git_ops(
        self,
        *,
        tracked: TrackedPullRequestState,
        checkout_path: Path,
        requests: tuple[GitOpRequest, ...],
    ) -> list[_GitOpOutcome]:
        outcomes: list[_GitOpOutcome] = []
        for request in requests:
            log_event(
                LOGGER,
                "feedback_git_op_requested",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                op=request.op,
            )
            try:
                self._execute_feedback_git_op(checkout_path=checkout_path, request=request)
            except Exception as exc:  # noqa: BLE001
                log_event(
                    LOGGER,
                    "feedback_git_op_failed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    op=request.op,
                    error_type=type(exc).__name__,
                )
                outcomes.append(
                    _GitOpOutcome(
                        op=request.op, success=False, detail=_summarize_git_error(str(exc))
                    )
                )
            else:
                log_event(
                    LOGGER,
                    "feedback_git_op_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    op=request.op,
                )
                outcomes.append(_GitOpOutcome(op=request.op, success=True, detail="ok"))
        return outcomes

    def _execute_feedback_git_op(self, *, checkout_path: Path, request: GitOpRequest) -> None:
        if request.op == "fetch_origin":
            self._git.fetch_origin(checkout_path)
            return
        if request.op == "merge_origin_default_branch":
            self._git.merge_origin_default_branch(checkout_path)
            return
        raise RuntimeError(f"Unsupported git operation request: {request.op}")

    def _normalize_review_events(
        self,
        *,
        pr_number: int,
        issue_number: int,
        comments: list[PullRequestReviewComment],
    ) -> list[tuple[FeedbackEventRecord, PullRequestReviewComment]]:
        normalized: list[tuple[FeedbackEventRecord, PullRequestReviewComment]] = []
        for comment in comments:
            if is_bot_login(comment.user_login):
                continue
            if has_action_token(comment.body):
                continue
            normalized.append(
                (
                    FeedbackEventRecord(
                        event_key=event_key(
                            pr_number=pr_number,
                            kind="review",
                            comment_id=comment.comment_id,
                            updated_at=comment.updated_at,
                        ),
                        pr_number=pr_number,
                        issue_number=issue_number,
                        kind="review",
                        comment_id=comment.comment_id,
                        updated_at=comment.updated_at,
                    ),
                    comment,
                )
            )
        return normalized

    def _normalize_issue_events(
        self,
        *,
        pr_number: int,
        issue_number: int,
        comments: list[PullRequestIssueComment],
    ) -> list[tuple[FeedbackEventRecord, PullRequestIssueComment]]:
        normalized: list[tuple[FeedbackEventRecord, PullRequestIssueComment]] = []
        for comment in comments:
            if is_bot_login(comment.user_login):
                continue
            if has_action_token(comment.body):
                continue
            normalized.append(
                (
                    FeedbackEventRecord(
                        event_key=event_key(
                            pr_number=pr_number,
                            kind="issue",
                            comment_id=comment.comment_id,
                            updated_at=comment.updated_at,
                        ),
                        pr_number=pr_number,
                        issue_number=issue_number,
                        kind="issue",
                        comment_id=comment.comment_id,
                        updated_at=comment.updated_at,
                    ),
                    comment,
                )
            )
        return normalized

    def _token_exists_remotely(self, pr_number: int, token: str) -> bool:
        return token in self._fetch_remote_action_tokens(pr_number)

    def _fetch_remote_action_tokens(self, pr_number: int) -> set[str]:
        review_comments = self._github.list_pull_request_review_comments(pr_number)
        issue_comments = self._github.list_pull_request_issue_comments(pr_number)
        tokens: set[str] = set()
        for comment in review_comments:
            tokens.update(extract_action_tokens(comment.body))
        for comment in issue_comments:
            tokens.update(extract_action_tokens(comment.body))
        return tokens


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "issue"


def _design_branch_slug(branch: str) -> str | None:
    prefix = "agent/design/"
    if not branch.startswith(prefix):
        return None
    slug = branch[len(prefix) :].strip()
    if not slug:
        return None
    return slug


def _trigger_labels(repo: RepoConfig) -> tuple[str, ...]:
    # Operators may intentionally configure overlapping labels across flows; dedupe avoids
    # repeated GitHub queries while preserving first-occurrence precedence semantics.
    labels: list[str] = []
    for label in (repo.trigger_label, repo.bugfix_label, repo.small_job_label):
        if label not in labels:
            labels.append(label)
    return tuple(labels)


def _resolve_issue_flow(
    *,
    issue: Issue,
    design_label: str,
    bugfix_label: str,
    small_job_label: str,
) -> IssueFlow | None:
    issue_labels = set(issue.labels)
    if bugfix_label in issue_labels:
        return "bugfix"
    if small_job_label in issue_labels:
        return "small_job"
    if design_label in issue_labels:
        return "design_doc"
    return None


def _issue_branch(*, flow: IssueFlow, issue_number: int, slug: str) -> str:
    if flow == "design_doc":
        return f"agent/design/{issue_number}-{slug}"
    if flow == "bugfix":
        return f"agent/bugfix/{issue_number}-{slug}"
    return f"agent/small/{issue_number}-{slug}"


def _default_commit_message(*, flow: IssueFlow, issue_number: int) -> str:
    if flow == "bugfix":
        return f"fix: resolve issue #{issue_number}"
    return f"feat: implement issue #{issue_number}"


def _has_regression_test_changes(paths: tuple[str, ...]) -> bool:
    return any(path == "tests" or path.startswith("tests/") for path in paths)


def _is_no_staged_changes_error(exc: RuntimeError) -> bool:
    return "No staged changes to commit" in str(exc)


def _render_git_op_result_comment(*, outcomes: list[_GitOpOutcome], round_number: int) -> str:
    header = f"MergeXO git operation results (round {round_number}):"
    lines = [header]
    for outcome in outcomes:
        status = "ok" if outcome.success else "failed"
        lines.append(f"- {outcome.op}: {status} ({outcome.detail})")
    lines.append(
        "Continue by editing files directly. If more repository git actions are needed, request them in git_ops."
    )
    return "\n".join(lines)


def _summarize_git_error(raw_error: str) -> str:
    normalized = " ".join(line.strip() for line in raw_error.splitlines() if line.strip())
    if not normalized:
        return "git operation failed"
    if len(normalized) > 240:
        return normalized[:237] + "..."
    return normalized


def _design_doc_url(*, repo_full_name: str, default_branch: str, design_doc_path: str) -> str:
    return f"https://github.com/{repo_full_name}/blob/{default_branch}/{design_doc_path}"


def _render_design_doc(*, issue: Issue, design: GeneratedDesign) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    touch_paths = "\n".join(f"  - {path}" for path in design.touch_paths)
    return (
        "---\n"
        f"issue: {issue.number}\n"
        "priority: 3\n"
        "touch_paths:\n"
        f"{touch_paths}\n"
        "depends_on: []\n"
        "estimated_size: M\n"
        f"generated_at: {now}\n"
        "---\n\n"
        f"# {design.title}\n\n"
        f"_Issue: #{issue.number} ({issue.html_url})_\n\n"
        f"## Summary\n\n{design.summary}\n\n"
        f"{design.design_doc_markdown.strip()}\n"
    )
