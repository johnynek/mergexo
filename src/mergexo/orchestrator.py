from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import logging
import queue
import re
import threading
import time
from typing import Callable, Literal, cast

from mergexo.agent_adapter import (
    AgentAdapter,
    DirectStartResult,
    AgentSession,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
)
from mergexo.config import AppConfig, RepoConfig
from mergexo.feedback_loop import (
    FeedbackEventRecord,
    ParsedOperatorCommand,
    append_action_token,
    compute_general_comment_token,
    compute_history_rewrite_token,
    compute_operator_command_token,
    compute_review_reply_token,
    compute_source_issue_redirect_token,
    compute_turn_key,
    event_key,
    extract_action_tokens,
    has_action_token,
    is_bot_login,
    operator_command_key,
    operator_commands_help,
    parse_operator_command,
)
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import CompareCommitsStatus, GitHubGateway, GitHubPollingError
from mergexo.models import (
    GeneratedDesign,
    Issue,
    IssueFlow,
    OperatorCommandRecord,
    OperatorCommandStatus,
    OperatorReplyStatus,
    RestartMode,
    PullRequestIssueComment,
    PullRequestSnapshot,
    PullRequestReviewComment,
    WorkResult,
)
from mergexo.observability import log_event, logging_repo_context
from mergexo.shell import CommandError, run
from mergexo.state import (
    AgentRunFailureClass,
    ImplementationCandidateState,
    PrePrFollowupState,
    StateStore,
    TrackedPullRequestState,
)


LOGGER = logging.getLogger("mergexo.orchestrator")
_MAX_FEEDBACK_GIT_OP_ROUNDS = 3
_MAX_FEEDBACK_GIT_OPS_PER_ROUND = 4
_MAX_REQUIRED_TEST_REPAIR_ROUNDS = 3
_MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS = 3
_REQUIRED_TEST_FAILURE_OUTPUT_LIMIT = 12000
_ALLOWED_LINEAR_HISTORY_STATUSES: tuple[CompareCommitsStatus, ...] = ("ahead", "identical")
_RESTART_OPERATION_NAME = "restart"
_RESTART_PENDING_STATUSES = {"pending", "running"}
_RECOVERABLE_PRE_PR_ERROR_SIGNATURES: tuple[str, ...] = (
    "flow blocked:",
    "required pre-push tests failed before pushing design branch",
    "required pre-push tests did not pass after automated repair attempts",
    "new_issue_comments_pending",
)
_PRE_PR_BLOCKED_FLOW_PATTERN = re.compile(r"(bugfix|small-job|implementation) flow blocked:")

PrePrFlow = Literal["design_doc", "bugfix", "small_job", "implementation"]


@dataclass(frozen=True)
class _SlotLease:
    slot: int
    path: Path


@dataclass(frozen=True)
class _FeedbackFuture:
    issue_number: int
    run_id: str
    future: Future[str]


@dataclass(frozen=True)
class _GitOpOutcome:
    op: str
    success: bool
    detail: str


@dataclass(frozen=True)
class _RunningIssueMetadata:
    run_id: str
    flow: PrePrFlow
    branch: str
    context_json: str
    source_issue_number: int
    consumed_comment_id_max: int


class DirectFlowError(RuntimeError):
    """Base class for direct-flow startup failures."""


class DirectFlowBlockedError(DirectFlowError):
    """Agent reported it cannot safely proceed without more context."""


class DirectFlowValidationError(DirectFlowError):
    """Direct-flow output failed deterministic policy checks."""


@dataclass(frozen=True)
class RestartRequested(RuntimeError):
    mode: RestartMode
    command_key: str
    repo_full_name: str = ""

    def __str__(self) -> str:
        return (
            f"Restart requested in mode={self.mode} via command_key={self.command_key} "
            f"(repo={self.repo_full_name})"
        )


class SlotPool:
    def __init__(self, manager: GitRepoManager, worker_count: int) -> None:
        self._manager = manager
        self._slots: queue.Queue[int] = queue.Queue(maxsize=worker_count)
        for slot in range(worker_count):
            self._slots.put(slot)

    def acquire(self, *, manager: GitRepoManager | None = None) -> _SlotLease:
        slot = self._slots.get(block=True)
        active_manager = manager if manager is not None else self._manager
        path = active_manager.ensure_checkout(slot)
        log_event(LOGGER, "slot_acquired", slot=slot)
        return _SlotLease(slot=slot, path=path)

    def release(self, lease: _SlotLease) -> None:
        self._slots.put(lease.slot)
        log_event(LOGGER, "slot_released", slot=lease.slot)


class GlobalWorkLimiter:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("GlobalWorkLimiter capacity must be at least 1")
        self._capacity = capacity
        self._in_flight = 0
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        with self._lock:
            if self._in_flight >= self._capacity:
                return False
            self._in_flight += 1
            return True

    def release(self) -> None:
        with self._lock:
            if self._in_flight < 1:
                raise RuntimeError("GlobalWorkLimiter release called when no work is in flight")
            self._in_flight -= 1

    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    def capacity(self) -> int:
        return self._capacity


class Phase1Orchestrator:
    def __init__(
        self,
        config: AppConfig,
        *,
        state: StateStore,
        github: GitHubGateway,
        git_manager: GitRepoManager,
        repo: RepoConfig | None = None,
        github_by_repo_full_name: dict[str, GitHubGateway] | None = None,
        agent: AgentAdapter,
        allow_runtime_restart: bool = False,
        work_limiter: GlobalWorkLimiter | None = None,
    ) -> None:
        self._config = config
        self._state = state
        self._github = github
        self._git = git_manager
        self._repo = repo if repo is not None else config.repo
        self._github_by_repo_full_name = dict(github_by_repo_full_name or {})
        self._github_by_repo_full_name.setdefault(self._repo.full_name, github)
        self._agent = agent
        self._allow_runtime_restart = allow_runtime_restart
        self._work_limiter = (
            work_limiter
            if work_limiter is not None
            else GlobalWorkLimiter(config.runtime.worker_count)
        )
        self._slot_pool = SlotPool(git_manager, config.runtime.worker_count)
        self._running: dict[int, Future[WorkResult]] = {}
        self._running_issue_metadata: dict[int, _RunningIssueMetadata] = {}
        self._running_feedback: dict[int, _FeedbackFuture] = {}
        self._running_lock = threading.Lock()
        self._poll_setup_done = False
        self._restart_drain_started_at_monotonic: float | None = None
        self._legacy_pre_pr_adopted = False
        self._authorized_operator_logins = {
            login.strip().lower() for login in self._repo.operator_logins if login.strip()
        }

    def run(self, *, once: bool) -> None:
        with logging_repo_context(self._repo.full_name):
            with ThreadPoolExecutor(max_workers=self._config.runtime.worker_count) as pool:
                while True:
                    self.poll_once(pool, allow_enqueue=True)
                    self._drain_for_pending_restart_if_needed()

                    if once:
                        self._wait_for_all(pool)
                        self._reap_finished()
                        if self._config.runtime.enable_github_operations:
                            self._run_poll_step(
                                step_name="scan_operator_commands_once",
                                fn=self._scan_operator_commands,
                            )
                        self._drain_for_pending_restart_if_needed()
                        if self._config.runtime.enable_issue_comment_routing:
                            self._run_poll_step(
                                step_name="scan_post_pr_source_issue_comment_redirects_once",
                                fn=self._scan_post_pr_source_issue_comment_redirects,
                            )
                        break

                    time.sleep(self._config.runtime.poll_interval_seconds)

    def poll_once(self, pool: ThreadPoolExecutor, *, allow_enqueue: bool = True) -> None:
        with logging_repo_context(self._repo.full_name):
            self._ensure_poll_setup()
            log_event(
                LOGGER,
                "poll_started",
                once=False,
                github_operations_enabled=self._config.runtime.enable_github_operations,
                allow_enqueue=allow_enqueue,
            )
            self._reap_finished()

            poll_had_github_errors = False
            restart_pending = self._is_restart_pending()
            # Restart drain mode is a hard ingestion stop for this process. Once restart is
            # pending we skip operator-command scans and all enqueue paths so no new GitHub
            # messages are consumed while draining. The only remaining work here is reaping
            # finished futures so terminal state is checkpointed in sqlite before supervisor
            # handoff/re-exec.
            if self._config.runtime.enable_github_operations and not restart_pending:
                if not self._run_poll_step(
                    step_name="scan_operator_commands",
                    fn=self._scan_operator_commands,
                ):
                    poll_had_github_errors = True

            if not restart_pending:
                restart_pending = self._is_restart_pending()
            enqueue_allowed = allow_enqueue and not restart_pending

            if enqueue_allowed:
                # TODO remove migration after updates
                if not self._run_poll_step(
                    step_name="repair_stale_running_runs",
                    fn=self._repair_stale_running_runs,
                ):
                    poll_had_github_errors = True
                if not self._run_poll_step(
                    step_name="repair_failed_no_staged_change_runs",
                    fn=self._repair_failed_no_staged_change_runs,
                ):
                    poll_had_github_errors = True
                if not self._run_poll_step(
                    step_name="enqueue_new_work",
                    fn=lambda: self._enqueue_new_work(pool),
                ):
                    poll_had_github_errors = True
                if not self._run_poll_step(
                    step_name="enqueue_implementation_work",
                    fn=lambda: self._enqueue_implementation_work(pool),
                ):
                    poll_had_github_errors = True
                if self._config.runtime.enable_issue_comment_routing:
                    if not self._run_poll_step(
                        step_name="enqueue_pre_pr_followup_work",
                        fn=lambda: self._enqueue_pre_pr_followup_work(pool),
                    ):
                        poll_had_github_errors = True
                if not self._run_poll_step(
                    step_name="enqueue_feedback_work",
                    fn=lambda: self._enqueue_feedback_work(pool),
                ):
                    poll_had_github_errors = True
                if self._config.runtime.enable_issue_comment_routing:
                    if not self._run_poll_step(
                        step_name="scan_post_pr_source_issue_comment_redirects",
                        fn=self._scan_post_pr_source_issue_comment_redirects,
                    ):
                        poll_had_github_errors = True

            log_event(
                LOGGER,
                "poll_completed",
                running_issue_count=len(self._running),
                running_feedback_count=len(self._running_feedback),
                draining_for_restart=not enqueue_allowed,
                poll_had_github_errors=poll_had_github_errors,
            )

    def pending_work_count(self) -> int:
        with self._running_lock:
            return len(self._running) + len(self._running_feedback)

    def queue_counts(self) -> tuple[int, int]:
        with self._running_lock:
            return len(self._running), len(self._running_feedback)

    def in_flight_work_count(self) -> int:
        return self._work_limiter.in_flight()

    def _ensure_poll_setup(self) -> None:
        if self._poll_setup_done:
            return
        self._git.ensure_layout()
        reconciled_count = self._state.reconcile_unfinished_agent_runs(
            repo_full_name=self._state_repo_full_name()
        )
        if reconciled_count > 0:
            log_event(
                LOGGER,
                "stale_agent_runs_reconciled",
                repo_full_name=self._state_repo_full_name(),
                reconciled_count=reconciled_count,
            )
        self._state.prune_observability_history(
            retention_days=self._config.runtime.observability_history_retention_days,
            repo_full_name=self._state_repo_full_name(),
        )
        if self._config.runtime.enable_issue_comment_routing:
            self._run_poll_step(
                step_name="adopt_legacy_failed_pre_pr_runs",
                fn=self._adopt_legacy_failed_pre_pr_runs,
            )
        self._poll_setup_done = True

    def _is_restart_pending(self) -> bool:
        operation = self._state.get_runtime_operation(_RESTART_OPERATION_NAME)
        return operation is not None and operation.status in _RESTART_PENDING_STATUSES

    def _run_poll_step(self, *, step_name: str, fn: Callable[[], None]) -> bool:
        try:
            fn()
            return True
        except GitHubPollingError as exc:
            log_event(
                LOGGER,
                "poll_step_failed",
                repo_full_name=self._state_repo_full_name(),
                step=step_name,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return False

    def _enqueue_new_work(self, pool: ThreadPoolExecutor) -> None:
        labels = _trigger_labels(self._repo)
        issues = self._github.list_open_issues_with_any_labels(labels)
        log_event(LOGGER, "issues_fetched", issue_count=len(issues), label_count=len(labels))
        for issue in issues:
            if not self._is_issue_author_allowed(
                issue_number=issue.number,
                author_login=issue.author_login,
                reason="unauthorized_issue_author",
            ):
                log_event(
                    LOGGER,
                    "issue_skipped",
                    issue_number=issue.number,
                    reason="unauthorized_issue_author",
                )
                continue

            flow = _resolve_issue_flow(
                issue=issue,
                design_label=self._repo.trigger_label,
                bugfix_label=self._repo.bugfix_label,
                small_job_label=self._repo.small_job_label,
            )
            if flow is None:
                log_event(
                    LOGGER,
                    "issue_skipped",
                    issue_number=issue.number,
                    reason="no_matching_trigger_label",
                )
                continue

            capacity_reserved = False
            try:
                with self._running_lock:
                    if not self._has_capacity_locked() or not self._work_limiter.try_acquire():
                        log_event(
                            LOGGER,
                            "issue_skipped",
                            issue_number=issue.number,
                            reason="worker_capacity_full",
                        )
                        return
                    capacity_reserved = True
                    if issue.number in self._running:
                        log_event(
                            LOGGER,
                            "issue_skipped",
                            issue_number=issue.number,
                            reason="already_running",
                        )
                        continue

                if not self._state.can_enqueue(
                    issue.number, repo_full_name=self._state_repo_full_name()
                ):
                    log_event(
                        LOGGER,
                        "issue_skipped",
                        issue_number=issue.number,
                        reason="already_processed",
                    )
                    continue

                branch = _branch_for_issue_flow(flow=flow, issue=issue)
                context_json = self._serialize_pre_pr_context_for_issue(
                    issue=issue,
                    flow=flow,
                    branch=branch,
                )
                consumed_comment_id_max = self._capture_run_start_comment_id_if_enabled(
                    issue.number
                )
                self._state.mark_running(issue.number, repo_full_name=self._state_repo_full_name())
                run_id = self._state.record_agent_run_start(
                    run_kind="issue_flow",
                    issue_number=issue.number,
                    pr_number=None,
                    flow=flow,
                    branch=branch,
                    repo_full_name=self._state_repo_full_name(),
                )
                fut = pool.submit(self._process_issue_worker, issue, flow, consumed_comment_id_max)
                # Ensure global capacity is returned even if worker code fails.
                fut.add_done_callback(lambda _: self._work_limiter.release())
                capacity_reserved = False
                with self._running_lock:
                    self._running[issue.number] = fut
                    self._running_issue_metadata[issue.number] = _RunningIssueMetadata(
                        run_id=run_id,
                        flow=flow,
                        branch=branch,
                        context_json=context_json,
                        source_issue_number=issue.number,
                        consumed_comment_id_max=consumed_comment_id_max,
                    )
                log_event(
                    LOGGER,
                    "issue_enqueued",
                    issue_number=issue.number,
                    flow=flow,
                    repo_full_name=self._state_repo_full_name(),
                    trigger_label=_flow_trigger_label(flow=flow, repo=self._repo),
                    issue_url=issue.html_url,
                )
            finally:
                if capacity_reserved:
                    self._work_limiter.release()

    def _enqueue_implementation_work(self, pool: ThreadPoolExecutor) -> None:
        candidates = self._state.list_implementation_candidates(
            repo_full_name=self._state_repo_full_name()
        )
        log_event(
            LOGGER,
            "implementation_candidates_fetched",
            candidate_count=len(candidates),
        )
        for candidate in candidates:
            capacity_reserved = False
            try:
                with self._running_lock:
                    if not self._has_capacity_locked() or not self._work_limiter.try_acquire():
                        log_event(
                            LOGGER,
                            "implementation_skipped",
                            issue_number=candidate.issue_number,
                            reason="worker_capacity_full",
                        )
                        return
                    capacity_reserved = True
                    if candidate.issue_number in self._running:
                        log_event(
                            LOGGER,
                            "implementation_skipped",
                            issue_number=candidate.issue_number,
                            reason="already_running",
                        )
                        continue

                branch = _branch_for_implementation_candidate(candidate)
                issue = self._github.get_issue(candidate.issue_number)
                context_json = self._serialize_pre_pr_context_for_implementation(
                    issue=issue,
                    candidate=candidate,
                    branch=branch,
                )
                consumed_comment_id_max = self._capture_run_start_comment_id_if_enabled(
                    candidate.issue_number
                )
                self._state.mark_running(
                    candidate.issue_number, repo_full_name=self._state_repo_full_name()
                )
                run_id = self._state.record_agent_run_start(
                    run_kind="implementation_flow",
                    issue_number=candidate.issue_number,
                    pr_number=None,
                    flow="implementation",
                    branch=branch,
                    repo_full_name=self._state_repo_full_name(),
                )
                fut = pool.submit(
                    self._process_implementation_candidate_worker,
                    candidate,
                    issue,
                    consumed_comment_id_max,
                )
                fut.add_done_callback(lambda _: self._work_limiter.release())
                capacity_reserved = False
                with self._running_lock:
                    self._running[candidate.issue_number] = fut
                    self._running_issue_metadata[candidate.issue_number] = _RunningIssueMetadata(
                        run_id=run_id,
                        flow="implementation",
                        branch=branch,
                        context_json=context_json,
                        source_issue_number=candidate.issue_number,
                        consumed_comment_id_max=consumed_comment_id_max,
                    )
                log_event(
                    LOGGER,
                    "implementation_enqueued",
                    issue_number=candidate.issue_number,
                    flow="implementation",
                )
            finally:
                if capacity_reserved:
                    self._work_limiter.release()

    def _enqueue_feedback_work(self, pool: ThreadPoolExecutor) -> None:
        tracked_prs = self._state.list_tracked_pull_requests(
            repo_full_name=self._state_repo_full_name()
        )
        log_event(LOGGER, "feedback_scan_started", tracked_pr_count=len(tracked_prs))
        for tracked in tracked_prs:
            capacity_reserved = False
            try:
                with self._running_lock:
                    if not self._has_capacity_locked() or not self._work_limiter.try_acquire():
                        log_event(
                            LOGGER,
                            "feedback_turn_blocked",
                            issue_number=tracked.issue_number,
                            pr_number=tracked.pr_number,
                            reason="worker_capacity_full",
                        )
                        return
                    capacity_reserved = True
                    if tracked.pr_number in self._running_feedback:
                        log_event(
                            LOGGER,
                            "feedback_turn_blocked",
                            issue_number=tracked.issue_number,
                            pr_number=tracked.pr_number,
                            reason="already_running",
                        )
                        continue
                run_id = self._state.record_agent_run_start(
                    run_kind="feedback_turn",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    flow=None,
                    branch=tracked.branch,
                    repo_full_name=self._state_repo_full_name(),
                )
                fut = pool.submit(self._process_feedback_turn_worker, tracked)
                fut.add_done_callback(lambda _: self._work_limiter.release())
                capacity_reserved = False
                with self._running_lock:
                    self._running_feedback[tracked.pr_number] = _FeedbackFuture(
                        issue_number=tracked.issue_number,
                        run_id=run_id,
                        future=fut,
                    )
            finally:
                if capacity_reserved:
                    self._work_limiter.release()

    def _enqueue_pre_pr_followup_work(self, pool: ThreadPoolExecutor) -> None:
        followups = self._state.list_pre_pr_followups(repo_full_name=self._state_repo_full_name())
        if not followups:
            return

        for followup in followups:
            capacity_reserved = False
            try:
                with self._running_lock:
                    if not self._has_capacity_locked() or not self._work_limiter.try_acquire():
                        log_event(
                            LOGGER,
                            "pre_pr_followup_enqueued",
                            repo_full_name=self._state_repo_full_name(),
                            issue_number=followup.issue_number,
                            reason="worker_capacity_full",
                        )
                        return
                    capacity_reserved = True
                    if followup.issue_number in self._running:
                        continue

                source_comments = self._github.list_issue_comments(followup.issue_number)
                cursor = self._state.get_issue_comment_cursor(
                    followup.issue_number,
                    repo_full_name=self._state_repo_full_name(),
                )
                pending_comments = self._pending_source_issue_followups(
                    comments=source_comments,
                    after_comment_id=cursor.pre_pr_last_consumed_comment_id,
                )
                if not pending_comments:
                    continue

                log_event(
                    LOGGER,
                    "pre_pr_followup_comments_detected",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=followup.issue_number,
                    pending_comment_count=len(pending_comments),
                    first_comment_id=pending_comments[0].comment_id,
                    last_comment_id=pending_comments[-1].comment_id,
                )

                issue, candidate = self._decode_pre_pr_context(followup=followup)
                if followup.flow == "implementation" and candidate is None:
                    log_event(
                        LOGGER,
                        "pre_pr_followup_waiting",
                        repo_full_name=self._state_repo_full_name(),
                        issue_number=followup.issue_number,
                        reason="missing_implementation_context",
                    )
                    continue

                resume_issue = self._augment_issue_with_pre_pr_followups(
                    issue=issue,
                    waiting_reason=followup.waiting_reason,
                    followups=pending_comments,
                )
                consumed_comment_id_max = pending_comments[-1].comment_id
                branch = _branch_for_pre_pr_followup(
                    flow=followup.flow,
                    issue=resume_issue,
                    stored_branch=followup.branch,
                    candidate=candidate,
                )
                context_json = self._serialize_pre_pr_context(
                    flow=followup.flow,
                    branch=branch,
                    issue=resume_issue,
                    candidate=candidate,
                )

                self._state.mark_running(
                    followup.issue_number,
                    repo_full_name=self._state_repo_full_name(),
                )
                run_id = self._state.record_agent_run_start(
                    run_kind="pre_pr_followup",
                    issue_number=followup.issue_number,
                    pr_number=None,
                    flow=followup.flow,
                    branch=branch,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "pre_pr_followup_resumed",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=followup.issue_number,
                    flow=followup.flow,
                    waiting_reason=followup.waiting_reason,
                )
                if followup.flow == "implementation":
                    assert candidate is not None
                    fut = pool.submit(
                        self._process_implementation_candidate_worker,
                        candidate,
                        resume_issue,
                        consumed_comment_id_max,
                    )
                else:
                    fut = pool.submit(
                        self._process_issue_worker,
                        resume_issue,
                        cast(IssueFlow, followup.flow),
                        consumed_comment_id_max,
                    )
                fut.add_done_callback(lambda _: self._work_limiter.release())
                capacity_reserved = False
                with self._running_lock:
                    self._running[followup.issue_number] = fut
                    self._running_issue_metadata[followup.issue_number] = _RunningIssueMetadata(
                        run_id=run_id,
                        flow=followup.flow,
                        branch=branch,
                        context_json=context_json,
                        source_issue_number=followup.issue_number,
                        consumed_comment_id_max=consumed_comment_id_max,
                    )
                log_event(
                    LOGGER,
                    "pre_pr_followup_enqueued",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=followup.issue_number,
                    flow=followup.flow,
                    first_comment_id=pending_comments[0].comment_id,
                    last_comment_id=consumed_comment_id_max,
                )
            finally:
                if capacity_reserved:
                    self._work_limiter.release()

    def _scan_post_pr_source_issue_comment_redirects(self) -> None:
        redirect_targets: dict[int, int] = {}
        for tracked in self._state.list_tracked_pull_requests(
            repo_full_name=self._state_repo_full_name()
        ):
            redirect_targets.setdefault(tracked.issue_number, tracked.pr_number)
        for blocked in self._state.list_blocked_pull_requests(
            repo_full_name=self._state_repo_full_name()
        ):
            redirect_targets.setdefault(blocked.issue_number, blocked.pr_number)

        if not redirect_targets:
            return

        for issue_number, pr_number in sorted(redirect_targets.items()):
            comments = self._github.list_issue_comments(issue_number)
            cursor = self._state.get_issue_comment_cursor(
                issue_number,
                repo_full_name=self._state_repo_full_name(),
            )
            max_scanned_comment_id = cursor.post_pr_last_redirected_comment_id
            known_tokens = {
                token for comment in comments for token in extract_action_tokens(comment.body)
            }
            pr_url = _pull_request_url(
                repo_full_name=self._state_repo_full_name(),
                pr_number=pr_number,
            )
            for comment in sorted(comments, key=lambda item: item.comment_id):
                if comment.comment_id <= cursor.post_pr_last_redirected_comment_id:
                    continue
                if comment.comment_id > max_scanned_comment_id:
                    max_scanned_comment_id = comment.comment_id
                if not self._is_qualifying_source_issue_comment(comment):
                    continue

                token = compute_source_issue_redirect_token(
                    issue_number=issue_number,
                    pr_number=pr_number,
                    comment_id=comment.comment_id,
                )
                if token not in known_tokens:
                    self._github.post_issue_comment(
                        issue_number=issue_number,
                        body=append_action_token(
                            body=_render_source_issue_redirect_comment(
                                pr_number=pr_number,
                                pr_url=pr_url,
                                source_comment_url=comment.html_url,
                            ),
                            token=token,
                        ),
                    )
                    known_tokens.add(token)
                log_event(
                    LOGGER,
                    "source_issue_comment_redirected",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=issue_number,
                    pr_number=pr_number,
                    comment_id=comment.comment_id,
                )

            if max_scanned_comment_id > cursor.post_pr_last_redirected_comment_id:
                self._state.advance_post_pr_last_redirected_comment_id(
                    issue_number=issue_number,
                    comment_id=max_scanned_comment_id,
                    repo_full_name=self._state_repo_full_name(),
                )

    def _adopt_legacy_failed_pre_pr_runs(self) -> None:
        if self._legacy_pre_pr_adopted:
            return
        self._legacy_pre_pr_adopted = True
        legacy_runs = self._state.list_legacy_failed_issue_runs_without_pr(
            repo_full_name=self._state_repo_full_name()
        )
        for legacy in legacy_runs:
            if not _is_recoverable_pre_pr_error(legacy.error or ""):
                continue
            issue = self._github.get_issue(legacy.issue_number)
            flow = _infer_pre_pr_flow_from_issue_and_error(
                issue=issue,
                error=legacy.error or "",
                design_label=self._repo.trigger_label,
                bugfix_label=self._repo.bugfix_label,
                small_job_label=self._repo.small_job_label,
            )
            if flow is None:
                continue
            if flow == "implementation":
                # Legacy implementation retries cannot be reconstructed safely without
                # candidate metadata (design branch/PR linkage).
                continue
            branch = _branch_for_issue_flow(flow=flow, issue=issue)
            context_json = self._serialize_pre_pr_context_for_issue(
                issue=issue,
                flow=flow,
                branch=branch,
            )
            self._state.mark_awaiting_issue_followup(
                issue_number=legacy.issue_number,
                flow=flow,
                branch=branch,
                context_json=context_json,
                waiting_reason=legacy.error or "legacy_pre_pr_failed_run_adopted",
                repo_full_name=self._state_repo_full_name(),
            )
            self._capture_run_start_comment_id_if_enabled(legacy.issue_number)
            log_event(
                LOGGER,
                "pre_pr_followup_waiting",
                repo_full_name=self._state_repo_full_name(),
                issue_number=legacy.issue_number,
                reason="legacy_failed_run_adopted",
            )

    # TODO remove migration after updates
    def _repair_failed_no_staged_change_runs(self) -> None:
        failed_runs = self._state.list_legacy_failed_issue_runs_without_pr(
            repo_full_name=self._state_repo_full_name()
        )
        for failed in failed_runs:
            branch = (failed.branch or "").strip()
            if not branch:
                continue
            if not _is_no_staged_changes_error_text(failed.error or ""):
                continue
            self._recover_missing_pr_branch(issue_number=failed.issue_number, branch=branch)

    # TODO remove migration after updates
    def _repair_stale_running_runs(self) -> None:
        running_runs = self._state.list_legacy_running_issue_runs_without_pr(
            repo_full_name=self._state_repo_full_name()
        )
        with self._running_lock:
            active_issue_numbers = set(self._running.keys())
        for running in running_runs:
            if running.issue_number in active_issue_numbers:
                continue
            branch = (running.branch or "").strip()
            if not branch:
                continue
            self._recover_missing_pr_branch(issue_number=running.issue_number, branch=branch)

    # TODO remove migration after updates
    def _recover_missing_pr_branch(self, *, issue_number: int, branch: str) -> None:
        issue = self._github.get_issue(issue_number)
        if not self._is_issue_author_allowed(
            issue_number=issue.number,
            author_login=issue.author_login,
            reason="unauthorized_issue_author_defensive",
        ):
            return

        title, body, flow_label = _recovery_pr_payload_for_issue(issue=issue, branch=branch)
        try:
            pr = self._github.create_pull_request(
                title=title,
                head=branch,
                base=self._repo.default_branch,
                body=body,
            )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "failed_branch_pr_repair_failed",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue.number,
                branch=branch,
                error_type=type(exc).__name__,
            )
            return

        self._state.mark_completed(
            issue_number=issue.number,
            branch=branch,
            pr_number=pr.number,
            pr_url=pr.html_url,
            repo_full_name=self._state_repo_full_name(),
        )

        try:
            self._github.post_issue_comment(
                issue_number=issue.number,
                body=f"Opened recovered {flow_label} PR: {pr.html_url}",
            )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "failed_branch_pr_repair_comment_failed",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue.number,
                branch=branch,
                pr_number=pr.number,
                error_type=type(exc).__name__,
            )

        log_event(
            LOGGER,
            "failed_branch_pr_repaired",
            repo_full_name=self._state_repo_full_name(),
            issue_number=issue.number,
            branch=branch,
            pr_number=pr.number,
            flow=flow_label,
        )

    def _has_capacity_locked(self) -> bool:
        local_active = len(self._running) + len(self._running_feedback)
        if local_active >= self._config.runtime.worker_count:
            return False
        return self._work_limiter.in_flight() < self._work_limiter.capacity()

    def _active_worker_count(self) -> int:
        with self._running_lock:
            return len(self._running) + len(self._running_feedback)

    def _scan_operator_commands(self) -> None:
        issue_numbers = self._operator_issue_numbers_to_scan()
        if not issue_numbers:
            return
        blocked_pr_numbers = {
            blocked.pr_number
            for blocked in self._state.list_blocked_pull_requests(
                repo_full_name=self._state_repo_full_name()
            )
        }

        for issue_number in issue_numbers:
            source_pr_number = issue_number if issue_number in blocked_pr_numbers else None
            comments = self._github.list_issue_comments(issue_number)
            for comment in comments:
                parsed = parse_operator_command(comment.body)
                if parsed is None:
                    continue
                if is_bot_login(comment.user_login):
                    continue

                command_key = operator_command_key(
                    issue_number=issue_number,
                    comment_id=comment.comment_id,
                    updated_at=comment.updated_at,
                )
                existing = self._state.get_operator_command(
                    command_key,
                    repo_full_name=self._state_repo_full_name(),
                )
                if existing is not None:
                    self._reconcile_operator_command(existing)
                    continue

                log_event(
                    LOGGER,
                    "operator_command_seen",
                    command_key=command_key,
                    actor=comment.user_login,
                    issue_number=issue_number,
                    command=parsed.command,
                )

                args_json = self._encode_operator_command_args(
                    parsed=parsed,
                    comment_url=comment.html_url,
                )

                if not self._is_authorized_operator(comment.user_login):
                    detail = f"@{comment.user_login} is not in repo.operator_logins, so this command was rejected."
                    record = self._state.record_operator_command(
                        command_key=command_key,
                        issue_number=issue_number,
                        pr_number=None,
                        comment_id=comment.comment_id,
                        author_login=comment.user_login,
                        command=parsed.command,
                        args_json=args_json,
                        status="rejected",
                        result=detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    self._post_operator_command_result(
                        record, reply_status="rejected", detail=detail
                    )
                    log_event(
                        LOGGER,
                        "operator_command_rejected",
                        command_key=command_key,
                        actor=comment.user_login,
                        issue_number=issue_number,
                        command=parsed.command,
                    )
                    continue

                if parsed.parse_error is not None:
                    detail = f"{parsed.parse_error}\n\n{operator_commands_help()}"
                    record = self._state.record_operator_command(
                        command_key=command_key,
                        issue_number=issue_number,
                        pr_number=None,
                        comment_id=comment.comment_id,
                        author_login=comment.user_login,
                        command="invalid",
                        args_json=args_json,
                        status="failed",
                        result=detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    self._post_operator_command_result(record, reply_status="failed", detail=detail)
                    log_event(
                        LOGGER,
                        "operator_command_failed",
                        command_key=command_key,
                        actor=comment.user_login,
                        issue_number=issue_number,
                        command="invalid",
                        reason="parse_error",
                    )
                    continue

                if parsed.command == "help":
                    detail = operator_commands_help()
                    record = self._state.record_operator_command(
                        command_key=command_key,
                        issue_number=issue_number,
                        pr_number=None,
                        comment_id=comment.comment_id,
                        author_login=comment.user_login,
                        command="help",
                        args_json=args_json,
                        status="applied",
                        result=detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    self._post_operator_command_result(record, reply_status="help", detail=detail)
                    log_event(
                        LOGGER,
                        "operator_command_applied",
                        command_key=command_key,
                        actor=comment.user_login,
                        issue_number=issue_number,
                        command="help",
                    )
                    continue

                if parsed.command == "unblock":
                    unblock_status, unblock_detail, target_pr_number = self._apply_unblock_command(
                        source_issue_number=issue_number,
                        source_pr_number=source_pr_number,
                        parsed=parsed,
                    )
                    record = self._state.record_operator_command(
                        command_key=command_key,
                        issue_number=issue_number,
                        pr_number=target_pr_number,
                        comment_id=comment.comment_id,
                        author_login=comment.user_login,
                        command="unblock",
                        args_json=args_json,
                        status=unblock_status,
                        result=unblock_detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    reply_status = cast(OperatorReplyStatus, unblock_status)
                    self._post_operator_command_result(
                        record,
                        reply_status=reply_status,
                        detail=unblock_detail,
                        issue_number_override=target_pr_number,
                    )
                    log_event(
                        LOGGER,
                        "operator_command_applied"
                        if unblock_status == "applied"
                        else "operator_command_failed",
                        command_key=command_key,
                        actor=comment.user_login,
                        issue_number=issue_number,
                        command="unblock",
                        target_pr_number=target_pr_number,
                    )
                    continue

                if parsed.command == "restart":
                    requested_mode = cast(
                        RestartMode,
                        parsed.get_arg("mode") or self._config.runtime.restart_default_mode,
                    )
                    if requested_mode not in self._config.runtime.restart_supported_modes:
                        detail = (
                            f"Requested mode={requested_mode} is not enabled in "
                            "runtime.restart_supported_modes."
                        )
                        record = self._state.record_operator_command(
                            command_key=command_key,
                            issue_number=issue_number,
                            pr_number=None,
                            comment_id=comment.comment_id,
                            author_login=comment.user_login,
                            command="restart",
                            args_json=args_json,
                            status="failed",
                            result=detail,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        self._post_operator_command_result(
                            record, reply_status="failed", detail=detail
                        )
                        log_event(
                            LOGGER,
                            "operator_command_failed",
                            command_key=command_key,
                            actor=comment.user_login,
                            issue_number=issue_number,
                            command="restart",
                            reason="unsupported_mode",
                            mode=requested_mode,
                        )
                        continue

                    if not self._allow_runtime_restart:
                        detail = (
                            "Automatic restart is only supported when MergeXO is launched via "
                            "`mergexo service`."
                        )
                        record = self._state.record_operator_command(
                            command_key=command_key,
                            issue_number=issue_number,
                            pr_number=None,
                            comment_id=comment.comment_id,
                            author_login=comment.user_login,
                            command="restart",
                            args_json=args_json,
                            status="failed",
                            result=detail,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        self._post_operator_command_result(
                            record, reply_status="failed", detail=detail
                        )
                        log_event(
                            LOGGER,
                            "operator_command_failed",
                            command_key=command_key,
                            actor=comment.user_login,
                            issue_number=issue_number,
                            command="restart",
                            reason="service_mode_required",
                        )
                        continue

                    # This sqlite row is the single-flight restart gate shared across repos.
                    # As soon as it flips to pending, every orchestrator poll enters
                    # restart-drain mode: no new command scans, no new work enqueue, only
                    # worker reaping/checkpointing. ServiceRunner waits for the global pending
                    # future count to reach zero before invoking restart/update.
                    operation, created = self._state.request_runtime_restart(
                        requested_by=comment.user_login,
                        request_command_key=command_key,
                        mode=requested_mode,
                        request_repo_full_name=self._state_repo_full_name(),
                    )
                    if created:
                        detail = (
                            f"Restart requested in mode={requested_mode}. "
                            "MergeXO is draining active workers before update and restart."
                        )
                        self._state.record_operator_command(
                            command_key=command_key,
                            issue_number=issue_number,
                            pr_number=None,
                            comment_id=comment.comment_id,
                            author_login=comment.user_login,
                            command="restart",
                            args_json=args_json,
                            status="applied",
                            result=detail,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        log_event(
                            LOGGER,
                            "restart_requested",
                            command_key=command_key,
                            actor=comment.user_login,
                            issue_number=issue_number,
                            mode=requested_mode,
                        )
                        log_event(
                            LOGGER,
                            "operator_command_applied",
                            command_key=command_key,
                            actor=comment.user_login,
                            issue_number=issue_number,
                            command="restart",
                            mode=requested_mode,
                        )
                    else:
                        detail = (
                            "Restart already in progress; this command was collapsed into "
                            f"the pending request {operation.request_command_key}."
                        )
                        record = self._state.record_operator_command(
                            command_key=command_key,
                            issue_number=issue_number,
                            pr_number=None,
                            comment_id=comment.comment_id,
                            author_login=comment.user_login,
                            command="restart",
                            args_json=args_json,
                            status="applied",
                            result=detail,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        self._post_operator_command_result(
                            record, reply_status="applied", detail=detail
                        )
                        log_event(
                            LOGGER,
                            "operator_command_applied",
                            command_key=command_key,
                            actor=comment.user_login,
                            issue_number=issue_number,
                            command="restart",
                            collapsed_into=operation.request_command_key,
                        )
                    continue

    def _operator_issue_numbers_to_scan(self) -> tuple[int, ...]:
        issue_numbers = {
            blocked.pr_number
            for blocked in self._state.list_blocked_pull_requests(
                repo_full_name=self._state_repo_full_name()
            )
        }
        if self._repo.operations_issue_number is not None:
            issue_numbers.add(self._repo.operations_issue_number)
        return tuple(sorted(issue_numbers))

    def _reconcile_operator_command(self, command: OperatorCommandRecord) -> None:
        if command.command == "restart":
            runtime_op = self._state.get_runtime_operation(_RESTART_OPERATION_NAME)
            if (
                runtime_op is not None
                and runtime_op.request_command_key == command.command_key
                and runtime_op.status in _RESTART_PENDING_STATUSES
            ):
                return
        reply_status = _operator_reply_status_for_record(command)
        self._post_operator_command_result(
            command,
            reply_status=reply_status,
            detail=command.result,
        )

    def _drain_for_pending_restart_if_needed(self) -> bool:
        operation = self._state.get_runtime_operation(_RESTART_OPERATION_NAME)
        if operation is None or operation.status not in _RESTART_PENDING_STATUSES:
            self._restart_drain_started_at_monotonic = None
            return False

        if not self._allow_runtime_restart:
            detail = (
                "Restart command was received, but this process was not started via "
                "`mergexo service`."
            )
            self._state.set_runtime_operation_status(
                op_name=_RESTART_OPERATION_NAME,
                status="failed",
                detail=detail,
            )
            command = self._state.update_operator_command_result(
                command_key=operation.request_command_key,
                status="failed",
                result=detail,
                repo_full_name=operation.request_repo_full_name,
            )
            if command is not None:
                self._post_operator_command_result(command, reply_status="failed", detail=detail)
            log_event(
                LOGGER,
                "operator_command_failed",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                command="restart",
                reason="service_mode_required",
            )
            return False

        now = time.monotonic()
        if self._restart_drain_started_at_monotonic is None:
            self._restart_drain_started_at_monotonic = now
            log_event(
                LOGGER,
                "restart_drain_started",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                mode=operation.mode,
            )

        active_workers = self._active_worker_count()
        if active_workers == 0:
            self._state.set_runtime_operation_status(
                op_name=_RESTART_OPERATION_NAME,
                status="running",
                detail="workers drained; supervisor update running",
            )
            log_event(
                LOGGER,
                "restart_drain_completed",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                mode=operation.mode,
            )
            raise RestartRequested(
                mode=operation.mode,
                command_key=operation.request_command_key,
                repo_full_name=operation.request_repo_full_name,
            )

        drain_timeout = self._config.runtime.restart_drain_timeout_seconds
        elapsed = now - self._restart_drain_started_at_monotonic
        if elapsed >= drain_timeout:
            detail = (
                f"Restart drain timed out after {drain_timeout} seconds with "
                f"{active_workers} active worker(s)."
            )
            self._state.set_runtime_operation_status(
                op_name=_RESTART_OPERATION_NAME,
                status="failed",
                detail=detail,
            )
            command = self._state.update_operator_command_result(
                command_key=operation.request_command_key,
                status="failed",
                result=detail,
                repo_full_name=operation.request_repo_full_name,
            )
            if command is not None:
                self._post_operator_command_result(command, reply_status="failed", detail=detail)
            self._restart_drain_started_at_monotonic = None
            log_event(
                LOGGER,
                "operator_command_failed",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                command="restart",
                reason="drain_timeout",
                active_workers=active_workers,
                timeout_seconds=drain_timeout,
            )
            return False

        return True

    def _apply_unblock_command(
        self,
        *,
        source_issue_number: int,
        source_pr_number: int | None,
        parsed: ParsedOperatorCommand,
    ) -> tuple[OperatorCommandStatus, str, int | None]:
        pr_arg = parsed.get_arg("pr")
        if pr_arg is not None:
            target_pr_number = int(pr_arg)
        elif source_pr_number is not None:
            target_pr_number = source_pr_number
        else:
            detail = (
                "No target PR was provided. On the operations issue, use "
                "`/mergexo unblock pr=<number>`."
            )
            return "failed", detail, None
        head_sha = parsed.get_arg("head_sha")

        reset_count = self._state.reset_blocked_pull_requests(
            pr_numbers=(target_pr_number,),
            last_seen_head_sha_override=head_sha,
            repo_full_name=self._state_repo_full_name(),
        )
        if reset_count == 0:
            return "failed", f"PR #{target_pr_number} is not currently blocked.", target_pr_number

        detail = f"Reset blocked state for PR #{target_pr_number}."
        if head_sha is not None:
            detail += f" Set last_seen_head_sha override to `{head_sha}`."
        return "applied", detail, target_pr_number

    def _encode_operator_command_args(
        self,
        *,
        parsed: ParsedOperatorCommand,
        comment_url: str,
    ) -> str:
        payload = {
            "normalized_command": parsed.normalized_command,
            "args": dict(parsed.args),
            "parse_error": parsed.parse_error,
            "comment_url": comment_url,
        }
        return json.dumps(payload, sort_keys=True)

    def _is_authorized_operator(self, login: str) -> bool:
        normalized = login.strip().lower()
        if not normalized:
            return False
        return normalized in self._authorized_operator_logins

    def _post_operator_command_result(
        self,
        command: OperatorCommandRecord,
        *,
        reply_status: OperatorReplyStatus,
        detail: str,
        issue_number_override: int | None = None,
    ) -> None:
        github = self._github_for_repo(command.repo_full_name)
        issue_number = issue_number_override or _operator_reply_issue_number(command)
        token = compute_operator_command_token(command_key=command.command_key)
        if self._issue_has_action_token(github=github, issue_number=issue_number, token=token):
            return
        body = _render_operator_command_result(
            normalized_command=_operator_normalized_command(command),
            status=reply_status,
            detail=detail,
            source_comment_url=_operator_source_comment_url(
                command=command,
                repo_full_name=command.repo_full_name or self._repo.full_name,
            ),
        )
        github.post_issue_comment(
            issue_number=issue_number,
            body=append_action_token(body=body, token=token),
        )

    def _issue_has_action_token(
        self, *, github: GitHubGateway, issue_number: int, token: str
    ) -> bool:
        issue_comments = github.list_issue_comments(issue_number)
        for comment in issue_comments:
            if token in extract_action_tokens(comment.body):
                return True
        return False

    def _github_for_repo(self, repo_full_name: str) -> GitHubGateway:
        normalized = repo_full_name.strip()
        if normalized and normalized in self._github_by_repo_full_name:
            return self._github_by_repo_full_name[normalized]
        return self._github

    def _state_repo_full_name(self) -> str:
        return self._repo.full_name

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
                metadata = self._running_issue_metadata.pop(issue_number, None)
                try:
                    result = fut.result()
                    self._state.mark_completed(
                        issue_number=result.issue_number,
                        branch=result.branch,
                        pr_number=result.pr_number,
                        pr_url=result.pr_url,
                        repo_full_name=result.repo_full_name or self._repo.full_name,
                    )
                    log_event(
                        LOGGER,
                        "issue_processing_completed",
                        issue_number=result.issue_number,
                        pr_number=result.pr_number,
                        branch=result.branch,
                    )
                    if self._config.runtime.enable_issue_comment_routing and metadata is not None:
                        self._state.clear_pre_pr_followup_state(
                            issue_number=issue_number,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        self._state.advance_pre_pr_last_consumed_comment_id(
                            issue_number=metadata.source_issue_number,
                            comment_id=metadata.consumed_comment_id_max,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        log_event(
                            LOGGER,
                            "pre_pr_followup_consumed",
                            repo_full_name=self._state_repo_full_name(),
                            issue_number=issue_number,
                            consumed_comment_id_max=metadata.consumed_comment_id_max,
                        )
                    if metadata is not None:
                        self._state.finish_agent_run(
                            run_id=metadata.run_id,
                            terminal_status="completed",
                        )
                except Exception as exc:  # noqa: BLE001
                    if (
                        self._config.runtime.enable_issue_comment_routing
                        and metadata is not None
                        and _is_recoverable_pre_pr_exception(exc)
                    ):
                        waiting_reason = str(exc).strip() or type(exc).__name__
                        self._state.mark_awaiting_issue_followup(
                            issue_number=issue_number,
                            flow=metadata.flow,
                            branch=metadata.branch,
                            context_json=metadata.context_json,
                            waiting_reason=waiting_reason,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        self._state.advance_pre_pr_last_consumed_comment_id(
                            issue_number=metadata.source_issue_number,
                            comment_id=metadata.consumed_comment_id_max,
                            repo_full_name=self._state_repo_full_name(),
                        )
                        log_event(
                            LOGGER,
                            "pre_pr_followup_consumed",
                            repo_full_name=self._state_repo_full_name(),
                            issue_number=issue_number,
                            consumed_comment_id_max=metadata.consumed_comment_id_max,
                        )
                        log_event(
                            LOGGER,
                            "pre_pr_followup_waiting",
                            repo_full_name=self._state_repo_full_name(),
                            issue_number=issue_number,
                            flow=metadata.flow,
                            reason=waiting_reason,
                        )
                        self._state.finish_agent_run(
                            run_id=metadata.run_id,
                            terminal_status="blocked",
                            failure_class=_failure_class_for_exception(exc),
                            error=waiting_reason,
                        )
                        continue

                    self._state.mark_failed(
                        issue_number=issue_number,
                        error=str(exc),
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "issue_processing_failed",
                        issue_number=issue_number,
                        error_type=type(exc).__name__,
                    )
                    if metadata is not None:
                        self._state.finish_agent_run(
                            run_id=metadata.run_id,
                            terminal_status="failed",
                            failure_class=_failure_class_for_exception(exc),
                            error=str(exc),
                        )

            for pr_number in finished_pr_numbers:
                handle = self._running_feedback.pop(pr_number)
                try:
                    run_terminal_status = _normalize_feedback_terminal_status(
                        handle.future.result()
                    )
                    self._state.finish_agent_run(
                        run_id=handle.run_id,
                        terminal_status=run_terminal_status,
                        failure_class="policy_block" if run_terminal_status == "blocked" else None,
                    )
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
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=handle.issue_number,
                        pr_number=pr_number,
                        reason=type(exc).__name__,
                    )
                    self._state.finish_agent_run(
                        run_id=handle.run_id,
                        terminal_status="failed",
                        failure_class=_failure_class_for_exception(exc),
                        error=str(exc),
                    )

    def _wait_for_all(self, pool: ThreadPoolExecutor) -> None:
        while True:
            self._reap_finished()
            with self._running_lock:
                if not self._running and not self._running_feedback:
                    return
            time.sleep(1.0)

    def _process_issue_worker(
        self,
        issue: Issue,
        flow: IssueFlow,
        consumed_comment_id_max: int,
    ) -> WorkResult:
        with logging_repo_context(self._repo.full_name):
            return self._process_issue(
                issue,
                flow,
                pre_pr_last_consumed_comment_id=consumed_comment_id_max,
            )

    def _process_issue(
        self,
        issue: Issue,
        flow: IssueFlow,
        *,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        if not self._is_issue_author_allowed(
            issue_number=issue.number,
            author_login=issue.author_login,
            reason="unauthorized_issue_author_defensive",
        ):
            raise DirectFlowValidationError(
                f"Issue #{issue.number} author is not allowed by repo.allowed_users"
            )

        lease = self._slot_pool.acquire()
        try:
            log_event(
                LOGGER,
                "issue_processing_started",
                issue_number=issue.number,
                slot=lease.slot,
                flow=flow,
            )
            self._github.post_issue_comment(
                issue_number=issue.number,
                body=_render_issue_start_comment(issue_number=issue.number, flow=flow),
            )
            self._git.prepare_checkout(lease.path)
            if flow == "design_doc":
                return self._process_design_issue(
                    issue=issue,
                    checkout_path=lease.path,
                    pre_pr_last_consumed_comment_id=pre_pr_last_consumed_comment_id,
                )
            return self._process_direct_issue(
                issue=issue,
                flow=flow,
                checkout_path=lease.path,
                pre_pr_last_consumed_comment_id=pre_pr_last_consumed_comment_id,
            )
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

    def _process_design_issue(
        self,
        *,
        issue: Issue,
        checkout_path: Path,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        slug = _slugify(issue.title)
        branch = _issue_branch(flow="design_doc", issue_number=issue.number, slug=slug)
        self._git.create_or_reset_branch(checkout_path, branch)

        design_relpath = f"{self._repo.design_docs_dir}/{issue.number}-{slug}.md"
        log_event(
            LOGGER,
            "design_turn_started",
            issue_number=issue.number,
            branch=branch,
        )
        start_result = self._agent.start_design_from_issue(
            issue=issue,
            repo_full_name=self._state_repo_full_name(),
            design_doc_path=design_relpath,
            default_branch=self._repo.default_branch,
            cwd=checkout_path,
        )
        log_event(LOGGER, "design_turn_completed", issue_number=issue.number)
        generated = start_result.design
        if start_result.session:
            self._state.save_agent_session(
                issue_number=issue.number,
                adapter=start_result.session.adapter,
                thread_id=start_result.session.thread_id,
                repo_full_name=self._state_repo_full_name(),
            )

        design_abs_path = checkout_path / design_relpath
        design_abs_path.parent.mkdir(parents=True, exist_ok=True)
        design_abs_path.write_text(
            _render_design_doc(issue=issue, design=generated),
            encoding="utf-8",
        )

        self._git.commit_all(checkout_path, f"docs: add design for issue #{issue.number}")
        required_tests_error = self._run_required_tests_before_push(checkout_path=checkout_path)
        if required_tests_error is not None:
            required_tests_command = self._repo.required_tests or "<unset>"
            error_summary = _summarize_git_error(required_tests_error)
            self._github.post_issue_comment(
                issue_number=issue.number,
                body=(
                    "MergeXO design flow could not push because the required pre-push test "
                    f"`{required_tests_command}` failed.\n"
                    f"Failure summary: {error_summary}"
                ),
            )
            raise DirectFlowValidationError(
                "required pre-push tests failed before pushing design branch"
            )
        self._git.push_branch(checkout_path, branch)
        self._run_pre_pr_ordering_gate(
            issue_number=issue.number,
            last_consumed_comment_id=pre_pr_last_consumed_comment_id,
        )

        pr = self._github.create_pull_request(
            title=f"Design doc for #{issue.number}: {generated.title}",
            head=branch,
            base=self._repo.default_branch,
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
            repo_full_name=self._state_repo_full_name(),
        )

    def _process_direct_issue(
        self,
        *,
        issue: Issue,
        flow: IssueFlow,
        checkout_path: Path,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        slug = _slugify(issue.title)
        branch = _issue_branch(flow=flow, issue_number=issue.number, slug=slug)
        self._git.create_or_reset_branch(checkout_path, branch)
        coding_guidelines_path = self._coding_guidelines_path_for_checkout(
            checkout_path=checkout_path
        )

        flow_label: str
        direct_turn: Callable[[Issue], DirectStartResult]
        if flow == "bugfix":
            flow_label = "bugfix"

            def run_direct_turn(agent_issue: Issue) -> DirectStartResult:
                return self._agent.start_bugfix_from_issue(
                    issue=agent_issue,
                    repo_full_name=self._state_repo_full_name(),
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                    cwd=checkout_path,
                )

            direct_turn = run_direct_turn
        elif flow == "small_job":
            flow_label = "small-job"

            def run_direct_turn(agent_issue: Issue) -> DirectStartResult:
                return self._agent.start_small_job_from_issue(
                    issue=agent_issue,
                    repo_full_name=self._state_repo_full_name(),
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                    cwd=checkout_path,
                )

            direct_turn = run_direct_turn
        else:
            raise DirectFlowValidationError(f"Unsupported direct flow: {flow}")

        default_commit_message = _default_commit_message(flow=flow, issue_number=issue.number)
        start_result = self._run_direct_turn_with_required_tests_repair(
            issue=issue,
            flow_label=flow_label,
            checkout_path=checkout_path,
            default_commit_message=default_commit_message,
            regression_test_file_regex=(self._repo.test_file_regex if flow == "bugfix" else None),
            direct_turn=direct_turn,
        )
        self._push_branch_with_merge_conflict_repair(
            issue=issue,
            flow_label=flow_label,
            checkout_path=checkout_path,
            branch=branch,
            default_commit_message=f"chore: resolve merge conflicts for issue #{issue.number}",
            direct_turn=direct_turn,
        )
        self._run_pre_pr_ordering_gate(
            issue_number=issue.number,
            last_consumed_comment_id=pre_pr_last_consumed_comment_id,
        )

        pr = self._github.create_pull_request(
            title=start_result.pr_title,
            head=branch,
            base=self._repo.default_branch,
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
            repo_full_name=self._state_repo_full_name(),
        )

    def _process_implementation_candidate(
        self,
        candidate: ImplementationCandidateState,
        *,
        issue_override: Issue | None = None,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        issue = issue_override or self._github.get_issue(candidate.issue_number)
        if not self._is_issue_author_allowed(
            issue_number=issue.number,
            author_login=issue.author_login,
            reason="unauthorized_issue_author_defensive",
        ):
            raise DirectFlowValidationError(
                f"Issue #{issue.number} author is not allowed by repo.allowed_users"
            )

        lease = self._slot_pool.acquire()
        try:
            log_event(
                LOGGER,
                "issue_processing_started",
                issue_number=candidate.issue_number,
                slot=lease.slot,
                flow="implementation",
            )
            self._github.post_issue_comment(
                issue_number=candidate.issue_number,
                body=_render_issue_start_comment(
                    issue_number=candidate.issue_number,
                    flow="implementation",
                ),
            )
            self._git.prepare_checkout(lease.path)
            slug = _design_branch_slug(candidate.design_branch)
            if slug is None:
                raise DirectFlowValidationError(
                    "Implementation candidate is missing a valid design branch suffix"
                )

            branch = f"agent/impl/{slug}"
            self._git.create_or_reset_branch(lease.path, branch)

            design_relpath = f"{self._repo.design_docs_dir}/{slug}.md"
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
                        f"`{design_relpath}` on `{self._repo.default_branch}`.{detail}"
                    ),
                )
                raise DirectFlowValidationError(
                    f"Implementation flow requires merged design doc at {design_relpath}"
                )
            design_doc_markdown = design_abs_path.read_text(encoding="utf-8")
            coding_guidelines_path = self._coding_guidelines_path_for_checkout(
                checkout_path=lease.path
            )

            def run_implementation_turn(agent_issue: Issue) -> DirectStartResult:
                return self._agent.start_implementation_from_design(
                    issue=agent_issue,
                    repo_full_name=self._state_repo_full_name(),
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                    design_doc_path=design_relpath,
                    design_doc_markdown=design_doc_markdown,
                    design_pr_number=candidate.design_pr_number,
                    design_pr_url=candidate.design_pr_url,
                    cwd=lease.path,
                )

            default_commit_message = _default_commit_message(
                flow="small_job", issue_number=issue.number
            )
            start_result = self._run_direct_turn_with_required_tests_repair(
                issue=issue,
                flow_label="implementation",
                checkout_path=lease.path,
                default_commit_message=default_commit_message,
                regression_test_file_regex=None,
                direct_turn=run_implementation_turn,
            )
            self._push_branch_with_merge_conflict_repair(
                issue=issue,
                flow_label="implementation",
                checkout_path=lease.path,
                branch=branch,
                default_commit_message=f"chore: resolve merge conflicts for issue #{issue.number}",
                direct_turn=run_implementation_turn,
            )
            self._run_pre_pr_ordering_gate(
                issue_number=issue.number,
                last_consumed_comment_id=pre_pr_last_consumed_comment_id,
            )

            design_doc_url = _design_doc_url(
                repo_full_name=self._state_repo_full_name(),
                default_branch=self._repo.default_branch,
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
                base=self._repo.default_branch,
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
                repo_full_name=self._state_repo_full_name(),
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

    def _process_implementation_candidate_worker(
        self,
        candidate: ImplementationCandidateState,
        issue: Issue,
        consumed_comment_id_max: int,
    ) -> WorkResult:
        with logging_repo_context(self._repo.full_name):
            return self._process_implementation_candidate(
                candidate,
                issue_override=issue,
                pre_pr_last_consumed_comment_id=consumed_comment_id_max,
            )

    def _process_feedback_turn_worker(self, tracked: TrackedPullRequestState) -> str:
        with logging_repo_context(self._repo.full_name):
            return self._process_feedback_turn(tracked)

    def _save_agent_session_if_present(
        self, *, issue_number: int, session: AgentSession | None
    ) -> None:
        if session is None:
            return
        self._state.save_agent_session(
            issue_number=issue_number,
            adapter=session.adapter,
            thread_id=session.thread_id,
            repo_full_name=self._state_repo_full_name(),
        )

    def _push_branch_with_merge_conflict_repair(
        self,
        *,
        issue: Issue,
        flow_label: str,
        checkout_path: Path,
        branch: str,
        default_commit_message: str,
        direct_turn: Callable[[Issue], DirectStartResult],
    ) -> None:
        conflict_repair_round = 0
        while True:
            try:
                self._git.push_branch(checkout_path, branch)
                return
            except CommandError as exc:
                detail = str(exc)
                if not _is_merge_conflict_error(detail):
                    raise
                conflict_repair_round += 1
                log_event(
                    LOGGER,
                    "pre_pr_push_merge_conflict_detected",
                    issue_number=issue.number,
                    branch=branch,
                    flow=flow_label,
                    repair_round=conflict_repair_round,
                )
                if conflict_repair_round > _MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS:
                    summary = _summarize_git_error(detail)
                    self._github.post_issue_comment(
                        issue_number=issue.number,
                        body=(
                            "MergeXO could not resolve push-time merge conflicts before opening a PR.\n"
                            f"- branch: `{branch}`\n"
                            f"- attempts: {_MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS}\n"
                            f"- last failure summary: {summary}"
                        ),
                    )
                    raise DirectFlowBlockedError(
                        f"{flow_label} flow blocked: push merge conflict unresolved after "
                        f"{_MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS} repair attempts"
                    )
                repair_issue = self._issue_with_push_merge_conflict(
                    issue=issue,
                    branch=branch,
                    failure_output=detail,
                    attempt=conflict_repair_round,
                    checkout_path=checkout_path,
                )
                _ = self._run_direct_turn_with_required_tests_repair(
                    issue=repair_issue,
                    flow_label=flow_label,
                    checkout_path=checkout_path,
                    default_commit_message=default_commit_message,
                    regression_test_file_regex=None,
                    direct_turn=direct_turn,
                )

    def _resolve_checkout_path(self, *, configured_path: str, checkout_path: Path) -> Path:
        path = Path(configured_path)
        if path.is_absolute():
            return path
        return checkout_path / path

    def _coding_guidelines_path_for_checkout(self, *, checkout_path: Path) -> str | None:
        configured_path = self._repo.coding_guidelines_path
        resolved_path = self._resolve_checkout_path(
            configured_path=configured_path, checkout_path=checkout_path
        )
        if resolved_path.is_file():
            return configured_path

        log_event(
            LOGGER,
            "coding_guidelines_missing",
            issue_repo_full_name=self._state_repo_full_name(),
            configured_path=configured_path,
            resolved_path=str(resolved_path),
            checkout_path=str(checkout_path),
        )
        return None

    def _required_tests_command_for_checkout(self, *, checkout_path: Path | None) -> str | None:
        required_tests_command = self._repo.required_tests
        if required_tests_command is None:
            return None
        if checkout_path is None:
            return required_tests_command

        resolved_path = self._resolve_checkout_path(
            configured_path=required_tests_command,
            checkout_path=checkout_path,
        )
        if resolved_path.is_file():
            return required_tests_command

        log_event(
            LOGGER,
            "required_tests_missing",
            issue_repo_full_name=self._state_repo_full_name(),
            command=required_tests_command,
            resolved_path=str(resolved_path),
            checkout_path=str(checkout_path),
        )
        return None

    def _run_direct_turn_with_required_tests_repair(
        self,
        *,
        issue: Issue,
        flow_label: str,
        checkout_path: Path,
        default_commit_message: str,
        regression_test_file_regex: tuple[re.Pattern[str], ...] | None,
        direct_turn: Callable[[Issue], DirectStartResult],
    ) -> DirectStartResult:
        result = direct_turn(
            self._issue_with_required_tests_reminder(issue, checkout_path=checkout_path)
        )
        repair_round = 0

        while True:
            self._save_agent_session_if_present(issue_number=issue.number, session=result.session)
            if result.blocked_reason:
                self._github.post_issue_comment(
                    issue_number=issue.number,
                    body=(
                        f"MergeXO {flow_label} flow was blocked for issue #{issue.number}: "
                        f"{result.blocked_reason}"
                    ),
                )
                raise DirectFlowBlockedError(f"{flow_label} flow blocked: {result.blocked_reason}")

            if regression_test_file_regex is not None:
                staged_files = self._git.list_staged_files(checkout_path)
                if not _has_regression_test_changes(staged_files, regression_test_file_regex):
                    rendered_patterns = _render_regex_patterns(regression_test_file_regex)
                    self._github.post_issue_comment(
                        issue_number=issue.number,
                        body=(
                            "MergeXO bugfix flow requires at least one staged regression test "
                            "file matching `repo.test_file_regex`. "
                            f"Configured patterns: {rendered_patterns}. No PR was opened."
                        ),
                    )
                    raise DirectFlowValidationError(
                        "Bugfix flow requires at least one staged regression test file matching "
                        "repo.test_file_regex"
                    )

            commit_message = result.commit_message or default_commit_message
            self._git.commit_all(checkout_path, commit_message)

            required_tests_error = self._run_required_tests_before_push(checkout_path=checkout_path)
            if required_tests_error is None:
                return result

            repair_round += 1
            if repair_round > _MAX_REQUIRED_TEST_REPAIR_ROUNDS:
                required_tests_command = self._repo.required_tests
                required_tests_display = required_tests_command or "<unset>"
                error_summary = _summarize_git_error(required_tests_error)
                self._github.post_issue_comment(
                    issue_number=issue.number,
                    body=(
                        "MergeXO could not satisfy the required pre-push test "
                        f"`{required_tests_display}` for issue #{issue.number} after "
                        f"{_MAX_REQUIRED_TEST_REPAIR_ROUNDS} repair attempts.\n"
                        f"Last failure summary: {error_summary}"
                    ),
                )
                raise DirectFlowValidationError(
                    "required pre-push tests did not pass after automated repair attempts"
                )

            repair_issue = self._issue_with_required_tests_failure(
                issue=issue,
                failure_output=required_tests_error,
                attempt=repair_round,
                checkout_path=checkout_path,
            )
            result = direct_turn(repair_issue)

    def _run_required_tests_before_push(self, *, checkout_path: Path) -> str | None:
        required_tests_command = self._required_tests_command_for_checkout(
            checkout_path=checkout_path
        )
        if required_tests_command is None:
            return None

        executable_path = self._resolve_checkout_path(
            configured_path=required_tests_command,
            checkout_path=checkout_path,
        )
        argv = [str(executable_path)]

        log_event(
            LOGGER,
            "required_tests_started",
            issue_repo_full_name=self._state_repo_full_name(),
            command=required_tests_command,
            checkout_path=str(checkout_path),
        )
        try:
            run(argv, cwd=checkout_path)
        except CommandError as exc:
            detail = str(exc)
            log_event(
                LOGGER,
                "required_tests_failed",
                issue_repo_full_name=self._state_repo_full_name(),
                command=required_tests_command,
                checkout_path=str(checkout_path),
                error_summary=_summarize_git_error(detail),
            )
            return detail
        except Exception as exc:  # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
            log_event(
                LOGGER,
                "required_tests_failed",
                issue_repo_full_name=self._state_repo_full_name(),
                command=required_tests_command,
                checkout_path=str(checkout_path),
                error_summary=_summarize_git_error(detail),
            )
            return detail

        log_event(
            LOGGER,
            "required_tests_passed",
            issue_repo_full_name=self._state_repo_full_name(),
            command=required_tests_command,
            checkout_path=str(checkout_path),
        )
        return None

    def _run_pre_pr_ordering_gate(
        self, *, issue_number: int, last_consumed_comment_id: int
    ) -> None:
        if not self._config.runtime.enable_issue_comment_routing:
            return
        comments = self._github.list_issue_comments(issue_number)
        pending = self._pending_source_issue_followups(
            comments=comments,
            after_comment_id=last_consumed_comment_id,
        )
        if not pending:
            return
        raise DirectFlowBlockedError("new_issue_comments_pending")

    def _pending_source_issue_followups(
        self,
        *,
        comments: list[PullRequestIssueComment],
        after_comment_id: int,
    ) -> list[PullRequestIssueComment]:
        pending = [
            comment
            for comment in sorted(comments, key=lambda item: item.comment_id)
            if comment.comment_id > after_comment_id
            and self._is_qualifying_source_issue_comment(comment)
        ]
        return pending

    def _is_qualifying_source_issue_comment(self, comment: PullRequestIssueComment) -> bool:
        if is_bot_login(comment.user_login):
            return False
        if not self._repo.allows(comment.user_login):
            return False
        if _is_mergexo_status_comment(comment.body):
            return False
        if has_action_token(comment.body):
            return False
        return True

    def _capture_run_start_comment_id_if_enabled(self, issue_number: int) -> int:
        if not self._config.runtime.enable_issue_comment_routing:
            return 0
        comments = self._github.list_issue_comments(issue_number)
        run_start_comment_id = max((comment.comment_id for comment in comments), default=0)
        self._state.advance_pre_pr_last_consumed_comment_id(
            issue_number=issue_number,
            comment_id=run_start_comment_id,
            repo_full_name=self._state_repo_full_name(),
        )
        return run_start_comment_id

    def _serialize_pre_pr_context_for_issue(
        self,
        *,
        issue: Issue,
        flow: IssueFlow,
        branch: str,
    ) -> str:
        return self._serialize_pre_pr_context(
            flow=flow,
            branch=branch,
            issue=issue,
            candidate=None,
        )

    def _serialize_pre_pr_context_for_implementation(
        self,
        *,
        issue: Issue,
        candidate: ImplementationCandidateState,
        branch: str,
    ) -> str:
        return self._serialize_pre_pr_context(
            flow="implementation",
            branch=branch,
            issue=issue,
            candidate=candidate,
        )

    def _serialize_pre_pr_context(
        self,
        *,
        flow: PrePrFlow,
        branch: str,
        issue: Issue,
        candidate: ImplementationCandidateState | None,
    ) -> str:
        payload: dict[str, object] = {
            "flow": flow,
            "branch": branch,
            "issue": _issue_to_json_dict(issue),
        }
        if candidate is not None:
            payload["candidate"] = _implementation_candidate_to_json_dict(candidate)
        return json.dumps(payload, sort_keys=True)

    def _decode_pre_pr_context(
        self,
        *,
        followup: PrePrFollowupState,
    ) -> tuple[Issue, ImplementationCandidateState | None]:
        issue_from_context: Issue | None = None
        candidate: ImplementationCandidateState | None = None
        try:
            payload_raw = json.loads(followup.context_json)
        except json.JSONDecodeError:
            payload_raw = {}
        if isinstance(payload_raw, dict):
            payload = cast(dict[str, object], payload_raw)
            issue_from_context = _issue_from_json_dict(payload.get("issue"))
            candidate = _implementation_candidate_from_json_dict(payload.get("candidate"))

        if issue_from_context is None:
            issue = self._github.get_issue(followup.issue_number)
        else:
            issue = issue_from_context
        return issue, candidate

    def _augment_issue_with_pre_pr_followups(
        self,
        *,
        issue: Issue,
        waiting_reason: str,
        followups: list[PullRequestIssueComment],
    ) -> Issue:
        followup_lines = [
            (
                f"- @{comment.user_login} ({comment.created_at}) [{comment.html_url}]"
                f"\n{comment.body.strip() or '<empty>'}"
            )
            for comment in followups
        ]
        followup_section = "\n\n".join(followup_lines)
        context = (
            "Previous MergeXO pre-PR wait reason:\n"
            f"{waiting_reason}\n\n"
            "Ordered issue follow-up comments:\n"
            f"{followup_section}"
        )
        base_body = issue.body.strip()
        merged_body = f"{base_body}\n\n{context}" if base_body else context
        return Issue(
            number=issue.number,
            title=issue.title,
            body=merged_body,
            html_url=issue.html_url,
            labels=issue.labels,
            author_login=issue.author_login,
        )

    def _issue_with_required_tests_reminder(
        self, issue: Issue, *, checkout_path: Path | None = None
    ) -> Issue:
        required_tests_command = self._required_tests_command_for_checkout(
            checkout_path=checkout_path
        )
        if required_tests_command is None:
            return issue

        reminder = (
            "Required pre-push test command:\n"
            f"- `{required_tests_command}`\n"
            "Before finalizing your response, run this command and ensure it passes."
        )
        if reminder in issue.body:
            return issue

        body = issue.body.strip()
        merged_body = f"{body}\n\n{reminder}" if body else reminder
        return Issue(
            number=issue.number,
            title=issue.title,
            body=merged_body,
            html_url=issue.html_url,
            labels=issue.labels,
            author_login=issue.author_login,
        )

    def _issue_with_required_tests_failure(
        self,
        *,
        issue: Issue,
        failure_output: str,
        attempt: int,
        checkout_path: Path | None = None,
    ) -> Issue:
        base_issue = self._issue_with_required_tests_reminder(issue, checkout_path=checkout_path)
        required_tests_command = self._required_tests_command_for_checkout(
            checkout_path=checkout_path
        )
        if required_tests_command is None:
            return base_issue

        trimmed_output = failure_output.strip()
        if len(trimmed_output) > _REQUIRED_TEST_FAILURE_OUTPUT_LIMIT:
            trimmed_output = (
                trimmed_output[:_REQUIRED_TEST_FAILURE_OUTPUT_LIMIT]
                + "\n... [truncated by MergeXO]"
            )
        if not trimmed_output:
            trimmed_output = "<empty>"

        context = (
            f"Required pre-push test failed on repair attempt {attempt}.\n"
            "Do not disable, remove, or weaken existing tests.\n"
            f"Repair the code so `{required_tests_command}` passes.\n"
            "If impossible, set blocked_reason with a concrete explanation.\n\n"
            "Failure output:\n"
            f"{trimmed_output}"
        )
        body = base_issue.body.strip()
        merged_body = f"{body}\n\n{context}" if body else context
        return Issue(
            number=base_issue.number,
            title=base_issue.title,
            body=merged_body,
            html_url=base_issue.html_url,
            labels=base_issue.labels,
            author_login=base_issue.author_login,
        )

    def _issue_with_push_merge_conflict(
        self,
        *,
        issue: Issue,
        branch: str,
        failure_output: str,
        attempt: int,
        checkout_path: Path | None = None,
    ) -> Issue:
        base_issue = self._issue_with_required_tests_reminder(issue, checkout_path=checkout_path)
        trimmed_output = failure_output.strip()
        if len(trimmed_output) > _REQUIRED_TEST_FAILURE_OUTPUT_LIMIT:
            trimmed_output = (
                trimmed_output[:_REQUIRED_TEST_FAILURE_OUTPUT_LIMIT]
                + "\n... [truncated by MergeXO]"
            )
        if not trimmed_output:
            trimmed_output = "<empty>"

        context = (
            f"Push-time merge conflict encountered on repair attempt {attempt} for `{branch}`.\n"
            "The remote branch was merged into this checkout and produced conflicts.\n"
            "Resolve all conflicts in the existing working tree.\n"
            "Preserve both local intent and remote updates.\n"
            "Do not run history-rewrite commands.\n"
            "After resolving conflicts, stage files and return commit_message.\n"
            "If impossible, set blocked_reason with a concrete explanation.\n\n"
            "Merge output:\n"
            f"{trimmed_output}"
        )
        body = base_issue.body.strip()
        merged_body = f"{body}\n\n{context}" if body else context
        return Issue(
            number=base_issue.number,
            title=base_issue.title,
            body=merged_body,
            html_url=base_issue.html_url,
            labels=base_issue.labels,
            author_login=base_issue.author_login,
        )

    def _process_feedback_turn(self, tracked: TrackedPullRequestState) -> str:
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
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_merged",
                )
                return "merged"
            if pr.state.lower() != "open":
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="closed",
                    last_seen_head_sha=pr.head_sha,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_closed",
                )
                return "closed"

            issue = self._github.get_issue(tracked.issue_number)
            if not self._repo.allows(issue.author_login):
                reason = "unauthorized_issue_author"
                error = "feedback ignored because issue author is not allowed by repo.allowed_users"
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="blocked",
                    last_seen_head_sha=pr.head_sha,
                    error=error,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "auth_pr_blocked",
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    author_login=issue.author_login,
                    reason=reason,
                )
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason=reason,
                )
                return "blocked"

            if tracked.last_seen_head_sha and tracked.last_seen_head_sha != pr.head_sha:
                transition_status = self._classify_remote_history_transition(
                    older_sha=tracked.last_seen_head_sha,
                    newer_sha=pr.head_sha,
                )
                if transition_status not in _ALLOWED_LINEAR_HISTORY_STATUSES:
                    self._block_feedback_history_rewrite(
                        tracked=tracked,
                        expected_head_sha=tracked.last_seen_head_sha,
                        observed_head_sha=pr.head_sha,
                        phase="cross_cycle_drift",
                        transition_status=transition_status,
                    )
                    return "blocked"

            review_comments = self._github.list_pull_request_review_comments(tracked.pr_number)
            issue_comments = self._github.list_pull_request_issue_comments(tracked.pr_number)
            changed_files = self._github.list_pull_request_files(tracked.pr_number)

            previous_pending = self._state.list_pending_feedback_events(
                tracked.pr_number,
                repo_full_name=self._state_repo_full_name(),
            )
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
            self._state.ingest_feedback_events(
                [event for event, _ in normalized_review],
                repo_full_name=self._state_repo_full_name(),
            )
            self._state.ingest_feedback_events(
                [event for event, _ in normalized_issue],
                repo_full_name=self._state_repo_full_name(),
            )

            pending_events = self._state.list_pending_feedback_events(
                tracked.pr_number,
                repo_full_name=self._state_repo_full_name(),
            )
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
                return "completed"

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
            turn_issue_comments = self._append_required_tests_feedback_reminder(
                pending_issue_comments,
                checkout_path=lease.path,
            )

            session_row = self._state.get_agent_session(
                tracked.issue_number,
                repo_full_name=self._state_repo_full_name(),
            )
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
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="missing_agent_session",
                )
                return "blocked"
            session = AgentSession(adapter=session_row[0], thread_id=session_row[1])

            if not self._git.restore_feedback_branch(lease.path, tracked.branch, pr.head_sha):
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="head_mismatch_retry",
                )
                return "blocked"

            turn_head_sha = (
                pr.head_sha if not previous_pending else (tracked.last_seen_head_sha or pr.head_sha)
            )
            self._state.mark_pr_status(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                status="awaiting_feedback",
                last_seen_head_sha=turn_head_sha,
                repo_full_name=self._state_repo_full_name(),
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
                issue_comments=turn_issue_comments,
                changed_files=changed_files,
            )
            turn_start_head = pr.head_sha

            feedback_outcome = self._run_feedback_agent_with_git_ops(
                tracked=tracked,
                session=session,
                turn=turn,
                checkout_path=lease.path,
                pull_request=pr,
            )
            if feedback_outcome is None:
                return self._feedback_terminal_status_from_state(
                    tracked=tracked, fallback="blocked"
                )
            result, pr = feedback_outcome

            local_head_sha = self._git.current_head_sha(lease.path)
            if not self._git.is_ancestor(lease.path, turn_start_head, local_head_sha):
                self._block_feedback_history_rewrite(
                    tracked=tracked,
                    expected_head_sha=turn_start_head,
                    observed_head_sha=local_head_sha,
                    phase="local_turn",
                    transition_status="local_non_ancestor",
                    state_head_sha=turn_start_head,
                )
                return "blocked"

            commit_outcome = self._commit_push_feedback_with_required_tests(
                tracked=tracked,
                checkout_path=lease.path,
                turn=turn,
                result=result,
                pull_request=pr,
                turn_start_head=turn_start_head,
            )
            if commit_outcome is None:
                return self._feedback_terminal_status_from_state(
                    tracked=tracked, fallback="blocked"
                )
            result, pr = commit_outcome

            refreshed_pr = self._github.get_pull_request(tracked.pr_number)
            if refreshed_pr.merged:
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="merged",
                    last_seen_head_sha=refreshed_pr.head_sha,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_merged",
                )
                return "merged"
            if refreshed_pr.state.lower() != "open":
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="closed",
                    last_seen_head_sha=refreshed_pr.head_sha,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_closed",
                )
                return "closed"

            transition_status = self._classify_remote_history_transition(
                older_sha=turn_start_head,
                newer_sha=refreshed_pr.head_sha,
            )
            if transition_status not in _ALLOWED_LINEAR_HISTORY_STATUSES:
                self._block_feedback_history_rewrite(
                    tracked=tracked,
                    expected_head_sha=turn_start_head,
                    observed_head_sha=refreshed_pr.head_sha,
                    phase="pre_finalize_remote",
                    transition_status=transition_status,
                )
                return "blocked"
            pr = refreshed_pr

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
                return "blocked"

            self._state.finalize_feedback_turn(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                processed_event_keys=pending_event_keys,
                session=result.session,
                head_sha=pr.head_sha,
                repo_full_name=self._state_repo_full_name(),
            )
            log_event(
                LOGGER,
                "feedback_turn_completed",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                turn_key=turn_key,
                processed_event_count=len(pending_event_keys),
            )
            return "completed"
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

    def _feedback_terminal_status_from_state(
        self,
        *,
        tracked: TrackedPullRequestState,
        fallback: str,
    ) -> str:
        state = self._state.get_pull_request_status(
            tracked.pr_number,
            repo_full_name=self._state_repo_full_name(),
        )
        if state is None:
            return fallback
        if state.status in {"blocked", "merged", "closed"}:
            return state.status
        return "completed"

    def _append_required_tests_feedback_reminder(
        self,
        issue_comments: tuple[PullRequestIssueComment, ...],
        *,
        checkout_path: Path | None = None,
    ) -> tuple[PullRequestIssueComment, ...]:
        required_tests_command = self._required_tests_command_for_checkout(
            checkout_path=checkout_path
        )
        if required_tests_command is None:
            return issue_comments

        reminder_body = (
            "MergeXO required pre-push test reminder:\n"
            f"- Run `{required_tests_command}` before returning commit_message.\n"
            "- Only finalize when that command passes."
        )
        reminder_comment = PullRequestIssueComment(
            comment_id=-9000,
            body=reminder_body,
            user_login="mergexo-system",
            html_url="",
            created_at="now",
            updated_at="now",
        )
        return issue_comments + (reminder_comment,)

    def _feedback_turn_with_required_tests_failure(
        self,
        *,
        turn: FeedbackTurn,
        pull_request: PullRequestSnapshot,
        repair_round: int,
        failure_output: str,
    ) -> FeedbackTurn:
        required_tests_command = self._repo.required_tests or "<unset>"
        trimmed_output = failure_output.strip()
        if len(trimmed_output) > _REQUIRED_TEST_FAILURE_OUTPUT_LIMIT:
            trimmed_output = (
                trimmed_output[:_REQUIRED_TEST_FAILURE_OUTPUT_LIMIT]
                + "\n... [truncated by MergeXO]"
            )
        if not trimmed_output:
            trimmed_output = "<empty>"

        failure_comment = PullRequestIssueComment(
            comment_id=-(9100 + repair_round),
            body=(
                "MergeXO required pre-push test failed.\n"
                f"- command: `{required_tests_command}`\n"
                f"- repair_round: {repair_round}\n"
                "- Do not disable, remove, or weaken existing tests.\n"
                "- Repair the code so the required command passes.\n"
                "- If impossible, set commit_message to null and explain why in "
                "general_comment.\n\n"
                "Failure output:\n"
                f"{trimmed_output}"
            ),
            user_login="mergexo-system",
            html_url="",
            created_at="now",
            updated_at="now",
        )
        return FeedbackTurn(
            turn_key=turn.turn_key,
            issue=turn.issue,
            pull_request=pull_request,
            review_comments=turn.review_comments,
            issue_comments=turn.issue_comments + (failure_comment,),
            changed_files=self._github.list_pull_request_files(pull_request.number),
        )

    def _commit_push_feedback_with_required_tests(
        self,
        *,
        tracked: TrackedPullRequestState,
        checkout_path: Path,
        turn: FeedbackTurn,
        result: FeedbackResult,
        pull_request: PullRequestSnapshot,
        turn_start_head: str,
    ) -> tuple[FeedbackResult, PullRequestSnapshot] | None:
        current_result = result
        current_turn = turn
        current_pr = pull_request
        repair_round = 0

        while current_result.commit_message:
            local_head_sha = self._git.current_head_sha(checkout_path)
            if not self._git.is_ancestor(checkout_path, turn_start_head, local_head_sha):
                self._block_feedback_history_rewrite(
                    tracked=tracked,
                    expected_head_sha=turn_start_head,
                    observed_head_sha=local_head_sha,
                    phase="required_tests_repair_local",
                    transition_status="local_non_ancestor",
                    state_head_sha=turn_start_head,
                )
                return None

            try:
                self._git.commit_all(checkout_path, current_result.commit_message)
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
                        last_seen_head_sha=current_pr.head_sha,
                        error=error,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="commit_message_without_changes",
                    )
                    return None
                raise

            required_tests_error = self._run_required_tests_before_push(checkout_path=checkout_path)
            if required_tests_error is not None:
                repair_round += 1
                if repair_round > _MAX_REQUIRED_TEST_REPAIR_ROUNDS:
                    error = (
                        "required pre-push tests failed after automated repair attempts; "
                        f"last failure: {_summarize_git_error(required_tests_error)}"
                    )
                    self._github.post_issue_comment(
                        issue_number=tracked.pr_number,
                        body=(
                            "MergeXO feedback automation is blocked because required pre-push "
                            f"tests kept failing after {_MAX_REQUIRED_TEST_REPAIR_ROUNDS} repair "
                            f"attempts.\n\n{error}"
                        ),
                    )
                    self._state.mark_pr_status(
                        pr_number=tracked.pr_number,
                        issue_number=tracked.issue_number,
                        status="blocked",
                        last_seen_head_sha=current_pr.head_sha,
                        error=error,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="required_tests_repair_limit_exceeded",
                    )
                    return None

                current_turn = self._feedback_turn_with_required_tests_failure(
                    turn=current_turn,
                    pull_request=current_pr,
                    repair_round=repair_round,
                    failure_output=required_tests_error,
                )
                repair_outcome = self._run_feedback_agent_with_git_ops(
                    tracked=tracked,
                    session=current_result.session,
                    turn=current_turn,
                    checkout_path=checkout_path,
                    pull_request=current_pr,
                )
                if repair_outcome is None:
                    return None

                current_result, current_pr = repair_outcome
                if current_result.commit_message is None:
                    detail = (
                        current_result.general_comment.strip()
                        if current_result.general_comment and current_result.general_comment.strip()
                        else "agent could not satisfy required pre-push tests"
                    )
                    self._github.post_issue_comment(
                        issue_number=tracked.pr_number,
                        body=(
                            "MergeXO feedback automation is blocked because required pre-push "
                            f"tests could not be satisfied:\n\n{detail}"
                        ),
                    )
                    self._state.mark_pr_status(
                        pr_number=tracked.pr_number,
                        issue_number=tracked.issue_number,
                        status="blocked",
                        last_seen_head_sha=current_pr.head_sha,
                        error=detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="required_tests_reported_impossible",
                    )
                    return None
                continue

            self._git.push_branch(checkout_path, tracked.branch)
            current_pr = self._github.get_pull_request(tracked.pr_number)
            if current_pr.merged:
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="merged",
                    last_seen_head_sha=current_pr.head_sha,
                    repo_full_name=self._state_repo_full_name(),
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
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="pr_closed",
                )
                return None
            return current_result, current_pr

        return current_result, current_pr

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
                    repo_full_name=self._state_repo_full_name(),
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
                    repo_full_name=self._state_repo_full_name(),
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
                    repo_full_name=self._state_repo_full_name(),
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
                    repo_full_name=self._state_repo_full_name(),
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
            if not self._repo.allows(comment.user_login):
                log_event(
                    LOGGER,
                    "auth_feedback_ignored",
                    pr_number=pr_number,
                    comment_id=comment.comment_id,
                    kind="review",
                    user_login=comment.user_login,
                )
                continue
            if has_action_token(comment.body):
                continue
            log_event(
                LOGGER,
                "monitored_comment_detected",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue_number,
                pr_number=pr_number,
                comment_kind="review",
                comment_id=comment.comment_id,
                comment_url=comment.html_url,
            )
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
            if not self._repo.allows(comment.user_login):
                log_event(
                    LOGGER,
                    "auth_feedback_ignored",
                    pr_number=pr_number,
                    comment_id=comment.comment_id,
                    kind="issue",
                    user_login=comment.user_login,
                )
                continue
            if has_action_token(comment.body):
                continue
            log_event(
                LOGGER,
                "monitored_comment_detected",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue_number,
                pr_number=pr_number,
                comment_kind="issue",
                comment_id=comment.comment_id,
                comment_url=comment.html_url,
            )
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

    def _classify_remote_history_transition(
        self, *, older_sha: str, newer_sha: str
    ) -> CompareCommitsStatus:
        if older_sha == newer_sha:
            return "identical"
        return self._github.compare_commits(older_sha, newer_sha)

    def _block_feedback_history_rewrite(
        self,
        *,
        tracked: TrackedPullRequestState,
        expected_head_sha: str,
        observed_head_sha: str,
        phase: str,
        transition_status: str,
        state_head_sha: str | None = None,
    ) -> None:
        token = compute_history_rewrite_token(
            pr_number=tracked.pr_number,
            expected_head_sha=expected_head_sha,
            observed_head_sha=observed_head_sha,
            reason=f"{phase}:{transition_status}",
        )
        if not self._token_exists_remotely(tracked.pr_number, token):
            comment = (
                "MergeXO feedback automation is blocked because the PR head moved in a "
                "non-fast-forward way.\n\n"
                f"- expected prior head: `{expected_head_sha}`\n"
                f"- observed head: `{observed_head_sha}`\n"
                f"- detected phase: `{phase}`\n"
                f"- transition status: `{transition_status}`\n\n"
                "Resolve by restoring linear history, or reset blocked state with an explicit "
                "`--head-sha` override once the canonical head is confirmed."
            )
            self._github.post_issue_comment(
                issue_number=tracked.pr_number,
                body=append_action_token(body=comment, token=token),
            )

        error = (
            "detected non-fast-forward PR history transition "
            f"(phase={phase}, status={transition_status}, expected_head_sha={expected_head_sha}, "
            f"observed_head_sha={observed_head_sha})"
        )
        self._state.mark_pr_status(
            pr_number=tracked.pr_number,
            issue_number=tracked.issue_number,
            status="blocked",
            last_seen_head_sha=state_head_sha if state_head_sha is not None else observed_head_sha,
            error=error,
            repo_full_name=self._state_repo_full_name(),
        )
        log_event(
            LOGGER,
            "history_rewrite_blocked",
            issue_number=tracked.issue_number,
            pr_number=tracked.pr_number,
            phase=phase,
            transition_status=transition_status,
            expected_head_sha=expected_head_sha,
            observed_head_sha=observed_head_sha,
        )
        log_event(
            LOGGER,
            "feedback_turn_blocked",
            issue_number=tracked.issue_number,
            pr_number=tracked.pr_number,
            reason="history_rewrite_detected",
            phase=phase,
            transition_status=transition_status,
        )

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

    def _is_issue_author_allowed(
        self, *, issue_number: int, author_login: str, reason: str
    ) -> bool:
        if self._repo.allows(author_login):
            return True
        log_event(
            LOGGER,
            "auth_issue_ignored",
            issue_number=issue_number,
            author_login=author_login,
            reason=reason,
        )
        return False


def _issue_to_json_dict(issue: Issue) -> dict[str, object]:
    return {
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "html_url": issue.html_url,
        "labels": list(issue.labels),
        "author_login": issue.author_login,
    }


def _issue_from_json_dict(raw: object) -> Issue | None:
    if not isinstance(raw, dict):
        return None
    if not all(isinstance(key, str) for key in raw.keys()):
        return None
    payload = cast(dict[str, object], raw)
    number = payload.get("number")
    title = payload.get("title")
    body = payload.get("body")
    html_url = payload.get("html_url")
    labels_raw = payload.get("labels")
    author_login = payload.get("author_login")
    if not isinstance(number, int):
        return None
    if not isinstance(title, str):
        return None
    if not isinstance(body, str):
        return None
    if not isinstance(html_url, str):
        return None
    if not isinstance(author_login, str):
        return None
    if not isinstance(labels_raw, list):
        return None
    labels: list[str] = []
    for label in labels_raw:
        if not isinstance(label, str):
            return None
        labels.append(label)
    return Issue(
        number=number,
        title=title,
        body=body,
        html_url=html_url,
        labels=tuple(labels),
        author_login=author_login,
    )


def _implementation_candidate_to_json_dict(
    candidate: ImplementationCandidateState,
) -> dict[str, object]:
    return {
        "issue_number": candidate.issue_number,
        "design_branch": candidate.design_branch,
        "design_pr_number": candidate.design_pr_number,
        "design_pr_url": candidate.design_pr_url,
        "repo_full_name": candidate.repo_full_name,
    }


def _implementation_candidate_from_json_dict(raw: object) -> ImplementationCandidateState | None:
    if not isinstance(raw, dict):
        return None
    if not all(isinstance(key, str) for key in raw.keys()):
        return None
    payload = cast(dict[str, object], raw)
    issue_number = payload.get("issue_number")
    design_branch = payload.get("design_branch")
    design_pr_number = payload.get("design_pr_number")
    design_pr_url = payload.get("design_pr_url")
    repo_full_name = payload.get("repo_full_name")
    if not isinstance(issue_number, int):
        return None
    if not isinstance(design_branch, str):
        return None
    if design_pr_number is not None and not isinstance(design_pr_number, int):
        return None
    if design_pr_url is not None and not isinstance(design_pr_url, str):
        return None
    if repo_full_name is not None and not isinstance(repo_full_name, str):
        return None
    return ImplementationCandidateState(
        issue_number=issue_number,
        design_branch=design_branch,
        design_pr_number=design_pr_number,
        design_pr_url=design_pr_url,
        repo_full_name=repo_full_name or "",
    )


def _branch_for_issue_flow(*, flow: IssueFlow, issue: Issue) -> str:
    return _issue_branch(flow=flow, issue_number=issue.number, slug=_slugify(issue.title))


def _branch_for_implementation_candidate(candidate: ImplementationCandidateState) -> str:
    slug = _design_branch_slug(candidate.design_branch)
    if slug is None:
        return candidate.design_branch
    return f"agent/impl/{slug}"


def _branch_for_pre_pr_followup(
    *,
    flow: PrePrFlow,
    issue: Issue,
    stored_branch: str,
    candidate: ImplementationCandidateState | None,
) -> str:
    if stored_branch.strip():
        return stored_branch
    if flow == "implementation" and candidate is not None:
        return _branch_for_implementation_candidate(candidate)
    if flow == "implementation":
        return "agent/impl/unknown"
    return _branch_for_issue_flow(flow=flow, issue=issue)


def _infer_pre_pr_flow_from_issue_and_error(
    *,
    issue: Issue,
    error: str,
    design_label: str,
    bugfix_label: str,
    small_job_label: str,
) -> PrePrFlow | None:
    resolved = _resolve_issue_flow(
        issue=issue,
        design_label=design_label,
        bugfix_label=bugfix_label,
        small_job_label=small_job_label,
    )
    if resolved is not None:
        return resolved
    error_match = _PRE_PR_BLOCKED_FLOW_PATTERN.search(error.lower())
    if error_match is None:
        return None
    matched = error_match.group(1)
    if matched == "small-job":
        return "small_job"
    return cast(PrePrFlow, matched)


def _is_recoverable_pre_pr_error(error: str) -> bool:
    normalized = error.strip().lower()
    if not normalized:
        return False
    return any(signature in normalized for signature in _RECOVERABLE_PRE_PR_ERROR_SIGNATURES)


def _is_recoverable_pre_pr_exception(exc: Exception) -> bool:
    if isinstance(exc, DirectFlowBlockedError):
        return True
    if isinstance(exc, DirectFlowValidationError):
        return _is_recoverable_pre_pr_error(str(exc))
    return _is_recoverable_pre_pr_error(str(exc))


def _pull_request_url(*, repo_full_name: str, pr_number: int) -> str:
    return f"https://github.com/{repo_full_name}/pull/{pr_number}"


def _render_source_issue_redirect_comment(
    *,
    pr_number: int,
    pr_url: str,
    source_comment_url: str,
) -> str:
    return (
        "MergeXO source-issue comment routing update:\n"
        f"- source comment: {source_comment_url}\n"
        f"- linked PR: #{pr_number} ({pr_url})\n\n"
        "Comments on the source issue are no longer actioned after a PR exists. "
        "Please comment on the PR thread instead."
    )


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


def _normalize_feedback_terminal_status(
    status: str,
) -> Literal["completed", "failed", "blocked", "merged", "closed", "interrupted"]:
    if status in {"completed", "failed", "blocked", "merged", "closed", "interrupted"}:
        return cast(
            Literal["completed", "failed", "blocked", "merged", "closed", "interrupted"],
            status,
        )
    return "completed"


def _failure_class_for_exception(exc: Exception) -> AgentRunFailureClass:
    normalized = str(exc).strip().lower()
    if "non-fast-forward" in normalized or "history transition" in normalized:
        return "history_rewrite"
    if isinstance(exc, GitHubPollingError):
        return "github_error"
    if (
        isinstance(exc, DirectFlowBlockedError)
        or " flow blocked:" in normalized
        or "missing saved agent session" in normalized
    ):
        return "policy_block"
    if "required pre-push test" in normalized or "required tests" in normalized:
        return "tests_failed"
    if "github" in normalized:
        return "github_error"
    if isinstance(exc, CommandError):
        return "agent_error"
    return "unknown"


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


def _flow_trigger_label(*, flow: IssueFlow, repo: RepoConfig) -> str:
    if flow == "design_doc":
        return repo.trigger_label
    if flow == "bugfix":
        return repo.bugfix_label
    return repo.small_job_label


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


def _render_issue_start_comment(
    *, issue_number: int, flow: IssueFlow | Literal["implementation"]
) -> str:
    if flow == "design_doc":
        action = "design work"
    elif flow == "bugfix":
        action = "bugfix PR work"
    elif flow == "small_job":
        action = "small-job PR work"
    else:
        action = "implementation PR work"
    return f"MergeXO assigned an agent and started {action} for issue #{issue_number}."


def _is_mergexo_status_comment(body: str) -> bool:
    normalized = " ".join(body.split()).strip().lower()
    if not normalized:
        return False
    return normalized.startswith("mergexo ")


def _has_regression_test_changes(
    paths: tuple[str, ...], test_file_regex: tuple[re.Pattern[str], ...]
) -> bool:
    return any(compiled.search(path) for path in paths for compiled in test_file_regex)


def _render_regex_patterns(patterns: tuple[re.Pattern[str], ...]) -> str:
    rendered = tuple(f"`{pattern.pattern}`" for pattern in patterns)
    if not rendered:
        return "<none>"
    if len(rendered) == 1:
        return rendered[0]
    if len(rendered) == 2:
        return f"{rendered[0]} or {rendered[1]}"
    return f"{', '.join(rendered[:-1])}, or {rendered[-1]}"


def _is_no_staged_changes_error(exc: RuntimeError) -> bool:
    return _is_no_staged_changes_error_text(str(exc))


def _is_no_staged_changes_error_text(error: str) -> bool:
    return "no staged changes to commit" in error.lower()


def _recovery_pr_payload_for_issue(*, issue: Issue, branch: str) -> tuple[str, str, str]:
    flow = _flow_label_from_branch(branch)
    source_line = f"Source issue: {issue.html_url}"
    if flow == "design":
        return (
            f"Design doc for #{issue.number}: {issue.title}",
            (
                "Recovered design PR from a previously pushed branch.\n\n"
                f"Refs #{issue.number}\n\n"
                f"{source_line}"
            ),
            flow,
        )
    if flow == "implementation":
        return (
            f"Implementation for #{issue.number}: {issue.title}",
            (
                "Recovered implementation PR from a previously pushed branch.\n\n"
                f"Fixes #{issue.number}\n\n"
                f"{source_line}"
            ),
            flow,
        )
    if flow == "bugfix":
        return (
            f"Bugfix for #{issue.number}: {issue.title}",
            (
                "Recovered bugfix PR from a previously pushed branch.\n\n"
                f"Fixes #{issue.number}\n\n"
                f"{source_line}"
            ),
            flow,
        )
    return (
        issue.title,
        (
            "Recovered small-job PR from a previously pushed branch.\n\n"
            f"Fixes #{issue.number}\n\n"
            f"{source_line}"
        ),
        flow,
    )


def _flow_label_from_branch(branch: str) -> str:
    if branch.startswith("agent/design/"):
        return "design"
    if branch.startswith("agent/bugfix/"):
        return "bugfix"
    if branch.startswith("agent/impl/"):
        return "implementation"
    return "small-job"


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


def _is_merge_conflict_error(raw_error: str) -> bool:
    normalized = raw_error.strip().lower()
    if not normalized:
        return False
    conflict_markers = (
        "automatic merge failed",
        "merge conflict",
        "conflict (",
        "fix conflicts and then commit",
    )
    return any(marker in normalized for marker in conflict_markers)


def _operator_args_payload(command: OperatorCommandRecord) -> dict[str, object]:
    try:
        parsed = json.loads(command.args_json)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    if not all(isinstance(key, str) for key in parsed.keys()):
        return {}
    return cast(dict[str, object], parsed)


def _operator_normalized_command(command: OperatorCommandRecord) -> str:
    payload = _operator_args_payload(command)
    normalized = payload.get("normalized_command")
    if isinstance(normalized, str) and normalized.strip():
        return normalized.strip()
    return f"/mergexo {command.command}"


def _operator_source_comment_url(*, command: OperatorCommandRecord, repo_full_name: str) -> str:
    payload = _operator_args_payload(command)
    source_url = payload.get("comment_url")
    if isinstance(source_url, str) and source_url.strip():
        return source_url.strip()
    return (
        f"https://github.com/{repo_full_name}/issues/{command.issue_number}"
        f"#issuecomment-{command.comment_id}"
    )


def _operator_reply_issue_number(command: OperatorCommandRecord) -> int:
    if command.command == "unblock" and command.pr_number is not None:
        return command.pr_number
    return command.issue_number


def _operator_reply_status_for_record(command: OperatorCommandRecord) -> OperatorReplyStatus:
    if command.command == "help":
        return "help"
    if command.status == "rejected":
        return "rejected"
    if command.status == "failed":
        return "failed"
    return "applied"


def _render_operator_command_result(
    *,
    normalized_command: str,
    status: OperatorReplyStatus,
    detail: str,
    source_comment_url: str,
) -> str:
    return (
        "MergeXO operator command result:\n"
        f"- command: `{normalized_command}`\n"
        f"- status: `{status}`\n"
        f"- detail: {detail}\n\n"
        f"Source command: {source_comment_url}"
    )


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
