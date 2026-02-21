from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import queue
import re
import threading
import time

from mergexo.agent_adapter import AgentAdapter, AgentSession, FeedbackTurn
from mergexo.config import AppConfig
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
    PullRequestIssueComment,
    PullRequestReviewComment,
    WorkResult,
)
from mergexo.state import StateStore, TrackedPullRequestState


@dataclass(frozen=True)
class _SlotLease:
    slot: int
    path: Path


@dataclass(frozen=True)
class _FeedbackFuture:
    issue_number: int
    future: Future[None]


class SlotPool:
    def __init__(self, manager: GitRepoManager, worker_count: int) -> None:
        self._manager = manager
        self._slots: queue.Queue[int] = queue.Queue(maxsize=worker_count)
        for slot in range(worker_count):
            self._slots.put(slot)

    def acquire(self) -> _SlotLease:
        slot = self._slots.get(block=True)
        path = self._manager.ensure_checkout(slot)
        return _SlotLease(slot=slot, path=path)

    def release(self, lease: _SlotLease) -> None:
        self._slots.put(lease.slot)


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
                self._reap_finished()
                self._enqueue_new_work(pool)
                if self._config.runtime.enable_feedback_loop:
                    self._enqueue_feedback_work(pool)

                if once:
                    self._wait_for_all(pool)
                    break

                time.sleep(self._config.runtime.poll_interval_seconds)

    def _enqueue_new_work(self, pool: ThreadPoolExecutor) -> None:
        issues = self._github.list_open_issues_with_label(self._config.repo.trigger_label)
        for issue in issues:
            with self._running_lock:
                if not self._has_capacity_locked():
                    return
                if issue.number in self._running:
                    continue

            if not self._state.can_enqueue(issue.number):
                continue

            self._state.mark_running(issue.number)
            fut = pool.submit(self._process_issue, issue)
            with self._running_lock:
                self._running[issue.number] = fut

    def _enqueue_feedback_work(self, pool: ThreadPoolExecutor) -> None:
        tracked_prs = self._state.list_tracked_pull_requests()
        for tracked in tracked_prs:
            with self._running_lock:
                if not self._has_capacity_locked():
                    return
                if tracked.pr_number in self._running_feedback:
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
                except Exception as exc:  # noqa: BLE001
                    self._state.mark_failed(issue_number=issue_number, error=str(exc))

            for pr_number in finished_pr_numbers:
                handle = self._running_feedback.pop(pr_number)
                try:
                    handle.future.result()
                except Exception as exc:  # noqa: BLE001
                    self._state.mark_pr_status(
                        pr_number=pr_number,
                        issue_number=handle.issue_number,
                        status="blocked",
                        error=str(exc),
                    )

    def _wait_for_all(self, pool: ThreadPoolExecutor) -> None:
        while True:
            self._reap_finished()
            with self._running_lock:
                if not self._running and not self._running_feedback:
                    return
            time.sleep(1.0)

    def _process_issue(self, issue: Issue) -> WorkResult:
        lease = self._slot_pool.acquire()
        try:
            self._git.prepare_checkout(lease.path)

            slug = _slugify(issue.title)
            branch = f"agent/design/{issue.number}-{slug}"
            self._git.create_or_reset_branch(lease.path, branch)

            design_relpath = f"{self._config.repo.design_docs_dir}/{issue.number}-{slug}.md"
            start_result = self._agent.start_design_from_issue(
                issue=issue,
                repo_full_name=self._config.repo.full_name,
                design_doc_path=design_relpath,
                default_branch=self._config.repo.default_branch,
                cwd=lease.path,
            )
            generated = start_result.design
            if start_result.session:
                self._state.save_agent_session(
                    issue_number=issue.number,
                    adapter=start_result.session.adapter,
                    thread_id=start_result.session.thread_id,
                )

            design_abs_path = lease.path / design_relpath
            design_abs_path.parent.mkdir(parents=True, exist_ok=True)
            design_abs_path.write_text(
                _render_design_doc(issue=issue, design=generated),
                encoding="utf-8",
            )

            self._git.commit_all(lease.path, f"docs: add design for issue #{issue.number}")
            self._git.push_branch(lease.path, branch)

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

            return WorkResult(
                issue_number=issue.number,
                branch=branch,
                pr_number=pr.number,
                pr_url=pr.html_url,
            )
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

    def _process_feedback_turn(self, tracked: TrackedPullRequestState) -> None:
        lease = self._slot_pool.acquire()
        try:
            pr = self._github.get_pull_request(tracked.pr_number)
            if pr.merged:
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="merged",
                    last_seen_head_sha=pr.head_sha,
                )
                return
            if pr.state.lower() != "open":
                self._state.mark_pr_status(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    status="closed",
                    last_seen_head_sha=pr.head_sha,
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
            if not pending_events:
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
                return
            session = AgentSession(adapter=session_row[0], thread_id=session_row[1])

            if not self._git.restore_feedback_branch(lease.path, tracked.branch, pr.head_sha):
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

            result = self._agent.respond_to_feedback(
                session=session,
                turn=turn,
                cwd=lease.path,
            )

            if result.commit_message:
                try:
                    self._git.commit_all(lease.path, result.commit_message)
                except RuntimeError as exc:
                    if "No staged changes to commit" not in str(exc):
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
                        return
                    if pr.state.lower() != "open":
                        self._state.mark_pr_status(
                            pr_number=tracked.pr_number,
                            issue_number=tracked.issue_number,
                            status="closed",
                            last_seen_head_sha=pr.head_sha,
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
                return

            self._state.finalize_feedback_turn(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                processed_event_keys=pending_event_keys,
                session=result.session,
                head_sha=pr.head_sha,
            )
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)

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
