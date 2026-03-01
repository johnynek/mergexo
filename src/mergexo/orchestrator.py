from __future__ import annotations

from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import logging
import queue
import re
import threading
import time
from typing import Callable, Literal, Sequence, cast

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
    compute_pre_pr_checkpoint_token,
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
from mergexo.github_gateway import (
    CompareCommitsStatus,
    GitHubAuthenticationError,
    GitHubGateway,
    GitHubPollingError,
)
from mergexo.models import (
    GeneratedDesign,
    Issue,
    IssueFlow,
    OperatorCommandRecord,
    OperatorCommandStatus,
    OperatorReplyStatus,
    PrActionsFeedbackPolicy,
    RestartMode,
    PullRequestIssueComment,
    PullRequest,
    PullRequestSnapshot,
    PullRequestReviewComment,
    WorkflowJobSnapshot,
    WorkflowRunSnapshot,
    WorkResult,
)
from mergexo.observability import log_event, logging_repo_context
from mergexo.prompts import (
    build_bugfix_prompt,
    build_design_prompt,
    build_feedback_prompt,
    build_implementation_prompt,
    build_small_job_prompt,
)
from mergexo.shell import CommandError, run
from mergexo.state import (
    AgentRunFailureClass,
    ActionTokenObservation,
    ActionTokenState,
    GitHubCallOutboxState,
    GitHubCommentSurface,
    GitHubCommentPollCursorState,
    ImplementationCandidateState,
    PendingFeedbackEvent,
    PollCursorUpdate,
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
_ACTIONS_ACTIVE_STATUSES = frozenset({"queued", "in_progress", "waiting", "requested", "pending"})
_ACTIONS_GREEN_CONCLUSIONS = frozenset({"success", "neutral", "skipped"})
_RESTART_OPERATION_NAME = "restart"
_RESTART_PENDING_STATUSES = {"pending", "running"}
_GITHUB_AUTH_FAILURE_POLL_THRESHOLD = 3
_RECOVERABLE_PRE_PR_ERROR_SIGNATURES: tuple[str, ...] = (
    "flow blocked:",
    "required pre-push tests failed before pushing design branch",
    "required pre-push tests did not pass after automated repair attempts",
    "new_issue_comments_pending",
)
_PRE_PR_BLOCKED_FLOW_PATTERN = re.compile(r"(bugfix|small-job|implementation) flow blocked:")
_COMMENT_CURSOR_EPOCH = "1970-01-01T00:00:00Z"
_SURFACE_PR_REVIEW_COMMENTS: GitHubCommentSurface = "pr_review_comments"
_SURFACE_PR_REVIEW_SUMMARIES: GitHubCommentSurface = "pr_review_summaries"
_SURFACE_PR_ISSUE_COMMENTS: GitHubCommentSurface = "pr_issue_comments"
_SURFACE_ISSUE_PRE_PR_FOLLOWUPS: GitHubCommentSurface = "issue_pre_pr_followups"
_SURFACE_ISSUE_POST_PR_REDIRECTS: GitHubCommentSurface = "issue_post_pr_redirects"
_SURFACE_ISSUE_OPERATOR_COMMANDS: GitHubCommentSurface = "issue_operator_commands"
_DEFAULT_RUN_META_JSON = json.dumps(
    {
        "codex_active": False,
        "codex_invocation_started_at": None,
        "codex_mode": None,
        "codex_session_id": None,
        "last_prompt": None,
    },
    sort_keys=True,
)

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


@dataclass(frozen=True)
class _IncrementalCommentScan:
    fetched: tuple[PullRequestIssueComment | PullRequestReviewComment, ...]
    new: tuple[PullRequestIssueComment | PullRequestReviewComment, ...]
    cursor_update: PollCursorUpdate


@dataclass(frozen=True)
class _CreatePullRequestOutboxPayload:
    issue_number: int
    title: str
    head: str
    base: str
    body: str


class DirectFlowError(RuntimeError):
    """Base class for direct-flow startup failures."""


class DirectFlowBlockedError(DirectFlowError):
    """Agent reported it cannot safely proceed without more context."""


class DirectFlowValidationError(DirectFlowError):
    """Direct-flow output failed deterministic policy checks."""


class CheckpointedPrePrBlockedError(DirectFlowBlockedError):
    def __init__(
        self,
        *,
        waiting_reason: str,
        checkpoint_branch: str,
        checkpoint_sha: str,
    ) -> None:
        normalized_reason = waiting_reason.strip() or "recoverable_pre_pr_blocked"
        super().__init__(normalized_reason)
        self.waiting_reason = normalized_reason
        self.checkpoint_branch = checkpoint_branch
        self.checkpoint_sha = checkpoint_sha


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

    def release(self, lease: _SlotLease, *, quarantine_reason: str | None = None) -> None:
        if quarantine_reason is not None:
            log_event(
                LOGGER,
                "slot_quarantined",
                slot=lease.slot,
                reason=quarantine_reason,
            )
            try:
                self._manager.recover_quarantined_slot(lease.slot)
            except Exception as exc:  # noqa: BLE001
                log_event(
                    LOGGER,
                    "slot_quarantine_recovery_failed",
                    slot=lease.slot,
                    error_type=type(exc).__name__,
                )
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
        self._run_meta_cache: dict[str, dict[str, object]] = {}
        self._running_lock = threading.Lock()
        self._poll_setup_done = False
        self._restart_drain_started_at_monotonic: float | None = None
        self._legacy_pre_pr_adopted = False
        self._authorized_operator_logins = {
            login.strip().lower() for login in self._repo.operator_logins if login.strip()
        }
        self._consecutive_github_auth_failure_polls = 0
        self._current_poll_had_auth_error = False
        self._github_auth_shutdown_pending = False
        self._last_github_auth_error: str | None = None
        self._poll_issue_cache: dict[int, Issue] = {}
        self._poll_takeover_synced_issue_numbers: set[int] = set()
        self._poll_takeover_active_issue_numbers: set[int] = set()

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
            self._current_poll_had_auth_error = False
            self._poll_issue_cache.clear()
            self._poll_takeover_synced_issue_numbers.clear()
            self._poll_takeover_active_issue_numbers.clear()
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
            auth_shutdown_pending = self._github_auth_shutdown_pending
            # Restart drain mode is a hard ingestion stop for this process. Once restart is
            # pending we skip operator-command scans and all enqueue paths so no new GitHub
            # messages are consumed while draining. The only remaining work here is reaping
            # finished futures so terminal state is checkpointed in sqlite before supervisor
            # handoff/re-exec.
            if (
                self._config.runtime.enable_github_operations
                and not restart_pending
                and not auth_shutdown_pending
            ):
                if not self._run_poll_step(
                    step_name="scan_operator_commands",
                    fn=self._scan_operator_commands,
                ):
                    poll_had_github_errors = True

            if not restart_pending:
                restart_pending = self._is_restart_pending()
            auth_shutdown_pending = self._github_auth_shutdown_pending
            enqueue_allowed = allow_enqueue and not restart_pending and not auth_shutdown_pending

            if enqueue_allowed:
                if not self._run_poll_step(
                    step_name="sync_takeover_states",
                    fn=self._sync_takeover_states,
                ):
                    poll_had_github_errors = True
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
                actions_policy = self._effective_pr_actions_feedback_policy()
                if actions_policy != "never":
                    if not self._run_poll_step(
                        step_name="monitor_pr_actions",
                        fn=self._monitor_pr_actions,
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
            if not self._current_poll_had_auth_error and not self._github_auth_shutdown_pending:
                self._consecutive_github_auth_failure_polls = 0

            log_event(
                LOGGER,
                "poll_completed",
                running_issue_count=len(self._running),
                running_feedback_count=len(self._running_feedback),
                draining_for_restart=not enqueue_allowed,
                draining_for_github_auth=self._github_auth_shutdown_pending,
                poll_had_github_errors=poll_had_github_errors,
                consecutive_github_auth_failure_polls=self._consecutive_github_auth_failure_polls,
            )

    def pending_work_count(self) -> int:
        with self._running_lock:
            return len(self._running) + len(self._running_feedback)

    def queue_counts(self) -> tuple[int, int]:
        with self._running_lock:
            return len(self._running), len(self._running_feedback)

    def in_flight_work_count(self) -> int:
        return self._work_limiter.in_flight()

    def github_auth_shutdown_pending(self) -> bool:
        return self._github_auth_shutdown_pending

    def github_auth_shutdown_reason(self) -> str:
        reason = (self._last_github_auth_error or "").strip()
        if reason:
            return reason
        return "GitHub CLI is not authenticated. Run `gh auth login` and restart MergeXO."

    def _ensure_poll_setup(self) -> None:
        if self._poll_setup_done:
            return
        self._git.ensure_layout()
        replayed_create_pr_call_count = 0

        def replay_create_pr_calls() -> None:
            nonlocal replayed_create_pr_call_count
            replayed_create_pr_call_count = self._replay_pending_create_pr_calls()

        self._run_poll_step(
            step_name="replay_pending_create_pr_calls",
            fn=replay_create_pr_calls,
        )
        if replayed_create_pr_call_count > 0:
            log_event(
                LOGGER,
                "pending_create_pr_calls_replayed",
                repo_full_name=self._state_repo_full_name(),
                replayed_count=replayed_create_pr_call_count,
            )
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
        reconciled_issue_runs_count = self._state.reconcile_stale_running_issue_runs_with_followups(
            repo_full_name=self._state_repo_full_name()
        )
        if reconciled_issue_runs_count > 0:
            log_event(
                LOGGER,
                "stale_issue_runs_reconciled_to_followup",
                repo_full_name=self._state_repo_full_name(),
                reconciled_count=reconciled_issue_runs_count,
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

    def _effective_pr_actions_feedback_policy(self) -> PrActionsFeedbackPolicy:
        if self._repo.pr_actions_feedback_policy is not None:
            return self._repo.pr_actions_feedback_policy
        if self._config.runtime.enable_pr_actions_monitoring:
            return "all_complete"
        return "never"

    def _run_poll_step(self, *, step_name: str, fn: Callable[[], None]) -> bool:
        try:
            fn()
            return True
        except GitHubPollingError as exc:
            if isinstance(exc, GitHubAuthenticationError):
                if not self._current_poll_had_auth_error:
                    self._current_poll_had_auth_error = True
                    self._consecutive_github_auth_failure_polls += 1
                self._last_github_auth_error = str(exc)
                if (
                    not self._github_auth_shutdown_pending
                    and self._consecutive_github_auth_failure_polls
                    >= _GITHUB_AUTH_FAILURE_POLL_THRESHOLD
                ):
                    self._github_auth_shutdown_pending = True
                    log_event(
                        LOGGER,
                        "github_auth_shutdown_pending",
                        repo_full_name=self._state_repo_full_name(),
                        consecutive_failure_polls=self._consecutive_github_auth_failure_polls,
                        threshold=_GITHUB_AUTH_FAILURE_POLL_THRESHOLD,
                        error=str(exc),
                    )
            log_event(
                LOGGER,
                "poll_step_failed",
                repo_full_name=self._state_repo_full_name(),
                step=step_name,
                error_type=type(exc).__name__,
                error=str(exc),
                github_auth_failure=isinstance(exc, GitHubAuthenticationError),
            )
            return False

    def _sync_takeover_states(self) -> None:
        repo_full_name = self._state_repo_full_name()
        issue_numbers = set(self._state.list_active_issue_takeovers(repo_full_name=repo_full_name))
        issue_numbers.update(
            followup.issue_number
            for followup in self._state.list_pre_pr_followups(repo_full_name=repo_full_name)
        )
        issue_numbers.update(
            candidate.issue_number
            for candidate in self._state.list_implementation_candidates(
                repo_full_name=repo_full_name
            )
        )
        issue_numbers.update(
            tracked.issue_number
            for tracked in self._state.list_tracked_pull_requests(repo_full_name=repo_full_name)
        )
        issue_numbers.update(
            blocked.issue_number
            for blocked in self._state.list_blocked_pull_requests(repo_full_name=repo_full_name)
        )
        for issue_number in sorted(issue_numbers):
            self._sync_takeover_state_for_issue(issue_number=issue_number)

    def _is_takeover_active(self, *, issue_number: int, issue: Issue | None = None) -> bool:
        return self._sync_takeover_state_for_issue(issue_number=issue_number, issue=issue)

    def _sync_takeover_state_for_issue(
        self, *, issue_number: int, issue: Issue | None = None
    ) -> bool:
        if issue is not None:
            self._poll_issue_cache[issue_number] = issue
        if issue_number in self._poll_takeover_synced_issue_numbers:
            return issue_number in self._poll_takeover_active_issue_numbers

        issue_snapshot = self._issue_snapshot_for_poll(issue_number=issue_number, issue=issue)
        ignore_label_active = self._repo.ignore_label in set(issue_snapshot.labels)
        repo_full_name = self._state_repo_full_name()
        was_active = self._state.get_issue_takeover_active(
            issue_number=issue_number,
            repo_full_name=repo_full_name,
        )
        if ignore_label_active or was_active:
            # During takeover we continuously move comment floors/cursors forward so
            # comments from the takeover period are never replayed on resume.
            self._snapshot_takeover_comment_boundaries(
                issue_number=issue_number,
                clear_pending_feedback=ignore_label_active and not was_active,
            )

        if ignore_label_active != was_active:
            self._state.set_issue_takeover_active(
                issue_number=issue_number,
                ignore_active=ignore_label_active,
                repo_full_name=repo_full_name,
            )
            log_event(
                LOGGER,
                "issue_takeover_state_changed",
                repo_full_name=repo_full_name,
                issue_number=issue_number,
                ignore_label=self._repo.ignore_label,
                from_active=was_active,
                to_active=ignore_label_active,
            )

        self._poll_takeover_synced_issue_numbers.add(issue_number)
        if ignore_label_active:
            self._poll_takeover_active_issue_numbers.add(issue_number)
        else:
            self._poll_takeover_active_issue_numbers.discard(issue_number)
        return ignore_label_active

    def _issue_snapshot_for_poll(self, *, issue_number: int, issue: Issue | None = None) -> Issue:
        if issue is not None:
            self._poll_issue_cache[issue_number] = issue
            return issue
        cached = self._poll_issue_cache.get(issue_number)
        if cached is not None:
            return cached
        snapshot = self._github.get_issue(issue_number)
        self._poll_issue_cache[issue_number] = snapshot
        return snapshot

    def _snapshot_takeover_comment_boundaries(
        self,
        *,
        issue_number: int,
        clear_pending_feedback: bool,
    ) -> None:
        repo_full_name = self._state_repo_full_name()
        issue_comments = self._github.list_issue_comments(issue_number)
        issue_comment_id_max = max((comment.comment_id for comment in issue_comments), default=0)
        self._state.advance_pre_pr_last_consumed_comment_id(
            issue_number=issue_number,
            comment_id=issue_comment_id_max,
            repo_full_name=repo_full_name,
        )
        self._state.advance_post_pr_last_redirected_comment_id(
            issue_number=issue_number,
            comment_id=issue_comment_id_max,
            repo_full_name=repo_full_name,
        )

        pr_numbers = self._state.list_feedback_pr_numbers_for_issue(
            issue_number=issue_number,
            repo_full_name=repo_full_name,
        )
        cleared_event_count = 0
        for pr_number in pr_numbers:
            review_comments = self._github.list_pull_request_review_comments(pr_number)
            review_summaries = self._github.list_pull_request_review_summaries(pr_number)
            issue_thread_comments = self._github.list_pull_request_issue_comments(pr_number)
            review_comment_id_max = max(
                (comment.comment_id for comment in review_comments),
                default=0,
            )
            issue_thread_comment_id_max = max(
                (comment.comment_id for comment in issue_thread_comments),
                default=0,
            )
            if review_summaries:
                latest_review_summary = max(
                    review_summaries,
                    key=lambda comment: (
                        _normalize_timestamp_for_compare(comment.updated_at),
                        comment.comment_id,
                    ),
                )
                self._state.upsert_poll_cursor(
                    surface=_SURFACE_PR_REVIEW_SUMMARIES,
                    scope_number=pr_number,
                    last_updated_at=_normalize_timestamp_for_compare(
                        latest_review_summary.updated_at
                    ),
                    last_comment_id=latest_review_summary.comment_id,
                    bootstrap_complete=True,
                    repo_full_name=repo_full_name,
                )
            self._state.advance_pr_takeover_comment_floors(
                pr_number=pr_number,
                review_floor_comment_id=review_comment_id_max,
                issue_floor_comment_id=issue_thread_comment_id_max,
                repo_full_name=repo_full_name,
            )
            if clear_pending_feedback:
                cleared_event_count += self._state.mark_pending_feedback_events_processed_for_pr(
                    pr_number=pr_number,
                    repo_full_name=repo_full_name,
                )
        log_event(
            LOGGER,
            "issue_takeover_boundaries_synced",
            repo_full_name=repo_full_name,
            issue_number=issue_number,
            issue_comment_floor=issue_comment_id_max,
            linked_pr_count=len(pr_numbers),
            pending_feedback_events_cleared=cleared_event_count,
        )

    def _enqueue_new_work(self, pool: ThreadPoolExecutor) -> None:
        labels = _trigger_labels(self._repo)
        issues = self._github.list_open_issues_with_any_labels(labels)
        log_event(LOGGER, "issues_fetched", issue_count=len(issues), label_count=len(labels))
        for issue in issues:
            if self._is_takeover_active(issue_number=issue.number, issue=issue):
                log_event(
                    LOGGER,
                    "issue_skipped",
                    issue_number=issue.number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
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
                ignore_label=self._repo.ignore_label,
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

                branch = _branch_for_issue_flow(flow=flow, issue=issue)
                source_issue_comments = self._github.list_issue_comments(issue.number)
                run_issue = self._augment_issue_with_first_invocation_source_comments(
                    issue=issue,
                    comments=source_issue_comments,
                )
                context_json = self._serialize_pre_pr_context_for_issue(
                    issue=run_issue,
                    flow=flow,
                    branch=branch,
                )
                consumed_comment_id_max = self._capture_run_start_comment_id_if_enabled(
                    issue.number,
                    comments=source_issue_comments,
                )
                run_id = self._state.claim_new_issue_run_start(
                    issue_number=issue.number,
                    flow=flow,
                    branch=branch,
                    meta_json=_DEFAULT_RUN_META_JSON,
                    repo_full_name=self._state_repo_full_name(),
                )
                if run_id is None:
                    log_event(
                        LOGGER,
                        "issue_skipped",
                        issue_number=issue.number,
                        reason="already_processed",
                    )
                    continue
                self._initialize_run_meta_cache(run_id)
                fut = pool.submit(
                    self._process_issue_worker,
                    run_issue,
                    flow,
                    branch,
                    consumed_comment_id_max,
                )
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
            if self._is_takeover_active(issue_number=candidate.issue_number):
                log_event(
                    LOGGER,
                    "implementation_skipped",
                    issue_number=candidate.issue_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
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
                issue = self._issue_snapshot_for_poll(issue_number=candidate.issue_number)
                context_json = self._serialize_pre_pr_context_for_implementation(
                    issue=issue,
                    candidate=candidate,
                    branch=branch,
                )
                consumed_comment_id_max = self._capture_run_start_comment_id_if_enabled(
                    candidate.issue_number
                )
                run_id = self._state.claim_implementation_issue_run_start(
                    issue_number=candidate.issue_number,
                    branch=branch,
                    meta_json=_DEFAULT_RUN_META_JSON,
                    repo_full_name=self._state_repo_full_name(),
                )
                if run_id is None:
                    log_event(
                        LOGGER,
                        "implementation_skipped",
                        issue_number=candidate.issue_number,
                        reason="already_processed",
                    )
                    continue
                self._initialize_run_meta_cache(run_id)
                fut = pool.submit(
                    self._process_implementation_candidate_worker,
                    candidate,
                    issue,
                    branch,
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
                continue
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
                continue
            if self._is_takeover_active(issue_number=tracked.issue_number):
                log_event(
                    LOGGER,
                    "feedback_turn_blocked",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
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
                run_id = self._state.claim_feedback_turn_start(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    branch=tracked.branch,
                    meta_json=_DEFAULT_RUN_META_JSON,
                    repo_full_name=self._state_repo_full_name(),
                )
                if run_id is None:
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="already_processed",
                    )
                    continue
                self._initialize_run_meta_cache(run_id)
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
            if self._is_takeover_active(issue_number=followup.issue_number):
                log_event(
                    LOGGER,
                    "pre_pr_followup_enqueued",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=followup.issue_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
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

                if self._incremental_comment_fetch_enabled():
                    incremental_scan = self._scan_incremental_issue_comments(
                        issue_number=followup.issue_number,
                        surface=_SURFACE_ISSUE_PRE_PR_FOLLOWUPS,
                        bootstrap_mode="seed_latest",
                    )
                    source_comments = [
                        cast(PullRequestIssueComment, comment) for comment in incremental_scan.new
                    ]
                    token_observations = self._action_token_observations_from_comments(
                        scope_kind="issue",
                        scope_number=followup.issue_number,
                        source=_SURFACE_ISSUE_PRE_PR_FOLLOWUPS,
                        comments=incremental_scan.fetched,
                    )
                    self._state.ingest_feedback_scan_batch(
                        events=(),
                        cursor_updates=(incremental_scan.cursor_update,),
                        token_observations=token_observations,
                        repo_full_name=self._state_repo_full_name(),
                    )
                else:
                    source_comments = self._github.list_issue_comments(followup.issue_number)
                    token_observations = self._action_token_observations_from_comments(
                        scope_kind="issue",
                        scope_number=followup.issue_number,
                        source=_SURFACE_ISSUE_PRE_PR_FOLLOWUPS,
                        comments=tuple(source_comments),
                    )
                    if token_observations:
                        self._state.ingest_feedback_scan_batch(
                            events=(),
                            cursor_updates=(),
                            token_observations=token_observations,
                            repo_full_name=self._state_repo_full_name(),
                        )
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

                run_id = self._state.claim_pre_pr_followup_run_start(
                    issue_number=followup.issue_number,
                    flow=followup.flow,
                    branch=branch,
                    meta_json=_DEFAULT_RUN_META_JSON,
                    repo_full_name=self._state_repo_full_name(),
                )
                if run_id is None:
                    log_event(
                        LOGGER,
                        "pre_pr_followup_enqueued",
                        repo_full_name=self._state_repo_full_name(),
                        issue_number=followup.issue_number,
                        reason="already_processed",
                    )
                    continue
                self._initialize_run_meta_cache(run_id)
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
                        branch,
                        consumed_comment_id_max,
                    )
                else:
                    fut = pool.submit(
                        self._process_issue_worker,
                        resume_issue,
                        cast(IssueFlow, followup.flow),
                        branch,
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

    def _monitor_pr_actions(self) -> None:
        policy = self._effective_pr_actions_feedback_policy()
        if policy == "never":
            return

        tracked_prs = self._state.list_tracked_pull_requests(
            repo_full_name=self._state_repo_full_name()
        )
        log_event(
            LOGGER,
            "actions_monitor_scan_started",
            tracked_pr_count=len(tracked_prs),
            policy=policy,
        )
        for tracked in tracked_prs:
            if self._is_takeover_active(issue_number=tracked.issue_number):
                log_event(
                    LOGGER,
                    "actions_monitor_skipped",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
            pr = self._github.get_pull_request(tracked.pr_number)
            if pr.merged or pr.state.lower() != "open":
                continue

            runs = self._github.list_workflow_runs_for_head(tracked.pr_number, pr.head_sha)
            failed_runs = tuple(
                run
                for run in runs
                if run.status == "completed" and run.conclusion not in _ACTIONS_GREEN_CONCLUSIONS
            )
            active_runs = tuple(run for run in runs if run.status != "completed")
            if active_runs:
                log_event(
                    LOGGER,
                    "actions_monitor_active_runs_detected",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    head_sha=pr.head_sha,
                    policy=policy,
                    active_run_count=len(active_runs),
                    failed_run_count=len(failed_runs),
                )
            if policy == "all_complete" and active_runs:
                continue
            if not failed_runs:
                continue

            feedback_events = tuple(
                self._actions_feedback_event_for_run(
                    pr_number=tracked.pr_number,
                    issue_number=tracked.issue_number,
                    run=run,
                )
                for run in failed_runs
            )
            self._state.ingest_feedback_events(
                feedback_events,
                repo_full_name=self._state_repo_full_name(),
            )
            log_event(
                LOGGER,
                "actions_failure_events_enqueued",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                head_sha=pr.head_sha,
                policy=policy,
                failed_run_count=len(failed_runs),
                active_run_count=len(active_runs),
            )

    def _actions_feedback_event_for_run(
        self,
        *,
        pr_number: int,
        issue_number: int,
        run: WorkflowRunSnapshot,
    ) -> FeedbackEventRecord:
        return FeedbackEventRecord(
            event_key=event_key(
                pr_number=pr_number,
                kind="actions",
                comment_id=run.run_id,
                updated_at=run.updated_at,
            ),
            pr_number=pr_number,
            issue_number=issue_number,
            kind="actions",
            comment_id=run.run_id,
            updated_at=run.updated_at,
        )

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
            if self._is_takeover_active(issue_number=issue_number):
                log_event(
                    LOGGER,
                    "source_issue_redirect_skipped",
                    repo_full_name=self._state_repo_full_name(),
                    issue_number=issue_number,
                    pr_number=pr_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
            if self._incremental_comment_fetch_enabled():
                incremental_scan = self._scan_incremental_issue_comments(
                    issue_number=issue_number,
                    surface=_SURFACE_ISSUE_POST_PR_REDIRECTS,
                    bootstrap_mode="seed_latest",
                )
                comments = [
                    cast(PullRequestIssueComment, comment) for comment in incremental_scan.new
                ]
                token_observations = self._action_token_observations_from_comments(
                    scope_kind="issue",
                    scope_number=issue_number,
                    source=_SURFACE_ISSUE_POST_PR_REDIRECTS,
                    comments=incremental_scan.fetched,
                )
                self._state.ingest_feedback_scan_batch(
                    events=(),
                    cursor_updates=(incremental_scan.cursor_update,),
                    token_observations=token_observations,
                    repo_full_name=self._state_repo_full_name(),
                )
            else:
                comments = self._github.list_issue_comments(issue_number)
                token_observations = self._action_token_observations_from_comments(
                    scope_kind="issue",
                    scope_number=issue_number,
                    source=_SURFACE_ISSUE_POST_PR_REDIRECTS,
                    comments=tuple(comments),
                )
                if token_observations:
                    self._state.ingest_feedback_scan_batch(
                        events=(),
                        cursor_updates=(),
                        token_observations=token_observations,
                        repo_full_name=self._state_repo_full_name(),
                    )
            cursor = self._state.get_issue_comment_cursor(
                issue_number,
                repo_full_name=self._state_repo_full_name(),
            )
            max_scanned_comment_id = cursor.post_pr_last_redirected_comment_id
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
                self._ensure_tokenized_issue_comment(
                    github=self._github,
                    issue_number=issue_number,
                    token=token,
                    body=_render_source_issue_redirect_comment(
                        pr_number=pr_number,
                        pr_url=pr_url,
                        source_comment_url=comment.html_url,
                    ),
                    source="source_issue_redirect",
                    repo_full_name=self._state_repo_full_name(),
                )
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
            if self._is_takeover_active(issue_number=legacy.issue_number):
                continue
            if not _is_recoverable_pre_pr_error(legacy.error or ""):
                continue
            issue = self._github.get_issue(legacy.issue_number)
            flow = _infer_pre_pr_flow_from_issue_and_error(
                issue=issue,
                error=legacy.error or "",
                design_label=self._repo.trigger_label,
                bugfix_label=self._repo.bugfix_label,
                small_job_label=self._repo.small_job_label,
                ignore_label=self._repo.ignore_label,
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
        if self._is_takeover_active(issue_number=issue.number, issue=issue):
            log_event(
                LOGGER,
                "failed_branch_pr_repair_skipped",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue.number,
                branch=branch,
                reason="ignore_label_active",
                ignore_label=self._repo.ignore_label,
            )
            return
        if not self._is_issue_author_allowed(
            issue_number=issue.number,
            author_login=issue.author_login,
            reason="unauthorized_issue_author_defensive",
        ):
            return

        title, body, flow_label = _recovery_pr_payload_for_issue(issue=issue, branch=branch)
        try:
            pr = self._create_pull_request_with_outbox(
                issue_number=issue.number,
                run_id=None,
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
        self._state.mark_create_pr_call_state_applied(
            issue_number=issue.number,
            branch=branch,
            pr_number=pr.number,
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

    def _create_pr_dedupe_key(
        self,
        *,
        issue_number: int,
        head: str,
        base: str,
        run_id: str | None,
    ) -> str:
        run_key = run_id if run_id is not None else "no-run-id"
        return f"create_pr:{issue_number}:{base}:{head}:{run_key}"

    def _parse_create_pr_outbox_payload(self, payload_json: str) -> _CreatePullRequestOutboxPayload:
        payload_obj = json.loads(payload_json)
        if not isinstance(payload_obj, dict):
            raise RuntimeError("Invalid create_pull_request outbox payload")
        issue_number = payload_obj.get("issue_number")
        title = payload_obj.get("title")
        head = payload_obj.get("head")
        base = payload_obj.get("base")
        body = payload_obj.get("body")
        if not isinstance(issue_number, int):
            raise RuntimeError("create_pull_request outbox payload is missing issue_number")
        if not isinstance(title, str):
            raise RuntimeError("create_pull_request outbox payload is missing title")
        if not isinstance(head, str):
            raise RuntimeError("create_pull_request outbox payload is missing head")
        if not isinstance(base, str):
            raise RuntimeError("create_pull_request outbox payload is missing base")
        if not isinstance(body, str):
            raise RuntimeError("create_pull_request outbox payload is missing body")
        return _CreatePullRequestOutboxPayload(
            issue_number=issue_number,
            title=title,
            head=head,
            base=base,
            body=body,
        )

    def _pull_request_from_outbox_result(self, result_json: str | None) -> PullRequest | None:
        if result_json is None:
            return None
        result_obj = json.loads(result_json)
        if not isinstance(result_obj, dict):
            return None
        pr_number = result_obj.get("pr_number")
        pr_url = result_obj.get("pr_url")
        if not isinstance(pr_number, int):
            return None
        if not isinstance(pr_url, str):
            return None
        return PullRequest(number=pr_number, html_url=pr_url)

    def _find_existing_pull_request_for_branch(
        self,
        *,
        head: str,
        base: str,
    ) -> PullRequest | None:
        finder = getattr(self._github, "find_pull_request_by_head", None)
        if not callable(finder):
            return None
        try:
            existing = finder(head=head, base=base, state="open")
        except TypeError:
            existing = finder(head=head, base=base)
        if existing is not None:
            return existing
        try:
            return finder(head=head, base=base, state="all")
        except TypeError:
            return finder(head=head, base=base)

    def _execute_create_pr_outbox_call(self, entry: GitHubCallOutboxState) -> PullRequest:
        payload = self._parse_create_pr_outbox_payload(entry.payload_json)
        if entry.status == "succeeded":
            completed = self._pull_request_from_outbox_result(entry.result_json)
            if completed is not None:
                return completed
            self._state.mark_github_call_pending_retry(
                call_id=entry.call_id,
                error="create_pull_request outbox row was succeeded without result payload",
                repo_full_name=self._state_repo_full_name(),
            )

        self._state.mark_github_call_in_progress(
            call_id=entry.call_id,
            repo_full_name=self._state_repo_full_name(),
        )
        try:
            pr = self._find_existing_pull_request_for_branch(head=payload.head, base=payload.base)
            if pr is None:
                try:
                    pr = self._github.create_pull_request(
                        title=payload.title,
                        head=payload.head,
                        base=payload.base,
                        body=payload.body,
                    )
                except Exception:
                    recovered = self._find_existing_pull_request_for_branch(
                        head=payload.head,
                        base=payload.base,
                    )
                    if recovered is None:
                        raise
                    pr = recovered
            self._state.mark_github_call_succeeded(
                call_id=entry.call_id,
                result_json=json.dumps(
                    {"pr_number": pr.number, "pr_url": pr.html_url},
                    sort_keys=True,
                ),
                pr_number=pr.number,
                repo_full_name=self._state_repo_full_name(),
            )
            return pr
        except Exception as exc:  # noqa: BLE001
            self._state.mark_github_call_pending_retry(
                call_id=entry.call_id,
                error=str(exc),
                repo_full_name=self._state_repo_full_name(),
            )
            raise

    def _create_pull_request_with_outbox(
        self,
        *,
        issue_number: int,
        run_id: str | None,
        title: str,
        head: str,
        base: str,
        body: str,
    ) -> PullRequest:
        dedupe_key = self._create_pr_dedupe_key(
            issue_number=issue_number,
            head=head,
            base=base,
            run_id=run_id,
        )
        entry = self._state.upsert_github_call_intent(
            call_kind="create_pull_request",
            dedupe_key=dedupe_key,
            payload_json=json.dumps(
                {
                    "issue_number": issue_number,
                    "title": title,
                    "head": head,
                    "base": base,
                    "body": body,
                },
                sort_keys=True,
            ),
            run_id=run_id,
            issue_number=issue_number,
            branch=head,
            repo_full_name=self._state_repo_full_name(),
        )
        return self._execute_create_pr_outbox_call(entry)

    def _replay_pending_create_pr_calls(self) -> int:
        replayed_count = 0
        entries = self._state.list_replayable_github_calls(
            call_kind="create_pull_request",
            repo_full_name=self._state_repo_full_name(),
        )
        for entry in entries:
            try:
                payload = self._parse_create_pr_outbox_payload(entry.payload_json)
                pr = self._execute_create_pr_outbox_call(entry)
                applied = self._state.apply_succeeded_create_pr_call(
                    call_id=entry.call_id,
                    issue_number=payload.issue_number,
                    branch=payload.head,
                    pr_number=pr.number,
                    pr_url=pr.html_url,
                    run_id=entry.run_id,
                    repo_full_name=self._state_repo_full_name(),
                )
                if applied:
                    replayed_count += 1
                    log_event(
                        LOGGER,
                        "pending_create_pr_call_applied",
                        repo_full_name=self._state_repo_full_name(),
                        issue_number=payload.issue_number,
                        branch=payload.head,
                        pr_number=pr.number,
                        call_id=entry.call_id,
                    )
            except GitHubPollingError:
                raise
            except Exception as exc:  # noqa: BLE001
                log_event(
                    LOGGER,
                    "pending_create_pr_call_replay_failed",
                    repo_full_name=self._state_repo_full_name(),
                    call_id=entry.call_id,
                    error_type=type(exc).__name__,
                )
        return replayed_count

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
        blocked_pr_issue_numbers = {
            blocked.pr_number: blocked.issue_number
            for blocked in self._state.list_blocked_pull_requests(
                repo_full_name=self._state_repo_full_name()
            )
        }

        for issue_number in issue_numbers:
            source_issue_number = blocked_pr_issue_numbers.get(issue_number)
            if source_issue_number is not None and self._is_takeover_active(
                issue_number=source_issue_number
            ):
                log_event(
                    LOGGER,
                    "operator_command_scan_skipped",
                    issue_number=issue_number,
                    source_issue_number=source_issue_number,
                    reason="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                continue
            source_pr_number = issue_number if source_issue_number is not None else None
            if self._incremental_comment_fetch_enabled():
                incremental_scan = self._scan_incremental_issue_comments(
                    issue_number=issue_number,
                    surface=_SURFACE_ISSUE_OPERATOR_COMMANDS,
                    bootstrap_mode="seed_latest",
                )
                comments = [cast(PullRequestIssueComment, c) for c in incremental_scan.new]
                token_observations = self._action_token_observations_from_comments(
                    scope_kind="issue",
                    scope_number=issue_number,
                    source=_SURFACE_ISSUE_OPERATOR_COMMANDS,
                    comments=incremental_scan.fetched,
                )
                self._state.ingest_feedback_scan_batch(
                    events=(),
                    cursor_updates=(incremental_scan.cursor_update,),
                    token_observations=token_observations,
                    repo_full_name=self._state_repo_full_name(),
                )
            else:
                comments = self._github.list_issue_comments(issue_number)
                token_observations = self._action_token_observations_from_comments(
                    scope_kind="issue",
                    scope_number=issue_number,
                    source=_SURFACE_ISSUE_OPERATOR_COMMANDS,
                    comments=tuple(comments),
                )
                if token_observations:
                    self._state.ingest_feedback_scan_batch(
                        events=(),
                        cursor_updates=(),
                        token_observations=token_observations,
                        repo_full_name=self._state_repo_full_name(),
                    )
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
        body = _render_operator_command_result(
            normalized_command=_operator_normalized_command(command),
            status=reply_status,
            detail=detail,
            source_comment_url=_operator_source_comment_url(
                command=command,
                repo_full_name=command.repo_full_name or self._repo.full_name,
            ),
        )
        self._ensure_tokenized_issue_comment(
            github=github,
            issue_number=issue_number,
            token=token,
            body=body,
            source="operator_command_reply",
            repo_full_name=command.repo_full_name or self._repo.full_name,
        )

    def _issue_has_action_token(
        self,
        *,
        github: GitHubGateway,
        issue_number: int,
        token: str,
        source: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = repo_full_name or self._state_repo_full_name()
        planned = self._state.record_action_token_planned(
            token=token,
            scope_kind="issue",
            scope_number=issue_number,
            source=source,
            repo_full_name=repo_key,
        )
        return self._is_action_token_observed(token_state=planned, github=github)

    def _checkpoint_recoverable_pre_pr_blocked(
        self,
        *,
        issue: Issue,
        flow: PrePrFlow,
        checkout_path: Path,
        branch: str,
        blocked_error: Exception,
    ) -> CheckpointedPrePrBlockedError:
        waiting_reason = str(blocked_error).strip() or type(blocked_error).__name__
        flow_label = _pre_pr_flow_label(flow)
        checkpoint_commit_message = (
            f"chore: checkpoint blocked {flow_label} flow for issue #{issue.number}"
        )
        log_event(
            LOGGER,
            "pre_pr_checkpoint_started",
            repo_full_name=self._state_repo_full_name(),
            issue_number=issue.number,
            flow=flow,
            branch=branch,
        )
        try:
            checkpoint_sha = self._git.persist_checkpoint_branch(
                checkout_path,
                branch,
                commit_message=checkpoint_commit_message,
            )
        except Exception as checkpoint_exc:  # noqa: BLE001
            checkpoint_detail = _summarize_git_error(str(checkpoint_exc))
            log_event(
                LOGGER,
                "pre_pr_checkpoint_failed",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue.number,
                flow=flow,
                branch=branch,
                error_type=type(checkpoint_exc).__name__,
            )
            self._github.post_issue_comment(
                issue_number=issue.number,
                body=_render_pre_pr_checkpoint_failure_comment(
                    branch=branch,
                    waiting_reason=waiting_reason,
                    checkpoint_error=checkpoint_detail,
                ),
            )
            raise DirectFlowValidationError(
                "checkpoint persistence failed before follow-up handoff: "
                f"issue #{issue.number} branch `{branch}` ({checkpoint_detail})"
            ) from checkpoint_exc

        self._post_pre_pr_checkpoint_comment(
            issue_number=issue.number,
            branch=branch,
            checkpoint_sha=checkpoint_sha,
            waiting_reason=waiting_reason,
        )
        log_event(
            LOGGER,
            "pre_pr_checkpoint_ready",
            repo_full_name=self._state_repo_full_name(),
            issue_number=issue.number,
            flow=flow,
            branch=branch,
            checkpoint_sha=checkpoint_sha,
        )
        return CheckpointedPrePrBlockedError(
            waiting_reason=waiting_reason,
            checkpoint_branch=branch,
            checkpoint_sha=checkpoint_sha,
        )

    def _post_pre_pr_checkpoint_comment(
        self,
        *,
        issue_number: int,
        branch: str,
        checkpoint_sha: str,
        waiting_reason: str,
    ) -> None:
        token = compute_pre_pr_checkpoint_token(
            issue_number=issue_number,
            checkpoint_sha=checkpoint_sha,
        )
        if self._issue_has_action_token(
            github=self._github,
            issue_number=issue_number,
            token=token,
            source="pre_pr_checkpoint",
            repo_full_name=self._state_repo_full_name(),
        ):
            log_event(
                LOGGER,
                "pre_pr_checkpoint_comment_skipped",
                repo_full_name=self._state_repo_full_name(),
                issue_number=issue_number,
                checkpoint_sha=checkpoint_sha,
                reason="token_already_present",
            )
            return

        tree_url = f"https://github.com/{self._state_repo_full_name()}/tree/{checkpoint_sha}"
        compare_url = (
            "https://github.com/"
            f"{self._state_repo_full_name()}/compare/"
            f"{self._repo.default_branch}...{checkpoint_sha}"
        )
        self._ensure_tokenized_issue_comment(
            github=self._github,
            issue_number=issue_number,
            token=token,
            body=_render_pre_pr_checkpoint_comment(
                waiting_reason=waiting_reason,
                checkpoint_branch=branch,
                checkpoint_sha=checkpoint_sha,
                tree_url=tree_url,
                compare_url=compare_url,
                default_branch=self._repo.default_branch,
            ),
            source="pre_pr_checkpoint",
            repo_full_name=self._state_repo_full_name(),
        )
        log_event(
            LOGGER,
            "pre_pr_checkpoint_comment_posted",
            repo_full_name=self._state_repo_full_name(),
            issue_number=issue_number,
            checkpoint_sha=checkpoint_sha,
        )

    def _github_for_repo(self, repo_full_name: str) -> GitHubGateway:
        normalized = repo_full_name.strip()
        if normalized and normalized in self._github_by_repo_full_name:
            return self._github_by_repo_full_name[normalized]
        return self._github

    def _state_repo_full_name(self) -> str:
        return self._repo.full_name

    def _incremental_comment_fetch_enabled(self) -> bool:
        return self._config.runtime.enable_incremental_comment_fetch

    def _load_poll_cursor(
        self, *, surface: GitHubCommentSurface, scope_number: int
    ) -> GitHubCommentPollCursorState:
        repo_full_name = self._state_repo_full_name()
        cursor = self._state.get_poll_cursor(
            surface=surface,
            scope_number=scope_number,
            repo_full_name=repo_full_name,
        )
        if cursor is None:
            return GitHubCommentPollCursorState(
                repo_full_name=repo_full_name,
                surface=surface,
                scope_number=scope_number,
                last_updated_at=_COMMENT_CURSOR_EPOCH,
                last_comment_id=0,
                bootstrap_complete=False,
                updated_at="",
            )

        parsed_cursor_time = _parse_utc_timestamp(cursor.last_updated_at)
        now_utc = datetime.now(timezone.utc)
        if (
            parsed_cursor_time is None
            or parsed_cursor_time > (now_utc + timedelta(seconds=1))
            or cursor.last_comment_id < 0
        ):
            reset_timestamp = _format_utc_timestamp(
                now_utc
                - timedelta(seconds=self._config.runtime.comment_fetch_safe_backfill_seconds)
            )
            log_event(
                LOGGER,
                "cursor_invalid",
                repo_full_name=repo_full_name,
                surface=surface,
                scope_number=scope_number,
                last_updated_at=cursor.last_updated_at,
                last_comment_id=cursor.last_comment_id,
                bootstrap_complete=cursor.bootstrap_complete,
                reset_last_updated_at=reset_timestamp,
            )
            return self._state.upsert_poll_cursor(
                surface=surface,
                scope_number=scope_number,
                last_updated_at=reset_timestamp,
                last_comment_id=0,
                bootstrap_complete=False,
                repo_full_name=repo_full_name,
            )

        normalized_timestamp = _format_utc_timestamp(parsed_cursor_time)
        if normalized_timestamp != cursor.last_updated_at:
            return self._state.upsert_poll_cursor(
                surface=surface,
                scope_number=scope_number,
                last_updated_at=normalized_timestamp,
                last_comment_id=cursor.last_comment_id,
                bootstrap_complete=cursor.bootstrap_complete,
                repo_full_name=repo_full_name,
            )
        return cursor

    def _since_for_cursor(self, *, last_updated_at: str) -> str:
        parsed = _parse_utc_timestamp(last_updated_at)
        if parsed is None:
            parsed = _parse_utc_timestamp(_COMMENT_CURSOR_EPOCH)
            if parsed is None:
                parsed = datetime.now(timezone.utc)
        since = parsed - timedelta(seconds=self._config.runtime.comment_fetch_overlap_seconds)
        epoch = _parse_utc_timestamp(_COMMENT_CURSOR_EPOCH)
        if epoch is not None and since < epoch:
            since = epoch
        return _format_utc_timestamp(since)

    def _build_incremental_scan(
        self,
        *,
        surface: GitHubCommentSurface,
        scope_number: int,
        cursor: GitHubCommentPollCursorState,
        comments: tuple[PullRequestIssueComment | PullRequestReviewComment, ...],
        bootstrap_mode: Literal["process_all", "seed_latest"],
    ) -> _IncrementalCommentScan:
        normalized_comments: list[
            tuple[str, int, PullRequestIssueComment | PullRequestReviewComment]
        ] = []
        for comment in comments:
            normalized_comments.append(
                (
                    _normalize_timestamp_for_compare(comment.updated_at),
                    comment.comment_id,
                    comment,
                )
            )
        normalized_comments.sort(key=lambda item: (item[0], item[1]))

        cursor_key = (cursor.last_updated_at, cursor.last_comment_id)
        if cursor.bootstrap_complete:
            new_comments = tuple(
                item[2] for item in normalized_comments if (item[0], item[1]) > cursor_key
            )
        elif bootstrap_mode == "process_all":
            new_comments = tuple(item[2] for item in normalized_comments)
        else:
            new_comments = ()

        if normalized_comments:
            next_updated_at, next_comment_id, _ = normalized_comments[-1]
        elif cursor.bootstrap_complete:
            next_updated_at, next_comment_id = cursor_key
        else:
            next_updated_at = _format_utc_timestamp(datetime.now(timezone.utc))
            next_comment_id = 0

        replay_count = max(0, len(normalized_comments) - len(new_comments))
        if not cursor.bootstrap_complete:
            log_event(
                LOGGER,
                "incremental_scan_backfill",
                repo_full_name=self._state_repo_full_name(),
                surface=surface,
                scope_number=scope_number,
                fetched_count=len(normalized_comments),
                processed_count=len(new_comments),
                bootstrap_mode=bootstrap_mode,
            )

        log_event(
            LOGGER,
            "incremental_scan_completed",
            repo_full_name=self._state_repo_full_name(),
            surface=surface,
            scope_number=scope_number,
            fetched_count=len(normalized_comments),
            new_count=len(new_comments),
            replay_count=replay_count,
            last_updated_at=next_updated_at,
            last_comment_id=next_comment_id,
        )
        return _IncrementalCommentScan(
            fetched=tuple(item[2] for item in normalized_comments),
            new=new_comments,
            cursor_update=PollCursorUpdate(
                surface=surface,
                scope_number=scope_number,
                last_updated_at=next_updated_at,
                last_comment_id=next_comment_id,
                bootstrap_complete=True,
            ),
        )

    def _scan_incremental_pr_review_comments(self, *, pr_number: int) -> _IncrementalCommentScan:
        cursor = self._load_poll_cursor(
            surface=_SURFACE_PR_REVIEW_COMMENTS,
            scope_number=pr_number,
        )
        since = (
            None
            if not cursor.bootstrap_complete
            else self._since_for_cursor(last_updated_at=cursor.last_updated_at)
        )
        log_event(
            LOGGER,
            "incremental_scan_started",
            repo_full_name=self._state_repo_full_name(),
            surface=_SURFACE_PR_REVIEW_COMMENTS,
            scope_number=pr_number,
            since=since,
            bootstrap_complete=cursor.bootstrap_complete,
        )
        comments = tuple(self._github.list_pull_request_review_comments(pr_number, since=since))
        return self._build_incremental_scan(
            surface=_SURFACE_PR_REVIEW_COMMENTS,
            scope_number=pr_number,
            cursor=cursor,
            comments=comments,
            bootstrap_mode="process_all",
        )

    def _scan_incremental_pr_review_summaries(self, *, pr_number: int) -> _IncrementalCommentScan:
        cursor = self._load_poll_cursor(
            surface=_SURFACE_PR_REVIEW_SUMMARIES,
            scope_number=pr_number,
        )
        log_event(
            LOGGER,
            "incremental_scan_started",
            repo_full_name=self._state_repo_full_name(),
            surface=_SURFACE_PR_REVIEW_SUMMARIES,
            scope_number=pr_number,
            since=None,
            bootstrap_complete=cursor.bootstrap_complete,
        )
        comments = tuple(self._github.list_pull_request_review_summaries(pr_number))
        return self._build_incremental_scan(
            surface=_SURFACE_PR_REVIEW_SUMMARIES,
            scope_number=pr_number,
            cursor=cursor,
            comments=comments,
            bootstrap_mode="process_all",
        )

    def _scan_incremental_pr_issue_comments(self, *, pr_number: int) -> _IncrementalCommentScan:
        cursor = self._load_poll_cursor(
            surface=_SURFACE_PR_ISSUE_COMMENTS,
            scope_number=pr_number,
        )
        since = (
            None
            if not cursor.bootstrap_complete
            else self._since_for_cursor(last_updated_at=cursor.last_updated_at)
        )
        log_event(
            LOGGER,
            "incremental_scan_started",
            repo_full_name=self._state_repo_full_name(),
            surface=_SURFACE_PR_ISSUE_COMMENTS,
            scope_number=pr_number,
            since=since,
            bootstrap_complete=cursor.bootstrap_complete,
        )
        comments = tuple(self._github.list_pull_request_issue_comments(pr_number, since=since))
        return self._build_incremental_scan(
            surface=_SURFACE_PR_ISSUE_COMMENTS,
            scope_number=pr_number,
            cursor=cursor,
            comments=comments,
            bootstrap_mode="process_all",
        )

    def _scan_incremental_issue_comments(
        self,
        *,
        issue_number: int,
        surface: GitHubCommentSurface,
        bootstrap_mode: Literal["process_all", "seed_latest"],
    ) -> _IncrementalCommentScan:
        cursor = self._load_poll_cursor(surface=surface, scope_number=issue_number)
        since = (
            None
            if not cursor.bootstrap_complete
            else self._since_for_cursor(last_updated_at=cursor.last_updated_at)
        )
        log_event(
            LOGGER,
            "incremental_scan_started",
            repo_full_name=self._state_repo_full_name(),
            surface=surface,
            scope_number=issue_number,
            since=since,
            bootstrap_complete=cursor.bootstrap_complete,
        )
        comments = tuple(self._github.list_issue_comments(issue_number, since=since))
        return self._build_incremental_scan(
            surface=surface,
            scope_number=issue_number,
            cursor=cursor,
            comments=comments,
            bootstrap_mode=bootstrap_mode,
        )

    def _action_token_observations_from_comments(
        self,
        *,
        scope_kind: Literal["pr", "issue"],
        scope_number: int,
        source: str,
        comments: tuple[PullRequestIssueComment | PullRequestReviewComment, ...],
    ) -> tuple[ActionTokenObservation, ...]:
        observed_by_token: dict[str, ActionTokenObservation] = {}
        for comment in comments:
            tokens = extract_action_tokens(comment.body)
            if not tokens:
                continue
            normalized_updated_at = _normalize_timestamp_for_compare(comment.updated_at)
            for token in tokens:
                candidate = ActionTokenObservation(
                    token=token,
                    scope_kind=scope_kind,
                    scope_number=scope_number,
                    source=source,
                    comment_id=comment.comment_id,
                    updated_at=normalized_updated_at,
                )
                existing = observed_by_token.get(token)
                if existing is None or (
                    candidate.updated_at,
                    candidate.comment_id,
                ) > (existing.updated_at, existing.comment_id):
                    observed_by_token[token] = candidate
        return tuple(observed_by_token[token] for token in sorted(observed_by_token))

    def _token_reconcile_since(self, *, created_at: str) -> str:
        created_at_dt = _parse_utc_timestamp(created_at) or datetime.now(timezone.utc)
        since = created_at_dt - timedelta(
            seconds=self._config.runtime.comment_fetch_overlap_seconds
        )
        safe_floor = datetime.now(timezone.utc) - timedelta(
            seconds=self._config.runtime.comment_fetch_safe_backfill_seconds
        )
        if since < safe_floor:
            since = safe_floor
        epoch = _parse_utc_timestamp(_COMMENT_CURSOR_EPOCH)
        if epoch is not None and since < epoch:
            since = epoch
        return _format_utc_timestamp(since)

    def _reconcile_action_token(
        self,
        *,
        token_state: ActionTokenState,
        github: GitHubGateway | None = None,
    ) -> bool:
        repo_full_name = token_state.repo_full_name or self._state_repo_full_name()
        github_client = github if github is not None else self._github_for_repo(repo_full_name)
        since = self._token_reconcile_since(created_at=token_state.created_at)

        if token_state.scope_kind == "pr":
            review_comments = tuple(
                github_client.list_pull_request_review_comments(
                    token_state.scope_number, since=since
                )
            )
            issue_comments = tuple(
                github_client.list_pull_request_issue_comments(
                    token_state.scope_number, since=since
                )
            )
            observations = self._action_token_observations_from_comments(
                scope_kind="pr",
                scope_number=token_state.scope_number,
                source="token_reconcile_pr_review",
                comments=review_comments,
            ) + self._action_token_observations_from_comments(
                scope_kind="pr",
                scope_number=token_state.scope_number,
                source="token_reconcile_pr_issue",
                comments=issue_comments,
            )
        else:
            issue_comments = tuple(
                github_client.list_issue_comments(token_state.scope_number, since=since)
            )
            observations = self._action_token_observations_from_comments(
                scope_kind="issue",
                scope_number=token_state.scope_number,
                source="token_reconcile_issue",
                comments=issue_comments,
            )

        if observations:
            self._state.ingest_feedback_scan_batch(
                events=(),
                cursor_updates=(),
                token_observations=observations,
                repo_full_name=repo_full_name,
            )

        refreshed = self._state.get_action_token(
            token_state.token,
            repo_full_name=repo_full_name,
        )
        if refreshed is not None and refreshed.status == "observed":
            log_event(
                LOGGER,
                "token_observed",
                repo_full_name=repo_full_name,
                token=token_state.token,
                scope_kind=token_state.scope_kind,
                scope_number=token_state.scope_number,
            )
            return True

        log_event(
            LOGGER,
            "token_reconcile_timeout",
            repo_full_name=repo_full_name,
            token=token_state.token,
            scope_kind=token_state.scope_kind,
            scope_number=token_state.scope_number,
            since=since,
        )
        return False

    def _is_action_token_observed(
        self,
        *,
        token_state: ActionTokenState,
        github: GitHubGateway | None = None,
    ) -> bool:
        if token_state.status == "observed":
            return True
        return self._reconcile_action_token(token_state=token_state, github=github)

    def _ensure_tokenized_issue_comment(
        self,
        *,
        github: GitHubGateway,
        issue_number: int,
        token: str,
        body: str,
        source: str,
        repo_full_name: str,
    ) -> bool:
        planned = self._state.record_action_token_planned(
            token=token,
            scope_kind="issue",
            scope_number=issue_number,
            source=source,
            repo_full_name=repo_full_name,
        )
        if self._is_action_token_observed(token_state=planned, github=github):
            return False
        github.post_issue_comment(
            issue_number=issue_number,
            body=append_action_token(body=body, token=token),
        )
        self._state.record_action_token_posted(
            token=token,
            scope_kind="issue",
            scope_number=issue_number,
            source=source,
            repo_full_name=repo_full_name,
        )
        return True

    def _ensure_tokenized_review_reply(
        self,
        *,
        pr_number: int,
        review_comment_id: int,
        token: str,
        body: str,
        source: str,
    ) -> bool:
        repo_full_name = self._state_repo_full_name()
        planned = self._state.record_action_token_planned(
            token=token,
            scope_kind="pr",
            scope_number=pr_number,
            source=source,
            repo_full_name=repo_full_name,
        )
        if self._is_action_token_observed(token_state=planned, github=self._github):
            return False
        self._github.post_review_comment_reply(
            pr_number=pr_number,
            review_comment_id=review_comment_id,
            body=append_action_token(body=body, token=token),
        )
        self._state.record_action_token_posted(
            token=token,
            scope_kind="pr",
            scope_number=pr_number,
            source=source,
            repo_full_name=repo_full_name,
        )
        return True

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
                    self._state.mark_create_pr_call_state_applied(
                        issue_number=result.issue_number,
                        branch=result.branch,
                        pr_number=result.pr_number,
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
                        checkpoint_branch = metadata.branch
                        last_checkpoint_sha: str | None = None
                        if isinstance(exc, CheckpointedPrePrBlockedError):
                            waiting_reason = exc.waiting_reason
                            checkpoint_branch = exc.checkpoint_branch
                            last_checkpoint_sha = exc.checkpoint_sha
                        self._state.mark_awaiting_issue_followup(
                            issue_number=issue_number,
                            flow=metadata.flow,
                            branch=checkpoint_branch,
                            context_json=metadata.context_json,
                            waiting_reason=waiting_reason,
                            last_checkpoint_sha=last_checkpoint_sha,
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
                            checkpoint_branch=checkpoint_branch,
                            checkpoint_sha=last_checkpoint_sha or "",
                        )
                        self._state.finish_agent_run(
                            run_id=metadata.run_id,
                            terminal_status="blocked",
                            failure_class=_failure_class_for_exception(exc),
                            error=waiting_reason,
                        )
                        continue

                    failure_class = _failure_class_for_exception(exc)
                    self._state.mark_failed(
                        issue_number=issue_number,
                        error=str(exc),
                        failure_class=failure_class,
                        retryable=_is_transient_issue_failure_class(failure_class),
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
                            failure_class=failure_class,
                            error=str(exc),
                        )
                finally:
                    if metadata is not None:
                        self._run_meta_cache.pop(metadata.run_id, None)

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
                    if isinstance(exc, GitHubPollingError):
                        # Polling errors are recoverable transport/API failures; keep PRs tracked
                        # so the next poll retries feedback instead of requiring manual unblock.
                        self._state.mark_pr_status(
                            pr_number=pr_number,
                            issue_number=handle.issue_number,
                            status="awaiting_feedback",
                            error=None,
                            reason="github_polling_retry",
                            detail=str(exc),
                            repo_full_name=self._state_repo_full_name(),
                        )
                        log_event(
                            LOGGER,
                            "feedback_turn_retry",
                            issue_number=handle.issue_number,
                            pr_number=pr_number,
                            reason="github_polling_error",
                        )
                        self._state.finish_agent_run(
                            run_id=handle.run_id,
                            terminal_status="failed",
                            failure_class=_failure_class_for_exception(exc),
                            error=str(exc),
                        )
                        continue

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
                finally:
                    self._state.release_feedback_turn_claim(
                        pr_number=pr_number,
                        run_id=handle.run_id,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    self._run_meta_cache.pop(handle.run_id, None)

    def _cleanup_and_release_slot(self, lease: _SlotLease) -> None:
        quarantine_reason: str | None = None
        try:
            self._git.cleanup_slot(lease.path)
        except Exception as exc:  # noqa: BLE001
            quarantine_reason = _summarize_git_error(str(exc))
            log_event(
                LOGGER,
                "slot_cleanup_failed",
                repo_full_name=self._state_repo_full_name(),
                slot=lease.slot,
                checkout_path=str(lease.path),
                error_type=type(exc).__name__,
            )
        self._slot_pool.release(lease, quarantine_reason=quarantine_reason)

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
        resume_branch: str,
        consumed_comment_id_max: int,
    ) -> WorkResult:
        with logging_repo_context(self._repo.full_name):
            return self._process_issue(
                issue,
                flow,
                branch_override=resume_branch,
                pre_pr_last_consumed_comment_id=consumed_comment_id_max,
            )

    def _process_issue(
        self,
        issue: Issue,
        flow: IssueFlow,
        *,
        branch_override: str | None = None,
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

        branch = branch_override or _branch_for_issue_flow(flow=flow, issue=issue)
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
                    branch=branch,
                    pre_pr_last_consumed_comment_id=pre_pr_last_consumed_comment_id,
                )
            return self._process_direct_issue(
                issue=issue,
                flow=flow,
                checkout_path=lease.path,
                branch=branch,
                pre_pr_last_consumed_comment_id=pre_pr_last_consumed_comment_id,
            )
        except Exception as exc:  # noqa: BLE001
            if (
                self._config.runtime.enable_issue_comment_routing
                and _is_recoverable_pre_pr_exception(exc)
            ):
                raise self._checkpoint_recoverable_pre_pr_blocked(
                    issue=issue,
                    flow=cast(PrePrFlow, flow),
                    checkout_path=lease.path,
                    branch=branch,
                    blocked_error=exc,
                ) from exc
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
            self._cleanup_and_release_slot(lease)

    def _process_design_issue(
        self,
        *,
        issue: Issue,
        checkout_path: Path,
        branch: str,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        self._git.create_or_reset_branch(checkout_path, branch)

        slug = _slugify(issue.title)
        design_relpath = f"{self._repo.design_docs_dir}/{issue.number}-{slug}.md"
        log_event(
            LOGGER,
            "design_turn_started",
            issue_number=issue.number,
            branch=branch,
        )
        run_id = self._active_run_id_for_issue(issue.number)
        design_prompt = build_design_prompt(
            issue=issue,
            repo_full_name=self._state_repo_full_name(),
            design_doc_path=design_relpath,
            default_branch=self._repo.default_branch,
        )
        self._mark_codex_invocation_started(
            run_id=run_id,
            mode="writing_doc",
            prompt=design_prompt,
            session_id=None,
        )
        try:
            start_result = self._agent.start_design_from_issue(
                issue=issue,
                repo_full_name=self._state_repo_full_name(),
                design_doc_path=design_relpath,
                default_branch=self._repo.default_branch,
                cwd=checkout_path,
            )
        except Exception:
            self._mark_codex_invocation_finished(run_id=run_id)
            raise
        self._mark_codex_invocation_finished(
            run_id=run_id,
            session_id=start_result.session.thread_id if start_result.session else None,
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

        pr = self._create_pull_request_with_outbox(
            issue_number=issue.number,
            run_id=run_id,
            title=f"Design doc for #{issue.number}: {generated.title}",
            head=branch,
            base=self._repo.default_branch,
            body=(f"Design doc.\n\nRefs #{issue.number}"),
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
        branch: str,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        self._git.create_or_reset_branch(checkout_path, branch)
        coding_guidelines_path = self._coding_guidelines_path_for_checkout(
            checkout_path=checkout_path
        )
        repo_full_name = self._state_repo_full_name()
        run_id = self._active_run_id_for_issue(issue.number)

        flow_label: str
        direct_turn: Callable[[Issue], DirectStartResult]
        if flow == "bugfix":
            flow_label = "bugfix"

            def run_direct_turn(agent_issue: Issue) -> DirectStartResult:
                prompt = build_bugfix_prompt(
                    issue=agent_issue,
                    repo_full_name=repo_full_name,
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                )
                self._mark_codex_invocation_started(
                    run_id=run_id,
                    mode="bugfix",
                    prompt=prompt,
                    session_id=None,
                )
                try:
                    result = self._agent.start_bugfix_from_issue(
                        issue=agent_issue,
                        repo_full_name=repo_full_name,
                        default_branch=self._repo.default_branch,
                        coding_guidelines_path=coding_guidelines_path,
                        cwd=checkout_path,
                    )
                except Exception:
                    self._mark_codex_invocation_finished(run_id=run_id)
                    raise
                self._mark_codex_invocation_finished(
                    run_id=run_id,
                    session_id=result.session.thread_id if result.session else None,
                )
                return result

            direct_turn = run_direct_turn
        elif flow == "small_job":
            flow_label = "small-job"

            def run_direct_turn(agent_issue: Issue) -> DirectStartResult:
                prompt = build_small_job_prompt(
                    issue=agent_issue,
                    repo_full_name=repo_full_name,
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                )
                self._mark_codex_invocation_started(
                    run_id=run_id,
                    mode="small-job",
                    prompt=prompt,
                    session_id=None,
                )
                try:
                    result = self._agent.start_small_job_from_issue(
                        issue=agent_issue,
                        repo_full_name=repo_full_name,
                        default_branch=self._repo.default_branch,
                        coding_guidelines_path=coding_guidelines_path,
                        cwd=checkout_path,
                    )
                except Exception:
                    self._mark_codex_invocation_finished(run_id=run_id)
                    raise
                self._mark_codex_invocation_finished(
                    run_id=run_id,
                    session_id=result.session.thread_id if result.session else None,
                )
                return result

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

        pr = self._create_pull_request_with_outbox(
            issue_number=issue.number,
            run_id=run_id,
            title=start_result.pr_title,
            head=branch,
            base=self._repo.default_branch,
            body=(f"{start_result.pr_summary}\n\nFixes #{issue.number}"),
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
        branch_override: str | None = None,
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

            branch = branch_override or f"agent/impl/{slug}"
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
            repo_full_name = self._state_repo_full_name()
            run_id = self._active_run_id_for_issue(issue.number)

            def run_implementation_turn(agent_issue: Issue) -> DirectStartResult:
                prompt = build_implementation_prompt(
                    issue=agent_issue,
                    repo_full_name=repo_full_name,
                    default_branch=self._repo.default_branch,
                    coding_guidelines_path=coding_guidelines_path,
                    design_doc_path=design_relpath,
                    design_doc_markdown=design_doc_markdown,
                    design_pr_number=candidate.design_pr_number,
                    design_pr_url=candidate.design_pr_url,
                )
                self._mark_codex_invocation_started(
                    run_id=run_id,
                    mode="implementation",
                    prompt=prompt,
                    session_id=None,
                )
                try:
                    result = self._agent.start_implementation_from_design(
                        issue=agent_issue,
                        repo_full_name=repo_full_name,
                        default_branch=self._repo.default_branch,
                        coding_guidelines_path=coding_guidelines_path,
                        design_doc_path=design_relpath,
                        design_doc_markdown=design_doc_markdown,
                        design_pr_number=candidate.design_pr_number,
                        design_pr_url=candidate.design_pr_url,
                        cwd=lease.path,
                    )
                except Exception:
                    self._mark_codex_invocation_finished(run_id=run_id)
                    raise
                self._mark_codex_invocation_finished(
                    run_id=run_id,
                    session_id=result.session.thread_id if result.session else None,
                )
                return result

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
            pr = self._create_pull_request_with_outbox(
                issue_number=issue.number,
                run_id=run_id,
                title=start_result.pr_title,
                head=branch,
                base=self._repo.default_branch,
                body=(
                    f"{start_result.pr_summary}\n\n"
                    f"Fixes #{issue.number}\n\n"
                    f"Implements design doc: [{design_relpath}]({design_doc_url})\n\n"
                    + design_pr_line.rstrip()
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
            if (
                self._config.runtime.enable_issue_comment_routing
                and _is_recoverable_pre_pr_exception(exc)
            ):
                checkpoint_branch = (
                    branch_override
                    if branch_override is not None
                    else _branch_for_implementation_candidate(candidate)
                )
                raise self._checkpoint_recoverable_pre_pr_blocked(
                    issue=issue,
                    flow="implementation",
                    checkout_path=lease.path,
                    branch=checkpoint_branch,
                    blocked_error=exc,
                ) from exc
            log_event(
                LOGGER,
                "issue_processing_failed",
                issue_number=candidate.issue_number,
                flow="implementation",
                error_type=type(exc).__name__,
            )
            raise
        finally:
            self._cleanup_and_release_slot(lease)

    def _process_implementation_candidate_worker(
        self,
        candidate: ImplementationCandidateState,
        issue: Issue,
        resume_branch: str,
        consumed_comment_id_max: int,
    ) -> WorkResult:
        with logging_repo_context(self._repo.full_name):
            return self._process_implementation_candidate(
                candidate,
                issue_override=issue,
                branch_override=resume_branch,
                pre_pr_last_consumed_comment_id=consumed_comment_id_max,
            )

    def _process_feedback_turn_worker(self, tracked: TrackedPullRequestState) -> str:
        with logging_repo_context(self._repo.full_name):
            return self._process_feedback_turn(tracked)

    def _active_run_id_for_issue(self, issue_number: int) -> str | None:
        for _ in range(20):
            with self._running_lock:
                metadata = self._running_issue_metadata.get(issue_number)
                if metadata is not None:
                    return metadata.run_id
            time.sleep(0.01)
        return None

    def _active_run_id_for_pr(self, pr_number: int) -> str | None:
        for _ in range(20):
            with self._running_lock:
                feedback = self._running_feedback.get(pr_number)
                if feedback is not None:
                    return feedback.run_id
            time.sleep(0.01)
        return None

    def _active_run_id_for_pr_now(self, pr_number: int) -> str | None:
        with self._running_lock:
            feedback = self._running_feedback.get(pr_number)
            if feedback is None:
                return None
            return feedback.run_id

    def _initialize_run_meta_cache(self, run_id: str) -> None:
        self._run_meta_cache[run_id] = json.loads(_DEFAULT_RUN_META_JSON)

    def _update_run_meta(self, run_id: str | None, **updates: object) -> None:
        if run_id is None:
            return
        current = dict(self._run_meta_cache.get(run_id, {}))
        current.update(updates)
        self._run_meta_cache[run_id] = current
        updated = self._state.update_agent_run_meta(
            run_id=run_id,
            meta_json=json.dumps(current, sort_keys=True),
        )
        if not updated:
            self._run_meta_cache.pop(run_id, None)

    def _record_run_prompt(self, run_id: str | None, prompt: str) -> None:
        if run_id is None:
            return
        self._update_run_meta(
            run_id,
            last_prompt=prompt,
        )

    def _mark_codex_invocation_started(
        self,
        *,
        run_id: str | None,
        mode: str,
        prompt: str,
        session_id: str | None,
    ) -> None:
        self._update_run_meta(
            run_id,
            codex_active=True,
            codex_invocation_started_at=datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            codex_mode=mode,
            codex_session_id=session_id,
            last_prompt=prompt,
        )

    def _mark_codex_invocation_finished(
        self,
        *,
        run_id: str | None,
        session_id: str | None = None,
    ) -> None:
        updates: dict[str, object] = {
            "codex_active": False,
            "codex_invocation_started_at": None,
        }
        if session_id is not None:
            updates["codex_session_id"] = session_id
        self._update_run_meta(run_id, **updates)

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

    def _capture_run_start_comment_id_if_enabled(
        self,
        issue_number: int,
        *,
        comments: Sequence[PullRequestIssueComment] | None = None,
    ) -> int:
        if not self._config.runtime.enable_issue_comment_routing:
            return 0
        source_comments = (
            self._github.list_issue_comments(issue_number) if comments is None else comments
        )
        run_start_comment_id = max((comment.comment_id for comment in source_comments), default=0)
        self._state.advance_pre_pr_last_consumed_comment_id(
            issue_number=issue_number,
            comment_id=run_start_comment_id,
            repo_full_name=self._state_repo_full_name(),
        )
        return run_start_comment_id

    def _augment_issue_with_first_invocation_source_comments(
        self,
        *,
        issue: Issue,
        comments: Sequence[PullRequestIssueComment],
    ) -> Issue:
        if not comments:
            return issue

        comment_lines = [
            (
                f"- @{comment.user_login} ({comment.created_at}) [{comment.html_url}]"
                f"\n{comment.body.strip() or '<empty>'}"
            )
            for comment in sorted(comments, key=lambda item: item.comment_id)
        ]
        comment_section = "\n\n".join(comment_lines)
        context = "Ordered issue comments at first invocation:\n" + comment_section
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

            issue = self._issue_snapshot_for_poll(issue_number=tracked.issue_number)
            if self._is_takeover_active(issue_number=tracked.issue_number, issue=issue):
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="ignore_label_active",
                    ignore_label=self._repo.ignore_label,
                )
                return "completed"
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

            review_summary_scan = self._scan_incremental_pr_review_summaries(
                pr_number=tracked.pr_number
            )
            review_summary_comments = [
                cast(PullRequestIssueComment, c) for c in review_summary_scan.new
            ]
            if self._incremental_comment_fetch_enabled():
                review_scan = self._scan_incremental_pr_review_comments(pr_number=tracked.pr_number)
                issue_scan = self._scan_incremental_pr_issue_comments(pr_number=tracked.pr_number)
                review_comments = [cast(PullRequestReviewComment, c) for c in review_scan.new]
                issue_comments = [cast(PullRequestIssueComment, c) for c in issue_scan.new]
                cursor_updates: tuple[PollCursorUpdate, ...] = (
                    review_scan.cursor_update,
                    review_summary_scan.cursor_update,
                    issue_scan.cursor_update,
                )
                token_observations = (
                    self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_REVIEW_COMMENTS,
                        comments=review_scan.fetched,
                    )
                    + self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_REVIEW_SUMMARIES,
                        comments=review_summary_scan.fetched,
                    )
                    + self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_ISSUE_COMMENTS,
                        comments=issue_scan.fetched,
                    )
                )
            else:
                review_comments = self._github.list_pull_request_review_comments(tracked.pr_number)
                issue_comments = self._github.list_pull_request_issue_comments(tracked.pr_number)
                cursor_updates = (review_summary_scan.cursor_update,)
                token_observations = (
                    self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_REVIEW_COMMENTS,
                        comments=tuple(review_comments),
                    )
                    + self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_REVIEW_SUMMARIES,
                        comments=review_summary_scan.fetched,
                    )
                    + self._action_token_observations_from_comments(
                        scope_kind="pr",
                        scope_number=tracked.pr_number,
                        source=_SURFACE_PR_ISSUE_COMMENTS,
                        comments=tuple(issue_comments),
                    )
                )
            changed_files = self._github.list_pull_request_files(tracked.pr_number)
            review_floor_comment_id, issue_floor_comment_id = (
                self._state.get_pr_takeover_comment_floors(
                    pr_number=tracked.pr_number,
                    repo_full_name=self._state_repo_full_name(),
                )
            )

            previous_pending = self._state.list_pending_feedback_events(
                tracked.pr_number,
                repo_full_name=self._state_repo_full_name(),
            )
            # PR feedback is sourced from both GitHub comment surfaces:
            # review comments (`/pulls/{pr}/comments`) and PR thread comments
            # (`/issues/{pr}/comments`).
            normalized_review = self._normalize_review_events(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                comments=review_comments,
                takeover_review_floor_comment_id=review_floor_comment_id,
            )
            normalized_issue = self._normalize_issue_events(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                comments=issue_comments,
                takeover_issue_floor_comment_id=issue_floor_comment_id,
            )
            normalized_review_summaries = self._normalize_review_summary_events(
                pr_number=tracked.pr_number,
                issue_number=tracked.issue_number,
                comments=review_summary_comments,
            )
            self._state.ingest_feedback_scan_batch(
                events=tuple(event for event, _ in normalized_review)
                + tuple(event for event, _ in normalized_review_summaries)
                + tuple(event for event, _ in normalized_issue),
                cursor_updates=cursor_updates,
                token_observations=token_observations,
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
            (
                pending_review_events,
                pending_review_summary_events,
                pending_issue_events,
                pending_actions_events,
            ) = self._partition_pending_feedback_events(pending_events)
            (
                actionable_actions_events,
                actions_synthetic_comments,
                stale_action_event_keys,
            ) = self._resolve_actions_feedback_events(
                pr_number=tracked.pr_number,
                head_sha=pr.head_sha,
                action_events=pending_actions_events,
            )

            pending_event_key_set = {
                event.event_key
                for event in pending_review_events
                + pending_review_summary_events
                + pending_issue_events
                + actionable_actions_events
            }
            if stale_action_event_keys:
                self._state.mark_feedback_events_processed(
                    event_keys=stale_action_event_keys,
                    repo_full_name=self._state_repo_full_name(),
                )
                log_event(
                    LOGGER,
                    "actions_failure_event_stale_resolved",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    stale_event_count=len(stale_action_event_keys),
                )

            if not pending_event_key_set:
                log_event(
                    LOGGER,
                    "feedback_turn_completed",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    result="no_pending_events",
                )
                return "completed"

            pending_event_keys = tuple(
                event.event_key
                for event in pending_review_events
                + pending_review_summary_events
                + pending_issue_events
                + actionable_actions_events
            )
            # Only unprocessed events are passed into the turn payload so each
            # comment/update is handled once per unique event key.
            pending_review_comments = tuple(
                comment
                for normalized_event, comment in normalized_review
                if normalized_event.event_key in pending_event_key_set
            )
            pending_issue_comments = tuple(
                comment
                for normalized_event, comment in normalized_review_summaries
                if normalized_event.event_key in pending_event_key_set
            ) + tuple(
                comment
                for normalized_event, comment in normalized_issue
                if normalized_event.event_key in pending_event_key_set
            )
            turn_issue_comments = self._append_required_tests_feedback_reminder(
                pending_issue_comments + actions_synthetic_comments,
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

            for review_reply in result.review_replies:
                token = compute_review_reply_token(
                    turn_key=turn_key,
                    review_comment_id=review_reply.review_comment_id,
                    body=review_reply.body,
                )
                self._ensure_tokenized_review_reply(
                    pr_number=tracked.pr_number,
                    review_comment_id=review_reply.review_comment_id,
                    token=token,
                    body=review_reply.body,
                    source="feedback_review_reply",
                )

            if result.general_comment:
                token = compute_general_comment_token(
                    turn_key=turn_key, body=result.general_comment
                )
                self._ensure_tokenized_issue_comment(
                    github=self._github,
                    issue_number=tracked.pr_number,
                    token=token,
                    body=result.general_comment,
                    source="feedback_general_comment",
                    repo_full_name=self._state_repo_full_name(),
                )

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
            self._cleanup_and_release_slot(lease)

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

    def _partition_pending_feedback_events(
        self,
        pending_events: tuple[PendingFeedbackEvent, ...],
    ) -> tuple[
        tuple[PendingFeedbackEvent, ...],
        tuple[PendingFeedbackEvent, ...],
        tuple[PendingFeedbackEvent, ...],
        tuple[PendingFeedbackEvent, ...],
    ]:
        review_events: list[PendingFeedbackEvent] = []
        review_summary_events: list[PendingFeedbackEvent] = []
        issue_events: list[PendingFeedbackEvent] = []
        actions_events: list[PendingFeedbackEvent] = []
        for pending in pending_events:
            if pending.kind == "review":
                review_events.append(pending)
            elif pending.kind == "review_summary":
                review_summary_events.append(pending)
            elif pending.kind == "issue":
                issue_events.append(pending)
            elif pending.kind == "actions":
                actions_events.append(pending)
        return (
            tuple(review_events),
            tuple(review_summary_events),
            tuple(issue_events),
            tuple(actions_events),
        )

    def _resolve_actions_feedback_events(
        self,
        *,
        pr_number: int,
        head_sha: str,
        action_events: tuple[PendingFeedbackEvent, ...],
    ) -> tuple[
        tuple[PendingFeedbackEvent, ...],
        tuple[PullRequestIssueComment, ...],
        tuple[str, ...],
    ]:
        if not action_events:
            return (), (), ()

        runs = self._github.list_workflow_runs_for_head(pr_number, head_sha)
        runs_by_id = {run.run_id: run for run in runs}

        stale_event_keys: list[str] = []
        actionable_events: list[PendingFeedbackEvent] = []
        actionable_runs: list[WorkflowRunSnapshot] = []
        for event in action_events:
            run = runs_by_id.get(event.comment_id)
            # Revalidate against current run snapshot so reruns/head changes do not trigger
            # remediation for stale failures.
            if run is None:
                stale_event_keys.append(event.event_key)
                continue
            if run.updated_at != event.updated_at:
                stale_event_keys.append(event.event_key)
                continue
            if run.status != "completed":
                stale_event_keys.append(event.event_key)
                continue
            if run.conclusion in _ACTIONS_GREEN_CONCLUSIONS:
                stale_event_keys.append(event.event_key)
                continue
            actionable_events.append(event)
            actionable_runs.append(run)

        synthetic_comments, failed_action_count = self._build_actions_failure_context_comments(
            actionable_runs
        )
        if actionable_runs:
            log_event(
                LOGGER,
                "actions_failure_context_loaded",
                pr_number=pr_number,
                head_sha=head_sha,
                failed_run_count=len(actionable_runs),
                failed_action_count=failed_action_count,
            )

        return tuple(actionable_events), synthetic_comments, tuple(stale_event_keys)

    def _build_actions_failure_context_comments(
        self,
        runs: list[WorkflowRunSnapshot],
    ) -> tuple[tuple[PullRequestIssueComment, ...], int]:
        if not runs:
            return (), 0

        tail_lines = self._config.runtime.pr_actions_log_tail_lines
        synthetic_comments: list[PullRequestIssueComment] = []
        failed_action_count = 0
        for index, run in enumerate(sorted(runs, key=lambda candidate: candidate.run_id), start=1):
            jobs = self._github.list_workflow_jobs(run.run_id)
            failed_jobs = tuple(
                job
                for job in jobs
                if job.status == "completed" and job.conclusion not in _ACTIONS_GREEN_CONCLUSIONS
            )
            failed_action_count += len(failed_jobs)
            log_tails_by_action = self._github.get_failed_run_log_tails(
                run.run_id,
                tail_lines_per_action=tail_lines,
            )
            synthetic_comments.append(
                PullRequestIssueComment(
                    comment_id=-(9200 + index),
                    body=self._render_actions_failure_context_comment(
                        run=run,
                        failed_jobs=failed_jobs,
                        log_tails_by_action=log_tails_by_action,
                        tail_lines=tail_lines,
                    ),
                    user_login="mergexo-system",
                    html_url=run.html_url,
                    created_at=run.updated_at or "now",
                    updated_at=run.updated_at or "now",
                )
            )

        return tuple(synthetic_comments), failed_action_count

    def _render_actions_failure_context_comment(
        self,
        *,
        run: WorkflowRunSnapshot,
        failed_jobs: tuple[WorkflowJobSnapshot, ...],
        log_tails_by_action: dict[str, str | None],
        tail_lines: int,
    ) -> str:
        lines = [
            "MergeXO GitHub Actions failure context:",
            f"- workflow run: {run.name}",
            f"- run_id: {run.run_id}",
            f"- status: {run.status}",
            f"- conclusion: {run.conclusion or '<none>'}",
            f"- run_url: {run.html_url}",
            f"- run_updated_at: {run.updated_at}",
            "",
        ]
        if not failed_jobs:
            lines.append("No failed actions were returned by the jobs API for this run.")
            return "\n".join(lines)

        job_name_counts = Counter(_normalized_actions_job_name(job.name) for job in failed_jobs)
        lines.append("Failed actions:")
        for job in failed_jobs:
            action_name = _actions_log_key_for_job(job=job, job_name_counts=job_name_counts)
            log_tail = log_tails_by_action.get(action_name)
            lines.extend(
                (
                    f"- action: {action_name}",
                    f"  conclusion: {job.conclusion or '<none>'}",
                    f"  action_url: {job.html_url}",
                    f"  last {tail_lines} log lines (truncated tail):",
                )
            )
            if log_tail is None:
                lines.append("  <logs unavailable for this action>")
                continue
            for tail_line in log_tail.splitlines() or ("<empty>",):
                lines.append(f"  {tail_line}")
        return "\n".join(lines)

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

    def _feedback_turn_with_push_merge_conflict(
        self,
        *,
        turn: FeedbackTurn,
        pull_request: PullRequestSnapshot,
        branch: str,
        repair_round: int,
        failure_output: str,
    ) -> FeedbackTurn:
        trimmed_output = failure_output.strip()
        if len(trimmed_output) > _REQUIRED_TEST_FAILURE_OUTPUT_LIMIT:
            trimmed_output = (
                trimmed_output[:_REQUIRED_TEST_FAILURE_OUTPUT_LIMIT]
                + "\n... [truncated by MergeXO]"
            )
        if not trimmed_output:
            trimmed_output = "<empty>"

        failure_comment = PullRequestIssueComment(
            comment_id=-(9300 + repair_round),
            body=(
                "MergeXO push-time merge conflict detected.\n"
                f"- branch: `{branch}`\n"
                f"- repair_round: {repair_round}\n"
                "- The remote branch changed while this turn was running.\n"
                "- Resolve conflicts in this checkout, preserve both local and remote intent,\n"
                "  then return commit_message so MergeXO can push.\n"
                "- If impossible, set commit_message to null and explain why in "
                "general_comment.\n\n"
                "Merge output:\n"
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
        push_merge_conflict_round = 0

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

            try:
                self._git.push_branch(checkout_path, tracked.branch)
            except CommandError as exc:
                detail = str(exc)
                if not _is_merge_conflict_error(detail):
                    raise
                push_merge_conflict_round += 1
                log_event(
                    LOGGER,
                    "feedback_push_merge_conflict_detected",
                    issue_number=tracked.issue_number,
                    pr_number=tracked.pr_number,
                    branch=tracked.branch,
                    repair_round=push_merge_conflict_round,
                )
                if push_merge_conflict_round > _MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS:
                    error = (
                        "push-time merge conflict unresolved after automated repair attempts; "
                        f"last failure: {_summarize_git_error(detail)}"
                    )
                    self._github.post_issue_comment(
                        issue_number=tracked.pr_number,
                        body=(
                            "MergeXO feedback automation is blocked because push-time merge "
                            "conflicts could not be resolved.\n"
                            f"- branch: `{tracked.branch}`\n"
                            f"- attempts: {_MAX_PUSH_MERGE_CONFLICT_REPAIR_ROUNDS}\n"
                            f"- last failure summary: {_summarize_git_error(detail)}"
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
                        reason="push_merge_conflict_repair_limit_exceeded",
                    )
                    return None

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

                current_turn = self._feedback_turn_with_push_merge_conflict(
                    turn=current_turn,
                    pull_request=current_pr,
                    branch=tracked.branch,
                    repair_round=push_merge_conflict_round,
                    failure_output=detail,
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
                    conflict_detail = (
                        current_result.general_comment.strip()
                        if current_result.general_comment and current_result.general_comment.strip()
                        else "agent could not resolve push-time merge conflicts"
                    )
                    self._github.post_issue_comment(
                        issue_number=tracked.pr_number,
                        body=(
                            "MergeXO feedback automation is blocked because push-time merge "
                            f"conflicts could not be resolved:\n\n{conflict_detail}"
                        ),
                    )
                    self._state.mark_pr_status(
                        pr_number=tracked.pr_number,
                        issue_number=tracked.issue_number,
                        status="blocked",
                        last_seen_head_sha=current_pr.head_sha,
                        error=conflict_detail,
                        repo_full_name=self._state_repo_full_name(),
                    )
                    log_event(
                        LOGGER,
                        "feedback_turn_blocked",
                        issue_number=tracked.issue_number,
                        pr_number=tracked.pr_number,
                        reason="push_merge_conflict_reported_impossible",
                    )
                    return None
                continue

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
            run_id = self._active_run_id_for_pr_now(tracked.pr_number)
            prompt = build_feedback_prompt(turn=current_turn)
            log_event(
                LOGGER,
                "feedback_agent_call_started",
                issue_number=tracked.issue_number,
                pr_number=tracked.pr_number,
                turn_key=current_turn.turn_key,
            )
            self._mark_codex_invocation_started(
                run_id=run_id,
                mode="respond_to_review",
                prompt=prompt,
                session_id=current_session.thread_id,
            )
            try:
                result = self._agent.respond_to_feedback(
                    session=current_session,
                    turn=current_turn,
                    cwd=checkout_path,
                )
            except Exception:
                self._mark_codex_invocation_finished(
                    run_id=run_id,
                    session_id=current_session.thread_id,
                )
                raise
            self._mark_codex_invocation_finished(
                run_id=run_id,
                session_id=result.session.thread_id,
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
        takeover_review_floor_comment_id: int = 0,
    ) -> list[tuple[FeedbackEventRecord, PullRequestReviewComment]]:
        normalized: list[tuple[FeedbackEventRecord, PullRequestReviewComment]] = []
        for comment in comments:
            if comment.comment_id <= takeover_review_floor_comment_id:
                continue
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
        takeover_issue_floor_comment_id: int = 0,
    ) -> list[tuple[FeedbackEventRecord, PullRequestIssueComment]]:
        normalized: list[tuple[FeedbackEventRecord, PullRequestIssueComment]] = []
        for comment in comments:
            if comment.comment_id <= takeover_issue_floor_comment_id:
                continue
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

    def _normalize_review_summary_events(
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
                    kind="review_summary",
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
                comment_kind="review_summary",
                comment_id=comment.comment_id,
                comment_url=comment.html_url,
            )
            normalized.append(
                (
                    FeedbackEventRecord(
                        event_key=event_key(
                            pr_number=pr_number,
                            kind="review_summary",
                            comment_id=comment.comment_id,
                            updated_at=comment.updated_at,
                        ),
                        pr_number=pr_number,
                        issue_number=issue_number,
                        kind="review_summary",
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
            self._ensure_tokenized_issue_comment(
                github=self._github,
                issue_number=tracked.pr_number,
                token=token,
                body=comment,
                source="history_rewrite_block",
                repo_full_name=self._state_repo_full_name(),
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
        planned = self._state.record_action_token_planned(
            token=token,
            scope_kind="pr",
            scope_number=pr_number,
            source="feedback_pr_token_check",
            repo_full_name=self._state_repo_full_name(),
        )
        return self._is_action_token_observed(token_state=planned, github=self._github)

    def _fetch_remote_action_tokens(self, pr_number: int) -> set[str]:
        since = _format_utc_timestamp(
            datetime.now(timezone.utc)
            - timedelta(seconds=self._config.runtime.comment_fetch_safe_backfill_seconds)
        )
        review_comments = tuple(
            self._github.list_pull_request_review_comments(pr_number, since=since)
        )
        issue_comments = tuple(
            self._github.list_pull_request_issue_comments(pr_number, since=since)
        )
        observations = self._action_token_observations_from_comments(
            scope_kind="pr",
            scope_number=pr_number,
            source="feedback_pr_review_scan",
            comments=review_comments,
        ) + self._action_token_observations_from_comments(
            scope_kind="pr",
            scope_number=pr_number,
            source="feedback_pr_issue_scan",
            comments=issue_comments,
        )
        if observations:
            self._state.ingest_feedback_scan_batch(
                events=(),
                cursor_updates=(),
                token_observations=observations,
                repo_full_name=self._state_repo_full_name(),
            )
        return {observation.token for observation in observations}

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


def _parse_utc_timestamp(value: str) -> datetime | None:
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.lower() == "now":
        return datetime.now(timezone.utc)
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_timestamp_for_compare(value: str) -> str:
    parsed = _parse_utc_timestamp(value)
    if parsed is None:
        return value
    return _format_utc_timestamp(parsed)


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
    ignore_label: str | None = None,
) -> PrePrFlow | None:
    resolved = _resolve_issue_flow(
        issue=issue,
        design_label=design_label,
        bugfix_label=bugfix_label,
        small_job_label=small_job_label,
        ignore_label=ignore_label,
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


def _render_pre_pr_checkpoint_comment(
    *,
    waiting_reason: str,
    checkpoint_branch: str,
    checkpoint_sha: str,
    tree_url: str,
    compare_url: str,
    default_branch: str,
) -> str:
    return (
        "MergeXO pre-PR flow is waiting for source-issue follow-up.\n"
        f"- blocked reason: {waiting_reason}\n"
        f"- checkpoint branch: `{checkpoint_branch}`\n"
        f"- checkpoint commit: `{checkpoint_sha}`\n"
        f"- checkpoint tree: {tree_url}\n"
        f"- compare vs `{default_branch}`: {compare_url}\n\n"
        "Next steps:\n"
        "1. Review the checkpoint tree/compare links above.\n"
        "2. Reply on this issue with clarifications or updated instructions.\n"
        "3. MergeXO will resume from this checkpoint branch head."
    )


def _render_pre_pr_checkpoint_failure_comment(
    *,
    branch: str,
    waiting_reason: str,
    checkpoint_error: str,
) -> str:
    return (
        "MergeXO could not persist a recoverable pre-PR checkpoint before cleanup.\n"
        f"- blocked reason: {waiting_reason}\n"
        f"- checkpoint branch: `{branch}`\n"
        f"- checkpoint persistence error: {checkpoint_error}\n\n"
        "Please inspect worker logs and recover manually from the local checkout if needed."
    )


def _pre_pr_flow_label(flow: PrePrFlow) -> str:
    if flow == "design_doc":
        return "design"
    if flow == "small_job":
        return "small-job"
    return flow


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


def _is_transient_issue_failure_class(failure_class: AgentRunFailureClass) -> bool:
    return failure_class in {"github_error", "unknown"}


def _resolve_issue_flow(
    *,
    issue: Issue,
    design_label: str,
    bugfix_label: str,
    small_job_label: str,
    ignore_label: str | None = None,
) -> IssueFlow | None:
    issue_labels = set(issue.labels)
    if ignore_label and ignore_label in issue_labels:
        return None
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
    if flow == "design":
        return (
            f"Design doc for #{issue.number}: {issue.title}",
            (f"Recovered design PR from a previously pushed branch.\n\nRefs #{issue.number}"),
            flow,
        )
    if flow == "implementation":
        return (
            f"Implementation for #{issue.number}: {issue.title}",
            (
                "Recovered implementation PR from a previously pushed branch.\n\n"
                f"Fixes #{issue.number}"
            ),
            flow,
        )
    if flow == "bugfix":
        return (
            f"Bugfix for #{issue.number}: {issue.title}",
            (f"Recovered bugfix PR from a previously pushed branch.\n\nFixes #{issue.number}"),
            flow,
        )
    return (
        issue.title,
        (f"Recovered small-job PR from a previously pushed branch.\n\nFixes #{issue.number}"),
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


def _normalized_actions_job_name(raw_name: str) -> str:
    normalized = raw_name.strip()
    return normalized or "unnamed-action"


def _actions_log_key_for_job(*, job: WorkflowJobSnapshot, job_name_counts: Counter[str]) -> str:
    base_name = _normalized_actions_job_name(job.name)
    if job_name_counts[base_name] == 1:
        return base_name
    return f"{base_name} [job {job.job_id}]"


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
