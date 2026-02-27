from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import threading
import uuid
from typing import Literal, cast

from mergexo.agent_adapter import AgentSession
from mergexo.feedback_loop import FeedbackEventRecord
from mergexo.models import (
    OperatorCommandName,
    OperatorCommandRecord,
    OperatorCommandStatus,
    RestartMode,
    RuntimeOperationRecord,
    RuntimeOperationStatus,
)


_DEFAULT_REPO_FULL_NAME = "__single_repo__"
_LEGACY_SCHEMA_REINIT_MESSAGE = (
    "Detected a legacy state schema that is incompatible with multi-repo support. "
    "Reinitialize state: stop MergeXO, remove the old state DB, run `mergexo init`, then restart."
)
_TRANSIENT_RETRY_MAX_ATTEMPTS = 3
_TRANSIENT_RETRY_INITIAL_DELAY_SECONDS = 30
_TRANSIENT_RETRY_MAX_DELAY_SECONDS = 300
_STALE_RUNNING_RETRY_ERROR = "stale_running_issue_without_active_run"
PrePrFollowupFlow = Literal["design_doc", "bugfix", "small_job", "implementation"]
AgentRunKind = Literal["issue_flow", "implementation_flow", "pre_pr_followup", "feedback_turn"]
AgentRunTerminalStatus = Literal[
    "completed",
    "failed",
    "blocked",
    "merged",
    "closed",
    "interrupted",
]
AgentRunFailureClass = Literal[
    "agent_error",
    "tests_failed",
    "policy_block",
    "github_error",
    "history_rewrite",
    "unknown",
]
GitHubCommentSurface = Literal[
    "pr_review_comments",
    "pr_issue_comments",
    "issue_pre_pr_followups",
    "issue_post_pr_redirects",
    "issue_operator_commands",
]
ActionTokenScopeKind = Literal["pr", "issue"]
ActionTokenStatus = Literal["planned", "posted", "observed", "failed"]
GitHubCallKind = Literal["create_pull_request"]
GitHubCallStatus = Literal["pending", "in_progress", "succeeded"]


@dataclass(frozen=True)
class TrackedPullRequestState:
    pr_number: int
    issue_number: int
    branch: str
    status: str
    last_seen_head_sha: str | None
    repo_full_name: str = ""


@dataclass(frozen=True)
class PendingFeedbackEvent:
    event_key: str
    kind: str
    comment_id: int
    updated_at: str


@dataclass(frozen=True)
class BlockedPullRequestState:
    pr_number: int
    issue_number: int
    branch: str
    last_seen_head_sha: str | None
    error: str | None
    updated_at: str
    pending_event_count: int
    repo_full_name: str = ""


@dataclass(frozen=True)
class ImplementationCandidateState:
    issue_number: int
    design_branch: str
    design_pr_number: int | None
    design_pr_url: str | None
    repo_full_name: str = ""


@dataclass(frozen=True)
class PrePrFollowupState:
    issue_number: int
    flow: PrePrFollowupFlow
    branch: str
    context_json: str
    waiting_reason: str
    last_checkpoint_sha: str | None
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class IssueCommentCursorState:
    issue_number: int
    pre_pr_last_consumed_comment_id: int
    post_pr_last_redirected_comment_id: int
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class IssueTakeoverState:
    issue_number: int
    ignore_active: bool
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class LegacyFailedIssueRunState:
    issue_number: int
    branch: str | None
    error: str | None
    repo_full_name: str = ""


@dataclass(frozen=True)
# TODO remove migration after updates
class LegacyRunningIssueRunState:
    issue_number: int
    branch: str | None
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class PullRequestStatusState:
    pr_number: int
    issue_number: int
    status: str
    branch: str
    last_seen_head_sha: str | None
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class GitHubCommentPollCursorState:
    surface: GitHubCommentSurface
    scope_number: int
    last_updated_at: str
    last_comment_id: int
    bootstrap_complete: bool
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class PollCursorUpdate:
    surface: GitHubCommentSurface
    scope_number: int
    last_updated_at: str
    last_comment_id: int
    bootstrap_complete: bool = True


@dataclass(frozen=True)
class ActionTokenState:
    token: str
    scope_kind: ActionTokenScopeKind
    scope_number: int
    source: str
    status: ActionTokenStatus
    attempt_count: int
    observed_comment_id: int | None
    observed_updated_at: str | None
    created_at: str
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class ActionTokenObservation:
    token: str
    scope_kind: ActionTokenScopeKind
    scope_number: int
    source: str
    comment_id: int
    updated_at: str


@dataclass(frozen=True)
class GitHubCallOutboxState:
    call_id: int
    call_kind: GitHubCallKind
    dedupe_key: str
    payload_json: str
    status: GitHubCallStatus
    state_applied: bool
    attempt_count: int
    last_error: str | None
    result_json: str | None
    run_id: str | None
    issue_number: int | None
    branch: str | None
    pr_number: int | None
    created_at: str
    updated_at: str
    repo_full_name: str = ""


class StateStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            self._assert_not_legacy_schema(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issue_runs (
                    repo_full_name TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    branch TEXT,
                    pr_number INTEGER,
                    pr_url TEXT,
                    error TEXT,
                    last_failure_class TEXT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    next_retry_at TEXT NULL,
                    active_run_id TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
                )
                """
            )
            issue_run_columns = _table_columns(conn, "issue_runs")
            if "active_run_id" not in issue_run_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_runs
                    ADD COLUMN active_run_id TEXT NULL
                    """
                )
            if "last_failure_class" not in issue_run_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_runs
                    ADD COLUMN last_failure_class TEXT NULL
                    """
                )
            if "retry_count" not in issue_run_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_runs
                    ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "next_retry_at" not in issue_run_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_runs
                    ADD COLUMN next_retry_at TEXT NULL
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pre_pr_followup_state (
                    repo_full_name TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    flow TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    waiting_reason TEXT NOT NULL,
                    last_checkpoint_sha TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
                )
                """
            )
            pre_pr_followup_columns = _table_columns(conn, "pre_pr_followup_state")
            if "last_checkpoint_sha" not in pre_pr_followup_columns:
                conn.execute(
                    """
                    ALTER TABLE pre_pr_followup_state
                    ADD COLUMN last_checkpoint_sha TEXT NULL
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issue_comment_cursors (
                    repo_full_name TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    pre_pr_last_consumed_comment_id INTEGER NOT NULL DEFAULT 0,
                    post_pr_last_redirected_comment_id INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issue_takeover_state (
                    repo_full_name TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    ignore_active INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
                )
                """
            )
            issue_takeover_columns = _table_columns(conn, "issue_takeover_state")
            if "ignore_active" not in issue_takeover_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_takeover_state
                    ADD COLUMN ignore_active INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "updated_at" not in issue_takeover_columns:
                conn.execute(
                    """
                    ALTER TABLE issue_takeover_state
                    ADD COLUMN updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS github_comment_poll_cursors (
                    repo_full_name TEXT NOT NULL,
                    surface TEXT NOT NULL,
                    scope_number INTEGER NOT NULL,
                    last_updated_at TEXT NOT NULL,
                    last_comment_id INTEGER NOT NULL,
                    bootstrap_complete INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, surface, scope_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_sessions (
                    repo_full_name TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    adapter TEXT NOT NULL,
                    thread_id TEXT,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS action_tokens (
                    repo_full_name TEXT NOT NULL,
                    token TEXT NOT NULL,
                    scope_kind TEXT NOT NULL,
                    scope_number INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    observed_comment_id INTEGER NULL,
                    observed_updated_at TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, token)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_action_tokens_scope_status
                ON action_tokens(repo_full_name, scope_kind, scope_number, status)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feedback_events (
                    repo_full_name TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    issue_number INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    comment_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    processed_at TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, event_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pr_feedback_state (
                    repo_full_name TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    issue_number INTEGER NOT NULL,
                    branch TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_seen_head_sha TEXT NULL,
                    active_run_id TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, pr_number)
                )
                """
            )
            pr_feedback_columns = _table_columns(conn, "pr_feedback_state")
            if "active_run_id" not in pr_feedback_columns:
                conn.execute(
                    """
                    ALTER TABLE pr_feedback_state
                    ADD COLUMN active_run_id TEXT NULL
                    """
                )
            if "takeover_review_floor_comment_id" not in pr_feedback_columns:
                conn.execute(
                    """
                    ALTER TABLE pr_feedback_state
                    ADD COLUMN takeover_review_floor_comment_id INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "takeover_issue_floor_comment_id" not in pr_feedback_columns:
                conn.execute(
                    """
                    ALTER TABLE pr_feedback_state
                    ADD COLUMN takeover_issue_floor_comment_id INTEGER NOT NULL DEFAULT 0
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_commands (
                    repo_full_name TEXT NOT NULL,
                    command_key TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    pr_number INTEGER NULL,
                    comment_id INTEGER NOT NULL,
                    author_login TEXT NOT NULL,
                    command TEXT NOT NULL,
                    args_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, command_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_operations (
                    op_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    request_command_key TEXT NOT NULL,
                    request_repo_full_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    detail TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_run_history (
                    run_id TEXT PRIMARY KEY,
                    repo_full_name TEXT NOT NULL,
                    run_kind TEXT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    pr_number INTEGER NULL,
                    flow TEXT NULL,
                    branch TEXT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NULL,
                    terminal_status TEXT NULL,
                    failure_class TEXT NULL,
                    error TEXT NULL,
                    duration_seconds REAL NULL,
                    meta_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_run_history_repo_finished
                ON agent_run_history(repo_full_name, finished_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_run_history_repo_started
                ON agent_run_history(repo_full_name, started_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_run_history_repo_status_started
                ON agent_run_history(repo_full_name, terminal_status, started_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pr_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_full_name TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    issue_number INTEGER NOT NULL,
                    from_status TEXT NULL,
                    to_status TEXT NOT NULL,
                    reason TEXT NULL,
                    detail TEXT NULL,
                    changed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pr_status_history_repo_pr_changed
                ON pr_status_history(repo_full_name, pr_number, changed_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pr_status_history_repo_changed
                ON pr_status_history(repo_full_name, changed_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS github_call_outbox (
                    call_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_full_name TEXT NOT NULL,
                    call_kind TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    state_applied INTEGER NOT NULL DEFAULT 0,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NULL,
                    result_json TEXT NULL,
                    run_id TEXT NULL,
                    issue_number INTEGER NULL,
                    branch TEXT NULL,
                    pr_number INTEGER NULL,
                    claimed_at TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    UNIQUE (repo_full_name, dedupe_key)
                )
                """
            )
            outbox_columns = _table_columns(conn, "github_call_outbox")
            if "repo_full_name" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN repo_full_name TEXT NOT NULL DEFAULT '__single_repo__'
                    """
                )
            if "call_kind" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN call_kind TEXT NOT NULL DEFAULT 'create_pull_request'
                    """
                )
            if "dedupe_key" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN dedupe_key TEXT NOT NULL DEFAULT ''
                    """
                )
            if "payload_json" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'
                    """
                )
            if "status" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'
                    """
                )
            if "state_applied" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN state_applied INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "attempt_count" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "last_error" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN last_error TEXT NULL
                    """
                )
            if "result_json" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN result_json TEXT NULL
                    """
                )
            if "run_id" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN run_id TEXT NULL
                    """
                )
            if "issue_number" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN issue_number INTEGER NULL
                    """
                )
            if "branch" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN branch TEXT NULL
                    """
                )
            if "pr_number" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN pr_number INTEGER NULL
                    """
                )
            if "claimed_at" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN claimed_at TEXT NULL
                    """
                )
            if "created_at" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    """
                )
            if "updated_at" not in outbox_columns:
                conn.execute(
                    """
                    ALTER TABLE github_call_outbox
                    ADD COLUMN updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    """
                )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_github_call_outbox_replay
                ON github_call_outbox(
                    repo_full_name,
                    call_kind,
                    status,
                    state_applied,
                    created_at
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_github_call_outbox_run
                ON github_call_outbox(repo_full_name, run_id)
                """
            )

    def _assert_not_legacy_schema(self, conn: sqlite3.Connection) -> None:
        if _table_exists(conn, "issue_runs"):
            issue_run_columns = _table_columns(conn, "issue_runs")
            if "repo_full_name" not in issue_run_columns:
                raise RuntimeError(_LEGACY_SCHEMA_REINIT_MESSAGE)
        if _table_exists(conn, "runtime_operations"):
            runtime_operation_columns = _table_columns(conn, "runtime_operations")
            if "request_repo_full_name" not in runtime_operation_columns:
                raise RuntimeError(_LEGACY_SCHEMA_REINIT_MESSAGE)

    def can_enqueue(self, issue_number: int, *, repo_full_name: str | None = None) -> bool:
        with self._lock, self._connect() as conn:
            if repo_full_name is None:
                row = conn.execute(
                    """
                    SELECT status
                    FROM issue_runs
                    WHERE issue_number = ?
                    LIMIT 1
                    """,
                    (issue_number,),
                ).fetchone()
            else:
                repo_key = _normalize_repo_full_name(repo_full_name)
                row = conn.execute(
                    """
                    SELECT status
                    FROM issue_runs
                    WHERE repo_full_name = ? AND issue_number = ?
                    """,
                    (repo_key, issue_number),
                ).fetchone()
        return row is None

    def claim_new_issue_run_start(
        self,
        *,
        issue_number: int,
        flow: str,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        run_key = run_id if run_id is not None else uuid.uuid4().hex
        with self._lock, self._connect() as conn:
            claimed = conn.execute(
                """
                INSERT INTO issue_runs(
                    repo_full_name,
                    issue_number,
                    status,
                    branch,
                    active_run_id
                )
                VALUES(?, ?, 'running', ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO NOTHING
                """,
                (repo_key, issue_number, branch, run_key),
            )
            branch_for_run = branch
            if claimed.rowcount <= 0:
                claimed = conn.execute(
                    """
                    UPDATE issue_runs
                    SET
                        status = 'running',
                        branch = COALESCE(?, branch),
                        error = NULL,
                        active_run_id = ?,
                        next_retry_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND issue_number = ?
                      AND status = 'failed'
                      AND pr_number IS NULL
                      AND active_run_id IS NULL
                      AND retry_count > 0
                      AND retry_count < ?
                      AND (
                            last_failure_class IN ('github_error', 'unknown')
                            OR error = ?
                        )
                      AND (
                            next_retry_at IS NULL
                            OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                        )
                    """,
                    (
                        branch,
                        run_key,
                        repo_key,
                        issue_number,
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                        _STALE_RUNNING_RETRY_ERROR,
                    ),
                )
                if claimed.rowcount <= 0:
                    return None
                row = conn.execute(
                    """
                    SELECT branch
                    FROM issue_runs
                    WHERE repo_full_name = ? AND issue_number = ?
                    """,
                    (repo_key, issue_number),
                ).fetchone()
                if row is not None and isinstance(row[0], str):
                    branch_for_run = row[0]
            self._insert_agent_run_start(
                conn=conn,
                run_id=run_key,
                repo_full_name=repo_key,
                run_kind="issue_flow",
                issue_number=issue_number,
                pr_number=None,
                flow=flow,
                branch=branch_for_run,
                started_at=started_at,
                meta_json=meta_json,
            )
        return run_key

    def _claim_existing_issue_run_start(
        self,
        *,
        issue_number: int,
        flow: str,
        branch: str,
        run_kind: Literal["implementation_flow", "pre_pr_followup"],
        required_status: Literal["merged", "awaiting_issue_followup"],
        require_pre_pr_followup_state: bool,
        meta_json: str,
        run_id: str | None,
        started_at: str | None,
        repo_full_name: str | None,
    ) -> str | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        run_key = run_id if run_id is not None else uuid.uuid4().hex
        with self._lock, self._connect() as conn:
            if require_pre_pr_followup_state:
                claimed = conn.execute(
                    """
                    UPDATE issue_runs
                    SET
                        status = 'running',
                        branch = COALESCE(?, branch),
                        error = NULL,
                        active_run_id = ?,
                        last_failure_class = NULL,
                        retry_count = 0,
                        next_retry_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND issue_number = ?
                      AND status = ?
                      AND active_run_id IS NULL
                      AND EXISTS(
                            SELECT 1
                            FROM pre_pr_followup_state AS p
                            WHERE p.repo_full_name = issue_runs.repo_full_name
                              AND p.issue_number = issue_runs.issue_number
                        )
                    """,
                    (branch, run_key, repo_key, issue_number, required_status),
                )
            else:
                claimed = conn.execute(
                    """
                    UPDATE issue_runs
                    SET
                        status = 'running',
                        branch = COALESCE(?, branch),
                        error = NULL,
                        active_run_id = ?,
                        last_failure_class = NULL,
                        retry_count = 0,
                        next_retry_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND issue_number = ?
                      AND active_run_id IS NULL
                      AND (
                            status = ?
                            OR (
                                status = 'failed'
                                AND retry_count > 0
                                AND retry_count < ?
                                AND (
                                      last_failure_class IN ('github_error', 'unknown')
                                      OR error = ?
                                )
                                AND (
                                      next_retry_at IS NULL
                                      OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                                )
                            )
                        )
                    """,
                    (
                        branch,
                        run_key,
                        repo_key,
                        issue_number,
                        required_status,
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                        _STALE_RUNNING_RETRY_ERROR,
                    ),
                )
            if claimed.rowcount <= 0:
                return None
            row = conn.execute(
                """
                SELECT branch
                FROM issue_runs
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            ).fetchone()
            branch_for_run = row[0] if row is not None and isinstance(row[0], str) else branch
            self._insert_agent_run_start(
                conn=conn,
                run_id=run_key,
                repo_full_name=repo_key,
                run_kind=run_kind,
                issue_number=issue_number,
                pr_number=None,
                flow=flow,
                branch=branch_for_run,
                started_at=started_at,
                meta_json=meta_json,
            )
        return run_key

    def claim_implementation_issue_run_start(
        self,
        *,
        issue_number: int,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        return self._claim_existing_issue_run_start(
            issue_number=issue_number,
            flow="implementation",
            branch=branch,
            run_kind="implementation_flow",
            required_status="merged",
            require_pre_pr_followup_state=False,
            meta_json=meta_json,
            run_id=run_id,
            started_at=started_at,
            repo_full_name=repo_full_name,
        )

    def claim_pre_pr_followup_run_start(
        self,
        *,
        issue_number: int,
        flow: PrePrFollowupFlow,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        return self._claim_existing_issue_run_start(
            issue_number=issue_number,
            flow=flow,
            branch=branch,
            run_kind="pre_pr_followup",
            required_status="awaiting_issue_followup",
            require_pre_pr_followup_state=True,
            meta_json=meta_json,
            run_id=run_id,
            started_at=started_at,
            repo_full_name=repo_full_name,
        )

    def claim_feedback_turn_start(
        self,
        *,
        pr_number: int,
        issue_number: int,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        run_key = run_id if run_id is not None else uuid.uuid4().hex
        with self._lock, self._connect() as conn:
            claimed = conn.execute(
                """
                UPDATE pr_feedback_state
                SET active_run_id = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND pr_number = ?
                  AND issue_number = ?
                  AND status = 'awaiting_feedback'
                  AND active_run_id IS NULL
                """,
                (run_key, repo_key, pr_number, issue_number),
            )
            if claimed.rowcount <= 0:
                return None
            row = conn.execute(
                """
                SELECT branch
                FROM pr_feedback_state
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (repo_key, pr_number),
            ).fetchone()
            branch_for_run = row[0] if row is not None and isinstance(row[0], str) else branch
            self._insert_agent_run_start(
                conn=conn,
                run_id=run_key,
                repo_full_name=repo_key,
                run_kind="feedback_turn",
                issue_number=issue_number,
                pr_number=pr_number,
                flow=None,
                branch=branch_for_run,
                started_at=started_at,
                meta_json=meta_json,
            )
        return run_key

    def release_feedback_turn_claim(
        self,
        *,
        pr_number: int,
        run_id: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE pr_feedback_state
                SET active_run_id = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND pr_number = ?
                  AND active_run_id = ?
                """,
                (repo_key, pr_number, run_id),
            )
        return cursor.rowcount > 0

    def _insert_agent_run_start(
        self,
        *,
        conn: sqlite3.Connection,
        run_id: str,
        repo_full_name: str,
        run_kind: AgentRunKind,
        issue_number: int,
        pr_number: int | None,
        flow: str | None,
        branch: str | None,
        started_at: str | None,
        meta_json: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_run_history(
                run_id,
                repo_full_name,
                run_kind,
                issue_number,
                pr_number,
                flow,
                branch,
                started_at,
                meta_json
            )
            VALUES(
                ?, ?, ?, ?, ?, ?, ?, COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), ?
            )
            """,
            (
                run_id,
                repo_full_name,
                run_kind,
                issue_number,
                pr_number,
                flow,
                branch,
                started_at,
                meta_json,
            ),
        )

    def record_issue_run_start(
        self,
        *,
        run_kind: Literal["issue_flow", "implementation_flow", "pre_pr_followup"],
        issue_number: int,
        flow: str,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str:
        repo_key = _normalize_repo_full_name(repo_full_name)
        run_key = run_id if run_id is not None else uuid.uuid4().hex
        with self._lock, self._connect() as conn:
            self._insert_agent_run_start(
                conn=conn,
                run_id=run_key,
                repo_full_name=repo_key,
                run_kind=run_kind,
                issue_number=issue_number,
                pr_number=None,
                flow=flow,
                branch=branch,
                started_at=started_at,
                meta_json=meta_json,
            )
            conn.execute(
                """
                INSERT INTO issue_runs(
                    repo_full_name,
                    issue_number,
                    status,
                    branch,
                    active_run_id
                )
                VALUES(?, ?, 'running', ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='running',
                    branch=COALESCE(excluded.branch, issue_runs.branch),
                    error=NULL,
                    active_run_id=excluded.active_run_id,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, branch, run_key),
            )
        return run_key

    def record_agent_run_start(
        self,
        *,
        run_kind: AgentRunKind,
        issue_number: int,
        pr_number: int | None,
        flow: str | None,
        branch: str | None,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str:
        repo_key = _normalize_repo_full_name(repo_full_name)
        run_key = run_id if run_id is not None else uuid.uuid4().hex
        with self._lock, self._connect() as conn:
            self._insert_agent_run_start(
                conn=conn,
                run_id=run_key,
                repo_full_name=repo_key,
                run_kind=run_kind,
                issue_number=issue_number,
                pr_number=pr_number,
                flow=flow,
                branch=branch,
                started_at=started_at,
                meta_json=meta_json,
            )
        return run_key

    def finish_agent_run(
        self,
        *,
        run_id: str,
        terminal_status: AgentRunTerminalStatus,
        failure_class: AgentRunFailureClass | None = None,
        error: str | None = None,
        finished_at: str | None = None,
    ) -> bool:
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE agent_run_history
                SET
                    finished_at = COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    terminal_status = ?,
                    failure_class = ?,
                    error = ?,
                    duration_seconds = MAX(
                        0.0,
                        (
                            julianday(COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
                            - julianday(started_at)
                        ) * 86400.0
                    )
                WHERE run_id = ?
                  AND finished_at IS NULL
                """,
                (finished_at, terminal_status, failure_class, error, finished_at, run_id),
            )
        return cursor.rowcount > 0

    def update_agent_run_meta(self, *, run_id: str, meta_json: str) -> bool:
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE agent_run_history
                SET meta_json = ?
                WHERE run_id = ?
                  AND finished_at IS NULL
                """,
                (meta_json, run_id),
            )
        return cursor.rowcount > 0

    def reconcile_unfinished_agent_runs(self, *, repo_full_name: str | None = None) -> int:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                cursor = conn.execute(
                    """
                    UPDATE agent_run_history
                    SET
                        finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        terminal_status = 'interrupted',
                        failure_class = 'unknown',
                        duration_seconds = MAX(
                            0.0,
                            (
                                julianday(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                                - julianday(started_at)
                            ) * 86400.0
                        )
                    WHERE finished_at IS NULL
                    """
                )
                conn.execute(
                    """
                    UPDATE issue_runs
                    SET active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE active_run_id IS NOT NULL
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.run_id = issue_runs.active_run_id
                              AND a.finished_at IS NULL
                        )
                    """
                )
                conn.execute(
                    """
                    UPDATE pr_feedback_state
                    SET active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE active_run_id IS NOT NULL
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.run_id = pr_feedback_state.active_run_id
                              AND a.finished_at IS NULL
                        )
                    """
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE agent_run_history
                    SET
                        finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        terminal_status = 'interrupted',
                        failure_class = 'unknown',
                        duration_seconds = MAX(
                            0.0,
                            (
                                julianday(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                                - julianday(started_at)
                            ) * 86400.0
                        )
                    WHERE finished_at IS NULL
                      AND repo_full_name = ?
                    """,
                    (repo_key,),
                )
                conn.execute(
                    """
                    UPDATE issue_runs
                    SET active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND active_run_id IS NOT NULL
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.run_id = issue_runs.active_run_id
                              AND a.finished_at IS NULL
                        )
                    """,
                    (repo_key,),
                )
                conn.execute(
                    """
                    UPDATE pr_feedback_state
                    SET active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND active_run_id IS NOT NULL
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.run_id = pr_feedback_state.active_run_id
                              AND a.finished_at IS NULL
                        )
                    """,
                    (repo_key,),
                )
        return cursor.rowcount

    def reconcile_stale_running_issue_runs_with_followups(
        self, *, repo_full_name: str | None = None
    ) -> int:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        reconciled_count = 0
        with self._lock, self._connect() as conn:
            if repo_key is None:
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = 'awaiting_issue_followup',
                        branch = COALESCE(
                            (
                                SELECT p.branch
                                FROM pre_pr_followup_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                            ),
                            i.branch
                        ),
                        pr_number = NULL,
                        pr_url = NULL,
                        error = COALESCE(
                            (
                                SELECT p.waiting_reason
                                FROM pre_pr_followup_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                            ),
                            i.error,
                            'stale_running_issue_reconciled_to_pre_pr_followup'
                        ),
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND i.pr_number IS NULL
                      AND EXISTS(
                            SELECT 1
                            FROM pre_pr_followup_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                        )
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """
                )
                reconciled_count += max(cursor.rowcount, 0)
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = (
                            SELECT p.status
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                            ORDER BY p.updated_at DESC, p.pr_number DESC
                            LIMIT 1
                        ),
                        branch = COALESCE(
                            (
                                SELECT p.branch
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ),
                            i.branch
                        ),
                        pr_number = COALESCE(
                            (
                                SELECT p.pr_number
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ),
                            i.pr_number
                        ),
                        error = CASE
                            WHEN (
                                SELECT p.status
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ) = 'blocked'
                            THEN COALESCE(i.error, 'stale_running_issue_reconciled_from_pr_state')
                            ELSE NULL
                        END,
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND EXISTS(
                            SELECT 1
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                        )
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """
                )
                reconciled_count += max(cursor.rowcount, 0)
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = 'failed',
                        error = COALESCE(i.error, ?),
                        last_failure_class = 'unknown',
                        retry_count = COALESCE(i.retry_count, 0) + 1,
                        next_retry_at = CASE
                            WHEN COALESCE(i.retry_count, 0) + 1 < ?
                            THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+30 seconds')
                            ELSE NULL
                        END,
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """,
                    (
                        _STALE_RUNNING_RETRY_ERROR,
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                    ),
                )
                reconciled_count += max(cursor.rowcount, 0)
            else:
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = 'awaiting_issue_followup',
                        branch = COALESCE(
                            (
                                SELECT p.branch
                                FROM pre_pr_followup_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                            ),
                            i.branch
                        ),
                        pr_number = NULL,
                        pr_url = NULL,
                        error = COALESCE(
                            (
                                SELECT p.waiting_reason
                                FROM pre_pr_followup_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                            ),
                            i.error,
                            'stale_running_issue_reconciled_to_pre_pr_followup'
                        ),
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND i.pr_number IS NULL
                      AND i.repo_full_name = ?
                      AND EXISTS(
                            SELECT 1
                            FROM pre_pr_followup_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                        )
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """,
                    (repo_key,),
                )
                reconciled_count += max(cursor.rowcount, 0)
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = (
                            SELECT p.status
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                            ORDER BY p.updated_at DESC, p.pr_number DESC
                            LIMIT 1
                        ),
                        branch = COALESCE(
                            (
                                SELECT p.branch
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ),
                            i.branch
                        ),
                        pr_number = COALESCE(
                            (
                                SELECT p.pr_number
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ),
                            i.pr_number
                        ),
                        error = CASE
                            WHEN (
                                SELECT p.status
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                ORDER BY p.updated_at DESC, p.pr_number DESC
                                LIMIT 1
                            ) = 'blocked'
                            THEN COALESCE(i.error, 'stale_running_issue_reconciled_from_pr_state')
                            ELSE NULL
                        END,
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND i.repo_full_name = ?
                      AND EXISTS(
                            SELECT 1
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                        )
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """,
                    (repo_key,),
                )
                reconciled_count += max(cursor.rowcount, 0)
                cursor = conn.execute(
                    """
                    UPDATE issue_runs AS i
                    SET
                        status = 'failed',
                        error = COALESCE(i.error, ?),
                        last_failure_class = 'unknown',
                        retry_count = COALESCE(i.retry_count, 0) + 1,
                        next_retry_at = CASE
                            WHEN COALESCE(i.retry_count, 0) + 1 < ?
                            THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+30 seconds')
                            ELSE NULL
                        END,
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE i.status = 'running'
                      AND i.repo_full_name = ?
                      AND NOT EXISTS(
                            SELECT 1
                            FROM agent_run_history AS a
                            WHERE a.repo_full_name = i.repo_full_name
                              AND a.issue_number = i.issue_number
                              AND a.finished_at IS NULL
                        )
                    """,
                    (
                        _STALE_RUNNING_RETRY_ERROR,
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                        repo_key,
                    ),
                )
                reconciled_count += max(cursor.rowcount, 0)
        return reconciled_count

    def upsert_github_call_intent(
        self,
        *,
        call_kind: GitHubCallKind,
        dedupe_key: str,
        payload_json: str,
        run_id: str | None = None,
        issue_number: int | None = None,
        branch: str | None = None,
        repo_full_name: str | None = None,
    ) -> GitHubCallOutboxState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    repo_full_name,
                    call_id,
                    call_kind,
                    dedupe_key,
                    payload_json,
                    status,
                    state_applied,
                    attempt_count,
                    last_error,
                    result_json,
                    run_id,
                    issue_number,
                    branch,
                    pr_number,
                    created_at,
                    updated_at
                FROM github_call_outbox
                WHERE repo_full_name = ? AND dedupe_key = ?
                """,
                (repo_key, dedupe_key),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO github_call_outbox(
                        repo_full_name,
                        call_kind,
                        dedupe_key,
                        payload_json,
                        status,
                        run_id,
                        issue_number,
                        branch
                    )
                    VALUES(?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        repo_key,
                        call_kind,
                        dedupe_key,
                        payload_json,
                        run_id,
                        issue_number,
                        branch,
                    ),
                )
                row = conn.execute(
                    """
                    SELECT
                        repo_full_name,
                        call_id,
                        call_kind,
                        dedupe_key,
                        payload_json,
                        status,
                        state_applied,
                        attempt_count,
                        last_error,
                        result_json,
                        run_id,
                        issue_number,
                        branch,
                        pr_number,
                        created_at,
                        updated_at
                    FROM github_call_outbox
                    WHERE repo_full_name = ? AND dedupe_key = ?
                    """,
                    (repo_key, dedupe_key),
                ).fetchone()
        if row is None:
            raise RuntimeError("Unable to persist GitHub call intent")
        return _parse_github_call_outbox_row(row)

    def mark_github_call_in_progress(
        self,
        *,
        call_id: int,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    status = 'in_progress',
                    attempt_count = attempt_count + 1,
                    claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND call_id = ?
                  AND status IN ('pending', 'in_progress')
                """,
                (repo_key, call_id),
            )
        return cursor.rowcount > 0

    def mark_github_call_pending_retry(
        self,
        *,
        call_id: int,
        error: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    status = 'pending',
                    last_error = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (error, repo_key, call_id),
            )
        return cursor.rowcount > 0

    def mark_github_call_succeeded(
        self,
        *,
        call_id: int,
        result_json: str,
        pr_number: int | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    status = 'succeeded',
                    result_json = ?,
                    pr_number = COALESCE(?, pr_number),
                    last_error = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (result_json, pr_number, repo_key, call_id),
            )
        return cursor.rowcount > 0

    def list_replayable_github_calls(
        self,
        *,
        call_kind: GitHubCallKind,
        repo_full_name: str | None = None,
    ) -> tuple[GitHubCallOutboxState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    repo_full_name,
                    call_id,
                    call_kind,
                    dedupe_key,
                    payload_json,
                    status,
                    state_applied,
                    attempt_count,
                    last_error,
                    result_json,
                    run_id,
                    issue_number,
                    branch,
                    pr_number,
                    created_at,
                    updated_at
                FROM github_call_outbox
                WHERE repo_full_name = ?
                  AND call_kind = ?
                  AND (
                        status IN ('pending', 'in_progress')
                        OR (status = 'succeeded' AND state_applied = 0)
                    )
                ORDER BY created_at ASC, call_id ASC
                """,
                (repo_key, call_kind),
            ).fetchall()
        return tuple(_parse_github_call_outbox_row(row) for row in rows)

    def mark_create_pr_call_state_applied(
        self,
        *,
        issue_number: int,
        branch: str,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> int:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    state_applied = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND call_kind = 'create_pull_request'
                  AND issue_number = ?
                  AND branch = ?
                  AND pr_number = ?
                  AND status = 'succeeded'
                  AND state_applied = 0
                """,
                (repo_key, issue_number, branch, pr_number),
            )
        return cursor.rowcount

    def apply_succeeded_create_pr_call(
        self,
        *,
        call_id: int,
        issue_number: int,
        branch: str,
        pr_number: int,
        pr_url: str,
        run_id: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, state_applied, run_id
                FROM github_call_outbox
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (repo_key, call_id),
            ).fetchone()
            if row is None:
                return False
            status, state_applied, stored_run_id = row
            if status != "succeeded":
                return False
            if int(state_applied) != 0:
                return False

            self._upsert_completed_issue_state(
                conn=conn,
                repo_full_name=repo_key,
                issue_number=issue_number,
                branch=branch,
                pr_number=pr_number,
                pr_url=pr_url,
                history_reason=None,
            )
            effective_run_id = run_id if run_id is not None else cast(str | None, stored_run_id)
            if effective_run_id is not None:
                conn.execute(
                    """
                    UPDATE agent_run_history
                    SET
                        finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        terminal_status = 'completed',
                        failure_class = NULL,
                        error = NULL,
                        duration_seconds = MAX(
                            0.0,
                            (
                                julianday(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                                - julianday(started_at)
                            ) * 86400.0
                        )
                    WHERE run_id = ?
                      AND finished_at IS NULL
                    """,
                    (effective_run_id,),
                )
            conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    pr_number = ?,
                    state_applied = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (pr_number, repo_key, call_id),
            )
        return True

    def prune_observability_history(
        self,
        *,
        retention_days: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, int]:
        if retention_days < 1:
            raise ValueError("retention_days must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        retention_cutoff = f"-{retention_days} days"
        with self._lock, self._connect() as conn:
            if repo_key is None:
                agent_cursor = conn.execute(
                    """
                    DELETE FROM agent_run_history
                    WHERE finished_at IS NOT NULL
                      AND finished_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                    """,
                    (retention_cutoff,),
                )
                pr_cursor = conn.execute(
                    """
                    DELETE FROM pr_status_history
                    WHERE changed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                    """,
                    (retention_cutoff,),
                )
            else:
                agent_cursor = conn.execute(
                    """
                    DELETE FROM agent_run_history
                    WHERE finished_at IS NOT NULL
                      AND finished_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                      AND repo_full_name = ?
                    """,
                    (retention_cutoff, repo_key),
                )
                pr_cursor = conn.execute(
                    """
                    DELETE FROM pr_status_history
                    WHERE changed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                      AND repo_full_name = ?
                    """,
                    (retention_cutoff, repo_key),
                )
        return agent_cursor.rowcount, pr_cursor.rowcount

    def get_pull_request_status(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> PullRequestStatusState | None:
        with self._lock, self._connect() as conn:
            if repo_full_name is None:
                rows = conn.execute(
                    """
                    SELECT
                        repo_full_name,
                        pr_number,
                        issue_number,
                        status,
                        branch,
                        last_seen_head_sha,
                        updated_at
                    FROM pr_feedback_state
                    WHERE pr_number = ?
                    ORDER BY repo_full_name ASC
                    """,
                    (pr_number,),
                ).fetchall()
                if not rows:
                    row = None
                else:
                    if len(rows) > 1:
                        raise RuntimeError(
                            f"Multiple pr_feedback_state rows found for pr_number={pr_number}; "
                            "specify repo_full_name."
                        )
                    row = rows[0]
            else:
                repo_key = _normalize_repo_full_name(repo_full_name)
                row = conn.execute(
                    """
                    SELECT
                        repo_full_name,
                        pr_number,
                        issue_number,
                        status,
                        branch,
                        last_seen_head_sha,
                        updated_at
                    FROM pr_feedback_state
                    WHERE repo_full_name = ? AND pr_number = ?
                    """,
                    (repo_key, pr_number),
                ).fetchone()
        if row is None:
            return None
        (
            row_repo_full_name,
            row_pr_number,
            issue_number,
            status,
            branch,
            last_seen_head_sha,
            updated_at,
        ) = row
        return PullRequestStatusState(
            repo_full_name=str(row_repo_full_name),
            pr_number=int(row_pr_number),
            issue_number=int(issue_number),
            status=str(status),
            branch=str(branch),
            last_seen_head_sha=str(last_seen_head_sha)
            if isinstance(last_seen_head_sha, str)
            else None,
            updated_at=str(updated_at),
        )

    def mark_running(self, issue_number: int, *, repo_full_name: str | None = None) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(repo_full_name, issue_number, status)
                VALUES(?, ?, 'running')
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='running',
                    error=NULL,
                    active_run_id=NULL,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number),
            )

    def _upsert_completed_issue_state(
        self,
        *,
        conn: sqlite3.Connection,
        repo_full_name: str,
        issue_number: int,
        branch: str,
        pr_number: int,
        pr_url: str,
        history_reason: str | None,
    ) -> None:
        previous_status = _select_pr_feedback_status(
            conn=conn,
            repo_full_name=repo_full_name,
            pr_number=pr_number,
        )
        conn.execute(
            """
            INSERT INTO issue_runs(
                repo_full_name,
                issue_number,
                status,
                branch,
                pr_number,
                pr_url
            )
            VALUES(?, ?, 'awaiting_feedback', ?, ?, ?)
            ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                status='awaiting_feedback',
                branch=excluded.branch,
                pr_number=excluded.pr_number,
                pr_url=excluded.pr_url,
                error=NULL,
                active_run_id=NULL,
                last_failure_class=NULL,
                retry_count=0,
                next_retry_at=NULL,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            """,
            (repo_full_name, issue_number, branch, pr_number, pr_url),
        )
        conn.execute(
            """
            INSERT INTO pr_feedback_state(
                repo_full_name,
                pr_number,
                issue_number,
                branch,
                status
            )
            VALUES(?, ?, ?, ?, 'awaiting_feedback')
            ON CONFLICT(repo_full_name, pr_number) DO UPDATE SET
                issue_number=excluded.issue_number,
                branch=excluded.branch,
                status='awaiting_feedback',
                active_run_id=NULL,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            """,
            (repo_full_name, pr_number, issue_number, branch),
        )
        conn.execute(
            """
            DELETE FROM pre_pr_followup_state
            WHERE repo_full_name = ? AND issue_number = ?
            """,
            (repo_full_name, issue_number),
        )
        _append_pr_status_history(
            conn=conn,
            repo_full_name=repo_full_name,
            pr_number=pr_number,
            issue_number=issue_number,
            from_status=previous_status,
            to_status="awaiting_feedback",
            reason=history_reason,
            detail=None,
        )

    def mark_completed(
        self,
        issue_number: int,
        branch: str,
        pr_number: int,
        pr_url: str,
        *,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            self._upsert_completed_issue_state(
                conn=conn,
                repo_full_name=repo_key,
                issue_number=issue_number,
                branch=branch,
                pr_number=pr_number,
                pr_url=pr_url,
                history_reason="issue_run_completed",
            )

    def mark_failed(
        self,
        issue_number: int,
        error: str,
        *,
        failure_class: AgentRunFailureClass | None = None,
        retryable: bool = False,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        effective_failure_class = (
            failure_class if failure_class is not None else ("unknown" if retryable else None)
        )
        next_retry_count = 0
        retry_delay_modifier: str | None = None
        with self._lock, self._connect() as conn:
            if retryable:
                existing_row = conn.execute(
                    """
                    SELECT retry_count
                    FROM issue_runs
                    WHERE repo_full_name = ? AND issue_number = ?
                    """,
                    (repo_key, issue_number),
                ).fetchone()
                previous_retry_count = (
                    int(existing_row[0])
                    if existing_row is not None and isinstance(existing_row[0], int)
                    else 0
                )
                next_retry_count = previous_retry_count + 1
                if next_retry_count < _TRANSIENT_RETRY_MAX_ATTEMPTS:
                    retry_delay_modifier = (
                        f"+{_transient_retry_delay_seconds(next_retry_count)} seconds"
                    )
            conn.execute(
                """
                INSERT INTO issue_runs(
                    repo_full_name,
                    issue_number,
                    status,
                    error,
                    last_failure_class,
                    retry_count,
                    next_retry_at
                )
                VALUES(
                    ?,
                    ?,
                    'failed',
                    ?,
                    ?,
                    ?,
                    CASE
                        WHEN ? IS NULL THEN NULL
                        ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                    END
                )
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='failed',
                    error=excluded.error,
                    active_run_id=NULL,
                    last_failure_class=excluded.last_failure_class,
                    retry_count=excluded.retry_count,
                    next_retry_at=excluded.next_retry_at,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    issue_number,
                    error,
                    effective_failure_class,
                    next_retry_count,
                    retry_delay_modifier,
                    retry_delay_modifier,
                ),
            )
            conn.execute(
                """
                DELETE FROM pre_pr_followup_state
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            )

    def mark_awaiting_issue_followup(
        self,
        *,
        issue_number: int,
        flow: PrePrFollowupFlow,
        branch: str,
        context_json: str,
        waiting_reason: str,
        last_checkpoint_sha: str | None = None,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(
                    repo_full_name,
                    issue_number,
                    status,
                    branch,
                    pr_number,
                    pr_url,
                    error
                )
                VALUES(?, ?, 'awaiting_issue_followup', ?, NULL, NULL, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='awaiting_issue_followup',
                    branch=excluded.branch,
                    pr_number=NULL,
                    pr_url=NULL,
                    error=excluded.error,
                    active_run_id=NULL,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, branch, waiting_reason),
            )
            conn.execute(
                """
                INSERT INTO pre_pr_followup_state(
                    repo_full_name,
                    issue_number,
                    flow,
                    branch,
                    context_json,
                    waiting_reason,
                    last_checkpoint_sha
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    flow=excluded.flow,
                    branch=excluded.branch,
                    context_json=excluded.context_json,
                    waiting_reason=excluded.waiting_reason,
                    last_checkpoint_sha=excluded.last_checkpoint_sha,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    issue_number,
                    flow,
                    branch,
                    context_json,
                    waiting_reason,
                    last_checkpoint_sha,
                ),
            )

    def list_pre_pr_followups(
        self, *, repo_full_name: str | None = None
    ) -> tuple[PrePrFollowupState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        p.repo_full_name,
                        p.issue_number,
                        p.flow,
                        p.branch,
                        p.context_json,
                        p.waiting_reason,
                        p.last_checkpoint_sha,
                        p.updated_at
                    FROM pre_pr_followup_state AS p
                    INNER JOIN issue_runs AS r
                        ON r.repo_full_name = p.repo_full_name
                       AND r.issue_number = p.issue_number
                    WHERE r.status = 'awaiting_issue_followup'
                    ORDER BY p.updated_at ASC, p.repo_full_name ASC, p.issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        p.repo_full_name,
                        p.issue_number,
                        p.flow,
                        p.branch,
                        p.context_json,
                        p.waiting_reason,
                        p.last_checkpoint_sha,
                        p.updated_at
                    FROM pre_pr_followup_state AS p
                    INNER JOIN issue_runs AS r
                        ON r.repo_full_name = p.repo_full_name
                       AND r.issue_number = p.issue_number
                    WHERE r.status = 'awaiting_issue_followup'
                      AND p.repo_full_name = ?
                    ORDER BY p.updated_at ASC, p.issue_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            PrePrFollowupState(
                repo_full_name=str(row_repo_full_name),
                issue_number=int(issue_number),
                flow=_parse_pre_pr_followup_flow(flow),
                branch=str(branch),
                context_json=str(context_json),
                waiting_reason=str(waiting_reason),
                last_checkpoint_sha=(
                    str(last_checkpoint_sha) if isinstance(last_checkpoint_sha, str) else None
                ),
                updated_at=str(updated_at),
            )
            for (
                row_repo_full_name,
                issue_number,
                flow,
                branch,
                context_json,
                waiting_reason,
                last_checkpoint_sha,
                updated_at,
            ) in rows
        )

    def clear_pre_pr_followup_state(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                DELETE FROM pre_pr_followup_state
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            )

    def list_legacy_failed_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[LegacyFailedIssueRunState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        r.repo_full_name,
                        r.issue_number,
                        r.branch,
                        r.error
                    FROM issue_runs AS r
                    LEFT JOIN pre_pr_followup_state AS p
                        ON p.repo_full_name = r.repo_full_name
                       AND p.issue_number = r.issue_number
                    WHERE r.status = 'failed'
                      AND r.pr_number IS NULL
                      AND p.issue_number IS NULL
                    ORDER BY r.updated_at ASC, r.repo_full_name ASC, r.issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        r.repo_full_name,
                        r.issue_number,
                        r.branch,
                        r.error
                    FROM issue_runs AS r
                    LEFT JOIN pre_pr_followup_state AS p
                        ON p.repo_full_name = r.repo_full_name
                       AND p.issue_number = r.issue_number
                    WHERE r.status = 'failed'
                      AND r.pr_number IS NULL
                      AND p.issue_number IS NULL
                      AND r.repo_full_name = ?
                    ORDER BY r.updated_at ASC, r.issue_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            LegacyFailedIssueRunState(
                repo_full_name=str(row_repo_full_name),
                issue_number=int(issue_number),
                branch=str(branch) if isinstance(branch, str) else None,
                error=str(error) if isinstance(error, str) else None,
            )
            for row_repo_full_name, issue_number, branch, error in rows
        )

    def list_legacy_running_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[LegacyRunningIssueRunState, ...]:
        # TODO remove migration after updates
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        r.repo_full_name,
                        r.issue_number,
                        r.branch,
                        r.updated_at
                    FROM issue_runs AS r
                    WHERE r.status = 'running'
                      AND r.pr_number IS NULL
                    ORDER BY r.updated_at ASC, r.repo_full_name ASC, r.issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        r.repo_full_name,
                        r.issue_number,
                        r.branch,
                        r.updated_at
                    FROM issue_runs AS r
                    WHERE r.status = 'running'
                      AND r.pr_number IS NULL
                      AND r.repo_full_name = ?
                    ORDER BY r.updated_at ASC, r.issue_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            LegacyRunningIssueRunState(
                repo_full_name=str(row_repo_full_name),
                issue_number=int(issue_number),
                branch=str(branch) if isinstance(branch, str) else None,
                updated_at=str(updated_at),
            )
            for row_repo_full_name, issue_number, branch, updated_at in rows
        )

    def get_issue_takeover_state(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> IssueTakeoverState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT ignore_active, updated_at
                FROM issue_takeover_state
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            ).fetchone()
        if row is None:
            return IssueTakeoverState(
                repo_full_name=repo_key,
                issue_number=issue_number,
                ignore_active=False,
                updated_at="",
            )

        ignore_active, updated_at = row
        if not isinstance(ignore_active, int):
            raise RuntimeError("Invalid ignore_active value stored in issue_takeover_state")
        if not isinstance(updated_at, str):
            raise RuntimeError("Invalid updated_at value stored in issue_takeover_state")
        return IssueTakeoverState(
            repo_full_name=repo_key,
            issue_number=issue_number,
            ignore_active=bool(ignore_active),
            updated_at=updated_at,
        )

    def get_issue_takeover_active(
        self,
        issue_number: int,
        *,
        repo_full_name: str | None = None,
    ) -> bool:
        return self.get_issue_takeover_state(
            issue_number,
            repo_full_name=repo_full_name,
        ).ignore_active

    def set_issue_takeover_active(
        self,
        *,
        issue_number: int,
        ignore_active: bool,
        repo_full_name: str | None = None,
    ) -> IssueTakeoverState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_takeover_state(
                    repo_full_name,
                    issue_number,
                    ignore_active
                )
                VALUES(?, ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    ignore_active = excluded.ignore_active,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, int(ignore_active)),
            )
        return self.get_issue_takeover_state(issue_number, repo_full_name=repo_key)

    def list_active_issue_takeovers(
        self,
        *,
        repo_full_name: str | None = None,
    ) -> tuple[int, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT issue_number
                FROM issue_takeover_state
                WHERE repo_full_name = ? AND ignore_active = 1
                ORDER BY issue_number ASC
                """,
                (repo_key,),
            ).fetchall()
        issue_numbers: list[int] = []
        for (issue_number,) in rows:
            if not isinstance(issue_number, int):
                raise RuntimeError("Invalid issue_number value stored in issue_takeover_state")
            issue_numbers.append(issue_number)
        return tuple(issue_numbers)

    def list_feedback_pr_numbers_for_issue(
        self,
        *,
        issue_number: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT pr_number
                FROM pr_feedback_state
                WHERE repo_full_name = ?
                  AND issue_number = ?
                  AND status IN ('awaiting_feedback', 'blocked')
                ORDER BY pr_number ASC
                """,
                (repo_key, issue_number),
            ).fetchall()
        pr_numbers: list[int] = []
        for (pr_number,) in rows:
            if not isinstance(pr_number, int):
                raise RuntimeError("Invalid pr_number value stored in pr_feedback_state")
            pr_numbers.append(pr_number)
        return tuple(pr_numbers)

    def get_pr_takeover_comment_floors(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, int]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT takeover_review_floor_comment_id, takeover_issue_floor_comment_id
                FROM pr_feedback_state
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (repo_key, pr_number),
            ).fetchone()
        if row is None:
            return 0, 0
        review_floor, issue_floor = row
        if not isinstance(review_floor, int):
            raise RuntimeError(
                "Invalid takeover_review_floor_comment_id value stored in pr_feedback_state"
            )
        if not isinstance(issue_floor, int):
            raise RuntimeError(
                "Invalid takeover_issue_floor_comment_id value stored in pr_feedback_state"
            )
        return review_floor, issue_floor

    def advance_pr_takeover_comment_floors(
        self,
        *,
        pr_number: int,
        review_floor_comment_id: int,
        issue_floor_comment_id: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, int]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE pr_feedback_state
                SET
                    takeover_review_floor_comment_id = CASE
                        WHEN takeover_review_floor_comment_id > ?
                        THEN takeover_review_floor_comment_id
                        ELSE ?
                    END,
                    takeover_issue_floor_comment_id = CASE
                        WHEN takeover_issue_floor_comment_id > ?
                        THEN takeover_issue_floor_comment_id
                        ELSE ?
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (
                    review_floor_comment_id,
                    review_floor_comment_id,
                    issue_floor_comment_id,
                    issue_floor_comment_id,
                    repo_key,
                    pr_number,
                ),
            )
        return self.get_pr_takeover_comment_floors(
            pr_number=pr_number,
            repo_full_name=repo_key,
        )

    def mark_pending_feedback_events_processed_for_pr(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> int:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE feedback_events
                SET processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND pr_number = ?
                  AND processed_at IS NULL
                """,
                (repo_key, pr_number),
            )
        return cursor.rowcount

    def get_issue_comment_cursor(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> IssueCommentCursorState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    pre_pr_last_consumed_comment_id,
                    post_pr_last_redirected_comment_id,
                    updated_at
                FROM issue_comment_cursors
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            ).fetchone()
        if row is None:
            return IssueCommentCursorState(
                repo_full_name=repo_key,
                issue_number=issue_number,
                pre_pr_last_consumed_comment_id=0,
                post_pr_last_redirected_comment_id=0,
                updated_at="",
            )

        pre_pr_last_consumed_comment_id, post_pr_last_redirected_comment_id, updated_at = row
        if not isinstance(pre_pr_last_consumed_comment_id, int):
            raise RuntimeError(
                "Invalid pre_pr_last_consumed_comment_id value stored in issue_comment_cursors"
            )
        if not isinstance(post_pr_last_redirected_comment_id, int):
            raise RuntimeError(
                "Invalid post_pr_last_redirected_comment_id value stored in issue_comment_cursors"
            )
        if not isinstance(updated_at, str):
            raise RuntimeError("Invalid updated_at value stored in issue_comment_cursors")
        return IssueCommentCursorState(
            repo_full_name=repo_key,
            issue_number=issue_number,
            pre_pr_last_consumed_comment_id=pre_pr_last_consumed_comment_id,
            post_pr_last_redirected_comment_id=post_pr_last_redirected_comment_id,
            updated_at=updated_at,
        )

    def advance_pre_pr_last_consumed_comment_id(
        self,
        *,
        issue_number: int,
        comment_id: int,
        repo_full_name: str | None = None,
    ) -> IssueCommentCursorState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_comment_cursors(
                    repo_full_name,
                    issue_number,
                    pre_pr_last_consumed_comment_id,
                    post_pr_last_redirected_comment_id
                )
                VALUES(?, ?, ?, 0)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    pre_pr_last_consumed_comment_id = CASE
                        WHEN issue_comment_cursors.pre_pr_last_consumed_comment_id > excluded.pre_pr_last_consumed_comment_id
                        THEN issue_comment_cursors.pre_pr_last_consumed_comment_id
                        ELSE excluded.pre_pr_last_consumed_comment_id
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, comment_id),
            )
        return self.get_issue_comment_cursor(issue_number, repo_full_name=repo_key)

    def advance_post_pr_last_redirected_comment_id(
        self,
        *,
        issue_number: int,
        comment_id: int,
        repo_full_name: str | None = None,
    ) -> IssueCommentCursorState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_comment_cursors(
                    repo_full_name,
                    issue_number,
                    pre_pr_last_consumed_comment_id,
                    post_pr_last_redirected_comment_id
                )
                VALUES(?, ?, 0, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    post_pr_last_redirected_comment_id = CASE
                        WHEN issue_comment_cursors.post_pr_last_redirected_comment_id > excluded.post_pr_last_redirected_comment_id
                        THEN issue_comment_cursors.post_pr_last_redirected_comment_id
                        ELSE excluded.post_pr_last_redirected_comment_id
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, comment_id),
            )
        return self.get_issue_comment_cursor(issue_number, repo_full_name=repo_key)

    def get_poll_cursor(
        self,
        *,
        surface: GitHubCommentSurface,
        scope_number: int,
        repo_full_name: str | None = None,
    ) -> GitHubCommentPollCursorState | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    surface,
                    scope_number,
                    last_updated_at,
                    last_comment_id,
                    bootstrap_complete,
                    updated_at
                FROM github_comment_poll_cursors
                WHERE repo_full_name = ? AND surface = ? AND scope_number = ?
                """,
                (repo_key, surface, scope_number),
            ).fetchone()
        if row is None:
            return None
        return _parse_poll_cursor_row(row, repo_full_name=repo_key)

    def upsert_poll_cursor(
        self,
        *,
        surface: GitHubCommentSurface,
        scope_number: int,
        last_updated_at: str,
        last_comment_id: int,
        bootstrap_complete: bool,
        repo_full_name: str | None = None,
    ) -> GitHubCommentPollCursorState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO github_comment_poll_cursors(
                    repo_full_name,
                    surface,
                    scope_number,
                    last_updated_at,
                    last_comment_id,
                    bootstrap_complete
                )
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, surface, scope_number) DO UPDATE SET
                    last_updated_at = excluded.last_updated_at,
                    last_comment_id = excluded.last_comment_id,
                    bootstrap_complete = excluded.bootstrap_complete,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    surface,
                    scope_number,
                    last_updated_at,
                    last_comment_id,
                    int(bootstrap_complete),
                ),
            )
            row = conn.execute(
                """
                SELECT
                    surface,
                    scope_number,
                    last_updated_at,
                    last_comment_id,
                    bootstrap_complete,
                    updated_at
                FROM github_comment_poll_cursors
                WHERE repo_full_name = ? AND surface = ? AND scope_number = ?
                """,
                (repo_key, surface, scope_number),
            ).fetchone()
        if row is None:
            raise RuntimeError("github_comment_poll_cursors row disappeared after upsert")
        return _parse_poll_cursor_row(row, repo_full_name=repo_key)

    def ingest_feedback_scan_batch(
        self,
        *,
        events: Iterable[FeedbackEventRecord],
        cursor_updates: Iterable[PollCursorUpdate],
        token_observations: Iterable[ActionTokenObservation] = (),
        repo_full_name: str | None = None,
    ) -> None:
        event_rows = tuple(events)
        cursor_rows = tuple(cursor_updates)
        observed_tokens = tuple(token_observations)
        if not event_rows and not cursor_rows and not observed_tokens:
            return

        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            if event_rows:
                conn.executemany(
                    """
                    INSERT INTO feedback_events(
                        repo_full_name,
                        event_key,
                        pr_number,
                        issue_number,
                        kind,
                        comment_id,
                        updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(repo_full_name, event_key) DO NOTHING
                    """,
                    [
                        (
                            repo_key,
                            event.event_key,
                            event.pr_number,
                            event.issue_number,
                            event.kind,
                            event.comment_id,
                            event.updated_at,
                        )
                        for event in event_rows
                    ],
                )
            if observed_tokens:
                conn.executemany(
                    """
                    INSERT INTO action_tokens(
                        repo_full_name,
                        token,
                        scope_kind,
                        scope_number,
                        source,
                        status,
                        observed_comment_id,
                        observed_updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, 'observed', ?, ?)
                    ON CONFLICT(repo_full_name, token) DO UPDATE SET
                        scope_kind = excluded.scope_kind,
                        scope_number = excluded.scope_number,
                        source = excluded.source,
                        status = 'observed',
                        observed_comment_id = excluded.observed_comment_id,
                        observed_updated_at = excluded.observed_updated_at,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    """,
                    [
                        (
                            repo_key,
                            observed.token,
                            observed.scope_kind,
                            observed.scope_number,
                            observed.source,
                            observed.comment_id,
                            observed.updated_at,
                        )
                        for observed in observed_tokens
                    ],
                )
            if cursor_rows:
                conn.executemany(
                    """
                    INSERT INTO github_comment_poll_cursors(
                        repo_full_name,
                        surface,
                        scope_number,
                        last_updated_at,
                        last_comment_id,
                        bootstrap_complete
                    )
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(repo_full_name, surface, scope_number) DO UPDATE SET
                        last_updated_at = excluded.last_updated_at,
                        last_comment_id = excluded.last_comment_id,
                        bootstrap_complete = excluded.bootstrap_complete,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    """,
                    [
                        (
                            repo_key,
                            update.surface,
                            update.scope_number,
                            update.last_updated_at,
                            update.last_comment_id,
                            int(update.bootstrap_complete),
                        )
                        for update in cursor_rows
                    ],
                )

    def seed_feedback_cursors_from_full_scan(
        self,
        *,
        pr_number: int,
        review_cursor: tuple[str, int],
        issue_cursor: tuple[str, int],
        repo_full_name: str | None = None,
    ) -> None:
        self.ingest_feedback_scan_batch(
            events=(),
            cursor_updates=(
                PollCursorUpdate(
                    surface="pr_review_comments",
                    scope_number=pr_number,
                    last_updated_at=review_cursor[0],
                    last_comment_id=review_cursor[1],
                    bootstrap_complete=True,
                ),
                PollCursorUpdate(
                    surface="pr_issue_comments",
                    scope_number=pr_number,
                    last_updated_at=issue_cursor[0],
                    last_comment_id=issue_cursor[1],
                    bootstrap_complete=True,
                ),
            ),
            repo_full_name=repo_full_name,
        )

    def get_action_token(
        self,
        token: str,
        *,
        repo_full_name: str | None = None,
    ) -> ActionTokenState | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    token,
                    scope_kind,
                    scope_number,
                    source,
                    status,
                    attempt_count,
                    observed_comment_id,
                    observed_updated_at,
                    created_at,
                    updated_at
                FROM action_tokens
                WHERE repo_full_name = ? AND token = ?
                """,
                (repo_key, token),
            ).fetchone()
        if row is None:
            return None
        return _parse_action_token_row(row, repo_full_name=repo_key)

    def record_action_token_planned(
        self,
        *,
        token: str,
        scope_kind: ActionTokenScopeKind,
        scope_number: int,
        source: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO action_tokens(
                    repo_full_name,
                    token,
                    scope_kind,
                    scope_number,
                    source,
                    status
                )
                VALUES(?, ?, ?, ?, ?, 'planned')
                ON CONFLICT(repo_full_name, token) DO UPDATE SET
                    scope_kind = excluded.scope_kind,
                    scope_number = excluded.scope_number,
                    source = excluded.source,
                    status = CASE
                        WHEN action_tokens.status = 'observed' THEN 'observed'
                        WHEN action_tokens.status = 'posted' THEN 'posted'
                        ELSE 'planned'
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, token, scope_kind, scope_number, source),
            )
            row = _select_action_token_row(conn=conn, repo_full_name=repo_key, token=token)
        if row is None:
            raise RuntimeError("action_tokens row disappeared after planned upsert")
        return _parse_action_token_row(row, repo_full_name=repo_key)

    def record_action_token_posted(
        self,
        *,
        token: str,
        scope_kind: ActionTokenScopeKind,
        scope_number: int,
        source: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO action_tokens(
                    repo_full_name,
                    token,
                    scope_kind,
                    scope_number,
                    source,
                    status,
                    attempt_count
                )
                VALUES(?, ?, ?, ?, ?, 'posted', 1)
                ON CONFLICT(repo_full_name, token) DO UPDATE SET
                    scope_kind = excluded.scope_kind,
                    scope_number = excluded.scope_number,
                    source = excluded.source,
                    status = CASE
                        WHEN action_tokens.status = 'observed' THEN 'observed'
                        ELSE 'posted'
                    END,
                    attempt_count = CASE
                        WHEN action_tokens.status = 'observed'
                        THEN action_tokens.attempt_count
                        ELSE action_tokens.attempt_count + 1
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, token, scope_kind, scope_number, source),
            )
            row = _select_action_token_row(conn=conn, repo_full_name=repo_key, token=token)
        if row is None:
            raise RuntimeError("action_tokens row disappeared after posted upsert")
        return _parse_action_token_row(row, repo_full_name=repo_key)

    def record_action_token_observed(
        self,
        *,
        token: str,
        scope_kind: ActionTokenScopeKind,
        scope_number: int,
        source: str,
        observed_comment_id: int,
        observed_updated_at: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO action_tokens(
                    repo_full_name,
                    token,
                    scope_kind,
                    scope_number,
                    source,
                    status,
                    observed_comment_id,
                    observed_updated_at
                )
                VALUES(?, ?, ?, ?, ?, 'observed', ?, ?)
                ON CONFLICT(repo_full_name, token) DO UPDATE SET
                    scope_kind = excluded.scope_kind,
                    scope_number = excluded.scope_number,
                    source = excluded.source,
                    status = 'observed',
                    observed_comment_id = excluded.observed_comment_id,
                    observed_updated_at = excluded.observed_updated_at,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    token,
                    scope_kind,
                    scope_number,
                    source,
                    observed_comment_id,
                    observed_updated_at,
                ),
            )
            row = _select_action_token_row(conn=conn, repo_full_name=repo_key, token=token)
        if row is None:
            raise RuntimeError("action_tokens row disappeared after observed upsert")
        return _parse_action_token_row(row, repo_full_name=repo_key)

    def save_agent_session(
        self,
        *,
        issue_number: int,
        adapter: str,
        thread_id: str | None,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_sessions(repo_full_name, issue_number, adapter, thread_id)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    adapter=excluded.adapter,
                    thread_id=excluded.thread_id,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, adapter, thread_id),
            )

    def get_agent_session(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> tuple[str, str | None] | None:
        with self._lock, self._connect() as conn:
            if repo_full_name is None:
                rows = conn.execute(
                    """
                    SELECT adapter, thread_id
                    FROM agent_sessions
                    WHERE issue_number = ?
                    ORDER BY repo_full_name ASC
                    """,
                    (issue_number,),
                ).fetchall()
                if not rows:
                    row = None
                else:
                    if len(rows) > 1:
                        raise RuntimeError(
                            f"Multiple agent session rows found for issue_number={issue_number}; "
                            "specify repo_full_name."
                        )
                    row = rows[0]
            else:
                repo_key = _normalize_repo_full_name(repo_full_name)
                row = conn.execute(
                    """
                    SELECT adapter, thread_id
                    FROM agent_sessions
                    WHERE repo_full_name = ? AND issue_number = ?
                    """,
                    (repo_key, issue_number),
                ).fetchone()
        if row is None:
            return None
        adapter, thread_id = row
        if not isinstance(adapter, str):
            raise RuntimeError("Invalid adapter value stored in agent_sessions")
        if thread_id is not None and not isinstance(thread_id, str):
            raise RuntimeError("Invalid thread_id value stored in agent_sessions")
        return adapter, thread_id

    def get_operator_command(
        self, command_key: str, *, repo_full_name: str | None = None
    ) -> OperatorCommandRecord | None:
        with self._lock, self._connect() as conn:
            row = _select_operator_command_row(
                conn=conn,
                command_key=command_key,
                repo_full_name=repo_full_name,
            )
        if row is None:
            return None
        return _parse_operator_command_row(row)

    def record_operator_command(
        self,
        *,
        command_key: str,
        issue_number: int,
        pr_number: int | None,
        comment_id: int,
        author_login: str,
        command: OperatorCommandName,
        args_json: str,
        status: OperatorCommandStatus,
        result: str,
        repo_full_name: str | None = None,
    ) -> OperatorCommandRecord:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO operator_commands(
                    repo_full_name,
                    command_key,
                    issue_number,
                    pr_number,
                    comment_id,
                    author_login,
                    command,
                    args_json,
                    status,
                    result
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, command_key) DO UPDATE SET
                    issue_number=excluded.issue_number,
                    pr_number=excluded.pr_number,
                    comment_id=excluded.comment_id,
                    author_login=excluded.author_login,
                    command=excluded.command,
                    args_json=excluded.args_json,
                    status=excluded.status,
                    result=excluded.result,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    command_key,
                    issue_number,
                    pr_number,
                    comment_id,
                    author_login,
                    command,
                    args_json,
                    status,
                    result,
                ),
            )
            row = conn.execute(
                """
                SELECT
                    repo_full_name,
                    command_key,
                    issue_number,
                    pr_number,
                    comment_id,
                    author_login,
                    command,
                    args_json,
                    status,
                    result,
                    created_at,
                    updated_at
                FROM operator_commands
                WHERE repo_full_name = ? AND command_key = ?
                """,
                (repo_key, command_key),
            ).fetchone()
        if row is None:
            raise RuntimeError("operator_commands row disappeared after upsert")
        return _parse_operator_command_row(row)

    def update_operator_command_result(
        self,
        *,
        command_key: str,
        status: OperatorCommandStatus,
        result: str,
        repo_full_name: str | None = None,
    ) -> OperatorCommandRecord | None:
        with self._lock, self._connect() as conn:
            target = _select_operator_command_row(
                conn=conn,
                command_key=command_key,
                repo_full_name=repo_full_name,
            )
            if target is None:
                return None
            parsed = _parse_operator_command_row(target)
            repo_key = _normalize_repo_full_name(parsed.repo_full_name)

            conn.execute(
                """
                UPDATE operator_commands
                SET status = ?,
                    result = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND command_key = ?
                """,
                (status, result, repo_key, command_key),
            )
            row = conn.execute(
                """
                SELECT
                    repo_full_name,
                    command_key,
                    issue_number,
                    pr_number,
                    comment_id,
                    author_login,
                    command,
                    args_json,
                    status,
                    result,
                    created_at,
                    updated_at
                FROM operator_commands
                WHERE repo_full_name = ? AND command_key = ?
                """,
                (repo_key, command_key),
            ).fetchone()
        if row is None:
            return None
        return _parse_operator_command_row(row)

    def get_runtime_operation(self, op_name: str) -> RuntimeOperationRecord | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
                    request_repo_full_name,
                    mode,
                    detail,
                    created_at,
                    updated_at
                FROM runtime_operations
                WHERE op_name = ?
                """,
                (op_name,),
            ).fetchone()
        if row is None:
            return None
        return _parse_runtime_operation_row(row)

    def request_runtime_restart(
        self,
        *,
        requested_by: str,
        request_command_key: str,
        mode: RestartMode,
        request_repo_full_name: str | None = None,
    ) -> tuple[RuntimeOperationRecord, bool]:
        request_repo_key = _normalize_repo_full_name(request_repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
                    request_repo_full_name,
                    mode,
                    detail,
                    created_at,
                    updated_at
                FROM runtime_operations
                WHERE op_name = 'restart'
                """
            ).fetchone()
            if row is not None:
                existing = _parse_runtime_operation_row(row)
                if existing.status in {"pending", "running"}:
                    return existing, False
                conn.execute(
                    """
                    UPDATE runtime_operations
                    SET status = 'pending',
                        requested_by = ?,
                        request_command_key = ?,
                        request_repo_full_name = ?,
                        mode = ?,
                        detail = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE op_name = 'restart'
                    """,
                    (requested_by, request_command_key, request_repo_key, mode),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO runtime_operations(
                        op_name,
                        status,
                        requested_by,
                        request_command_key,
                        request_repo_full_name,
                        mode,
                        detail
                    )
                    VALUES('restart', 'pending', ?, ?, ?, ?, NULL)
                    """,
                    (requested_by, request_command_key, request_repo_key, mode),
                )
            updated = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
                    request_repo_full_name,
                    mode,
                    detail,
                    created_at,
                    updated_at
                FROM runtime_operations
                WHERE op_name = 'restart'
                """
            ).fetchone()
        if updated is None:
            raise RuntimeError("runtime restart row disappeared after upsert")
        return _parse_runtime_operation_row(updated), True

    def set_runtime_operation_status(
        self,
        *,
        op_name: str,
        status: RuntimeOperationStatus,
        detail: str | None,
    ) -> RuntimeOperationRecord | None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE runtime_operations
                SET status = ?,
                    detail = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE op_name = ?
                """,
                (status, detail, op_name),
            )
            row = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
                    request_repo_full_name,
                    mode,
                    detail,
                    created_at,
                    updated_at
                FROM runtime_operations
                WHERE op_name = ?
                """,
                (op_name,),
            ).fetchone()
        if row is None:
            return None
        return _parse_runtime_operation_row(row)

    def list_tracked_pull_requests(
        self, *, repo_full_name: str | None = None
    ) -> tuple[TrackedPullRequestState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT repo_full_name, pr_number, issue_number, branch, status, last_seen_head_sha
                    FROM pr_feedback_state
                    WHERE status = 'awaiting_feedback'
                      AND active_run_id IS NULL
                    ORDER BY updated_at ASC, repo_full_name ASC, pr_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT repo_full_name, pr_number, issue_number, branch, status, last_seen_head_sha
                    FROM pr_feedback_state
                    WHERE status = 'awaiting_feedback'
                      AND active_run_id IS NULL
                      AND repo_full_name = ?
                    ORDER BY updated_at ASC, pr_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            TrackedPullRequestState(
                repo_full_name=str(row_repo_full_name),
                pr_number=int(pr_number),
                issue_number=int(issue_number),
                branch=str(branch),
                status=str(status),
                last_seen_head_sha=str(last_seen_head_sha)
                if isinstance(last_seen_head_sha, str)
                else None,
            )
            for row_repo_full_name, pr_number, issue_number, branch, status, last_seen_head_sha in rows
        )

    def list_blocked_pull_requests(
        self, *, repo_full_name: str | None = None
    ) -> tuple[BlockedPullRequestState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        p.repo_full_name,
                        p.pr_number,
                        p.issue_number,
                        p.branch,
                        p.last_seen_head_sha,
                        r.error,
                        p.updated_at,
                        COUNT(e.event_key) AS pending_event_count
                    FROM pr_feedback_state AS p
                    LEFT JOIN issue_runs AS r
                        ON r.repo_full_name = p.repo_full_name
                       AND r.issue_number = p.issue_number
                    LEFT JOIN feedback_events AS e
                        ON e.repo_full_name = p.repo_full_name
                       AND e.pr_number = p.pr_number
                       AND e.processed_at IS NULL
                    WHERE p.status = 'blocked'
                    GROUP BY
                        p.repo_full_name,
                        p.pr_number,
                        p.issue_number,
                        p.branch,
                        p.last_seen_head_sha,
                        r.error,
                        p.updated_at
                    ORDER BY p.updated_at ASC, p.repo_full_name ASC, p.pr_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        p.repo_full_name,
                        p.pr_number,
                        p.issue_number,
                        p.branch,
                        p.last_seen_head_sha,
                        r.error,
                        p.updated_at,
                        COUNT(e.event_key) AS pending_event_count
                    FROM pr_feedback_state AS p
                    LEFT JOIN issue_runs AS r
                        ON r.repo_full_name = p.repo_full_name
                       AND r.issue_number = p.issue_number
                    LEFT JOIN feedback_events AS e
                        ON e.repo_full_name = p.repo_full_name
                       AND e.pr_number = p.pr_number
                       AND e.processed_at IS NULL
                    WHERE p.status = 'blocked'
                      AND p.repo_full_name = ?
                    GROUP BY
                        p.repo_full_name,
                        p.pr_number,
                        p.issue_number,
                        p.branch,
                        p.last_seen_head_sha,
                        r.error,
                        p.updated_at
                    ORDER BY p.updated_at ASC, p.pr_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            BlockedPullRequestState(
                repo_full_name=str(row_repo_full_name),
                pr_number=int(pr_number),
                issue_number=int(issue_number),
                branch=str(branch),
                last_seen_head_sha=str(last_seen_head_sha)
                if isinstance(last_seen_head_sha, str)
                else None,
                error=str(error) if isinstance(error, str) else None,
                updated_at=str(updated_at),
                pending_event_count=int(pending_event_count),
            )
            for (
                row_repo_full_name,
                pr_number,
                issue_number,
                branch,
                last_seen_head_sha,
                error,
                updated_at,
                pending_event_count,
            ) in rows
        )

    def list_implementation_candidates(
        self, *, repo_full_name: str | None = None
    ) -> tuple[ImplementationCandidateState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            query = """
                SELECT
                    i.repo_full_name,
                    i.issue_number,
                    COALESCE(
                        CASE WHEN i.branch LIKE 'agent/design/%' THEN i.branch END,
                        (
                            SELECT p.branch
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                              AND p.status = 'merged'
                              AND p.branch LIKE 'agent/design/%'
                            ORDER BY p.updated_at DESC, p.pr_number DESC
                            LIMIT 1
                        )
                    ) AS design_branch,
                    COALESCE(
                        CASE
                            WHEN i.pr_number IS NOT NULL AND i.branch LIKE 'agent/design/%'
                            THEN i.pr_number
                        END,
                        (
                            SELECT p.pr_number
                            FROM pr_feedback_state AS p
                            WHERE p.repo_full_name = i.repo_full_name
                              AND p.issue_number = i.issue_number
                              AND p.status = 'merged'
                              AND p.branch LIKE 'agent/design/%'
                            ORDER BY p.updated_at DESC, p.pr_number DESC
                            LIMIT 1
                        ),
                        i.pr_number
                    ) AS design_pr_number,
                    i.pr_url
                FROM issue_runs AS i
                WHERE (
                        (i.status = 'merged' AND i.branch LIKE 'agent/design/%')
                        OR (
                            i.status = 'failed'
                            AND i.active_run_id IS NULL
                            AND i.retry_count > 0
                            AND i.retry_count < ?
                            AND (
                                  i.last_failure_class IN ('github_error', 'unknown')
                                  OR i.error = ?
                            )
                            AND (
                                  i.next_retry_at IS NULL
                                  OR i.next_retry_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                            )
                            AND EXISTS(
                                SELECT 1
                                FROM pr_feedback_state AS p
                                WHERE p.repo_full_name = i.repo_full_name
                                  AND p.issue_number = i.issue_number
                                  AND p.status = 'merged'
                                  AND p.branch LIKE 'agent/design/%'
                            )
                        )
                    )
            """
            if repo_key is None:
                rows = conn.execute(
                    query + " ORDER BY i.updated_at ASC, i.repo_full_name ASC, i.issue_number ASC",
                    (
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                        _STALE_RUNNING_RETRY_ERROR,
                    ),
                ).fetchall()
            else:
                rows = conn.execute(
                    query
                    + " AND i.repo_full_name = ? ORDER BY i.updated_at ASC, i.issue_number ASC",
                    (
                        _TRANSIENT_RETRY_MAX_ATTEMPTS,
                        _STALE_RUNNING_RETRY_ERROR,
                        repo_key,
                    ),
                ).fetchall()
        return tuple(
            ImplementationCandidateState(
                repo_full_name=str(row_repo_full_name),
                issue_number=int(issue_number),
                design_branch=str(branch),
                design_pr_number=int(pr_number) if isinstance(pr_number, int) else None,
                design_pr_url=str(pr_url) if isinstance(pr_url, str) else None,
            )
            for row_repo_full_name, issue_number, branch, pr_number, pr_url in rows
        )

    def reset_blocked_pull_requests(
        self,
        *,
        pr_numbers: tuple[int, ...] | None = None,
        last_seen_head_sha_override: str | None = None,
        repo_full_name: str | None = None,
    ) -> int:
        if pr_numbers is not None and not pr_numbers:
            return 0

        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            where_clauses = ["status = 'blocked'"]
            params: list[object] = []
            if repo_key is not None:
                where_clauses.append("repo_full_name = ?")
                params.append(repo_key)
            if pr_numbers is not None:
                placeholders = ",".join("?" for _ in pr_numbers)
                where_clauses.append(f"pr_number IN ({placeholders})")
                params.extend(pr_numbers)

            rows = conn.execute(
                f"""
                SELECT repo_full_name, pr_number, issue_number
                FROM pr_feedback_state
                WHERE {" AND ".join(where_clauses)}
                """,
                tuple(params),
            ).fetchall()
            if not rows:
                return 0

            blocked_keys = tuple((str(row[0]), int(row[1])) for row in rows)
            issue_keys = tuple(sorted({(str(row[0]), int(row[2])) for row in rows}))

            if last_seen_head_sha_override is None:
                conn.executemany(
                    """
                    UPDATE pr_feedback_state
                    SET status = 'awaiting_feedback',
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ? AND pr_number = ?
                    """,
                    blocked_keys,
                )
            else:
                conn.executemany(
                    """
                    UPDATE pr_feedback_state
                    SET status = 'awaiting_feedback',
                        last_seen_head_sha = ?,
                        active_run_id = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ? AND pr_number = ?
                    """,
                    [
                        (last_seen_head_sha_override, key_repo_full_name, pr_number)
                        for key_repo_full_name, pr_number in blocked_keys
                    ],
                )

            conn.executemany(
                """
                INSERT INTO issue_runs(repo_full_name, issue_number, status, error)
                VALUES(?, ?, 'awaiting_feedback', NULL)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='awaiting_feedback',
                    error=NULL,
                    active_run_id=NULL,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                issue_keys,
            )
            for row_repo_full_name, row_pr_number, row_issue_number in rows:
                _append_pr_status_history(
                    conn=conn,
                    repo_full_name=str(row_repo_full_name),
                    pr_number=int(row_pr_number),
                    issue_number=int(row_issue_number),
                    from_status="blocked",
                    to_status="awaiting_feedback",
                    reason="manual_unblock",
                    detail=(
                        f"last_seen_head_sha_override={last_seen_head_sha_override}"
                        if last_seen_head_sha_override is not None
                        else None
                    ),
                )

        return len(blocked_keys)

    def mark_pr_status(
        self,
        *,
        pr_number: int,
        issue_number: int,
        status: str,
        last_seen_head_sha: str | None = None,
        error: str | None = None,
        reason: str | None = None,
        detail: str | None = None,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            previous_status = _select_pr_feedback_status(
                conn=conn,
                repo_full_name=repo_key,
                pr_number=pr_number,
            )
            conn.execute(
                """
                UPDATE pr_feedback_state
                SET status = ?,
                    last_seen_head_sha = COALESCE(?, last_seen_head_sha),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (status, last_seen_head_sha, repo_key, pr_number),
            )
            conn.execute(
                """
                INSERT INTO issue_runs(repo_full_name, issue_number, status, error)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status=excluded.status,
                    error=excluded.error,
                    active_run_id=NULL,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, status, error),
            )
            _append_pr_status_history(
                conn=conn,
                repo_full_name=repo_key,
                pr_number=pr_number,
                issue_number=issue_number,
                from_status=previous_status,
                to_status=status,
                reason=reason,
                detail=detail if detail is not None else error,
            )

    def ingest_feedback_events(
        self,
        events: Iterable[FeedbackEventRecord],
        *,
        repo_full_name: str | None = None,
    ) -> None:
        rows = tuple(events)
        if not rows:
            return
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO feedback_events(
                    repo_full_name,
                    event_key,
                    pr_number,
                    issue_number,
                    kind,
                    comment_id,
                    updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, event_key) DO NOTHING
                """,
                [
                    (
                        repo_key,
                        event.event_key,
                        event.pr_number,
                        event.issue_number,
                        event.kind,
                        event.comment_id,
                        event.updated_at,
                    )
                    for event in rows
                ],
            )

    def list_pending_feedback_events(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> tuple[PendingFeedbackEvent, ...]:
        with self._lock, self._connect() as conn:
            if repo_full_name is None:
                rows = conn.execute(
                    """
                    SELECT event_key, kind, comment_id, updated_at
                    FROM feedback_events
                    WHERE pr_number = ?
                      AND processed_at IS NULL
                    ORDER BY created_at ASC, event_key ASC
                    """,
                    (pr_number,),
                ).fetchall()
            else:
                repo_key = _normalize_repo_full_name(repo_full_name)
                rows = conn.execute(
                    """
                    SELECT event_key, kind, comment_id, updated_at
                    FROM feedback_events
                    WHERE repo_full_name = ?
                      AND pr_number = ?
                      AND processed_at IS NULL
                    ORDER BY created_at ASC, event_key ASC
                    """,
                    (repo_key, pr_number),
                ).fetchall()
        return tuple(
            PendingFeedbackEvent(
                event_key=str(event_key),
                kind=str(kind),
                comment_id=int(comment_id),
                updated_at=str(updated_at),
            )
            for event_key, kind, comment_id, updated_at in rows
        )

    def mark_feedback_events_processed(
        self,
        *,
        event_keys: tuple[str, ...],
        repo_full_name: str | None = None,
    ) -> None:
        if not event_keys:
            return
        placeholders = ",".join("?" for _ in event_keys)
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                f"""
                UPDATE feedback_events
                SET processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND event_key IN ({placeholders})
                  AND processed_at IS NULL
                """,
                (repo_key, *event_keys),
            )

    def finalize_feedback_turn(
        self,
        *,
        pr_number: int,
        issue_number: int,
        processed_event_keys: tuple[str, ...],
        session: AgentSession,
        head_sha: str,
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            previous_status = _select_pr_feedback_status(
                conn=conn,
                repo_full_name=repo_key,
                pr_number=pr_number,
            )
            if processed_event_keys:
                placeholders = ",".join("?" for _ in processed_event_keys)
                conn.execute(
                    f"""
                    UPDATE feedback_events
                    SET processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND event_key IN ({placeholders})
                    """,
                    (repo_key, *processed_event_keys),
                )

            conn.execute(
                """
                INSERT INTO agent_sessions(repo_full_name, issue_number, adapter, thread_id)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    adapter=excluded.adapter,
                    thread_id=excluded.thread_id,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, session.adapter, session.thread_id),
            )

            conn.execute(
                """
                UPDATE pr_feedback_state
                SET status='awaiting_feedback',
                    last_seen_head_sha=?,
                    active_run_id=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name=? AND pr_number=?
                """,
                (head_sha, repo_key, pr_number),
            )

            conn.execute(
                """
                INSERT INTO issue_runs(repo_full_name, issue_number, status)
                VALUES(?, ?, 'awaiting_feedback')
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='awaiting_feedback',
                    error=NULL,
                    active_run_id=NULL,
                    last_failure_class=NULL,
                    retry_count=0,
                    next_retry_at=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number),
            )
            _append_pr_status_history(
                conn=conn,
                repo_full_name=repo_key,
                pr_number=pr_number,
                issue_number=issue_number,
                from_status=previous_status,
                to_status="awaiting_feedback",
                reason=None,
                detail=None,
            )


def _select_pr_feedback_status(
    *, conn: sqlite3.Connection, repo_full_name: str, pr_number: int
) -> str | None:
    row = conn.execute(
        """
        SELECT status
        FROM pr_feedback_state
        WHERE repo_full_name = ? AND pr_number = ?
        """,
        (repo_full_name, pr_number),
    ).fetchone()
    if row is None:
        return None
    return str(row[0])


def _append_pr_status_history(
    *,
    conn: sqlite3.Connection,
    repo_full_name: str,
    pr_number: int,
    issue_number: int,
    from_status: str | None,
    to_status: str,
    reason: str | None,
    detail: str | None,
) -> None:
    if from_status == to_status and reason is None and detail is None:
        return
    conn.execute(
        """
        INSERT INTO pr_status_history(
            repo_full_name,
            pr_number,
            issue_number,
            from_status,
            to_status,
            reason,
            detail
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            repo_full_name,
            pr_number,
            issue_number,
            from_status,
            to_status,
            reason,
            detail,
        ),
    )


def _select_operator_command_row(
    *, conn: sqlite3.Connection, command_key: str, repo_full_name: str | None
) -> tuple[object, ...] | None:
    if repo_full_name is not None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        return conn.execute(
            """
            SELECT
                repo_full_name,
                command_key,
                issue_number,
                pr_number,
                comment_id,
                author_login,
                command,
                args_json,
                status,
                result,
                created_at,
                updated_at
            FROM operator_commands
            WHERE repo_full_name = ? AND command_key = ?
            """,
            (repo_key, command_key),
        ).fetchone()

    rows = conn.execute(
        """
        SELECT
            repo_full_name,
            command_key,
            issue_number,
            pr_number,
            comment_id,
            author_login,
            command,
            args_json,
            status,
            result,
            created_at,
            updated_at
        FROM operator_commands
        WHERE command_key = ?
        ORDER BY repo_full_name ASC
        """,
        (command_key,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise RuntimeError(
            f"Multiple operator commands found for command_key={command_key!r}; "
            "specify repo_full_name."
        )
    return rows[0]


def _select_action_token_row(
    *, conn: sqlite3.Connection, repo_full_name: str, token: str
) -> tuple[object, ...] | None:
    return conn.execute(
        """
        SELECT
            token,
            scope_kind,
            scope_number,
            source,
            status,
            attempt_count,
            observed_comment_id,
            observed_updated_at,
            created_at,
            updated_at
        FROM action_tokens
        WHERE repo_full_name = ? AND token = ?
        """,
        (repo_full_name, token),
    ).fetchone()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> tuple[str, ...]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: list[str] = []
    for row in rows:
        if len(row) < 2:
            continue
        name = row[1]
        if isinstance(name, str):
            columns.append(name)
    return tuple(columns)


def _normalize_repo_full_name(repo_full_name: str | None) -> str:
    if repo_full_name is None:
        return _DEFAULT_REPO_FULL_NAME
    normalized = repo_full_name.strip()
    if not normalized:
        return _DEFAULT_REPO_FULL_NAME
    return normalized


def _transient_retry_delay_seconds(retry_count: int) -> int:
    if retry_count < 1:
        raise ValueError("retry_count must be >= 1")
    delay = _TRANSIENT_RETRY_INITIAL_DELAY_SECONDS * (2 ** (retry_count - 1))
    return min(delay, _TRANSIENT_RETRY_MAX_DELAY_SECONDS)


def _parse_poll_cursor_row(
    row: tuple[object, ...], *, repo_full_name: str
) -> GitHubCommentPollCursorState:
    if len(row) != 6:
        raise RuntimeError("Invalid github_comment_poll_cursors row width")
    surface, scope_number, last_updated_at, last_comment_id, bootstrap_complete, updated_at = row
    if not isinstance(surface, str):
        raise RuntimeError("Invalid surface value stored in github_comment_poll_cursors")
    if not isinstance(scope_number, int):
        raise RuntimeError("Invalid scope_number value stored in github_comment_poll_cursors")
    if not isinstance(last_updated_at, str):
        raise RuntimeError("Invalid last_updated_at value stored in github_comment_poll_cursors")
    if not isinstance(last_comment_id, int):
        raise RuntimeError("Invalid last_comment_id value stored in github_comment_poll_cursors")
    if not isinstance(bootstrap_complete, int):
        raise RuntimeError("Invalid bootstrap_complete value stored in github_comment_poll_cursors")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in github_comment_poll_cursors")
    return GitHubCommentPollCursorState(
        repo_full_name=repo_full_name,
        surface=_parse_github_comment_surface(surface),
        scope_number=scope_number,
        last_updated_at=last_updated_at,
        last_comment_id=last_comment_id,
        bootstrap_complete=bootstrap_complete != 0,
        updated_at=updated_at,
    )


def _parse_action_token_row(row: tuple[object, ...], *, repo_full_name: str) -> ActionTokenState:
    if len(row) != 10:
        raise RuntimeError("Invalid action_tokens row width")
    (
        token,
        scope_kind,
        scope_number,
        source,
        status,
        attempt_count,
        observed_comment_id,
        observed_updated_at,
        created_at,
        updated_at,
    ) = row
    if not isinstance(token, str):
        raise RuntimeError("Invalid token value stored in action_tokens")
    if not isinstance(scope_kind, str):
        raise RuntimeError("Invalid scope_kind value stored in action_tokens")
    if not isinstance(scope_number, int):
        raise RuntimeError("Invalid scope_number value stored in action_tokens")
    if not isinstance(source, str):
        raise RuntimeError("Invalid source value stored in action_tokens")
    if not isinstance(status, str):
        raise RuntimeError("Invalid status value stored in action_tokens")
    if not isinstance(attempt_count, int):
        raise RuntimeError("Invalid attempt_count value stored in action_tokens")
    if observed_comment_id is not None and not isinstance(observed_comment_id, int):
        raise RuntimeError("Invalid observed_comment_id value stored in action_tokens")
    if observed_updated_at is not None and not isinstance(observed_updated_at, str):
        raise RuntimeError("Invalid observed_updated_at value stored in action_tokens")
    if not isinstance(created_at, str):
        raise RuntimeError("Invalid created_at value stored in action_tokens")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in action_tokens")
    return ActionTokenState(
        repo_full_name=repo_full_name,
        token=token,
        scope_kind=_parse_action_token_scope_kind(scope_kind),
        scope_number=scope_number,
        source=source,
        status=_parse_action_token_status(status),
        attempt_count=attempt_count,
        observed_comment_id=observed_comment_id,
        observed_updated_at=observed_updated_at,
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse_operator_command_row(row: tuple[object, ...]) -> OperatorCommandRecord:
    if len(row) == 11:
        (
            command_key,
            issue_number,
            pr_number,
            comment_id,
            author_login,
            command,
            args_json,
            status,
            result,
            created_at,
            updated_at,
        ) = row
        repo_full_name = _DEFAULT_REPO_FULL_NAME
    elif len(row) == 12:
        (
            repo_full_name,
            command_key,
            issue_number,
            pr_number,
            comment_id,
            author_login,
            command,
            args_json,
            status,
            result,
            created_at,
            updated_at,
        ) = row
    else:
        raise RuntimeError("Invalid operator_commands row width")

    if not isinstance(repo_full_name, str):
        raise RuntimeError("Invalid repo_full_name value stored in operator_commands")
    if not isinstance(command_key, str):
        raise RuntimeError("Invalid command_key value stored in operator_commands")
    if not isinstance(issue_number, int):
        raise RuntimeError("Invalid issue_number value stored in operator_commands")
    if pr_number is not None and not isinstance(pr_number, int):
        raise RuntimeError("Invalid pr_number value stored in operator_commands")
    if not isinstance(comment_id, int):
        raise RuntimeError("Invalid comment_id value stored in operator_commands")
    if not isinstance(author_login, str):
        raise RuntimeError("Invalid author_login value stored in operator_commands")
    if not isinstance(args_json, str):
        raise RuntimeError("Invalid args_json value stored in operator_commands")
    if not isinstance(result, str):
        raise RuntimeError("Invalid result value stored in operator_commands")
    if not isinstance(created_at, str):
        raise RuntimeError("Invalid created_at value stored in operator_commands")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in operator_commands")

    return OperatorCommandRecord(
        command_key=command_key,
        issue_number=issue_number,
        pr_number=pr_number,
        comment_id=comment_id,
        author_login=author_login,
        command=_parse_operator_command_name(command),
        args_json=args_json,
        status=_parse_operator_command_status(status),
        result=result,
        created_at=created_at,
        updated_at=updated_at,
        repo_full_name=repo_full_name,
    )


def _parse_runtime_operation_row(row: tuple[object, ...]) -> RuntimeOperationRecord:
    if len(row) == 8:
        (
            op_name,
            status,
            requested_by,
            request_command_key,
            mode,
            detail,
            created_at,
            updated_at,
        ) = row
        request_repo_full_name = _DEFAULT_REPO_FULL_NAME
    elif len(row) == 9:
        (
            op_name,
            status,
            requested_by,
            request_command_key,
            request_repo_full_name,
            mode,
            detail,
            created_at,
            updated_at,
        ) = row
    else:
        raise RuntimeError("Invalid runtime_operations row width")

    if not isinstance(op_name, str):
        raise RuntimeError("Invalid op_name value stored in runtime_operations")
    if not isinstance(requested_by, str):
        raise RuntimeError("Invalid requested_by value stored in runtime_operations")
    if not isinstance(request_command_key, str):
        raise RuntimeError("Invalid request_command_key value stored in runtime_operations")
    if not isinstance(request_repo_full_name, str):
        raise RuntimeError("Invalid request_repo_full_name value stored in runtime_operations")
    if detail is not None and not isinstance(detail, str):
        raise RuntimeError("Invalid detail value stored in runtime_operations")
    if not isinstance(created_at, str):
        raise RuntimeError("Invalid created_at value stored in runtime_operations")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in runtime_operations")

    return RuntimeOperationRecord(
        op_name=op_name,
        status=_parse_runtime_operation_status(status),
        requested_by=requested_by,
        request_command_key=request_command_key,
        mode=_parse_restart_mode(mode),
        detail=detail,
        created_at=created_at,
        updated_at=updated_at,
        request_repo_full_name=request_repo_full_name,
    )


def _parse_operator_command_name(value: object) -> OperatorCommandName:
    if not isinstance(value, str):
        raise RuntimeError("Invalid command value stored in operator_commands")
    if value not in {"unblock", "restart", "help", "invalid"}:
        raise RuntimeError(f"Unknown command value stored in operator_commands: {value}")
    return cast(OperatorCommandName, value)


def _parse_github_call_kind(value: object) -> GitHubCallKind:
    if not isinstance(value, str):
        raise RuntimeError("Invalid call_kind value stored in github_call_outbox")
    if value not in {"create_pull_request"}:
        raise RuntimeError(f"Unknown call_kind value stored in github_call_outbox: {value}")
    return cast(GitHubCallKind, value)


def _parse_github_call_status(value: object) -> GitHubCallStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in github_call_outbox")
    if value not in {"pending", "in_progress", "succeeded"}:
        raise RuntimeError(f"Unknown status value stored in github_call_outbox: {value}")
    return cast(GitHubCallStatus, value)


def _parse_github_call_outbox_row(row: object) -> GitHubCallOutboxState:
    if not isinstance(row, tuple) or len(row) != 16:
        raise RuntimeError("Invalid github_call_outbox row width")
    (
        repo_full_name,
        call_id,
        call_kind,
        dedupe_key,
        payload_json,
        status,
        state_applied,
        attempt_count,
        last_error,
        result_json,
        run_id,
        issue_number,
        branch,
        pr_number,
        created_at,
        updated_at,
    ) = row
    if not isinstance(repo_full_name, str):
        raise RuntimeError("Invalid repo_full_name value stored in github_call_outbox")
    if not isinstance(call_id, int):
        raise RuntimeError("Invalid call_id value stored in github_call_outbox")
    if not isinstance(dedupe_key, str):
        raise RuntimeError("Invalid dedupe_key value stored in github_call_outbox")
    if not isinstance(payload_json, str):
        raise RuntimeError("Invalid payload_json value stored in github_call_outbox")
    if not isinstance(state_applied, int):
        raise RuntimeError("Invalid state_applied value stored in github_call_outbox")
    if not isinstance(attempt_count, int):
        raise RuntimeError("Invalid attempt_count value stored in github_call_outbox")
    if last_error is not None and not isinstance(last_error, str):
        raise RuntimeError("Invalid last_error value stored in github_call_outbox")
    if result_json is not None and not isinstance(result_json, str):
        raise RuntimeError("Invalid result_json value stored in github_call_outbox")
    if run_id is not None and not isinstance(run_id, str):
        raise RuntimeError("Invalid run_id value stored in github_call_outbox")
    if issue_number is not None and not isinstance(issue_number, int):
        raise RuntimeError("Invalid issue_number value stored in github_call_outbox")
    if branch is not None and not isinstance(branch, str):
        raise RuntimeError("Invalid branch value stored in github_call_outbox")
    if pr_number is not None and not isinstance(pr_number, int):
        raise RuntimeError("Invalid pr_number value stored in github_call_outbox")
    if not isinstance(created_at, str):
        raise RuntimeError("Invalid created_at value stored in github_call_outbox")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in github_call_outbox")
    return GitHubCallOutboxState(
        call_id=call_id,
        call_kind=_parse_github_call_kind(call_kind),
        dedupe_key=dedupe_key,
        payload_json=payload_json,
        status=_parse_github_call_status(status),
        state_applied=state_applied != 0,
        attempt_count=attempt_count,
        last_error=last_error,
        result_json=result_json,
        run_id=run_id,
        issue_number=issue_number,
        branch=branch,
        pr_number=pr_number,
        created_at=created_at,
        updated_at=updated_at,
        repo_full_name=repo_full_name,
    )


def _parse_operator_command_status(value: object) -> OperatorCommandStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in operator_commands")
    if value not in {"applied", "rejected", "failed"}:
        raise RuntimeError(f"Unknown status value stored in operator_commands: {value}")
    return cast(OperatorCommandStatus, value)


def _parse_runtime_operation_status(value: object) -> RuntimeOperationStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in runtime_operations")
    if value not in {"pending", "running", "failed", "completed"}:
        raise RuntimeError(f"Unknown status value stored in runtime_operations: {value}")
    return cast(RuntimeOperationStatus, value)


def _parse_restart_mode(value: object) -> RestartMode:
    if not isinstance(value, str):
        raise RuntimeError("Invalid mode value stored in runtime_operations")
    if value not in {"git_checkout", "pypi"}:
        raise RuntimeError(f"Unknown mode value stored in runtime_operations: {value}")
    return cast(RestartMode, value)


def _parse_pre_pr_followup_flow(value: object) -> PrePrFollowupFlow:
    if not isinstance(value, str):
        raise RuntimeError("Invalid flow value stored in pre_pr_followup_state")
    if value not in {"design_doc", "bugfix", "small_job", "implementation"}:
        raise RuntimeError(f"Unknown flow value stored in pre_pr_followup_state: {value}")
    return cast(PrePrFollowupFlow, value)


def _parse_github_comment_surface(value: object) -> GitHubCommentSurface:
    if not isinstance(value, str):
        raise RuntimeError("Invalid surface value stored in github_comment_poll_cursors")
    if value not in {
        "pr_review_comments",
        "pr_issue_comments",
        "issue_pre_pr_followups",
        "issue_post_pr_redirects",
        "issue_operator_commands",
    }:
        raise RuntimeError(f"Unknown surface value stored in github_comment_poll_cursors: {value}")
    return cast(GitHubCommentSurface, value)


def _parse_action_token_scope_kind(value: object) -> ActionTokenScopeKind:
    if not isinstance(value, str):
        raise RuntimeError("Invalid scope_kind value stored in action_tokens")
    if value not in {"pr", "issue"}:
        raise RuntimeError(f"Unknown scope_kind value stored in action_tokens: {value}")
    return cast(ActionTokenScopeKind, value)


def _parse_action_token_status(value: object) -> ActionTokenStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in action_tokens")
    if value not in {"planned", "posted", "observed", "failed"}:
        raise RuntimeError(f"Unknown status value stored in action_tokens: {value}")
    return cast(ActionTokenStatus, value)
