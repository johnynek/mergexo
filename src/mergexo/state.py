from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
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
    RoadmapDependency,
    RoadmapNodeKind,
    RoadmapNode,
    RoadmapDependencyRequirement,
    RestartMode,
    RuntimeOperationRecord,
    RuntimeOperationStatus,
)
from mergexo.roadmap_parser import RoadmapGraph
from mergexo.roadmap_transition_validator import (
    ExistingRoadmapNodeState,
    RoadmapGraphTransitionError,
    validate_roadmap_graph_transition,
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
_ROADMAP_ADJUSTMENT_STALE_AFTER = timedelta(minutes=30)
_ROADMAP_NODE_CLAIM_STALE_AFTER = timedelta(minutes=30)
PrePrFollowupFlow = Literal["design_doc", "bugfix", "small_job", "roadmap", "implementation"]
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
    "timeout",
    "unknown",
]
GitHubCommentSurface = Literal[
    "pr_review_comments",
    "pr_issue_comments",
    "pr_review_summaries",
    "issue_pre_pr_followups",
    "issue_post_pr_redirects",
    "issue_operator_commands",
]
ActionTokenScopeKind = Literal["pr", "issue"]
ActionTokenStatus = Literal["planned", "posted", "observed", "failed"]
GitHubCallKind = Literal["create_pull_request", "create_issue", "post_issue_comment"]
GitHubCallStatus = Literal["pending", "in_progress", "succeeded"]
PrFlakeStatus = Literal[
    "awaiting_rerun_result",
    "resolved_after_rerun",
    "blocked_after_second_failure",
]
RoadmapStatus = Literal["active", "superseded", "abandoned", "completed"]
RoadmapNodeStatus = Literal["pending", "issued", "completed", "blocked", "abandoned"]
RoadmapAdjustmentState = Literal["idle", "evaluating", "awaiting_revision_merge"]


def _format_db_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass(frozen=True)
class RoadmapDependencyState:
    node_id: str
    requires: RoadmapDependencyRequirement


@dataclass(frozen=True)
class RoadmapNodeGraphInput:
    node_id: str
    kind: RoadmapNodeKind
    title: str
    body_markdown: str
    dependencies: tuple[RoadmapDependencyState, ...]


@dataclass(frozen=True)
class RoadmapStateRecord:
    repo_full_name: str
    roadmap_issue_number: int
    roadmap_pr_number: int | None
    roadmap_doc_path: str
    graph_path: str
    graph_checksum: str
    graph_version: int
    status: RoadmapStatus
    adjustment_state: RoadmapAdjustmentState
    adjustment_claim_token: str | None
    adjustment_started_at: str | None
    adjustment_request_version: int | None
    pending_revision_pr_number: int | None
    pending_revision_pr_url: str | None
    pending_revision_head_sha: str | None
    last_adjustment_basis_digest: str | None
    parent_roadmap_issue_number: int | None
    superseding_roadmap_issue_number: int | None
    revision_requested_at: str | None
    last_error: str | None
    updated_at: str


@dataclass(frozen=True)
class RoadmapNodeRecord:
    repo_full_name: str
    roadmap_issue_number: int
    node_id: str
    kind: RoadmapNodeKind
    title: str
    body_markdown: str
    dependencies_json: str
    introduced_in_version: int
    retired_in_version: int | None
    is_active: bool
    child_issue_number: int | None
    child_issue_url: str | None
    status: RoadmapNodeStatus
    planned_at: str | None
    implemented_at: str | None
    status_changed_at: str | None
    last_progress_at: str | None
    blocked_since_at: str | None
    claim_token: str | None
    updated_at: str


@dataclass(frozen=True)
class RoadmapActivationCandidate:
    repo_full_name: str
    roadmap_issue_number: int
    branch: str
    roadmap_pr_number: int | None
    roadmap_pr_url: str | None


@dataclass(frozen=True)
class RoadmapRevisionRecord:
    repo_full_name: str
    roadmap_issue_number: int
    version: int
    roadmap_pr_number: int | None
    roadmap_doc_path: str
    graph_path: str
    graph_checksum: str
    applied_at: str


@dataclass(frozen=True)
class RoadmapRevisionDraftRecord:
    repo_full_name: str
    roadmap_issue_number: int
    request_version: int
    summary: str
    details: str
    updated_roadmap_markdown: str
    updated_canonical_graph_json: str
    source_kind: str
    ready_node_ids_json: str | None
    request_reason: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ReadyRoadmapNodeClaim:
    repo_full_name: str
    roadmap_issue_number: int
    node_id: str
    kind: RoadmapNodeKind
    title: str
    body_markdown: str
    dependencies_json: str
    claim_token: str


@dataclass(frozen=True)
class RoadmapChildIssueLookup:
    repo_full_name: str
    roadmap_issue_number: int
    node_id: str
    roadmap_status: RoadmapStatus


@dataclass(frozen=True)
class RoadmapStatusSnapshotRow:
    repo_full_name: str
    roadmap_issue_number: int
    node_id: str
    kind: RoadmapNodeKind
    status: RoadmapNodeStatus
    dependency_summary: str
    child_issue_number: int | None
    child_issue_url: str | None
    last_progress_at: str | None
    blocked_since_at: str | None


@dataclass(frozen=True)
class RoadmapBlockerRow:
    repo_full_name: str
    roadmap_issue_number: int
    node_id: str
    status: RoadmapNodeStatus
    blocked_since_at: str
    child_issue_number: int | None
    child_issue_url: str | None
    last_progress_at: str | None


@dataclass(frozen=True)
class IssueRunRecord:
    repo_full_name: str
    issue_number: int
    status: str
    branch: str | None
    pr_number: int | None
    pr_url: str | None
    error: str | None


@dataclass(frozen=True)
class TrackedPullRequestState:
    pr_number: int
    issue_number: int
    branch: str
    status: str
    last_seen_head_sha: str | None
    repo_full_name: str = ""


@dataclass(frozen=True)
class UnfinishedAgentRunState:
    run_id: str
    repo_full_name: str
    run_kind: AgentRunKind
    issue_number: int
    pr_number: int | None
    flow: str | None
    branch: str | None
    started_at: str
    meta_json: str


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
class FailedIssueRunState:
    issue_number: int
    branch: str | None
    error: str | None
    updated_at: str
    repo_full_name: str = ""


LegacyFailedIssueRunState = FailedIssueRunState


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


@dataclass(frozen=True)
class PrFlakeState:
    repo_full_name: str
    pr_number: int
    issue_number: int
    head_sha: str
    run_id: int
    initial_run_updated_at: str
    status: PrFlakeStatus
    flake_issue_number: int
    flake_issue_url: str
    report_title: str
    report_summary: str
    report_excerpt: str
    full_log_context_markdown: str
    rerun_requested_at: str | None
    created_at: str
    updated_at: str


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
                CREATE TABLE IF NOT EXISTS pr_flake_state (
                    repo_full_name TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    issue_number INTEGER NOT NULL,
                    head_sha TEXT NOT NULL,
                    run_id INTEGER NOT NULL,
                    initial_run_updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    flake_issue_number INTEGER NOT NULL,
                    flake_issue_url TEXT NOT NULL,
                    report_title TEXT NOT NULL,
                    report_summary TEXT NOT NULL,
                    report_excerpt TEXT NOT NULL,
                    full_log_context_markdown TEXT NOT NULL,
                    rerun_requested_at TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, pr_number)
                )
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS roadmap_state (
                    repo_full_name TEXT NOT NULL,
                    roadmap_issue_number INTEGER NOT NULL,
                    roadmap_pr_number INTEGER NULL,
                    roadmap_doc_path TEXT NOT NULL,
                    graph_path TEXT NOT NULL,
                    graph_checksum TEXT NOT NULL,
                    graph_version INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL,
                    adjustment_state TEXT NOT NULL DEFAULT 'idle',
                    adjustment_claim_token TEXT NULL,
                    adjustment_started_at TEXT NULL,
                    adjustment_request_version INTEGER NULL,
                    pending_revision_pr_number INTEGER NULL,
                    pending_revision_pr_url TEXT NULL,
                    pending_revision_head_sha TEXT NULL,
                    last_adjustment_basis_digest TEXT NULL,
                    parent_roadmap_issue_number INTEGER NULL,
                    superseding_roadmap_issue_number INTEGER NULL,
                    revision_requested_at TEXT NULL,
                    last_error TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, roadmap_issue_number)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_roadmap_state_status
                ON roadmap_state(repo_full_name, status, updated_at)
                """
            )
            roadmap_state_columns = _table_columns(conn, "roadmap_state")
            if "graph_version" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN graph_version INTEGER NOT NULL DEFAULT 1
                    """
                )
            if "adjustment_state" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN adjustment_state TEXT NOT NULL DEFAULT 'idle'
                    """
                )
            if "adjustment_claim_token" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN adjustment_claim_token TEXT NULL
                    """
                )
            if "adjustment_started_at" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN adjustment_started_at TEXT NULL
                    """
                )
            if "adjustment_request_version" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN adjustment_request_version INTEGER NULL
                    """
                )
            if "pending_revision_pr_number" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN pending_revision_pr_number INTEGER NULL
                    """
                )
            if "pending_revision_pr_url" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN pending_revision_pr_url TEXT NULL
                    """
                )
            if "pending_revision_head_sha" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN pending_revision_head_sha TEXT NULL
                    """
                )
            if "last_adjustment_basis_digest" not in roadmap_state_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_state
                    ADD COLUMN last_adjustment_basis_digest TEXT NULL
                    """
                )
            conn.execute(
                """
                UPDATE roadmap_state
                SET
                    status = 'active',
                    adjustment_state = CASE
                        WHEN adjustment_state = 'idle' THEN 'awaiting_revision_merge'
                        ELSE adjustment_state
                    END,
                    adjustment_request_version = COALESCE(adjustment_request_version, graph_version)
                WHERE status = 'revision_requested'
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS roadmap_nodes (
                    repo_full_name TEXT NOT NULL,
                    roadmap_issue_number INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body_markdown TEXT NOT NULL,
                    dependencies_json TEXT NOT NULL,
                    introduced_in_version INTEGER NOT NULL DEFAULT 1,
                    retired_in_version INTEGER NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    child_issue_number INTEGER NULL,
                    child_issue_url TEXT NULL,
                    status TEXT NOT NULL,
                    planned_at TEXT NULL,
                    implemented_at TEXT NULL,
                    status_changed_at TEXT NULL,
                    last_progress_at TEXT NULL,
                    blocked_since_at TEXT NULL,
                    claim_token TEXT NULL,
                    claim_started_at TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, roadmap_issue_number, node_id)
                )
                """
            )
            roadmap_node_columns = _table_columns(conn, "roadmap_nodes")
            if "introduced_in_version" not in roadmap_node_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_nodes
                    ADD COLUMN introduced_in_version INTEGER NOT NULL DEFAULT 1
                    """
                )
            if "retired_in_version" not in roadmap_node_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_nodes
                    ADD COLUMN retired_in_version INTEGER NULL
                    """
                )
            if "is_active" not in roadmap_node_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_nodes
                    ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1
                    """
                )
            if "claim_token" not in roadmap_node_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_nodes
                    ADD COLUMN claim_token TEXT NULL
                    """
                )
            if "claim_started_at" not in roadmap_node_columns:
                conn.execute(
                    """
                    ALTER TABLE roadmap_nodes
                    ADD COLUMN claim_started_at TEXT NULL
                    """
                )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_roadmap_nodes_status
                ON roadmap_nodes(repo_full_name, roadmap_issue_number, is_active, status, updated_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_roadmap_nodes_child_issue
                ON roadmap_nodes(repo_full_name, child_issue_number)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS roadmap_revisions (
                    repo_full_name TEXT NOT NULL,
                    roadmap_issue_number INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    roadmap_pr_number INTEGER NULL,
                    roadmap_doc_path TEXT NOT NULL,
                    graph_path TEXT NOT NULL,
                    graph_checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, roadmap_issue_number, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS roadmap_revision_drafts (
                    repo_full_name TEXT NOT NULL,
                    roadmap_issue_number INTEGER NOT NULL,
                    request_version INTEGER NOT NULL,
                    summary TEXT NOT NULL,
                    details TEXT NOT NULL,
                    updated_roadmap_markdown TEXT NOT NULL,
                    updated_canonical_graph_json TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    ready_node_ids_json TEXT NULL,
                    request_reason TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, roadmap_issue_number)
                )
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

    def list_unfinished_agent_runs(
        self, *, repo_full_name: str | None = None
    ) -> tuple[UnfinishedAgentRunState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        run_id,
                        repo_full_name,
                        run_kind,
                        issue_number,
                        pr_number,
                        flow,
                        branch,
                        started_at,
                        meta_json
                    FROM agent_run_history
                    WHERE finished_at IS NULL
                    ORDER BY started_at ASC, repo_full_name ASC, issue_number ASC, run_id ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        run_id,
                        repo_full_name,
                        run_kind,
                        issue_number,
                        pr_number,
                        flow,
                        branch,
                        started_at,
                        meta_json
                    FROM agent_run_history
                    WHERE finished_at IS NULL
                      AND repo_full_name = ?
                    ORDER BY started_at ASC, issue_number ASC, run_id ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            UnfinishedAgentRunState(
                run_id=cast(str, row[0]),
                repo_full_name=cast(str, row[1]),
                run_kind=cast(AgentRunKind, row[2]),
                issue_number=cast(int, row[3]),
                pr_number=cast(int | None, row[4]),
                flow=cast(str | None, row[5]),
                branch=cast(str | None, row[6]),
                started_at=cast(str, row[7]),
                meta_json=cast(str, row[8]),
            )
            for row in rows
        )

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

    def mark_github_call_state_applied(
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
                    state_applied = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND call_id = ?
                  AND status = 'succeeded'
                  AND state_applied = 0
                """,
                (repo_key, call_id),
            )
        return cursor.rowcount > 0

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

    def apply_succeeded_create_issue_call(
        self,
        *,
        call_id: int,
        roadmap_issue_number: int,
        node_id: str,
        child_issue_number: int,
        child_issue_url: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, state_applied
                FROM github_call_outbox
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (repo_key, call_id),
            ).fetchone()
            if row is None:
                return False
            status, state_applied = row
            if status != "succeeded":
                return False
            if int(state_applied) != 0:
                return False

            node_row = conn.execute(
                """
                SELECT child_issue_number, kind, status
                FROM roadmap_nodes
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND node_id = ?
                """,
                (repo_key, roadmap_issue_number, node_id),
            ).fetchone()
            if node_row is None:
                return False
            existing_child_issue_number, kind, node_status = node_row
            if existing_child_issue_number is None:
                conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        child_issue_number = ?,
                        child_issue_url = ?,
                        status = CASE
                            WHEN status = 'completed' THEN status
                            ELSE 'issued'
                        END,
                        planned_at = CASE
                            WHEN kind = 'small_job'
                            THEN COALESCE(planned_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                            ELSE planned_at
                        END,
                        status_changed_at = CASE
                            WHEN status = 'completed'
                            THEN status_changed_at
                            ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                        END,
                        last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        blocked_since_at = NULL,
                        claim_token = NULL,
                        claim_started_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND node_id = ?
                    """,
                    (
                        child_issue_number,
                        child_issue_url,
                        repo_key,
                        roadmap_issue_number,
                        node_id,
                    ),
                )
            elif int(existing_child_issue_number) != child_issue_number:
                return False
            elif kind == "small_job" and node_status == "pending":
                conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        planned_at = COALESCE(planned_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                        status = 'issued',
                        status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        blocked_since_at = NULL,
                        claim_token = NULL,
                        claim_started_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND node_id = ?
                    """,
                    (repo_key, roadmap_issue_number, node_id),
                )

            conn.execute(
                """
                UPDATE github_call_outbox
                SET
                    state_applied = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (repo_key, call_id),
            )
        return True

    def apply_succeeded_post_issue_comment_call(
        self,
        *,
        call_id: int,
        issue_number: int,
        token: str,
        source: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, state_applied
                FROM github_call_outbox
                WHERE repo_full_name = ? AND call_id = ?
                """,
                (repo_key, call_id),
            ).fetchone()
            if row is None:
                return False
            status, state_applied = row
            if status != "succeeded":
                return False
            if int(state_applied) != 0:
                return False
        self.record_action_token_posted(
            token=token,
            scope_kind="issue",
            scope_number=issue_number,
            source=source,
            repo_full_name=repo_key,
        )
        return self.mark_github_call_state_applied(call_id=call_id, repo_full_name=repo_key)

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

    def upsert_pr_flake_state(
        self,
        *,
        pr_number: int,
        issue_number: int,
        head_sha: str,
        run_id: int,
        initial_run_updated_at: str,
        status: PrFlakeStatus,
        flake_issue_number: int,
        flake_issue_url: str,
        report_title: str,
        report_summary: str,
        report_excerpt: str,
        full_log_context_markdown: str,
        rerun_requested_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> PrFlakeState:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pr_flake_state(
                    repo_full_name,
                    pr_number,
                    issue_number,
                    head_sha,
                    run_id,
                    initial_run_updated_at,
                    status,
                    flake_issue_number,
                    flake_issue_url,
                    report_title,
                    report_summary,
                    report_excerpt,
                    full_log_context_markdown,
                    rerun_requested_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, pr_number) DO UPDATE SET
                    issue_number=excluded.issue_number,
                    head_sha=excluded.head_sha,
                    run_id=excluded.run_id,
                    initial_run_updated_at=excluded.initial_run_updated_at,
                    status=excluded.status,
                    flake_issue_number=excluded.flake_issue_number,
                    flake_issue_url=excluded.flake_issue_url,
                    report_title=excluded.report_title,
                    report_summary=excluded.report_summary,
                    report_excerpt=excluded.report_excerpt,
                    full_log_context_markdown=excluded.full_log_context_markdown,
                    rerun_requested_at=COALESCE(
                        excluded.rerun_requested_at,
                        pr_flake_state.rerun_requested_at
                    ),
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    pr_number,
                    issue_number,
                    head_sha,
                    run_id,
                    initial_run_updated_at,
                    status,
                    flake_issue_number,
                    flake_issue_url,
                    report_title,
                    report_summary,
                    report_excerpt,
                    full_log_context_markdown,
                    rerun_requested_at,
                ),
            )
        state = self.get_pr_flake_state(pr_number, repo_full_name=repo_key)
        if state is None:
            raise RuntimeError(f"Missing pr_flake_state row for pr_number={pr_number}")
        return state

    def get_pr_flake_state(
        self,
        pr_number: int,
        *,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        with self._lock, self._connect() as conn:
            if repo_full_name is None:
                rows = conn.execute(
                    """
                    SELECT
                        repo_full_name,
                        pr_number,
                        issue_number,
                        head_sha,
                        run_id,
                        initial_run_updated_at,
                        status,
                        flake_issue_number,
                        flake_issue_url,
                        report_title,
                        report_summary,
                        report_excerpt,
                        full_log_context_markdown,
                        rerun_requested_at,
                        created_at,
                        updated_at
                    FROM pr_flake_state
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
                            f"Multiple pr_flake_state rows found for pr_number={pr_number}; "
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
                        head_sha,
                        run_id,
                        initial_run_updated_at,
                        status,
                        flake_issue_number,
                        flake_issue_url,
                        report_title,
                        report_summary,
                        report_excerpt,
                        full_log_context_markdown,
                        rerun_requested_at,
                        created_at,
                        updated_at
                    FROM pr_flake_state
                    WHERE repo_full_name = ? AND pr_number = ?
                    """,
                    (repo_key, pr_number),
                ).fetchone()
        if row is None:
            return None
        return _parse_pr_flake_state_row(row)

    def get_active_pr_flake_state(
        self,
        pr_number: int,
        *,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        state = self.get_pr_flake_state(pr_number, repo_full_name=repo_full_name)
        if state is None:
            return None
        if state.status != "awaiting_rerun_result":
            return None
        return state

    def set_pr_flake_state_status(
        self,
        *,
        pr_number: int,
        status: PrFlakeStatus,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE pr_flake_state
                SET status = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (status, repo_key, pr_number),
            )
        if cursor.rowcount <= 0:
            return None
        return self.get_pr_flake_state(pr_number, repo_full_name=repo_key)

    def mark_pr_flake_rerun_requested(
        self,
        *,
        pr_number: int,
        rerun_requested_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        requested_at = rerun_requested_at or _now_iso_utc()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE pr_flake_state
                SET rerun_requested_at = COALESCE(rerun_requested_at, ?),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (requested_at, repo_key, pr_number),
            )
        if cursor.rowcount <= 0:
            return None
        return self.get_pr_flake_state(pr_number, repo_full_name=repo_key)

    def clear_pr_flake_state(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                DELETE FROM pr_flake_state
                WHERE repo_full_name = ? AND pr_number = ?
                """,
                (repo_key, pr_number),
            )
        return cursor.rowcount > 0

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

    def list_failed_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[FailedIssueRunState, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        r.repo_full_name,
                        r.issue_number,
                        r.branch,
                        r.error,
                        r.updated_at
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
                        r.error,
                        r.updated_at
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
            FailedIssueRunState(
                repo_full_name=str(row_repo_full_name),
                issue_number=int(issue_number),
                branch=str(branch) if isinstance(branch, str) else None,
                error=str(error) if isinstance(error, str) else None,
                updated_at=str(updated_at),
            )
            for row_repo_full_name, issue_number, branch, error, updated_at in rows
        )

    def list_legacy_failed_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[LegacyFailedIssueRunState, ...]:
        return self.list_failed_issue_runs_without_pr(repo_full_name=repo_full_name)

    def retry_failed_issue_runs(
        self,
        *,
        issue_numbers: tuple[int, ...] | None = None,
        repo_full_name: str | None = None,
    ) -> int:
        if issue_numbers is not None and len(issue_numbers) == 0:
            return 0

        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        repo_filter_sql = "AND issue_runs.repo_full_name = ?" if repo_key is not None else ""
        params: list[object] = []
        if repo_key is not None:
            params.append(repo_key)

        issue_filter_sql = ""
        if issue_numbers is not None:
            issue_filter_sql = (
                "AND issue_runs.issue_number IN (" + ", ".join("?" for _ in issue_numbers) + ")"
            )
            params.extend(issue_numbers)

        with self._lock, self._connect() as conn:
            updated = conn.execute(
                f"""
                UPDATE issue_runs
                SET
                    active_run_id = NULL,
                    last_failure_class = 'unknown',
                    retry_count = 1,
                    next_retry_at = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE status = 'failed'
                  AND pr_number IS NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pre_pr_followup_state AS p
                        WHERE p.repo_full_name = issue_runs.repo_full_name
                          AND p.issue_number = issue_runs.issue_number
                    )
                  {repo_filter_sql}
                  {issue_filter_sql}
                """,
                tuple(params),
            )
        return int(updated.rowcount)

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
            if status in {"merged", "closed"}:
                conn.execute(
                    """
                    DELETE FROM pr_flake_state
                    WHERE repo_full_name = ? AND pr_number = ?
                    """,
                    (repo_key, pr_number),
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

    def get_issue_run_state(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> tuple[str, str | None] | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, branch
                FROM issue_runs
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            ).fetchone()
        if row is None:
            return None
        status, branch = row
        if not isinstance(status, str):
            raise RuntimeError("Invalid status value stored in issue_runs")
        if branch is not None and not isinstance(branch, str):
            raise RuntimeError("Invalid branch value stored in issue_runs")
        return status, branch

    def get_issue_run_record(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> IssueRunRecord | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, branch, pr_number, pr_url, error
                FROM issue_runs
                WHERE repo_full_name = ? AND issue_number = ?
                """,
                (repo_key, issue_number),
            ).fetchone()
        if row is None:
            return None
        status, branch, pr_number, pr_url, error = row
        if not isinstance(status, str):
            raise RuntimeError("Invalid status value stored in issue_runs")
        if branch is not None and not isinstance(branch, str):
            raise RuntimeError("Invalid branch value stored in issue_runs")
        if pr_number is not None and not isinstance(pr_number, int):
            raise RuntimeError("Invalid pr_number value stored in issue_runs")
        if pr_url is not None and not isinstance(pr_url, str):
            raise RuntimeError("Invalid pr_url value stored in issue_runs")
        if error is not None and not isinstance(error, str):
            raise RuntimeError("Invalid error value stored in issue_runs")
        return IssueRunRecord(
            repo_full_name=repo_key,
            issue_number=issue_number,
            status=status,
            branch=branch,
            pr_number=pr_number,
            pr_url=pr_url,
            error=error,
        )

    def get_roadmap_state(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> RoadmapStateRecord | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                    graph_version,
                    status,
                    adjustment_state,
                    adjustment_claim_token,
                    adjustment_started_at,
                    adjustment_request_version,
                    pending_revision_pr_number,
                    pending_revision_pr_url,
                    pending_revision_head_sha,
                    last_adjustment_basis_digest,
                    parent_roadmap_issue_number,
                    superseding_roadmap_issue_number,
                    revision_requested_at,
                    last_error,
                    updated_at
                FROM roadmap_state
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            ).fetchone()
        if row is None:
            return None
        return _parse_roadmap_state_row(row, repo_full_name=repo_key)

    def list_roadmap_nodes(
        self,
        *,
        roadmap_issue_number: int,
        repo_full_name: str | None = None,
        include_retired: bool = False,
    ) -> tuple[RoadmapNodeRecord, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            if include_retired:
                rows = conn.execute(
                    """
                    SELECT
                        roadmap_issue_number,
                        node_id,
                        kind,
                        title,
                        body_markdown,
                        dependencies_json,
                        introduced_in_version,
                        retired_in_version,
                        is_active,
                        child_issue_number,
                        child_issue_url,
                        status,
                        planned_at,
                        implemented_at,
                        status_changed_at,
                        last_progress_at,
                        blocked_since_at,
                        claim_token,
                        updated_at
                    FROM roadmap_nodes
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    ORDER BY is_active DESC, node_id ASC
                    """,
                    (repo_key, roadmap_issue_number),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        roadmap_issue_number,
                        node_id,
                        kind,
                        title,
                        body_markdown,
                        dependencies_json,
                        introduced_in_version,
                        retired_in_version,
                        is_active,
                        child_issue_number,
                        child_issue_url,
                        status,
                        planned_at,
                        implemented_at,
                        status_changed_at,
                        last_progress_at,
                        blocked_since_at,
                        claim_token,
                        updated_at
                    FROM roadmap_nodes
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND is_active = 1
                    ORDER BY node_id ASC
                    """,
                    (repo_key, roadmap_issue_number),
                ).fetchall()
        return tuple(_parse_roadmap_node_row(row, repo_full_name=repo_key) for row in rows)

    def upsert_roadmap_graph(
        self,
        *,
        roadmap_issue_number: int,
        roadmap_pr_number: int | None,
        roadmap_doc_path: str,
        graph_path: str,
        graph_checksum: str,
        nodes: tuple[RoadmapNodeGraphInput, ...],
        graph_version: int = 1,
        parent_roadmap_issue_number: int | None = None,
        repo_full_name: str | None = None,
    ) -> RoadmapStateRecord:
        if not nodes:
            raise ValueError("nodes must be non-empty")
        if graph_version < 1:
            raise ValueError("graph_version must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            state_row = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                    graph_version,
                    status,
                    adjustment_state,
                    adjustment_claim_token,
                    adjustment_started_at,
                    adjustment_request_version,
                    pending_revision_pr_number,
                    pending_revision_pr_url,
                    pending_revision_head_sha,
                    last_adjustment_basis_digest,
                    parent_roadmap_issue_number,
                    superseding_roadmap_issue_number,
                    revision_requested_at,
                    last_error,
                    updated_at
                FROM roadmap_state
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            ).fetchone()
            existing_state = (
                _parse_roadmap_state_row(state_row, repo_full_name=repo_key)
                if state_row is not None
                else None
            )
            existing_rows = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    node_id,
                    kind,
                    title,
                    body_markdown,
                    dependencies_json,
                    introduced_in_version,
                    retired_in_version,
                    is_active,
                    child_issue_number,
                    child_issue_url,
                    status,
                    planned_at,
                    implemented_at,
                    status_changed_at,
                    last_progress_at,
                    blocked_since_at,
                    claim_token,
                    updated_at
                FROM roadmap_nodes
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND is_active = 1
                ORDER BY node_id ASC
                """,
                (repo_key, roadmap_issue_number),
            ).fetchall()
            existing_nodes = tuple(
                _parse_roadmap_node_row(row, repo_full_name=repo_key) for row in existing_rows
            )
            if existing_state is not None and graph_version < existing_state.graph_version:
                raise RoadmapGraphTransitionError(
                    "roadmap graph version cannot move backwards: "
                    f"current={existing_state.graph_version}, proposed={graph_version}"
                )
            if existing_state is not None and graph_version == existing_state.graph_version:
                if graph_checksum != existing_state.graph_checksum:
                    raise RoadmapGraphTransitionError(
                        "roadmap graph checksum changed without a version bump"
                    )
            transition = None
            if existing_state is not None and graph_version > existing_state.graph_version:
                transition = validate_roadmap_graph_transition(
                    current_graph_version=existing_state.graph_version,
                    current_nodes=tuple(
                        ExistingRoadmapNodeState(
                            node_id=node.node_id,
                            kind=node.kind,
                            title=node.title,
                            body_markdown=node.body_markdown,
                            depends_on=_roadmap_dependencies_from_json(node.dependencies_json),
                            status=node.status,
                            child_issue_number=node.child_issue_number,
                            implemented_at=node.implemented_at,
                        )
                        for node in existing_nodes
                    ),
                    proposed_graph=RoadmapGraph(
                        roadmap_issue_number=roadmap_issue_number,
                        version=graph_version,
                        nodes=tuple(
                            RoadmapNode(
                                node_id=node.node_id,
                                kind=node.kind,
                                title=node.title,
                                body_markdown=node.body_markdown,
                                depends_on=tuple(
                                    RoadmapDependency(
                                        node_id=dependency.node_id,
                                        requires=dependency.requires,
                                    )
                                    for dependency in node.dependencies
                                ),
                            )
                            for node in nodes
                        ),
                    ),
                )
            if existing_state is None:
                conn.execute(
                    """
                    INSERT INTO roadmap_state(
                        repo_full_name,
                        roadmap_issue_number,
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        status,
                        adjustment_state,
                        adjustment_claim_token,
                        adjustment_started_at,
                        adjustment_request_version,
                        pending_revision_pr_number,
                        pending_revision_pr_url,
                        pending_revision_head_sha,
                        last_adjustment_basis_digest,
                        parent_roadmap_issue_number,
                        superseding_roadmap_issue_number,
                        revision_requested_at,
                        last_error
                    )
                    VALUES(
                        ?, ?, ?, ?, ?, ?, ?, 'active', 'idle', NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL
                    )
                    """,
                    (
                        repo_key,
                        roadmap_issue_number,
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        parent_roadmap_issue_number,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE roadmap_state
                    SET
                        roadmap_pr_number = COALESCE(?, roadmap_pr_number),
                        roadmap_doc_path = ?,
                        graph_path = ?,
                        graph_checksum = ?,
                        graph_version = ?,
                        status = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN status
                            ELSE 'active'
                        END,
                        adjustment_state = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN adjustment_state
                            ELSE 'idle'
                        END,
                        adjustment_claim_token = NULL,
                        adjustment_started_at = NULL,
                        adjustment_request_version = CASE
                            WHEN adjustment_state = 'awaiting_revision_merge'
                            THEN ?
                            ELSE adjustment_request_version
                        END,
                        pending_revision_pr_number = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN pending_revision_pr_number
                            ELSE NULL
                        END,
                        pending_revision_pr_url = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN pending_revision_pr_url
                            ELSE NULL
                        END,
                        pending_revision_head_sha = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN pending_revision_head_sha
                            ELSE NULL
                        END,
                        last_adjustment_basis_digest = CASE
                            WHEN status IN ('superseded', 'abandoned', 'completed')
                            THEN last_adjustment_basis_digest
                            ELSE NULL
                        END,
                        parent_roadmap_issue_number = COALESCE(
                            ?,
                            parent_roadmap_issue_number
                        ),
                        revision_requested_at = CASE
                            WHEN adjustment_state = 'awaiting_revision_merge'
                            THEN NULL
                            ELSE revision_requested_at
                        END,
                        last_error = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        graph_version,
                        parent_roadmap_issue_number,
                        repo_key,
                        roadmap_issue_number,
                    ),
                )
            if transition is not None:
                for node_id in transition.retired_node_ids:
                    conn.execute(
                        """
                        UPDATE roadmap_nodes
                        SET
                            is_active = 0,
                            retired_in_version = ?,
                            claim_token = NULL,
                            claim_started_at = NULL,
                            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                        WHERE repo_full_name = ?
                          AND roadmap_issue_number = ?
                          AND node_id = ?
                        """,
                        (graph_version, repo_key, roadmap_issue_number, node_id),
                    )
            conn.execute(
                """
                DELETE FROM roadmap_revision_drafts
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            )
            node_payloads = [
                (
                    repo_key,
                    roadmap_issue_number,
                    node.node_id,
                    node.kind,
                    node.title,
                    node.body_markdown,
                    json.dumps(
                        [
                            {"node_id": dep.node_id, "requires": dep.requires}
                            for dep in node.dependencies
                        ],
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    graph_version,
                )
                for node in nodes
            ]
            conn.executemany(
                """
                INSERT INTO roadmap_nodes(
                    repo_full_name,
                    roadmap_issue_number,
                    node_id,
                    kind,
                    title,
                    body_markdown,
                    dependencies_json,
                    introduced_in_version,
                    retired_in_version,
                    is_active,
                    status,
                    status_changed_at
                )
                VALUES(
                    ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, 'pending',
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
                ON CONFLICT(repo_full_name, roadmap_issue_number, node_id) DO UPDATE SET
                    kind = excluded.kind,
                    title = excluded.title,
                    body_markdown = excluded.body_markdown,
                    dependencies_json = excluded.dependencies_json,
                    retired_in_version = NULL,
                    is_active = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                node_payloads,
            )
            conn.execute(
                """
                INSERT INTO roadmap_revisions(
                    repo_full_name,
                    roadmap_issue_number,
                    version,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_full_name, roadmap_issue_number, version) DO NOTHING
                """,
                (
                    repo_key,
                    roadmap_issue_number,
                    graph_version,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                ),
            )
            row = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                    graph_version,
                    status,
                    adjustment_state,
                    adjustment_claim_token,
                    adjustment_started_at,
                    adjustment_request_version,
                    pending_revision_pr_number,
                    pending_revision_pr_url,
                    pending_revision_head_sha,
                    last_adjustment_basis_digest,
                    parent_roadmap_issue_number,
                    superseding_roadmap_issue_number,
                    revision_requested_at,
                    last_error,
                    updated_at
                FROM roadmap_state
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            ).fetchone()
        if row is None:
            raise RuntimeError("roadmap_state row disappeared after upsert")
        return _parse_roadmap_state_row(row, repo_full_name=repo_key)

    def list_roadmap_activation_candidates(
        self, *, repo_full_name: str | None = None
    ) -> tuple[RoadmapActivationCandidate, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        i.repo_full_name,
                        i.issue_number,
                        i.branch,
                        i.pr_number,
                        i.pr_url
                    FROM issue_runs AS i
                    LEFT JOIN roadmap_state AS r
                        ON r.repo_full_name = i.repo_full_name
                       AND r.roadmap_issue_number = i.issue_number
                    WHERE i.status = 'merged'
                      AND i.branch LIKE 'agent/roadmap/%'
                      AND r.roadmap_issue_number IS NULL
                    ORDER BY i.updated_at ASC, i.repo_full_name ASC, i.issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        i.repo_full_name,
                        i.issue_number,
                        i.branch,
                        i.pr_number,
                        i.pr_url
                    FROM issue_runs AS i
                    LEFT JOIN roadmap_state AS r
                        ON r.repo_full_name = i.repo_full_name
                       AND r.roadmap_issue_number = i.issue_number
                    WHERE i.repo_full_name = ?
                      AND i.status = 'merged'
                      AND i.branch LIKE 'agent/roadmap/%'
                      AND r.roadmap_issue_number IS NULL
                    ORDER BY i.updated_at ASC, i.issue_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        return tuple(
            RoadmapActivationCandidate(
                repo_full_name=str(row_repo_full_name),
                roadmap_issue_number=int(roadmap_issue_number),
                branch=str(branch),
                roadmap_pr_number=int(roadmap_pr_number)
                if isinstance(roadmap_pr_number, int)
                else None,
                roadmap_pr_url=str(roadmap_pr_url) if isinstance(roadmap_pr_url, str) else None,
            )
            for (
                row_repo_full_name,
                roadmap_issue_number,
                branch,
                roadmap_pr_number,
                roadmap_pr_url,
            ) in rows
        )

    def list_active_roadmaps(
        self, *, repo_full_name: str | None = None
    ) -> tuple[RoadmapStateRecord, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name) if repo_full_name is not None else None
        with self._lock, self._connect() as conn:
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT
                        repo_full_name,
                        roadmap_issue_number,
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        status,
                        adjustment_state,
                        adjustment_claim_token,
                        adjustment_started_at,
                        adjustment_request_version,
                        pending_revision_pr_number,
                        pending_revision_pr_url,
                        pending_revision_head_sha,
                        last_adjustment_basis_digest,
                        parent_roadmap_issue_number,
                        superseding_roadmap_issue_number,
                        revision_requested_at,
                        last_error,
                        updated_at
                    FROM roadmap_state
                    WHERE status = 'active'
                    ORDER BY updated_at ASC, repo_full_name ASC, roadmap_issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        roadmap_issue_number,
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        status,
                        adjustment_state,
                        adjustment_claim_token,
                        adjustment_started_at,
                        adjustment_request_version,
                        pending_revision_pr_number,
                        pending_revision_pr_url,
                        pending_revision_head_sha,
                        last_adjustment_basis_digest,
                        parent_roadmap_issue_number,
                        superseding_roadmap_issue_number,
                        revision_requested_at,
                        last_error,
                        updated_at
                    FROM roadmap_state
                    WHERE repo_full_name = ?
                      AND status = 'active'
                    ORDER BY updated_at ASC, roadmap_issue_number ASC
                    """,
                    (repo_key,),
                ).fetchall()
        if repo_key is None:
            return tuple(
                _parse_roadmap_state_row(
                    (
                        roadmap_issue_number,
                        roadmap_pr_number,
                        roadmap_doc_path,
                        graph_path,
                        graph_checksum,
                        graph_version,
                        status,
                        adjustment_state,
                        adjustment_claim_token,
                        adjustment_started_at,
                        adjustment_request_version,
                        pending_revision_pr_number,
                        pending_revision_pr_url,
                        pending_revision_head_sha,
                        last_adjustment_basis_digest,
                        parent_roadmap_issue_number,
                        superseding_roadmap_issue_number,
                        revision_requested_at,
                        last_error,
                        updated_at,
                    ),
                    repo_full_name=str(row_repo_full_name),
                )
                for (
                    row_repo_full_name,
                    roadmap_issue_number,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                    graph_version,
                    status,
                    adjustment_state,
                    adjustment_claim_token,
                    adjustment_started_at,
                    adjustment_request_version,
                    pending_revision_pr_number,
                    pending_revision_pr_url,
                    pending_revision_head_sha,
                    last_adjustment_basis_digest,
                    parent_roadmap_issue_number,
                    superseding_roadmap_issue_number,
                    revision_requested_at,
                    last_error,
                    updated_at,
                ) in rows
            )
        return tuple(_parse_roadmap_state_row(row, repo_full_name=repo_key) for row in rows)

    def list_roadmap_revisions(
        self,
        *,
        roadmap_issue_number: int,
        limit: int = 5,
        repo_full_name: str | None = None,
    ) -> tuple[RoadmapRevisionRecord, ...]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    version,
                    roadmap_pr_number,
                    roadmap_doc_path,
                    graph_path,
                    graph_checksum,
                    applied_at
                FROM roadmap_revisions
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                ORDER BY version DESC
                LIMIT ?
                """,
                (repo_key, roadmap_issue_number, limit),
            ).fetchall()
        return tuple(_parse_roadmap_revision_row(row, repo_full_name=repo_key) for row in rows)

    def get_roadmap_revision_draft(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> RoadmapRevisionDraftRecord | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    roadmap_issue_number,
                    request_version,
                    summary,
                    details,
                    updated_roadmap_markdown,
                    updated_canonical_graph_json,
                    source_kind,
                    ready_node_ids_json,
                    request_reason,
                    created_at,
                    updated_at
                FROM roadmap_revision_drafts
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            ).fetchone()
        if row is None:
            return None
        return _parse_roadmap_revision_draft_row(row, repo_full_name=repo_key)

    def prepare_roadmap_revision_draft_from_adjustment(
        self,
        *,
        roadmap_issue_number: int,
        claim_token: str,
        request_version: int,
        summary: str,
        details: str,
        updated_roadmap_markdown: str,
        updated_canonical_graph_json: str,
        ready_node_ids_json: str | None,
        repo_full_name: str | None = None,
    ) -> bool:
        if request_version < 1:
            raise ValueError("request_version must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = 'awaiting_revision_merge',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = ?,
                    pending_revision_pr_number = NULL,
                    pending_revision_pr_url = NULL,
                    pending_revision_head_sha = NULL,
                    last_adjustment_basis_digest = NULL,
                    revision_requested_at = COALESCE(
                        revision_requested_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    last_error = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                  AND adjustment_state = 'evaluating'
                  AND adjustment_claim_token = ?
                """,
                (
                    request_version,
                    summary,
                    repo_key,
                    roadmap_issue_number,
                    claim_token,
                ),
            )
            if cursor.rowcount <= 0:
                return False
            conn.execute(
                """
                INSERT INTO roadmap_revision_drafts(
                    repo_full_name,
                    roadmap_issue_number,
                    request_version,
                    summary,
                    details,
                    updated_roadmap_markdown,
                    updated_canonical_graph_json,
                    source_kind,
                    ready_node_ids_json,
                    request_reason
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, 'adjustment', ?, NULL)
                ON CONFLICT(repo_full_name, roadmap_issue_number) DO UPDATE SET
                    request_version = excluded.request_version,
                    summary = excluded.summary,
                    details = excluded.details,
                    updated_roadmap_markdown = excluded.updated_roadmap_markdown,
                    updated_canonical_graph_json = excluded.updated_canonical_graph_json,
                    source_kind = excluded.source_kind,
                    ready_node_ids_json = excluded.ready_node_ids_json,
                    request_reason = excluded.request_reason,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    roadmap_issue_number,
                    request_version,
                    summary,
                    details,
                    updated_roadmap_markdown,
                    updated_canonical_graph_json,
                    ready_node_ids_json,
                ),
            )
        return True

    def prepare_requested_roadmap_revision_draft(
        self,
        *,
        roadmap_issue_number: int,
        request_version: int,
        summary: str,
        details: str,
        updated_roadmap_markdown: str,
        updated_canonical_graph_json: str,
        request_reason: str,
        repo_full_name: str | None = None,
    ) -> bool:
        if request_version < 1:
            raise ValueError("request_version must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_request_version = ?,
                    last_error = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                  AND adjustment_state = 'awaiting_revision_merge'
                  AND pending_revision_pr_number IS NULL
                """,
                (
                    request_version,
                    summary,
                    repo_key,
                    roadmap_issue_number,
                ),
            )
            if cursor.rowcount <= 0:
                return False
            conn.execute(
                """
                INSERT INTO roadmap_revision_drafts(
                    repo_full_name,
                    roadmap_issue_number,
                    request_version,
                    summary,
                    details,
                    updated_roadmap_markdown,
                    updated_canonical_graph_json,
                    source_kind,
                    ready_node_ids_json,
                    request_reason
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, 'manual', NULL, ?)
                ON CONFLICT(repo_full_name, roadmap_issue_number) DO UPDATE SET
                    request_version = excluded.request_version,
                    summary = excluded.summary,
                    details = excluded.details,
                    updated_roadmap_markdown = excluded.updated_roadmap_markdown,
                    updated_canonical_graph_json = excluded.updated_canonical_graph_json,
                    source_kind = excluded.source_kind,
                    ready_node_ids_json = excluded.ready_node_ids_json,
                    request_reason = excluded.request_reason,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (
                    repo_key,
                    roadmap_issue_number,
                    request_version,
                    summary,
                    details,
                    updated_roadmap_markdown,
                    updated_canonical_graph_json,
                    request_reason,
                ),
            )
        return True

    def mark_roadmap_revision_materialized(
        self,
        *,
        roadmap_issue_number: int,
        request_version: int,
        pr_number: int,
        pr_url: str,
        head_sha: str | None,
        repo_full_name: str | None = None,
    ) -> bool:
        if request_version < 1:
            raise ValueError("request_version must be >= 1")
        if pr_number < 1:
            raise ValueError("pr_number must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = 'awaiting_revision_merge',
                    adjustment_request_version = ?,
                    pending_revision_pr_number = ?,
                    pending_revision_pr_url = ?,
                    pending_revision_head_sha = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                  AND adjustment_state = 'awaiting_revision_merge'
                  AND pending_revision_pr_number IS NULL
                """,
                (
                    request_version,
                    pr_number,
                    pr_url,
                    head_sha,
                    repo_key,
                    roadmap_issue_number,
                ),
            )
            if cursor.rowcount <= 0:
                return False
            conn.execute(
                """
                DELETE FROM roadmap_revision_drafts
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (repo_key, roadmap_issue_number),
            )
        return True

    def claim_ready_roadmap_nodes(
        self,
        *,
        limit: int = 32,
        repo_full_name: str | None = None,
    ) -> tuple[ReadyRoadmapNodeClaim, ...]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        stale_before = _format_db_timestamp(
            datetime.now(timezone.utc) - _ROADMAP_NODE_CLAIM_STALE_AFTER
        )
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    n.roadmap_issue_number,
                    n.node_id,
                    n.kind,
                    n.title,
                    n.body_markdown,
                    n.dependencies_json,
                    n.status,
                    n.planned_at,
                    n.implemented_at,
                    n.child_issue_number,
                    n.claim_token,
                    n.claim_started_at
                FROM roadmap_nodes AS n
                INNER JOIN roadmap_state AS r
                    ON r.repo_full_name = n.repo_full_name
                   AND r.roadmap_issue_number = n.roadmap_issue_number
                WHERE n.repo_full_name = ?
                  AND n.is_active = 1
                  AND r.status = 'active'
                  AND r.adjustment_state = 'idle'
                ORDER BY n.roadmap_issue_number ASC, n.node_id ASC
                """,
                (repo_key,),
            ).fetchall()

            nodes_by_roadmap: dict[int, dict[str, tuple[object, ...]]] = {}
            for row in rows:
                roadmap_issue_number = int(row[0])
                node_id = str(row[1])
                nodes_by_roadmap.setdefault(roadmap_issue_number, {})[node_id] = row

            claims: list[ReadyRoadmapNodeClaim] = []
            for roadmap_issue_number in sorted(nodes_by_roadmap):
                if len(claims) >= limit:
                    break
                node_rows = nodes_by_roadmap[roadmap_issue_number]
                for node_id in sorted(node_rows):
                    if len(claims) >= limit:
                        break
                    row = node_rows[node_id]
                    status = _parse_roadmap_node_status(row[6])
                    child_issue_number = int(row[9]) if isinstance(row[9], int) else None
                    claim_token = str(row[10]) if isinstance(row[10], str) else None
                    claim_started_at = str(row[11]) if isinstance(row[11], str) else None
                    if status != "pending" or child_issue_number is not None:
                        continue
                    stale_claim = _roadmap_node_claim_is_stale(
                        claim_token=claim_token,
                        claim_started_at=claim_started_at,
                        stale_before=stale_before,
                    )
                    if claim_token is not None and not stale_claim:
                        continue
                    dependencies = _parse_roadmap_dependencies_json(str(row[5]))
                    if not _roadmap_node_dependencies_satisfied(
                        dependencies=dependencies,
                        node_rows=node_rows,
                    ):
                        continue
                    claim = uuid.uuid4().hex
                    cursor = conn.execute(
                        """
                        UPDATE roadmap_nodes
                        SET claim_token = ?,
                            claim_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                        WHERE repo_full_name = ?
                          AND roadmap_issue_number = ?
                          AND node_id = ?
                          AND is_active = 1
                          AND status = 'pending'
                          AND child_issue_number IS NULL
                          AND (
                                claim_token IS NULL
                                OR (
                                    claim_token = ?
                                    AND (
                                        claim_started_at IS NULL
                                        OR claim_started_at < ?
                                    )
                                )
                          )
                        """,
                        (
                            claim,
                            repo_key,
                            roadmap_issue_number,
                            node_id,
                            claim_token,
                            stale_before,
                        ),
                    )
                    if cursor.rowcount <= 0:
                        continue
                    claims.append(
                        ReadyRoadmapNodeClaim(
                            repo_full_name=repo_key,
                            roadmap_issue_number=roadmap_issue_number,
                            node_id=node_id,
                            kind=cast(RoadmapNodeKind, str(row[2])),
                            title=str(row[3]),
                            body_markdown=str(row[4]),
                            dependencies_json=str(row[5]),
                            claim_token=claim,
                        )
                    )
            return tuple(claims)

    def list_ready_roadmap_frontier(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> tuple[str, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        stale_before = _format_db_timestamp(
            datetime.now(timezone.utc) - _ROADMAP_NODE_CLAIM_STALE_AFTER
        )
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    node_id,
                    dependencies_json,
                    status,
                    planned_at,
                    implemented_at,
                    child_issue_number,
                    claim_token,
                    claim_started_at
                FROM roadmap_nodes
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND is_active = 1
                ORDER BY node_id ASC
                """,
                (repo_key, roadmap_issue_number),
            ).fetchall()
        node_rows = {
            str(node_id): (
                roadmap_issue_number,
                str(node_id),
                "",
                "",
                "",
                str(dependencies_json),
                status,
                planned_at,
                implemented_at,
                child_issue_number,
                claim_token,
                claim_started_at,
            )
            for (
                node_id,
                dependencies_json,
                status,
                planned_at,
                implemented_at,
                child_issue_number,
                claim_token,
                claim_started_at,
            ) in rows
        }
        ready: list[str] = []
        for node_id, row in sorted(node_rows.items()):
            status = _parse_roadmap_node_status(row[6])
            child_issue_number = int(row[9]) if isinstance(row[9], int) else None
            claim_token = str(row[10]) if isinstance(row[10], str) else None
            claim_started_at = str(row[11]) if isinstance(row[11], str) else None
            if status != "pending" or child_issue_number is not None:
                continue
            if (
                not _roadmap_node_claim_is_stale(
                    claim_token=claim_token,
                    claim_started_at=claim_started_at,
                    stale_before=stale_before,
                )
                and claim_token is not None
            ):
                continue
            dependencies = _parse_roadmap_dependencies_json(str(row[5]))
            if _roadmap_node_dependencies_satisfied(
                dependencies=dependencies,
                node_rows=node_rows,
            ):
                ready.append(node_id)
        return tuple(ready)

    def claim_roadmap_adjustment(
        self,
        *,
        roadmap_issue_number: int,
        repo_full_name: str | None = None,
    ) -> str | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        claim = uuid.uuid4().hex
        stale_before = _format_db_timestamp(
            datetime.now(timezone.utc) - _ROADMAP_ADJUSTMENT_STALE_AFTER
        )
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = 'evaluating',
                    adjustment_claim_token = ?,
                    adjustment_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                  AND (
                        adjustment_state = 'idle'
                        OR (
                            adjustment_state = 'evaluating'
                            AND (
                                adjustment_started_at IS NULL
                                OR adjustment_started_at < ?
                            )
                        )
                  )
                """,
                (claim, repo_key, roadmap_issue_number, stale_before),
            )
        if cursor.rowcount <= 0:
            return None
        return claim

    def release_roadmap_adjustment(
        self,
        *,
        roadmap_issue_number: int,
        claim_token: str,
        basis_digest: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = 'idle',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    last_adjustment_basis_digest = CASE
                        WHEN ? IS NULL THEN last_adjustment_basis_digest
                        ELSE ?
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND adjustment_state = 'evaluating'
                  AND adjustment_claim_token = ?
                """,
                (basis_digest, basis_digest, repo_key, roadmap_issue_number, claim_token),
            )
        return cursor.rowcount > 0

    def release_roadmap_node_claim(
        self,
        *,
        roadmap_issue_number: int,
        node_id: str,
        claim_token: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_nodes
                SET claim_token = NULL,
                    claim_started_at = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND node_id = ?
                  AND is_active = 1
                  AND claim_token = ?
                """,
                (repo_key, roadmap_issue_number, node_id, claim_token),
            )
        return cursor.rowcount > 0

    def mark_roadmap_node_issue_created(
        self,
        *,
        roadmap_issue_number: int,
        node_id: str,
        claim_token: str,
        child_issue_number: int,
        child_issue_url: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_nodes
                SET
                    child_issue_number = ?,
                    child_issue_url = ?,
                    status = 'issued',
                    planned_at = CASE
                        WHEN kind = 'small_job'
                        THEN COALESCE(planned_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                        ELSE planned_at
                    END,
                    status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    blocked_since_at = NULL,
                    claim_token = NULL,
                    claim_started_at = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND node_id = ?
                  AND is_active = 1
                  AND claim_token = ?
                """,
                (
                    child_issue_number,
                    child_issue_url,
                    repo_key,
                    roadmap_issue_number,
                    node_id,
                    claim_token,
                ),
            )
            if cursor.rowcount > 0:
                conn.execute(
                    """
                    DELETE FROM roadmap_revision_drafts
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (repo_key, roadmap_issue_number),
                )
        return cursor.rowcount > 0

    def record_roadmap_node_milestone(
        self,
        *,
        roadmap_issue_number: int,
        node_id: str,
        milestone: RoadmapDependencyRequirement,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            if milestone == "planned":
                cursor = conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        planned_at = COALESCE(planned_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                        status = CASE
                            WHEN status = 'pending' AND child_issue_number IS NOT NULL
                            THEN 'issued'
                            ELSE status
                        END,
                        status_changed_at = CASE
                            WHEN status = 'pending' AND child_issue_number IS NOT NULL
                            THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                            ELSE status_changed_at
                        END,
                        last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND node_id = ?
                      AND is_active = 1
                      AND planned_at IS NULL
                    """,
                    (repo_key, roadmap_issue_number, node_id),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        implemented_at = COALESCE(
                            implemented_at,
                            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                        ),
                        status = 'completed',
                        status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        blocked_since_at = NULL,
                        claim_token = NULL,
                        claim_started_at = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND node_id = ?
                      AND is_active = 1
                      AND implemented_at IS NULL
                    """,
                    (repo_key, roadmap_issue_number, node_id),
                )
        return cursor.rowcount > 0

    def mark_roadmap_node_blocked(
        self,
        *,
        roadmap_issue_number: int,
        node_id: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_nodes
                SET
                    status = CASE
                        WHEN status IN ('completed', 'abandoned') THEN status
                        ELSE 'blocked'
                    END,
                    blocked_since_at = COALESCE(
                        blocked_since_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    status_changed_at = CASE
                        WHEN status IN ('completed', 'abandoned', 'blocked')
                        THEN status_changed_at
                        ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND node_id = ?
                  AND is_active = 1
                """,
                (repo_key, roadmap_issue_number, node_id),
            )
        return cursor.rowcount > 0

    def mark_roadmap_node_unblocked(
        self,
        *,
        roadmap_issue_number: int,
        node_id: str,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_nodes
                SET
                    status = 'issued',
                    blocked_since_at = NULL,
                    status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    last_progress_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND node_id = ?
                  AND is_active = 1
                  AND status = 'blocked'
                """,
                (repo_key, roadmap_issue_number, node_id),
            )
        return cursor.rowcount > 0

    def mark_roadmap_revision_requested(
        self,
        *,
        roadmap_issue_number: int,
        last_error: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = CASE
                        WHEN status = 'active'
                        THEN 'awaiting_revision_merge'
                        ELSE adjustment_state
                    END,
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = graph_version,
                    last_adjustment_basis_digest = NULL,
                    revision_requested_at = COALESCE(
                        revision_requested_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    last_error = COALESCE(?, last_error),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                """,
                (last_error, repo_key, roadmap_issue_number),
            )
        return cursor.rowcount > 0

    def mark_roadmap_revision_pending(
        self,
        *,
        roadmap_issue_number: int,
        claim_token: str,
        request_version: int,
        pr_number: int,
        pr_url: str,
        head_sha: str | None,
        last_error: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        if request_version < 1:
            raise ValueError("request_version must be >= 1")
        if pr_number < 1:
            raise ValueError("pr_number must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_state = 'awaiting_revision_merge',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = ?,
                    pending_revision_pr_number = ?,
                    pending_revision_pr_url = ?,
                    pending_revision_head_sha = ?,
                    last_adjustment_basis_digest = NULL,
                    revision_requested_at = COALESCE(
                        revision_requested_at,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    ),
                    last_error = COALESCE(?, last_error),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                  AND adjustment_state = 'evaluating'
                  AND adjustment_claim_token = ?
                """,
                (
                    request_version,
                    pr_number,
                    pr_url,
                    head_sha,
                    last_error,
                    repo_key,
                    roadmap_issue_number,
                    claim_token,
                ),
            )
        return cursor.rowcount > 0

    def set_roadmap_adjustment_request_version(
        self,
        *,
        roadmap_issue_number: int,
        request_version: int | None,
        repo_full_name: str | None = None,
    ) -> bool:
        if request_version is not None and request_version < 1:
            raise ValueError("request_version must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    adjustment_request_version = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                """,
                (request_version, repo_key, roadmap_issue_number),
            )
        return cursor.rowcount > 0

    def set_roadmap_pending_revision_pr(
        self,
        *,
        roadmap_issue_number: int,
        pr_number: int | None,
        pr_url: str | None,
        head_sha: str | None,
        repo_full_name: str | None = None,
    ) -> bool:
        if pr_number is not None and pr_number < 1:
            raise ValueError("pr_number must be >= 1")
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    pending_revision_pr_number = ?,
                    pending_revision_pr_url = ?,
                    pending_revision_head_sha = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                """,
                (pr_number, pr_url, head_sha, repo_key, roadmap_issue_number),
            )
            if cursor.rowcount > 0 and pr_number is not None:
                conn.execute(
                    """
                    DELETE FROM roadmap_revision_drafts
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (repo_key, roadmap_issue_number),
                )
        return cursor.rowcount > 0

    def set_roadmap_superseding_issue(
        self,
        *,
        roadmap_issue_number: int,
        superseding_roadmap_issue_number: int,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET superseding_roadmap_issue_number = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (superseding_roadmap_issue_number, repo_key, roadmap_issue_number),
            )
        return cursor.rowcount > 0

    def mark_roadmap_superseded(
        self,
        *,
        roadmap_issue_number: int,
        superseding_roadmap_issue_number: int,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    status = 'superseded',
                    superseding_roadmap_issue_number = ?,
                    adjustment_state = 'idle',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = NULL,
                    pending_revision_pr_number = NULL,
                    pending_revision_pr_url = NULL,
                    pending_revision_head_sha = NULL,
                    last_adjustment_basis_digest = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                """,
                (superseding_roadmap_issue_number, repo_key, roadmap_issue_number),
            )
            if cursor.rowcount > 0:
                conn.execute(
                    """
                    DELETE FROM roadmap_revision_drafts
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (repo_key, roadmap_issue_number),
                )
                conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        status = 'abandoned',
                        status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        claim_token = NULL,
                        claim_started_at = NULL
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND is_active = 1
                      AND status IN ('pending', 'issued', 'blocked')
                    """,
                    (repo_key, roadmap_issue_number),
                )
        return cursor.rowcount > 0

    def mark_roadmap_abandoned(
        self,
        *,
        roadmap_issue_number: int,
        last_error: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET
                    status = 'abandoned',
                    adjustment_state = 'idle',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = NULL,
                    pending_revision_pr_number = NULL,
                    pending_revision_pr_url = NULL,
                    pending_revision_head_sha = NULL,
                    last_adjustment_basis_digest = NULL,
                    last_error = COALESCE(?, last_error),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                """,
                (last_error, repo_key, roadmap_issue_number),
            )
            if cursor.rowcount > 0:
                conn.execute(
                    """
                    DELETE FROM roadmap_revision_drafts
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (repo_key, roadmap_issue_number),
                )
                conn.execute(
                    """
                    UPDATE roadmap_nodes
                    SET
                        status = 'abandoned',
                        status_changed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        claim_token = NULL,
                        claim_started_at = NULL
                    WHERE repo_full_name = ?
                      AND roadmap_issue_number = ?
                      AND is_active = 1
                      AND status IN ('pending', 'issued', 'blocked')
                    """,
                    (repo_key, roadmap_issue_number),
                )
        return cursor.rowcount > 0

    def mark_roadmap_completed(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET status = 'completed',
                    adjustment_state = 'idle',
                    adjustment_claim_token = NULL,
                    adjustment_started_at = NULL,
                    adjustment_request_version = NULL,
                    pending_revision_pr_number = NULL,
                    pending_revision_pr_url = NULL,
                    pending_revision_head_sha = NULL,
                    last_adjustment_basis_digest = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND status = 'active'
                """,
                (repo_key, roadmap_issue_number),
            )
            if cursor.rowcount > 0:
                conn.execute(
                    """
                    DELETE FROM roadmap_revision_drafts
                    WHERE repo_full_name = ? AND roadmap_issue_number = ?
                    """,
                    (repo_key, roadmap_issue_number),
                )
        return cursor.rowcount > 0

    def set_roadmap_last_error(
        self,
        *,
        roadmap_issue_number: int,
        error: str | None,
        repo_full_name: str | None = None,
    ) -> bool:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE roadmap_state
                SET last_error = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE repo_full_name = ? AND roadmap_issue_number = ?
                """,
                (error, repo_key, roadmap_issue_number),
            )
        return cursor.rowcount > 0

    def find_roadmap_by_child_issue(
        self, *, child_issue_number: int, repo_full_name: str | None = None
    ) -> RoadmapChildIssueLookup | None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    n.roadmap_issue_number,
                    n.node_id,
                    r.status
                FROM roadmap_nodes AS n
                INNER JOIN roadmap_state AS r
                    ON r.repo_full_name = n.repo_full_name
                   AND r.roadmap_issue_number = n.roadmap_issue_number
                WHERE n.repo_full_name = ?
                  AND n.child_issue_number = ?
                  AND n.is_active = 1
                ORDER BY n.roadmap_issue_number DESC, n.node_id ASC
                LIMIT 1
                """,
                (repo_key, child_issue_number),
            ).fetchone()
        if row is None:
            return None
        roadmap_issue_number, node_id, roadmap_status = row
        return RoadmapChildIssueLookup(
            repo_full_name=repo_key,
            roadmap_issue_number=int(roadmap_issue_number),
            node_id=str(node_id),
            roadmap_status=_parse_roadmap_status(roadmap_status),
        )

    def list_roadmap_status_snapshot(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> tuple[RoadmapStatusSnapshotRow, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    node_id,
                    kind,
                    status,
                    dependencies_json,
                    child_issue_number,
                    child_issue_url,
                    last_progress_at,
                    blocked_since_at
                FROM roadmap_nodes
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND is_active = 1
                ORDER BY node_id ASC
                """,
                (repo_key, roadmap_issue_number),
            ).fetchall()
        snapshots: list[RoadmapStatusSnapshotRow] = []
        for row in rows:
            (
                node_id,
                kind,
                status,
                dependencies_json,
                child_issue_number,
                child_issue_url,
                last_progress_at,
                blocked_since_at,
            ) = row
            dependencies = _parse_roadmap_dependencies_json(str(dependencies_json))
            dependency_summary = (
                ", ".join(f"{dep.node_id}:{dep.requires}" for dep in dependencies)
                if dependencies
                else "-"
            )
            snapshots.append(
                RoadmapStatusSnapshotRow(
                    repo_full_name=repo_key,
                    roadmap_issue_number=roadmap_issue_number,
                    node_id=str(node_id),
                    kind=_parse_roadmap_node_kind(kind),
                    status=_parse_roadmap_node_status(status),
                    dependency_summary=dependency_summary,
                    child_issue_number=int(child_issue_number)
                    if isinstance(child_issue_number, int)
                    else None,
                    child_issue_url=str(child_issue_url)
                    if isinstance(child_issue_url, str)
                    else None,
                    last_progress_at=str(last_progress_at)
                    if isinstance(last_progress_at, str)
                    else None,
                    blocked_since_at=str(blocked_since_at)
                    if isinstance(blocked_since_at, str)
                    else None,
                )
            )
        return tuple(snapshots)

    def list_roadmap_blockers_oldest_first(
        self, *, roadmap_issue_number: int, repo_full_name: str | None = None
    ) -> tuple[RoadmapBlockerRow, ...]:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    node_id,
                    status,
                    blocked_since_at,
                    child_issue_number,
                    child_issue_url,
                    last_progress_at
                FROM roadmap_nodes
                WHERE repo_full_name = ?
                  AND roadmap_issue_number = ?
                  AND is_active = 1
                  AND blocked_since_at IS NOT NULL
                ORDER BY blocked_since_at ASC, node_id ASC
                """,
                (repo_key, roadmap_issue_number),
            ).fetchall()
        return tuple(
            RoadmapBlockerRow(
                repo_full_name=repo_key,
                roadmap_issue_number=roadmap_issue_number,
                node_id=str(node_id),
                status=_parse_roadmap_node_status(status),
                blocked_since_at=str(blocked_since_at),
                child_issue_number=int(child_issue_number)
                if isinstance(child_issue_number, int)
                else None,
                child_issue_url=str(child_issue_url) if isinstance(child_issue_url, str) else None,
                last_progress_at=str(last_progress_at)
                if isinstance(last_progress_at, str)
                else None,
            )
            for (
                node_id,
                status,
                blocked_since_at,
                child_issue_number,
                child_issue_url,
                last_progress_at,
            ) in rows
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


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _transient_retry_delay_seconds(retry_count: int) -> int:
    if retry_count < 1:
        raise ValueError("retry_count must be >= 1")
    delay = _TRANSIENT_RETRY_INITIAL_DELAY_SECONDS * (2 ** (retry_count - 1))
    return min(delay, _TRANSIENT_RETRY_MAX_DELAY_SECONDS)


def _parse_pr_flake_state_row(row: tuple[object, ...]) -> PrFlakeState:
    if len(row) != 16:
        raise RuntimeError("Invalid pr_flake_state row width")
    (
        repo_full_name,
        pr_number,
        issue_number,
        head_sha,
        run_id,
        initial_run_updated_at,
        status,
        flake_issue_number,
        flake_issue_url,
        report_title,
        report_summary,
        report_excerpt,
        full_log_context_markdown,
        rerun_requested_at,
        created_at,
        updated_at,
    ) = row
    if not isinstance(repo_full_name, str):
        raise RuntimeError("Invalid repo_full_name value stored in pr_flake_state")
    for field_name, value in (
        ("pr_number", pr_number),
        ("issue_number", issue_number),
        ("run_id", run_id),
        ("flake_issue_number", flake_issue_number),
    ):
        if not isinstance(value, int):
            raise RuntimeError(f"Invalid {field_name} value stored in pr_flake_state")
    for field_name, value in (
        ("head_sha", head_sha),
        ("initial_run_updated_at", initial_run_updated_at),
        ("status", status),
        ("flake_issue_url", flake_issue_url),
        ("report_title", report_title),
        ("report_summary", report_summary),
        ("report_excerpt", report_excerpt),
        ("full_log_context_markdown", full_log_context_markdown),
        ("created_at", created_at),
        ("updated_at", updated_at),
    ):
        if not isinstance(value, str):
            raise RuntimeError(f"Invalid {field_name} value stored in pr_flake_state")
    if rerun_requested_at is not None and not isinstance(rerun_requested_at, str):
        raise RuntimeError("Invalid rerun_requested_at value stored in pr_flake_state")
    return PrFlakeState(
        repo_full_name=repo_full_name,
        pr_number=cast(int, pr_number),
        issue_number=cast(int, issue_number),
        head_sha=cast(str, head_sha),
        run_id=cast(int, run_id),
        initial_run_updated_at=cast(str, initial_run_updated_at),
        status=_parse_pr_flake_status(status),
        flake_issue_number=cast(int, flake_issue_number),
        flake_issue_url=cast(str, flake_issue_url),
        report_title=cast(str, report_title),
        report_summary=cast(str, report_summary),
        report_excerpt=cast(str, report_excerpt),
        full_log_context_markdown=cast(str, full_log_context_markdown),
        rerun_requested_at=rerun_requested_at,
        created_at=cast(str, created_at),
        updated_at=cast(str, updated_at),
    )


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
    if value not in {"unblock", "retry", "restart", "help", "invalid"}:
        raise RuntimeError(f"Unknown command value stored in operator_commands: {value}")
    return cast(OperatorCommandName, value)


def _parse_github_call_kind(value: object) -> GitHubCallKind:
    if not isinstance(value, str):
        raise RuntimeError("Invalid call_kind value stored in github_call_outbox")
    if value not in {"create_pull_request", "create_issue", "post_issue_comment"}:
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
    if value not in {"design_doc", "bugfix", "small_job", "roadmap", "implementation"}:
        raise RuntimeError(f"Unknown flow value stored in pre_pr_followup_state: {value}")
    return cast(PrePrFollowupFlow, value)


def _parse_roadmap_revision_row(
    row: tuple[object, ...], *, repo_full_name: str
) -> RoadmapRevisionRecord:
    if len(row) != 7:
        raise RuntimeError("Invalid roadmap_revisions row shape")
    (
        roadmap_issue_number,
        version,
        roadmap_pr_number,
        roadmap_doc_path,
        graph_path,
        graph_checksum,
        applied_at,
    ) = row
    if not isinstance(roadmap_issue_number, int):
        raise RuntimeError("Invalid roadmap_issue_number value stored in roadmap_revisions")
    if not isinstance(version, int):
        raise RuntimeError("Invalid version value stored in roadmap_revisions")
    if roadmap_pr_number is not None and not isinstance(roadmap_pr_number, int):
        raise RuntimeError("Invalid roadmap_pr_number value stored in roadmap_revisions")
    if not isinstance(roadmap_doc_path, str):
        raise RuntimeError("Invalid roadmap_doc_path value stored in roadmap_revisions")
    if not isinstance(graph_path, str):
        raise RuntimeError("Invalid graph_path value stored in roadmap_revisions")
    if not isinstance(graph_checksum, str):
        raise RuntimeError("Invalid graph_checksum value stored in roadmap_revisions")
    if not isinstance(applied_at, str):
        raise RuntimeError("Invalid applied_at value stored in roadmap_revisions")
    return RoadmapRevisionRecord(
        repo_full_name=repo_full_name,
        roadmap_issue_number=roadmap_issue_number,
        version=version,
        roadmap_pr_number=roadmap_pr_number,
        roadmap_doc_path=roadmap_doc_path,
        graph_path=graph_path,
        graph_checksum=graph_checksum,
        applied_at=applied_at,
    )


def _parse_roadmap_revision_draft_row(
    row: tuple[object, ...], *, repo_full_name: str
) -> RoadmapRevisionDraftRecord:
    if len(row) != 11:
        raise RuntimeError("Invalid roadmap_revision_drafts row shape")
    (
        roadmap_issue_number,
        request_version,
        summary,
        details,
        updated_roadmap_markdown,
        updated_canonical_graph_json,
        source_kind,
        ready_node_ids_json,
        request_reason,
        created_at,
        updated_at,
    ) = row
    if not isinstance(roadmap_issue_number, int):
        raise RuntimeError("Invalid roadmap_issue_number value stored in roadmap_revision_drafts")
    if not isinstance(request_version, int):
        raise RuntimeError("Invalid request_version value stored in roadmap_revision_drafts")
    if not isinstance(summary, str):
        raise RuntimeError("Invalid summary value stored in roadmap_revision_drafts")
    if not isinstance(details, str):
        raise RuntimeError("Invalid details value stored in roadmap_revision_drafts")
    if not isinstance(updated_roadmap_markdown, str):
        raise RuntimeError(
            "Invalid updated_roadmap_markdown value stored in roadmap_revision_drafts"
        )
    if not isinstance(updated_canonical_graph_json, str):
        raise RuntimeError(
            "Invalid updated_canonical_graph_json value stored in roadmap_revision_drafts"
        )
    if not isinstance(source_kind, str):
        raise RuntimeError("Invalid source_kind value stored in roadmap_revision_drafts")
    if ready_node_ids_json is not None and not isinstance(ready_node_ids_json, str):
        raise RuntimeError("Invalid ready_node_ids_json value stored in roadmap_revision_drafts")
    if request_reason is not None and not isinstance(request_reason, str):
        raise RuntimeError("Invalid request_reason value stored in roadmap_revision_drafts")
    if not isinstance(created_at, str):
        raise RuntimeError("Invalid created_at value stored in roadmap_revision_drafts")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in roadmap_revision_drafts")
    return RoadmapRevisionDraftRecord(
        repo_full_name=repo_full_name,
        roadmap_issue_number=roadmap_issue_number,
        request_version=request_version,
        summary=summary,
        details=details,
        updated_roadmap_markdown=updated_roadmap_markdown,
        updated_canonical_graph_json=updated_canonical_graph_json,
        source_kind=source_kind,
        ready_node_ids_json=ready_node_ids_json,
        request_reason=request_reason,
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse_roadmap_status(value: object) -> RoadmapStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in roadmap_state")
    if value not in {"active", "superseded", "abandoned", "completed"}:
        raise RuntimeError(f"Unknown status value stored in roadmap_state: {value}")
    return cast(RoadmapStatus, value)


def _parse_roadmap_adjustment_state(value: object) -> RoadmapAdjustmentState:
    if not isinstance(value, str):
        raise RuntimeError("Invalid adjustment_state value stored in roadmap_state")
    if value not in {"idle", "evaluating", "awaiting_revision_merge"}:
        raise RuntimeError(f"Unknown adjustment_state value stored in roadmap_state: {value}")
    return cast(RoadmapAdjustmentState, value)


def _parse_roadmap_node_status(value: object) -> RoadmapNodeStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in roadmap_nodes")
    if value not in {"pending", "issued", "completed", "blocked", "abandoned"}:
        raise RuntimeError(f"Unknown status value stored in roadmap_nodes: {value}")
    return cast(RoadmapNodeStatus, value)


def _parse_roadmap_node_kind(value: object) -> RoadmapNodeKind:
    if not isinstance(value, str):
        raise RuntimeError("Invalid kind value stored in roadmap_nodes")
    if value not in {"design_doc", "small_job", "roadmap"}:
        raise RuntimeError(f"Unknown kind value stored in roadmap_nodes: {value}")
    return cast(RoadmapNodeKind, value)


def _parse_roadmap_state_row(row: tuple[object, ...], *, repo_full_name: str) -> RoadmapStateRecord:
    if len(row) == 11:
        (
            roadmap_issue_number,
            roadmap_pr_number,
            roadmap_doc_path,
            graph_path,
            graph_checksum,
            status,
            parent_roadmap_issue_number,
            superseding_roadmap_issue_number,
            revision_requested_at,
            last_error,
            updated_at,
        ) = row
        graph_version = 1
        adjustment_state = "idle"
        adjustment_claim_token = None
        adjustment_started_at = None
        adjustment_request_version = None
        pending_revision_pr_number = None
        pending_revision_pr_url = None
        pending_revision_head_sha = None
        last_adjustment_basis_digest = None
    elif len(row) == 16:
        (
            roadmap_issue_number,
            roadmap_pr_number,
            roadmap_doc_path,
            graph_path,
            graph_checksum,
            graph_version,
            status,
            adjustment_state,
            adjustment_claim_token,
            adjustment_started_at,
            adjustment_request_version,
            parent_roadmap_issue_number,
            superseding_roadmap_issue_number,
            revision_requested_at,
            last_error,
            updated_at,
        ) = row
        pending_revision_pr_number = None
        pending_revision_pr_url = None
        pending_revision_head_sha = None
        last_adjustment_basis_digest = None
    elif len(row) == 19:
        (
            roadmap_issue_number,
            roadmap_pr_number,
            roadmap_doc_path,
            graph_path,
            graph_checksum,
            graph_version,
            status,
            adjustment_state,
            adjustment_claim_token,
            adjustment_started_at,
            adjustment_request_version,
            pending_revision_pr_number,
            pending_revision_pr_url,
            pending_revision_head_sha,
            parent_roadmap_issue_number,
            superseding_roadmap_issue_number,
            revision_requested_at,
            last_error,
            updated_at,
        ) = row
        last_adjustment_basis_digest = None
    elif len(row) == 20:
        (
            roadmap_issue_number,
            roadmap_pr_number,
            roadmap_doc_path,
            graph_path,
            graph_checksum,
            graph_version,
            status,
            adjustment_state,
            adjustment_claim_token,
            adjustment_started_at,
            adjustment_request_version,
            pending_revision_pr_number,
            pending_revision_pr_url,
            pending_revision_head_sha,
            last_adjustment_basis_digest,
            parent_roadmap_issue_number,
            superseding_roadmap_issue_number,
            revision_requested_at,
            last_error,
            updated_at,
        ) = row
    else:
        raise RuntimeError("Invalid roadmap_state row shape")
    if not isinstance(roadmap_issue_number, int):
        raise RuntimeError("Invalid roadmap_issue_number value stored in roadmap_state")
    if roadmap_pr_number is not None and not isinstance(roadmap_pr_number, int):
        raise RuntimeError("Invalid roadmap_pr_number value stored in roadmap_state")
    if not isinstance(roadmap_doc_path, str):
        raise RuntimeError("Invalid roadmap_doc_path value stored in roadmap_state")
    if not isinstance(graph_path, str):
        raise RuntimeError("Invalid graph_path value stored in roadmap_state")
    if not isinstance(graph_checksum, str):
        raise RuntimeError("Invalid graph_checksum value stored in roadmap_state")
    if not isinstance(graph_version, int):
        raise RuntimeError("Invalid graph_version value stored in roadmap_state")
    if adjustment_claim_token is not None and not isinstance(adjustment_claim_token, str):
        raise RuntimeError("Invalid adjustment_claim_token value stored in roadmap_state")
    if adjustment_started_at is not None and not isinstance(adjustment_started_at, str):
        raise RuntimeError("Invalid adjustment_started_at value stored in roadmap_state")
    if adjustment_request_version is not None and not isinstance(adjustment_request_version, int):
        raise RuntimeError("Invalid adjustment_request_version value stored in roadmap_state")
    if pending_revision_pr_number is not None and not isinstance(pending_revision_pr_number, int):
        raise RuntimeError("Invalid pending_revision_pr_number value stored in roadmap_state")
    if pending_revision_pr_url is not None and not isinstance(pending_revision_pr_url, str):
        raise RuntimeError("Invalid pending_revision_pr_url value stored in roadmap_state")
    if pending_revision_head_sha is not None and not isinstance(pending_revision_head_sha, str):
        raise RuntimeError("Invalid pending_revision_head_sha value stored in roadmap_state")
    if last_adjustment_basis_digest is not None and not isinstance(
        last_adjustment_basis_digest, str
    ):
        raise RuntimeError("Invalid last_adjustment_basis_digest value stored in roadmap_state")
    if parent_roadmap_issue_number is not None and not isinstance(parent_roadmap_issue_number, int):
        raise RuntimeError("Invalid parent_roadmap_issue_number value stored in roadmap_state")
    if superseding_roadmap_issue_number is not None and not isinstance(
        superseding_roadmap_issue_number, int
    ):
        raise RuntimeError("Invalid superseding_roadmap_issue_number value stored in roadmap_state")
    if revision_requested_at is not None and not isinstance(revision_requested_at, str):
        raise RuntimeError("Invalid revision_requested_at value stored in roadmap_state")
    if last_error is not None and not isinstance(last_error, str):
        raise RuntimeError("Invalid last_error value stored in roadmap_state")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in roadmap_state")
    return RoadmapStateRecord(
        repo_full_name=repo_full_name,
        roadmap_issue_number=roadmap_issue_number,
        roadmap_pr_number=roadmap_pr_number,
        roadmap_doc_path=roadmap_doc_path,
        graph_path=graph_path,
        graph_checksum=graph_checksum,
        graph_version=graph_version,
        status=_parse_roadmap_status(status),
        adjustment_state=_parse_roadmap_adjustment_state(adjustment_state),
        adjustment_claim_token=adjustment_claim_token,
        adjustment_started_at=adjustment_started_at,
        adjustment_request_version=adjustment_request_version,
        pending_revision_pr_number=pending_revision_pr_number,
        pending_revision_pr_url=pending_revision_pr_url,
        pending_revision_head_sha=pending_revision_head_sha,
        last_adjustment_basis_digest=last_adjustment_basis_digest,
        parent_roadmap_issue_number=parent_roadmap_issue_number,
        superseding_roadmap_issue_number=superseding_roadmap_issue_number,
        revision_requested_at=revision_requested_at,
        last_error=last_error,
        updated_at=updated_at,
    )


def _parse_roadmap_node_row(row: tuple[object, ...], *, repo_full_name: str) -> RoadmapNodeRecord:
    if len(row) == 16:
        (
            roadmap_issue_number,
            node_id,
            kind,
            title,
            body_markdown,
            dependencies_json,
            child_issue_number,
            child_issue_url,
            status,
            planned_at,
            implemented_at,
            status_changed_at,
            last_progress_at,
            blocked_since_at,
            claim_token,
            updated_at,
        ) = row
        introduced_in_version = 1
        retired_in_version = None
        is_active = 1
    elif len(row) == 19:
        (
            roadmap_issue_number,
            node_id,
            kind,
            title,
            body_markdown,
            dependencies_json,
            introduced_in_version,
            retired_in_version,
            is_active,
            child_issue_number,
            child_issue_url,
            status,
            planned_at,
            implemented_at,
            status_changed_at,
            last_progress_at,
            blocked_since_at,
            claim_token,
            updated_at,
        ) = row
    else:
        raise RuntimeError("Invalid roadmap_nodes row shape")
    if not isinstance(roadmap_issue_number, int):
        raise RuntimeError("Invalid roadmap_issue_number value stored in roadmap_nodes")
    if not isinstance(node_id, str):
        raise RuntimeError("Invalid node_id value stored in roadmap_nodes")
    if not isinstance(title, str):
        raise RuntimeError("Invalid title value stored in roadmap_nodes")
    if not isinstance(body_markdown, str):
        raise RuntimeError("Invalid body_markdown value stored in roadmap_nodes")
    if not isinstance(dependencies_json, str):
        raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
    if not isinstance(introduced_in_version, int):
        raise RuntimeError("Invalid introduced_in_version value stored in roadmap_nodes")
    if retired_in_version is not None and not isinstance(retired_in_version, int):
        raise RuntimeError("Invalid retired_in_version value stored in roadmap_nodes")
    if not isinstance(is_active, int) or is_active not in {0, 1}:
        raise RuntimeError("Invalid is_active value stored in roadmap_nodes")
    if child_issue_number is not None and not isinstance(child_issue_number, int):
        raise RuntimeError("Invalid child_issue_number value stored in roadmap_nodes")
    if child_issue_url is not None and not isinstance(child_issue_url, str):
        raise RuntimeError("Invalid child_issue_url value stored in roadmap_nodes")
    if planned_at is not None and not isinstance(planned_at, str):
        raise RuntimeError("Invalid planned_at value stored in roadmap_nodes")
    if implemented_at is not None and not isinstance(implemented_at, str):
        raise RuntimeError("Invalid implemented_at value stored in roadmap_nodes")
    if status_changed_at is not None and not isinstance(status_changed_at, str):
        raise RuntimeError("Invalid status_changed_at value stored in roadmap_nodes")
    if last_progress_at is not None and not isinstance(last_progress_at, str):
        raise RuntimeError("Invalid last_progress_at value stored in roadmap_nodes")
    if blocked_since_at is not None and not isinstance(blocked_since_at, str):
        raise RuntimeError("Invalid blocked_since_at value stored in roadmap_nodes")
    if claim_token is not None and not isinstance(claim_token, str):
        raise RuntimeError("Invalid claim_token value stored in roadmap_nodes")
    if not isinstance(updated_at, str):
        raise RuntimeError("Invalid updated_at value stored in roadmap_nodes")
    return RoadmapNodeRecord(
        repo_full_name=repo_full_name,
        roadmap_issue_number=roadmap_issue_number,
        node_id=node_id,
        kind=_parse_roadmap_node_kind(kind),
        title=title,
        body_markdown=body_markdown,
        dependencies_json=dependencies_json,
        introduced_in_version=introduced_in_version,
        retired_in_version=retired_in_version,
        is_active=bool(is_active),
        child_issue_number=child_issue_number,
        child_issue_url=child_issue_url,
        status=_parse_roadmap_node_status(status),
        planned_at=planned_at,
        implemented_at=implemented_at,
        status_changed_at=status_changed_at,
        last_progress_at=last_progress_at,
        blocked_since_at=blocked_since_at,
        claim_token=claim_token,
        updated_at=updated_at,
    )


def _parse_roadmap_dependencies_json(value: str) -> tuple[RoadmapDependencyState, ...]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes") from exc
    if not isinstance(payload, list):
        raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
    parsed: list[RoadmapDependencyState] = []
    for item in payload:
        if not isinstance(item, dict):
            raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
        if not all(isinstance(key, str) for key in item.keys()):
            raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
        dep_obj = cast(dict[str, object], item)
        node_id = dep_obj.get("node_id")
        requires = dep_obj.get("requires", "implemented")
        if not isinstance(node_id, str) or not node_id:
            raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
        if not isinstance(requires, str) or requires not in {"planned", "implemented"}:
            raise RuntimeError("Invalid dependencies_json value stored in roadmap_nodes")
        parsed.append(
            RoadmapDependencyState(
                node_id=node_id,
                requires=cast(RoadmapDependencyRequirement, requires),
            )
        )
    return tuple(parsed)


def _roadmap_dependencies_from_json(value: str) -> tuple[RoadmapDependency, ...]:
    return tuple(
        RoadmapDependency(node_id=dependency.node_id, requires=dependency.requires)
        for dependency in _parse_roadmap_dependencies_json(value)
    )


def _roadmap_node_dependencies_satisfied(
    *,
    dependencies: tuple[RoadmapDependencyState, ...],
    node_rows: dict[str, tuple[object, ...]],
) -> bool:
    for dependency in dependencies:
        dep_row = node_rows.get(dependency.node_id)
        if dep_row is None:
            return False
        dep_planned_at = dep_row[7] if isinstance(dep_row[7], str) else None
        dep_implemented_at = dep_row[8] if isinstance(dep_row[8], str) else None
        if dependency.requires == "planned":
            if dep_planned_at is None and dep_implemented_at is None:
                return False
            continue
        if dep_implemented_at is None:
            return False
    return True


def _roadmap_node_claim_is_stale(
    *,
    claim_token: str | None,
    claim_started_at: str | None,
    stale_before: str,
) -> bool:
    return claim_token is not None and (claim_started_at is None or claim_started_at < stale_before)


def _parse_github_comment_surface(value: object) -> GitHubCommentSurface:
    if not isinstance(value, str):
        raise RuntimeError("Invalid surface value stored in github_comment_poll_cursors")
    if value not in {
        "pr_review_comments",
        "pr_issue_comments",
        "pr_review_summaries",
        "issue_pre_pr_followups",
        "issue_post_pr_redirects",
        "issue_operator_commands",
    }:
        raise RuntimeError(f"Unknown surface value stored in github_comment_poll_cursors: {value}")
    return cast(GitHubCommentSurface, value)


def _parse_pr_flake_status(value: object) -> PrFlakeStatus:
    if not isinstance(value, str):
        raise RuntimeError("Invalid status value stored in pr_flake_state")
    if value not in {
        "awaiting_rerun_result",
        "resolved_after_rerun",
        "blocked_after_second_failure",
    }:
        raise RuntimeError(f"Unknown status value stored in pr_flake_state: {value}")
    return cast(PrFlakeStatus, value)


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
