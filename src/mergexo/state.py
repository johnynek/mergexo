from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import threading
from typing import cast

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
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    PRIMARY KEY (repo_full_name, issue_number)
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number),
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, branch, pr_number, pr_url),
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, pr_number, issue_number, branch),
            )

    def mark_failed(
        self, issue_number: int, error: str, *, repo_full_name: str | None = None
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(repo_full_name, issue_number, status, error)
                VALUES(?, ?, 'failed', ?)
                ON CONFLICT(repo_full_name, issue_number) DO UPDATE SET
                    status='failed',
                    error=excluded.error,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, error),
            )

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
                    ORDER BY updated_at ASC, repo_full_name ASC, pr_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT repo_full_name, pr_number, issue_number, branch, status, last_seen_head_sha
                    FROM pr_feedback_state
                    WHERE status = 'awaiting_feedback'
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
            if repo_key is None:
                rows = conn.execute(
                    """
                    SELECT repo_full_name, issue_number, branch, pr_number, pr_url
                    FROM issue_runs
                    WHERE status = 'merged'
                      AND branch LIKE 'agent/design/%'
                    ORDER BY updated_at ASC, repo_full_name ASC, issue_number ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT repo_full_name, issue_number, branch, pr_number, pr_url
                    FROM issue_runs
                    WHERE status = 'merged'
                      AND branch LIKE 'agent/design/%'
                      AND repo_full_name = ?
                    ORDER BY updated_at ASC, issue_number ASC
                    """,
                    (repo_key,),
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                issue_keys,
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
        repo_full_name: str | None = None,
    ) -> None:
        repo_key = _normalize_repo_full_name(repo_full_name)
        with self._lock, self._connect() as conn:
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number, status, error),
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
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (repo_key, issue_number),
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
