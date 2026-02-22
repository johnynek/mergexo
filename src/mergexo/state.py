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


@dataclass(frozen=True)
class TrackedPullRequestState:
    pr_number: int
    issue_number: int
    branch: str
    status: str
    last_seen_head_sha: str | None


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


@dataclass(frozen=True)
class ImplementationCandidateState:
    issue_number: int
    design_branch: str
    design_pr_number: int | None
    design_pr_url: str | None


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issue_runs (
                    issue_number INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    branch TEXT,
                    pr_number INTEGER,
                    pr_url TEXT,
                    error TEXT,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_sessions (
                    issue_number INTEGER PRIMARY KEY,
                    adapter TEXT NOT NULL,
                    thread_id TEXT,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feedback_events (
                    event_key TEXT PRIMARY KEY,
                    pr_number INTEGER NOT NULL,
                    issue_number INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    comment_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    processed_at TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pr_feedback_state (
                    pr_number INTEGER PRIMARY KEY,
                    issue_number INTEGER NOT NULL,
                    branch TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_seen_head_sha TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_commands (
                    command_key TEXT PRIMARY KEY,
                    issue_number INTEGER NOT NULL,
                    pr_number INTEGER NULL,
                    comment_id INTEGER NOT NULL,
                    author_login TEXT NOT NULL,
                    command TEXT NOT NULL,
                    args_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
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
                    mode TEXT NOT NULL,
                    detail TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )

    def can_enqueue(self, issue_number: int) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM issue_runs WHERE issue_number = ?",
                (issue_number,),
            ).fetchone()
        return row is None

    def mark_running(self, issue_number: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(issue_number, status)
                VALUES(?, 'running')
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='running',
                    error=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number,),
            )

    def mark_completed(self, issue_number: int, branch: str, pr_number: int, pr_url: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(issue_number, status, branch, pr_number, pr_url)
                VALUES(?, 'awaiting_feedback', ?, ?, ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='awaiting_feedback',
                    branch=excluded.branch,
                    pr_number=excluded.pr_number,
                    pr_url=excluded.pr_url,
                    error=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, branch, pr_number, pr_url),
            )
            conn.execute(
                """
                INSERT INTO pr_feedback_state(pr_number, issue_number, branch, status)
                VALUES(?, ?, ?, 'awaiting_feedback')
                ON CONFLICT(pr_number) DO UPDATE SET
                    issue_number=excluded.issue_number,
                    branch=excluded.branch,
                    status='awaiting_feedback',
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (pr_number, issue_number, branch),
            )

    def mark_failed(self, issue_number: int, error: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO issue_runs(issue_number, status, error)
                VALUES(?, 'failed', ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='failed',
                    error=excluded.error,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, error),
            )

    def save_agent_session(self, *, issue_number: int, adapter: str, thread_id: str | None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_sessions(issue_number, adapter, thread_id)
                VALUES(?, ?, ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    adapter=excluded.adapter,
                    thread_id=excluded.thread_id,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, adapter, thread_id),
            )

    def get_agent_session(self, issue_number: int) -> tuple[str, str | None] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT adapter, thread_id FROM agent_sessions WHERE issue_number = ?",
                (issue_number,),
            ).fetchone()
        if row is None:
            return None
        adapter, thread_id = row
        if not isinstance(adapter, str):
            raise RuntimeError("Invalid adapter value stored in agent_sessions")
        if thread_id is not None and not isinstance(thread_id, str):
            raise RuntimeError("Invalid thread_id value stored in agent_sessions")
        return adapter, thread_id

    def get_operator_command(self, command_key: str) -> OperatorCommandRecord | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
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
                """,
                (command_key,),
            ).fetchone()
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
    ) -> OperatorCommandRecord:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO operator_commands(
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
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(command_key) DO UPDATE SET
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
                """,
                (command_key,),
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
    ) -> OperatorCommandRecord | None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE operator_commands
                SET status = ?,
                    result = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE command_key = ?
                """,
                (status, result, command_key),
            )
            row = conn.execute(
                """
                SELECT
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
                """,
                (command_key,),
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
    ) -> tuple[RuntimeOperationRecord, bool]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
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
                        mode = ?,
                        detail = NULL,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE op_name = 'restart'
                    """,
                    (requested_by, request_command_key, mode),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO runtime_operations(
                        op_name,
                        status,
                        requested_by,
                        request_command_key,
                        mode,
                        detail
                    )
                    VALUES('restart', 'pending', ?, ?, ?, NULL)
                    """,
                    (requested_by, request_command_key, mode),
                )
            updated = conn.execute(
                """
                SELECT
                    op_name,
                    status,
                    requested_by,
                    request_command_key,
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

    def list_tracked_pull_requests(self) -> tuple[TrackedPullRequestState, ...]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT pr_number, issue_number, branch, status, last_seen_head_sha
                FROM pr_feedback_state
                WHERE status = 'awaiting_feedback'
                ORDER BY updated_at ASC, pr_number ASC
                """
            ).fetchall()
        return tuple(
            TrackedPullRequestState(
                pr_number=int(pr_number),
                issue_number=int(issue_number),
                branch=str(branch),
                status=str(status),
                last_seen_head_sha=str(last_seen_head_sha)
                if isinstance(last_seen_head_sha, str)
                else None,
            )
            for pr_number, issue_number, branch, status, last_seen_head_sha in rows
        )

    def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.pr_number,
                    p.issue_number,
                    p.branch,
                    p.last_seen_head_sha,
                    r.error,
                    p.updated_at,
                    COUNT(e.event_key) AS pending_event_count
                FROM pr_feedback_state AS p
                LEFT JOIN issue_runs AS r
                    ON r.issue_number = p.issue_number
                LEFT JOIN feedback_events AS e
                    ON e.pr_number = p.pr_number
                   AND e.processed_at IS NULL
                WHERE p.status = 'blocked'
                GROUP BY
                    p.pr_number,
                    p.issue_number,
                    p.branch,
                    p.last_seen_head_sha,
                    r.error,
                    p.updated_at
                ORDER BY p.updated_at ASC, p.pr_number ASC
                """
            ).fetchall()
        return tuple(
            BlockedPullRequestState(
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
                pr_number,
                issue_number,
                branch,
                last_seen_head_sha,
                error,
                updated_at,
                pending_event_count,
            ) in rows
        )

    def list_implementation_candidates(self) -> tuple[ImplementationCandidateState, ...]:
        with self._lock, self._connect() as conn:
            # Lifecycle for a design doc promoted to implementation:
            # 1) Design PR merge marks the issue row as status='merged' with branch='agent/design/...'.
            # 2) Scheduler picks candidates from this exact state and marks status='running' when
            #    implementation work starts.
            # 3) Opening the implementation PR calls mark_completed(), transitioning to
            #    status='awaiting_feedback' with branch='agent/impl/...'.
            # 4) Feedback loop then transitions to terminal PR states like merged/closed/blocked.
            # Because only step (1) is eligible, selecting merged design branches here is intentional.
            rows = conn.execute(
                """
                SELECT issue_number, branch, pr_number, pr_url
                FROM issue_runs
                WHERE status = 'merged'
                  AND branch LIKE 'agent/design/%'
                ORDER BY updated_at ASC, issue_number ASC
                """
            ).fetchall()
        return tuple(
            ImplementationCandidateState(
                issue_number=int(issue_number),
                design_branch=str(branch),
                design_pr_number=int(pr_number) if isinstance(pr_number, int) else None,
                design_pr_url=str(pr_url) if isinstance(pr_url, str) else None,
            )
            for issue_number, branch, pr_number, pr_url in rows
        )

    def reset_blocked_pull_requests(
        self,
        *,
        pr_numbers: tuple[int, ...] | None = None,
        last_seen_head_sha_override: str | None = None,
    ) -> int:
        if pr_numbers is not None and not pr_numbers:
            return 0

        with self._lock, self._connect() as conn:
            where_clauses = ["status = 'blocked'"]
            params: list[object] = []
            if pr_numbers is not None:
                placeholders = ",".join("?" for _ in pr_numbers)
                where_clauses.append(f"pr_number IN ({placeholders})")
                params.extend(pr_numbers)

            rows = conn.execute(
                f"""
                SELECT pr_number, issue_number
                FROM pr_feedback_state
                WHERE {" AND ".join(where_clauses)}
                """,
                tuple(params),
            ).fetchall()
            if not rows:
                return 0

            blocked_pr_numbers = tuple(int(row[0]) for row in rows)
            issue_numbers = tuple(sorted({int(row[1]) for row in rows}))

            pr_placeholders = ",".join("?" for _ in blocked_pr_numbers)
            if last_seen_head_sha_override is None:
                conn.execute(
                    f"""
                    UPDATE pr_feedback_state
                    SET status = 'awaiting_feedback',
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE pr_number IN ({pr_placeholders})
                    """,
                    blocked_pr_numbers,
                )
            else:
                conn.execute(
                    f"""
                    UPDATE pr_feedback_state
                    SET status = 'awaiting_feedback',
                        last_seen_head_sha = ?,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE pr_number IN ({pr_placeholders})
                    """,
                    tuple([last_seen_head_sha_override, *blocked_pr_numbers]),
                )

            conn.executemany(
                """
                INSERT INTO issue_runs(issue_number, status, error)
                VALUES(?, 'awaiting_feedback', NULL)
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='awaiting_feedback',
                    error=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                [(issue_number,) for issue_number in issue_numbers],
            )

        return len(blocked_pr_numbers)

    def mark_pr_status(
        self,
        *,
        pr_number: int,
        issue_number: int,
        status: str,
        last_seen_head_sha: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE pr_feedback_state
                SET status = ?,
                    last_seen_head_sha = COALESCE(?, last_seen_head_sha),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE pr_number = ?
                """,
                (status, last_seen_head_sha, pr_number),
            )
            conn.execute(
                """
                INSERT INTO issue_runs(issue_number, status, error)
                VALUES(?, ?, ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    status=excluded.status,
                    error=excluded.error,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, status, error),
            )

    def ingest_feedback_events(self, events: Iterable[FeedbackEventRecord]) -> None:
        rows = tuple(events)
        if not rows:
            return
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO feedback_events(event_key, pr_number, issue_number, kind, comment_id, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                """,
                [
                    (
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

    def list_pending_feedback_events(self, pr_number: int) -> tuple[PendingFeedbackEvent, ...]:
        with self._lock, self._connect() as conn:
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
    ) -> None:
        with self._lock, self._connect() as conn:
            if processed_event_keys:
                placeholders = ",".join("?" for _ in processed_event_keys)
                conn.execute(
                    f"""
                    UPDATE feedback_events
                    SET processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE event_key IN ({placeholders})
                    """,
                    processed_event_keys,
                )

            conn.execute(
                """
                INSERT INTO agent_sessions(issue_number, adapter, thread_id)
                VALUES(?, ?, ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    adapter=excluded.adapter,
                    thread_id=excluded.thread_id,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, session.adapter, session.thread_id),
            )

            conn.execute(
                """
                UPDATE pr_feedback_state
                SET status='awaiting_feedback',
                    last_seen_head_sha=?,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE pr_number=?
                """,
                (head_sha, pr_number),
            )

            conn.execute(
                """
                INSERT INTO issue_runs(issue_number, status)
                VALUES(?, 'awaiting_feedback')
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='awaiting_feedback',
                    error=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number,),
            )


def _parse_operator_command_row(row: tuple[object, ...]) -> OperatorCommandRecord:
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
    )


def _parse_runtime_operation_row(row: tuple[object, ...]) -> RuntimeOperationRecord:
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
    if not isinstance(op_name, str):
        raise RuntimeError("Invalid op_name value stored in runtime_operations")
    if not isinstance(requested_by, str):
        raise RuntimeError("Invalid requested_by value stored in runtime_operations")
    if not isinstance(request_command_key, str):
        raise RuntimeError("Invalid request_command_key value stored in runtime_operations")
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
