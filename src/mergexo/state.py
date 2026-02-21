from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading


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

    def can_enqueue(self, issue_number: int) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM issue_runs WHERE issue_number = ?", (issue_number,)
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
                VALUES(?, 'completed', ?, ?, ?)
                ON CONFLICT(issue_number) DO UPDATE SET
                    status='completed',
                    branch=excluded.branch,
                    pr_number=excluded.pr_number,
                    pr_url=excluded.pr_url,
                    error=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                """,
                (issue_number, branch, pr_number, pr_url),
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
