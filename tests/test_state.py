from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from mergexo.state import StateStore


def _get_row(db_path: Path, issue_number: int) -> tuple[str, str | None, int | None, str | None, str | None]:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, branch, pr_number, pr_url, error FROM issue_runs WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
        assert row is not None
        status, branch, pr_number, pr_url, error = row
        assert isinstance(status, str)
        assert branch is None or isinstance(branch, str)
        assert pr_number is None or isinstance(pr_number, int)
        assert pr_url is None or isinstance(pr_url, str)
        assert error is None or isinstance(error, str)
        return status, branch, pr_number, pr_url, error
    finally:
        conn.close()


def test_state_store_transitions(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    assert store.can_enqueue(42) is True

    store.mark_running(42)
    assert store.can_enqueue(42) is False
    assert _get_row(db_path, 42)[0] == "running"

    store.mark_failed(42, "boom")
    status, branch, pr_number, pr_url, error = _get_row(db_path, 42)
    assert status == "failed"
    assert branch is None
    assert pr_number is None
    assert pr_url is None
    assert error == "boom"

    store.mark_completed(42, "feature", 9, "https://example/pr/9")
    status, branch, pr_number, pr_url, error = _get_row(db_path, 42)
    assert status == "completed"
    assert branch == "feature"
    assert pr_number == 9
    assert pr_url == "https://example/pr/9"
    assert error is None

    store.save_agent_session(issue_number=42, adapter="codex", thread_id="thread-1")
    assert store.get_agent_session(42) == ("codex", "thread-1")
    assert store.get_agent_session(999) is None


def test_state_store_connection_rolls_back_on_error(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    with pytest.raises(RuntimeError, match="boom"):
        with store._connect() as conn:
            conn.execute("SELECT 1")
            raise RuntimeError("boom")

    # Connection remains usable after rollback path.
    assert store.can_enqueue(999) is True


def test_get_agent_session_rejects_invalid_adapter_type(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO agent_sessions(issue_number, adapter, thread_id) VALUES(?, ?, ?)",
            (1, sqlite3.Binary(b"\x01"), None),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="Invalid adapter"):
        store.get_agent_session(1)


def test_get_agent_session_rejects_invalid_thread_id_type(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO agent_sessions(issue_number, adapter, thread_id) VALUES(?, ?, ?)",
            (2, "codex", sqlite3.Binary(b"\x02")),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="Invalid thread_id"):
        store.get_agent_session(2)
