from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from mergexo.agent_adapter import AgentSession
from mergexo.feedback_loop import FeedbackEventRecord
from mergexo.state import StateStore


def _get_row(
    db_path: Path, issue_number: int
) -> tuple[str, str | None, int | None, str | None, str | None]:
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


def test_state_store_transitions_and_feedback_tracking(tmp_path: Path) -> None:
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
    assert status == "awaiting_feedback"
    assert branch == "feature"
    assert pr_number == 9
    assert pr_url == "https://example/pr/9"
    assert error is None

    tracked = store.list_tracked_pull_requests()
    assert len(tracked) == 1
    assert tracked[0].pr_number == 9
    assert tracked[0].issue_number == 42
    assert tracked[0].branch == "feature"

    store.save_agent_session(issue_number=42, adapter="codex", thread_id="thread-1")
    assert store.get_agent_session(42) == ("codex", "thread-1")
    assert store.get_agent_session(999) is None


def test_feedback_event_ingest_and_finalize(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.mark_completed(7, "agent/design/7", 100, "https://example/pr/100")

    events = (
        FeedbackEventRecord(
            event_key="100:review:1:2026-02-21T00:00:00Z",
            pr_number=100,
            issue_number=7,
            kind="review",
            comment_id=1,
            updated_at="2026-02-21T00:00:00Z",
        ),
        FeedbackEventRecord(
            event_key="100:issue:2:2026-02-21T00:00:01Z",
            pr_number=100,
            issue_number=7,
            kind="issue",
            comment_id=2,
            updated_at="2026-02-21T00:00:01Z",
        ),
    )
    store.ingest_feedback_events(events)
    # Duplicate ingest should be ignored.
    store.ingest_feedback_events(events)

    pending = store.list_pending_feedback_events(100)
    assert {event.event_key for event in pending} == {
        "100:review:1:2026-02-21T00:00:00Z",
        "100:issue:2:2026-02-21T00:00:01Z",
    }

    store.finalize_feedback_turn(
        pr_number=100,
        issue_number=7,
        processed_event_keys=tuple(event.event_key for event in pending),
        session=AgentSession(adapter="codex", thread_id="thread-new"),
        head_sha="abc123",
    )

    assert store.list_pending_feedback_events(100) == ()
    assert store.get_agent_session(7) == ("codex", "thread-new")
    tracked = store.list_tracked_pull_requests()
    assert tracked[0].last_seen_head_sha == "abc123"

    conn = sqlite3.connect(db_path)
    try:
        processed_count = conn.execute(
            "SELECT COUNT(*) FROM feedback_events WHERE processed_at IS NOT NULL AND pr_number = 100"
        ).fetchone()
        assert processed_count is not None
        assert int(processed_count[0]) == 2
    finally:
        conn.close()


def test_mark_pr_status_updates_run_and_tracking_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.mark_completed(8, "agent/design/8", 101, "https://example/pr/101")

    store.mark_pr_status(pr_number=101, issue_number=8, status="merged", last_seen_head_sha="head1")
    tracked = store.list_tracked_pull_requests()
    assert tracked == ()
    status, *_ = _get_row(db_path, 8)
    assert status == "merged"


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
