from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from mergexo.agent_adapter import AgentSession
from mergexo.feedback_loop import FeedbackEventRecord
from mergexo.state import (
    StateStore,
    _normalize_repo_full_name,
    _parse_operator_command_name,
    _parse_operator_command_row,
    _parse_operator_command_status,
    _parse_pre_pr_followup_flow,
    _parse_restart_mode,
    _parse_runtime_operation_row,
    _parse_runtime_operation_status,
    _table_columns,
)


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


def test_pre_pr_followup_state_and_issue_comment_cursors(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    store.mark_awaiting_issue_followup(
        issue_number=7,
        flow="small_job",
        branch="agent/small/7-worker",
        context_json='{"flow":"small_job"}',
        waiting_reason="small-job flow blocked: waiting on reporter context",
    )
    status, branch, pr_number, pr_url, error = _get_row(db_path, 7)
    assert status == "awaiting_issue_followup"
    assert branch == "agent/small/7-worker"
    assert pr_number is None
    assert pr_url is None
    assert error is not None

    followups = store.list_pre_pr_followups()
    assert len(followups) == 1
    followup = followups[0]
    assert followup.issue_number == 7
    assert followup.flow == "small_job"
    assert followup.context_json == '{"flow":"small_job"}'

    cursor0 = store.get_issue_comment_cursor(7)
    assert cursor0.pre_pr_last_consumed_comment_id == 0
    assert cursor0.post_pr_last_redirected_comment_id == 0

    cursor1 = store.advance_pre_pr_last_consumed_comment_id(issue_number=7, comment_id=11)
    assert cursor1.pre_pr_last_consumed_comment_id == 11
    cursor2 = store.advance_pre_pr_last_consumed_comment_id(issue_number=7, comment_id=9)
    assert cursor2.pre_pr_last_consumed_comment_id == 11

    cursor3 = store.advance_post_pr_last_redirected_comment_id(issue_number=7, comment_id=21)
    assert cursor3.post_pr_last_redirected_comment_id == 21
    cursor4 = store.advance_post_pr_last_redirected_comment_id(issue_number=7, comment_id=20)
    assert cursor4.post_pr_last_redirected_comment_id == 21

    store.clear_pre_pr_followup_state(7)
    assert store.list_pre_pr_followups() == ()


def test_list_legacy_failed_issue_runs_without_pr_filters_rows(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")

    store.mark_failed(1, "bugfix flow blocked: waiting for repro")
    store.mark_completed(2, "agent/design/2", 101, "https://example/pr/101")
    store.mark_failed(2, "small-job flow blocked: waiting for context")

    legacy = store.list_legacy_failed_issue_runs_without_pr()
    assert len(legacy) == 1
    assert legacy[0].issue_number == 1
    assert "flow blocked" in (legacy[0].error or "")


def test_list_legacy_running_issue_runs_without_pr_filters_rows(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")

    store.mark_awaiting_issue_followup(
        issue_number=1,
        flow="design_doc",
        branch="agent/design/1-foo",
        context_json='{"flow":"design_doc"}',
        waiting_reason="temporary",
    )
    store.mark_running(1)

    store.mark_completed(2, "agent/design/2-bar", 101, "https://example/pr/101")
    store.mark_running(2)

    running = store.list_legacy_running_issue_runs_without_pr()
    assert len(running) == 1
    assert running[0].issue_number == 1
    assert running[0].branch == "agent/design/1-foo"


def test_mark_pr_status_updates_run_and_tracking_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.mark_completed(8, "agent/design/8", 101, "https://example/pr/101")

    store.mark_pr_status(pr_number=101, issue_number=8, status="merged", last_seen_head_sha="head1")
    tracked = store.list_tracked_pull_requests()
    assert tracked == ()
    status, *_ = _get_row(db_path, 8)
    assert status == "merged"


def test_list_implementation_candidates_reads_merged_design_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.mark_completed(8, "agent/design/8-foo", 101, "https://example/pr/101")
    store.mark_pr_status(pr_number=101, issue_number=8, status="merged", last_seen_head_sha="head1")

    # Non-design merged runs should not be considered implementation candidates.
    store.mark_completed(9, "agent/bugfix/9-bar", 102, "https://example/pr/102")
    store.mark_pr_status(pr_number=102, issue_number=9, status="merged", last_seen_head_sha="head2")

    candidates = store.list_implementation_candidates()
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.issue_number == 8
    assert candidate.design_branch == "agent/design/8-foo"
    assert candidate.design_pr_number == 101
    assert candidate.design_pr_url == "https://example/pr/101"


def test_list_implementation_candidates_repo_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.mark_completed(
        8,
        "agent/design/8-foo",
        101,
        "https://example/pr/101",
        repo_full_name="o/repo-a",
    )
    store.mark_pr_status(
        pr_number=101,
        issue_number=8,
        status="merged",
        last_seen_head_sha="head1",
        repo_full_name="o/repo-a",
    )
    store.mark_completed(
        9,
        "agent/design/9-bar",
        102,
        "https://example/pr/102",
        repo_full_name="o/repo-b",
    )
    store.mark_pr_status(
        pr_number=102,
        issue_number=9,
        status="merged",
        last_seen_head_sha="head2",
        repo_full_name="o/repo-b",
    )

    repo_a = store.list_implementation_candidates(repo_full_name="o/repo-a")
    assert len(repo_a) == 1
    assert repo_a[0].repo_full_name == "o/repo-a"


def test_list_blocked_and_reset_pull_requests(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    store.mark_completed(8, "agent/design/8", 101, "https://example/pr/101")
    store.mark_pr_status(
        pr_number=101,
        issue_number=8,
        status="blocked",
        last_seen_head_sha="head-1",
        error="resume failed",
    )

    store.mark_completed(9, "agent/design/9", 102, "https://example/pr/102")
    store.mark_pr_status(
        pr_number=102,
        issue_number=9,
        status="blocked",
        last_seen_head_sha="head-2",
        error="missing session",
    )

    store.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:review:1:2026-02-22T00:00:00Z",
                pr_number=101,
                issue_number=8,
                kind="review",
                comment_id=1,
                updated_at="2026-02-22T00:00:00Z",
            ),
            FeedbackEventRecord(
                event_key="101:issue:2:2026-02-22T00:01:00Z",
                pr_number=101,
                issue_number=8,
                kind="issue",
                comment_id=2,
                updated_at="2026-02-22T00:01:00Z",
            ),
        )
    )
    store.finalize_feedback_turn(
        pr_number=101,
        issue_number=8,
        processed_event_keys=("101:issue:2:2026-02-22T00:01:00Z",),
        session=AgentSession(adapter="codex", thread_id="thread-1"),
        head_sha="head-1",
    )
    store.mark_pr_status(
        pr_number=101,
        issue_number=8,
        status="blocked",
        last_seen_head_sha="head-1",
        error="resume failed",
    )

    blocked = store.list_blocked_pull_requests()
    blocked_by_pr = {item.pr_number: item for item in blocked}
    assert set(blocked_by_pr) == {101, 102}
    assert blocked_by_pr[101].error == "resume failed"
    assert blocked_by_pr[101].pending_event_count == 1
    assert blocked_by_pr[102].pending_event_count == 0

    reset_count = store.reset_blocked_pull_requests(pr_numbers=(101, 999))
    assert reset_count == 1

    blocked_after_single_reset = store.list_blocked_pull_requests()
    assert [item.pr_number for item in blocked_after_single_reset] == [102]

    tracked_after_single_reset = {
        item.pr_number: item for item in store.list_tracked_pull_requests()
    }
    assert tracked_after_single_reset[101].last_seen_head_sha == "head-1"

    pending_after_reset = store.list_pending_feedback_events(101)
    assert len(pending_after_reset) == 1
    assert pending_after_reset[0].event_key == "101:review:1:2026-02-22T00:00:00Z"

    status, *_rest, error = _get_row(db_path, 8)
    assert status == "awaiting_feedback"
    assert error is None

    reset_all_count = store.reset_blocked_pull_requests()
    assert reset_all_count == 1
    assert store.list_blocked_pull_requests() == ()


def test_reset_blocked_pull_requests_with_head_override_updates_last_seen(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    store.mark_completed(8, "agent/design/8", 101, "https://example/pr/101")
    store.mark_pr_status(
        pr_number=101,
        issue_number=8,
        status="blocked",
        last_seen_head_sha="head-old",
        error="rewrite detected",
    )

    store.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:review:1:2026-02-22T00:00:00Z",
                pr_number=101,
                issue_number=8,
                kind="review",
                comment_id=1,
                updated_at="2026-02-22T00:00:00Z",
            ),
        )
    )

    reset_count = store.reset_blocked_pull_requests(
        pr_numbers=(101,),
        last_seen_head_sha_override="head-new",
    )
    assert reset_count == 1

    tracked = store.list_tracked_pull_requests()
    assert len(tracked) == 1
    assert tracked[0].last_seen_head_sha == "head-new"
    pending = store.list_pending_feedback_events(101)
    assert len(pending) == 1
    assert pending[0].event_key == "101:review:1:2026-02-22T00:00:00Z"


def test_reset_blocked_pull_requests_noop_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    assert store.reset_blocked_pull_requests(pr_numbers=()) == 0
    assert store.reset_blocked_pull_requests(pr_numbers=(999,)) == 0


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
            "INSERT INTO agent_sessions(repo_full_name, issue_number, adapter, thread_id) VALUES(?, ?, ?, ?)",
            ("__single_repo__", 1, sqlite3.Binary(b"\x01"), None),
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
            "INSERT INTO agent_sessions(repo_full_name, issue_number, adapter, thread_id) VALUES(?, ?, ?, ?)",
            ("__single_repo__", 2, "codex", sqlite3.Binary(b"\x02")),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="Invalid thread_id"):
        store.get_agent_session(2)


def test_get_agent_session_requires_repo_when_issue_number_collides(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.save_agent_session(
        issue_number=5, adapter="codex", thread_id="thread-a", repo_full_name="o/a"
    )
    store.save_agent_session(
        issue_number=5, adapter="codex", thread_id="thread-b", repo_full_name="o/b"
    )
    with pytest.raises(RuntimeError, match="specify repo_full_name"):
        store.get_agent_session(5)


def test_get_issue_comment_cursor_rejects_invalid_column_types(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.advance_pre_pr_last_consumed_comment_id(issue_number=7, comment_id=1)

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        conn.execute(
            "UPDATE issue_comment_cursors SET pre_pr_last_consumed_comment_id = ? WHERE issue_number = ?",
            (sqlite3.Binary(b"\x01"), 7),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="pre_pr_last_consumed_comment_id"):
        store.get_issue_comment_cursor(7)

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        conn.execute(
            "UPDATE issue_comment_cursors SET pre_pr_last_consumed_comment_id = ?, post_pr_last_redirected_comment_id = ? WHERE issue_number = ?",
            (1, sqlite3.Binary(b"\x02"), 7),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="post_pr_last_redirected_comment_id"):
        store.get_issue_comment_cursor(7)

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        conn.execute(
            "UPDATE issue_comment_cursors SET post_pr_last_redirected_comment_id = ?, updated_at = ? WHERE issue_number = ?",
            (2, sqlite3.Binary(b"\x03"), 7),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="updated_at"):
        store.get_issue_comment_cursor(7)


def test_operator_commands_and_runtime_operations(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    command = store.record_operator_command(
        command_key="99:100:2026-02-22T10:00:00Z",
        issue_number=99,
        pr_number=101,
        comment_id=100,
        author_login="alice",
        command="unblock",
        args_json='{"normalized_command":"/mergexo unblock"}',
        status="applied",
        result="ok",
    )
    assert command.command_key == "99:100:2026-02-22T10:00:00Z"
    assert command.pr_number == 101
    assert command.command == "unblock"

    fetched = store.get_operator_command("99:100:2026-02-22T10:00:00Z")
    assert fetched is not None
    assert fetched.result == "ok"

    updated = store.update_operator_command_result(
        command_key="99:100:2026-02-22T10:00:00Z",
        status="failed",
        result="nope",
    )
    assert updated is not None
    assert updated.status == "failed"
    assert updated.result == "nope"
    assert (
        store.update_operator_command_result(command_key="missing", status="failed", result="x")
        is None
    )

    op, created = store.request_runtime_restart(
        requested_by="alice",
        request_command_key="99:100:2026-02-22T10:00:00Z",
        mode="git_checkout",
    )
    assert created is True
    assert op.status == "pending"
    assert op.mode == "git_checkout"
    assert op.request_command_key == "99:100:2026-02-22T10:00:00Z"
    assert op.request_repo_full_name == "__single_repo__"

    op_again, created_again = store.request_runtime_restart(
        requested_by="bob",
        request_command_key="99:101:2026-02-22T10:01:00Z",
        mode="pypi",
    )
    assert created_again is False
    assert op_again.requested_by == "alice"
    assert op_again.mode == "git_checkout"

    running = store.set_runtime_operation_status(
        op_name="restart",
        status="running",
        detail="draining done",
    )
    assert running is not None
    assert running.status == "running"
    assert running.detail == "draining done"

    fetched_running = store.get_runtime_operation("restart")
    assert fetched_running is not None
    assert fetched_running.status == "running"

    assert (
        store.set_runtime_operation_status(op_name="missing", status="failed", detail="x") is None
    )

    completed, created_completed = store.request_runtime_restart(
        requested_by="carol",
        request_command_key="99:102:2026-02-22T10:02:00Z",
        mode="pypi",
    )
    # running restarts are single-flight and collapse.
    assert created_completed is False
    assert completed.status == "running"

    store.set_runtime_operation_status(
        op_name="restart",
        status="completed",
        detail="done",
    )
    refreshed, created_refreshed = store.request_runtime_restart(
        requested_by="dana",
        request_command_key="99:103:2026-02-22T10:03:00Z",
        mode="pypi",
    )
    assert created_refreshed is True
    assert refreshed.status == "pending"
    assert refreshed.mode == "pypi"
    assert refreshed.requested_by == "dana"


def test_operator_and_runtime_row_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO operator_commands(
                repo_full_name, command_key, issue_number, pr_number, comment_id, author_login,
                command, args_json, status, result
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "__single_repo__",
                "k",
                1,
                None,
                2,
                "alice",
                "bad-command",
                "{}",
                "applied",
                "ok",
            ),
        )
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
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            ("bad-op", "pending", "alice", "k", "__single_repo__", "bad-mode", None),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="Unknown command value"):
        store.get_operator_command("k")

    with pytest.raises(RuntimeError, match="Unknown mode value"):
        store.get_runtime_operation("bad-op")


def test_runtime_operation_none_and_disappearing_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    assert store.get_runtime_operation("restart") is None

    class FakeCursor:
        def __init__(self, row: object) -> None:
            self._row = row

        def fetchone(self) -> object:
            return self._row

    class FakeConn:
        def __init__(self) -> None:
            self.select_count = 0

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> FakeCursor:
            _ = params
            if "FROM operator_commands" in sql and "SELECT" in sql:
                return FakeCursor(None)
            if "FROM runtime_operations" in sql and "SELECT" in sql:
                self.select_count += 1
                if self.select_count == 1:
                    return FakeCursor(None)
                return FakeCursor(None)
            return FakeCursor(None)

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    from contextlib import contextmanager

    @contextmanager
    def fake_connect():  # type: ignore[no-untyped-def]
        yield FakeConn()

    monkeypatch.setattr(store, "_connect", fake_connect)

    with pytest.raises(RuntimeError, match="operator_commands row disappeared"):
        store.record_operator_command(
            command_key="k",
            issue_number=1,
            pr_number=None,
            comment_id=1,
            author_login="alice",
            command="help",
            args_json="{}",
            status="applied",
            result="ok",
        )

    with pytest.raises(RuntimeError, match="runtime restart row disappeared"):
        store.request_runtime_restart(
            requested_by="alice",
            request_command_key="k",
            mode="git_checkout",
        )


def test_update_operator_command_result_returns_none_when_row_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = StateStore(tmp_path / "state.db")

    row = (
        "__single_repo__",
        "k",
        1,
        None,
        2,
        "alice",
        "help",
        "{}",
        "applied",
        "ok",
        "t1",
        "t2",
    )
    monkeypatch.setattr(
        "mergexo.state._select_operator_command_row",
        lambda **kwargs: row,  # type: ignore[no-untyped-def]
    )
    updated = store.update_operator_command_result(command_key="k", status="failed", result="nope")
    assert updated is None


def test_parse_operator_command_row_validations() -> None:
    valid = ("k", 1, None, 2, "alice", "help", "{}", "applied", "ok", "t1", "t2")

    with pytest.raises(RuntimeError, match="command_key"):
        _parse_operator_command_row((1, *valid[1:]))
    with pytest.raises(RuntimeError, match="issue_number"):
        _parse_operator_command_row((valid[0], "1", *valid[2:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="pr_number"):
        _parse_operator_command_row((valid[0], valid[1], "bad", *valid[3:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="comment_id"):
        _parse_operator_command_row((valid[0], valid[1], valid[2], "x", *valid[4:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="author_login"):
        _parse_operator_command_row((valid[0], valid[1], valid[2], valid[3], 4, *valid[5:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="args_json"):
        _parse_operator_command_row(
            (valid[0], valid[1], valid[2], valid[3], valid[4], valid[5], 7, *valid[7:])
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="result"):
        _parse_operator_command_row(
            (
                valid[0],
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                valid[5],
                valid[6],
                valid[7],
                8,
                *valid[9:],
            )
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="created_at"):
        _parse_operator_command_row(
            (
                valid[0],
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                valid[5],
                valid[6],
                valid[7],
                valid[8],
                9,
                valid[10],
            )
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="updated_at"):
        _parse_operator_command_row(
            (
                valid[0],
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                valid[5],
                valid[6],
                valid[7],
                valid[8],
                valid[9],
                10,
            )
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="Invalid operator_commands row width"):
        _parse_operator_command_row(())
    with pytest.raises(RuntimeError, match="Invalid repo_full_name"):
        _parse_operator_command_row(
            (1, "k", 1, None, 2, "alice", "help", "{}", "applied", "ok", "t1", "t2")
        )


def test_parse_runtime_operation_row_validations() -> None:
    valid = ("restart", "pending", "alice", "k", "git_checkout", None, "t1", "t2")

    with pytest.raises(RuntimeError, match="op_name"):
        _parse_runtime_operation_row((1, *valid[1:]))
    with pytest.raises(RuntimeError, match="requested_by"):
        _parse_runtime_operation_row((valid[0], valid[1], 2, *valid[3:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="request_command_key"):
        _parse_runtime_operation_row((valid[0], valid[1], valid[2], 3, *valid[4:]))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="detail"):
        _parse_runtime_operation_row(
            (valid[0], valid[1], valid[2], valid[3], valid[4], 4, *valid[6:])
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="created_at"):
        _parse_runtime_operation_row(
            (valid[0], valid[1], valid[2], valid[3], valid[4], valid[5], 5, valid[7])
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="updated_at"):
        _parse_runtime_operation_row(
            (valid[0], valid[1], valid[2], valid[3], valid[4], valid[5], valid[6], 6)
        )  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="Invalid runtime_operations row width"):
        _parse_runtime_operation_row(())
    with pytest.raises(RuntimeError, match="Invalid request_repo_full_name"):
        _parse_runtime_operation_row(
            ("restart", "pending", "alice", "k", 1, "git_checkout", None, "t1", "t2")
        )


def test_parse_enum_helpers_validate_types_and_values() -> None:
    with pytest.raises(RuntimeError, match="Invalid command value"):
        _parse_operator_command_name(1)
    with pytest.raises(RuntimeError, match="Unknown command value"):
        _parse_operator_command_name("other")

    with pytest.raises(RuntimeError, match="Invalid status value"):
        _parse_operator_command_status(1)
    with pytest.raises(RuntimeError, match="Unknown status value"):
        _parse_operator_command_status("other")

    with pytest.raises(RuntimeError, match="Invalid status value"):
        _parse_runtime_operation_status(1)
    with pytest.raises(RuntimeError, match="Unknown status value"):
        _parse_runtime_operation_status("other")

    with pytest.raises(RuntimeError, match="Invalid mode value"):
        _parse_restart_mode(1)
    with pytest.raises(RuntimeError, match="Unknown mode value"):
        _parse_restart_mode("other")

    with pytest.raises(RuntimeError, match="Invalid flow value"):
        _parse_pre_pr_followup_flow(1)
    with pytest.raises(RuntimeError, match="Unknown flow value"):
        _parse_pre_pr_followup_flow("other")


def test_state_store_repo_scoped_isolation(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    store.mark_running(7, repo_full_name="o/repo-a")
    store.mark_running(7, repo_full_name="o/repo-b")
    assert store.can_enqueue(7, repo_full_name="o/repo-a") is False
    assert store.can_enqueue(7, repo_full_name="o/repo-b") is False

    store.mark_completed(
        7, "agent/design/7-a", 101, "https://example/pr/101", repo_full_name="o/repo-a"
    )
    store.mark_completed(
        7, "agent/design/7-b", 101, "https://example/pr/101", repo_full_name="o/repo-b"
    )

    tracked_a = store.list_tracked_pull_requests(repo_full_name="o/repo-a")
    tracked_b = store.list_tracked_pull_requests(repo_full_name="o/repo-b")
    assert tracked_a[0].repo_full_name == "o/repo-a"
    assert tracked_b[0].repo_full_name == "o/repo-b"


def test_get_operator_command_requires_repo_when_command_key_collides(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.record_operator_command(
        command_key="k",
        issue_number=1,
        pr_number=None,
        comment_id=10,
        author_login="alice",
        command="help",
        args_json="{}",
        status="applied",
        result="ok",
        repo_full_name="o/a",
    )
    store.record_operator_command(
        command_key="k",
        issue_number=2,
        pr_number=None,
        comment_id=11,
        author_login="bob",
        command="help",
        args_json="{}",
        status="applied",
        result="ok",
        repo_full_name="o/b",
    )
    with pytest.raises(RuntimeError, match="specify repo_full_name"):
        store.get_operator_command("k")


def test_state_store_rejects_legacy_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE issue_runs (
                issue_number INTEGER PRIMARY KEY,
                status TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="legacy state schema"):
        StateStore(db_path)


def test_state_store_rejects_legacy_runtime_operation_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE runtime_operations (
                op_name TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                request_command_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                detail TEXT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="legacy state schema"):
        StateStore(db_path)


def test_table_columns_skips_short_rows() -> None:
    class FakeCursor:
        def fetchall(self) -> list[tuple[object, ...]]:
            return [(0,), (0, "col_a"), (0, 3), (0, "col_b")]

    class FakeConn:
        def execute(self, sql: str) -> FakeCursor:
            assert "PRAGMA table_info" in sql
            return FakeCursor()

    columns = _table_columns(FakeConn(), "x")  # type: ignore[arg-type]
    assert columns == ("col_a", "col_b")


def test_normalize_repo_full_name_blank_defaults_to_single_repo() -> None:
    assert _normalize_repo_full_name(None) == "__single_repo__"
    assert _normalize_repo_full_name("   ") == "__single_repo__"
