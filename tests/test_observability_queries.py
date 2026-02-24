from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mergexo.feedback_loop import FeedbackEventRecord
from mergexo import observability_queries as oq
from mergexo.state import StateStore


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _seed_state(store: StateStore) -> None:
    now = datetime.now(timezone.utc)

    store.record_agent_run_start(
        run_id="run-completed-a",
        run_kind="issue_flow",
        issue_number=1,
        pr_number=101,
        flow="design_doc",
        branch="agent/design/1",
        started_at=_iso(now - timedelta(minutes=30)),
        repo_full_name="o/repo-a",
    )
    store.finish_agent_run(
        run_id="run-completed-a",
        terminal_status="completed",
        finished_at=_iso(now - timedelta(minutes=28, seconds=20)),
    )

    store.record_agent_run_start(
        run_id="run-failed-a",
        run_kind="issue_flow",
        issue_number=2,
        pr_number=None,
        flow="bugfix",
        branch="agent/bugfix/2",
        started_at=_iso(now - timedelta(minutes=15)),
        repo_full_name="o/repo-a",
    )
    store.finish_agent_run(
        run_id="run-failed-a",
        terminal_status="failed",
        failure_class="agent_error",
        error="boom",
        finished_at=_iso(now - timedelta(minutes=14)),
    )

    store.record_agent_run_start(
        run_id="run-blocked-b",
        run_kind="feedback_turn",
        issue_number=3,
        pr_number=301,
        flow=None,
        branch="agent/design/3",
        started_at=_iso(now - timedelta(minutes=6)),
        repo_full_name="o/repo-b",
    )
    store.finish_agent_run(
        run_id="run-blocked-b",
        terminal_status="blocked",
        failure_class="policy_block",
        error="waiting on human",
        finished_at=_iso(now - timedelta(minutes=5, seconds=30)),
    )

    store.record_agent_run_start(
        run_id="run-interrupted-b",
        run_kind="issue_flow",
        issue_number=4,
        pr_number=None,
        flow="small_job",
        branch="agent/small/4",
        started_at=_iso(now - timedelta(days=10)),
        repo_full_name="o/repo-b",
    )
    store.finish_agent_run(
        run_id="run-interrupted-b",
        terminal_status="interrupted",
        failure_class="unknown",
        error="restart",
        finished_at=_iso(now - timedelta(days=10) + timedelta(seconds=40)),
    )

    store.record_agent_run_start(
        run_id="run-active-a",
        run_kind="pre_pr_followup",
        issue_number=5,
        pr_number=None,
        flow="small_job",
        branch="agent/small/5",
        started_at=_iso(now - timedelta(minutes=2)),
        repo_full_name="o/repo-a",
    )

    store.mark_completed(
        10, "agent/design/10", 1010, "https://example/pr/1010", repo_full_name="o/repo-a"
    )
    store.mark_pr_status(
        pr_number=1010,
        issue_number=10,
        status="blocked",
        error="rewrite",
        repo_full_name="o/repo-a",
    )
    store.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="1010:review:1:2026-02-24T00:00:00Z",
                pr_number=1010,
                issue_number=10,
                kind="review",
                comment_id=1,
                updated_at="2026-02-24T00:00:00Z",
            ),
        ),
        repo_full_name="o/repo-a",
    )
    store.mark_completed(
        11, "agent/design/11", 1111, "https://example/pr/1111", repo_full_name="o/repo-b"
    )
    store.mark_awaiting_issue_followup(
        issue_number=12,
        flow="small_job",
        branch="agent/small/12",
        context_json='{"source":"issue_comment"}',
        waiting_reason="waiting for reporter clarification",
        repo_full_name="o/repo-b",
    )


def test_observability_queries_end_to_end(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    _seed_state(store)

    overview = oq.load_overview(db_path, window="24h")
    assert overview.active_agents == 1
    assert overview.blocked_prs == 2
    assert overview.tracked_prs == 1
    assert overview.failures == 1
    assert overview.mean_runtime_seconds > 0
    assert overview.stddev_runtime_seconds >= 0

    active_rows = oq.load_active_agents(db_path, limit=20)
    assert len(active_rows) == 1
    assert active_rows[0].run_id == "run-active-a"
    assert active_rows[0].issue_number == 5
    assert active_rows[0].elapsed_seconds >= 0

    tracked_rows = oq.load_tracked_and_blocked(db_path, limit=20)
    assert [row.status for row in tracked_rows] == [
        "blocked",
        "awaiting_issue_followup",
        "awaiting_feedback",
    ]
    assert tracked_rows[0].pending_event_count == 1
    assert tracked_rows[0].blocked_reason == "rewrite"
    assert tracked_rows[1].pr_number is None
    assert tracked_rows[1].issue_number == 12
    assert tracked_rows[1].blocked_reason == "waiting for reporter clarification"

    issue_history = oq.load_issue_history(db_path, None, issue_number=2, limit=10)
    assert len(issue_history) == 1
    assert issue_history[0].terminal_status == "failed"
    assert issue_history[0].failure_class == "agent_error"

    pr_history = oq.load_pr_history(db_path, None, pr_number=1010, limit=10)
    assert len(pr_history) == 2
    assert pr_history[0].to_status == "blocked"
    assert pr_history[1].to_status == "awaiting_feedback"

    metrics = oq.load_metrics(db_path, window="24h")
    assert metrics.overall.terminal_count == 3
    assert metrics.overall.failed_count == 1
    assert 0.3 < metrics.overall.failure_rate < 0.4
    assert {item.repo_full_name for item in metrics.per_repo} == {"o/repo-a", "o/repo-b"}

    repo_a_metrics = oq.load_metrics(db_path, repo_filter="o/repo-a", window="24h")
    assert repo_a_metrics.overall.terminal_count == 2
    assert repo_a_metrics.overall.failed_count == 1
    assert repo_a_metrics.overall.failure_rate == 0.5

    assert oq.load_metrics(db_path, window="1h").overall.terminal_count == 3
    assert oq.load_metrics(db_path, window="30d").overall.terminal_count == 4


def test_observability_queries_empty_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    StateStore(db_path)

    overview = oq.load_overview(db_path, window="24h")
    assert overview.active_agents == 0
    assert overview.blocked_prs == 0
    assert overview.tracked_prs == 0
    assert overview.failures == 0
    assert overview.mean_runtime_seconds == 0.0
    assert overview.stddev_runtime_seconds == 0.0
    assert oq.load_active_agents(db_path) == ()
    assert oq.load_tracked_and_blocked(db_path) == ()
    assert oq.load_issue_history(db_path, None, issue_number=1, limit=1) == ()
    assert oq.load_pr_history(db_path, None, pr_number=1, limit=1) == ()


def test_observability_query_validation_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    StateStore(db_path)

    with pytest.raises(ValueError, match="Unsupported window"):
        oq.load_overview(db_path, window="2h")
    with pytest.raises(ValueError, match="limit"):
        oq.load_active_agents(db_path, limit=0)
    with pytest.raises(ValueError, match="limit"):
        oq.load_tracked_and_blocked(db_path, limit=0)
    with pytest.raises(ValueError, match="limit"):
        oq.load_issue_history(db_path, None, issue_number=1, limit=0)
    with pytest.raises(ValueError, match="limit"):
        oq.load_pr_history(db_path, None, pr_number=1, limit=0)

    assert oq._window_modifier("1h") == "-1 hours"
    assert oq._window_modifier("24h") == "-24 hours"
    assert oq._window_modifier("7d") == "-7 days"
    assert oq._window_modifier("30d") == "-30 days"
    with pytest.raises(ValueError, match="Unsupported window"):
        oq._window_modifier("bad")

    assert oq._repo_filter_sql(None) == ("", ())
    assert oq._repo_filter_sql("   ") == ("", ())
    assert oq._repo_filter_sql("o/r") == ("AND repo_full_name = ?", ("o/r",))
    assert oq._repo_filter_sql("o/r", prefix="p") == ("AND p.repo_full_name = ?", ("o/r",))

    assert oq._as_int(2, "f") == 2
    assert oq._as_int(None, "f") == 0
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_int("x", "f")
    assert oq._as_str("x", "f") == "x"
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_str(1, "f")
    assert oq._as_optional_str(None, "f") is None
    assert oq._as_optional_str("x", "f") == "x"
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_optional_str(1, "f")
    assert oq._as_optional_int(None, "f") is None
    assert oq._as_optional_int(3, "f") == 3
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_optional_int("x", "f")
    assert oq._as_float(1.5, "f") == 1.5
    assert oq._as_float(1, "f") == 1.0
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_float("x", "f")
    assert oq._as_optional_float(None, "f") is None
    assert oq._as_optional_float(1.5, "f") == 1.5
    with pytest.raises(RuntimeError, match="Invalid f"):
        oq._as_optional_float("x", "f")

    metric = oq._build_metric(
        repo_full_name="all",
        terminal_count=0,
        failed_count=0,
        mean_runtime_seconds=0.0,
        mean_runtime_sq=0.0,
    )
    assert metric.failure_rate == 0.0


def test_observability_query_internal_error_paths() -> None:
    class FakeCursor:
        def __init__(self, row: object) -> None:
            self._row = row

        def fetchone(self) -> object:
            return self._row

        def fetchall(self) -> list[object]:
            return []

    class FakeConn:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, sql: str, params: tuple[object, ...]) -> FakeCursor:
            _ = sql, params
            self.calls += 1
            if self.calls == 1:
                return FakeCursor(None)
            return FakeCursor((0,))

    with pytest.raises(RuntimeError, match="metrics query unexpectedly returned no rows"):
        oq._load_metrics(conn=FakeConn(), repo_filter=None, window="24h")

    class EmptyCountConn:
        def execute(self, sql: str, params: tuple[object, ...]) -> FakeCursor:
            _ = sql, params
            return FakeCursor(None)

    with pytest.raises(RuntimeError, match="count query"):
        oq._fetch_int(EmptyCountConn(), "SELECT 1", ())
