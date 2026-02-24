from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from textual.widgets import DataTable, Static

from mergexo import observability_tui as tui
from mergexo.observability_queries import (
    ActiveAgentRow,
    IssueHistoryRow,
    MetricsStats,
    OverviewStats,
    PrHistoryRow,
    RuntimeMetric,
    TrackedOrBlockedRow,
)


def test_observability_tui_helper_functions() -> None:
    assert tui._normalize_window("7D") == "7d"
    assert tui._normalize_window("bad") == "24h"
    assert tui._next_window("24h") == "7d"
    assert tui._next_window("30d") == "1h"
    assert tui._next_repo_filter(None, ("o/a", "o/b")) == "o/a"
    assert tui._next_repo_filter("o/a", ("o/a", "o/b")) == "o/b"
    assert tui._next_repo_filter("o/b", ("o/a", "o/b")) is None
    assert tui._next_repo_filter("missing", ("o/a",)) is None
    assert tui._render_seconds(3.2).endswith("s")
    assert tui._render_seconds(120.0).endswith("m")
    assert tui._render_seconds(7200.0).endswith("h")
    assert tui._render_ratio(0.125) == "12.5%"

    summary = tui._summary_text(
        overview=OverviewStats(
            active_agents=1,
            blocked_prs=2,
            tracked_prs=3,
            failures=4,
            mean_runtime_seconds=5.0,
            stddev_runtime_seconds=6.0,
        ),
        repo_filter="o/a",
        window="24h",
    )
    assert "repo=o/a" in summary
    assert "window=24h" in summary
    assert "active=1" in summary


def test_history_fill_helpers() -> None:
    class FakeTable:
        def __init__(self) -> None:
            self.rows: list[tuple[object, ...]] = []

        @property
        def row_count(self) -> int:
            return len(self.rows)

        def add_row(self, *items: object) -> None:
            self.rows.append(items)

        def clear(self, *, columns: bool = False) -> None:
            _ = columns
            self.rows.clear()

    issue_table = FakeTable()
    tui._fill_issue_history(issue_table, ())
    assert issue_table.row_count == 1

    issue_table.clear(columns=False)
    tui._fill_issue_history(
        issue_table,
        (
            IssueHistoryRow(
                run_id="r",
                repo_full_name="o/a",
                run_kind="issue_flow",
                issue_number=1,
                pr_number=None,
                flow="design_doc",
                branch="agent/design/1",
                started_at="2026-02-24T00:00:00.000Z",
                finished_at="2026-02-24T00:01:00.000Z",
                terminal_status="completed",
                failure_class=None,
                error=None,
                duration_seconds=60.0,
            ),
        ),
    )
    assert issue_table.row_count == 1

    pr_table = FakeTable()
    tui._fill_pr_history(pr_table, ())
    assert pr_table.row_count == 1

    pr_table.clear(columns=False)
    tui._fill_pr_history(
        pr_table,
        (
            PrHistoryRow(
                id=1,
                repo_full_name="o/a",
                pr_number=1,
                issue_number=1,
                from_status="awaiting_feedback",
                to_status="blocked",
                reason="history_rewrite",
                detail=None,
                changed_at="2026-02-24T00:00:00.000Z",
            ),
        ),
    )
    assert pr_table.row_count == 1


def test_run_observability_tui_runs_app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called: dict[str, object] = {}

    class FakeApp:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            called["kwargs"] = kwargs

        def run(self) -> None:
            called["ran"] = True

    monkeypatch.setattr(tui, "ObservabilityApp", FakeApp)
    tui.run_observability_tui(
        db_path=tmp_path / "state.db",
        refresh_seconds=3,
        default_window="7d",
        row_limit=10,
    )
    assert called["ran"] is True
    kwargs = called["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["refresh_seconds"] == 3
    assert kwargs["default_window"] == "7d"
    assert kwargs["row_limit"] == 10


def test_observability_app_refresh_and_keybindings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: dict[str, int] = {"issue_history": 0, "pr_history": 0}
    overview = OverviewStats(
        active_agents=1,
        blocked_prs=1,
        tracked_prs=1,
        failures=1,
        mean_runtime_seconds=10.0,
        stddev_runtime_seconds=3.0,
    )
    active_rows = (
        ActiveAgentRow(
            run_id="run-1",
            repo_full_name="o/repo-a",
            run_kind="issue_flow",
            issue_number=7,
            pr_number=None,
            flow="design_doc",
            branch="agent/design/7",
            started_at="2026-02-24T00:00:00.000Z",
            elapsed_seconds=12.0,
        ),
    )
    tracked_rows = (
        TrackedOrBlockedRow(
            repo_full_name="o/repo-a",
            pr_number=101,
            issue_number=7,
            status="blocked",
            branch="agent/design/7",
            last_seen_head_sha="head",
            blocked_reason="boom",
            pending_event_count=1,
            updated_at="2026-02-24T00:00:00.000Z",
        ),
    )
    metrics = MetricsStats(
        overall=RuntimeMetric(
            repo_full_name="__all__",
            terminal_count=3,
            failed_count=1,
            failure_rate=1 / 3,
            mean_runtime_seconds=5.0,
            stddev_runtime_seconds=1.0,
        ),
        per_repo=(
            RuntimeMetric(
                repo_full_name="o/repo-a",
                terminal_count=2,
                failed_count=1,
                failure_rate=0.5,
                mean_runtime_seconds=6.0,
                stddev_runtime_seconds=2.0,
            ),
        ),
    )
    issue_history = (
        IssueHistoryRow(
            run_id="run-1",
            repo_full_name="o/repo-a",
            run_kind="issue_flow",
            issue_number=7,
            pr_number=None,
            flow="design_doc",
            branch="agent/design/7",
            started_at="2026-02-24T00:00:00.000Z",
            finished_at="2026-02-24T00:01:00.000Z",
            terminal_status="completed",
            failure_class=None,
            error=None,
            duration_seconds=60.0,
        ),
    )
    pr_history = (
        PrHistoryRow(
            id=1,
            repo_full_name="o/repo-a",
            pr_number=101,
            issue_number=7,
            from_status="awaiting_feedback",
            to_status="blocked",
            reason="history_rewrite",
            detail="detail",
            changed_at="2026-02-24T00:00:00.000Z",
        ),
    )

    monkeypatch.setattr(tui, "load_overview", lambda *args, **kwargs: overview)
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: active_rows)
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: tracked_rows)
    monkeypatch.setattr(tui, "load_metrics", lambda *args, **kwargs: metrics)

    def fake_issue_history(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["issue_history"] += 1
        return issue_history

    def fake_pr_history(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["pr_history"] += 1
        return pr_history

    monkeypatch.setattr(tui, "load_issue_history", fake_issue_history)
    monkeypatch.setattr(tui, "load_pr_history", fake_pr_history)

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )

    async def run_app() -> None:
        async with app.run_test() as _pilot:
            await _pilot.pause()
            active = app.query_one("#active-table", DataTable)
            tracked = app.query_one("#tracked-table", DataTable)
            summary = app.query_one("#summary", Static)
            assert active.row_count == 1
            assert tracked.row_count == 1
            assert "window=24h" in str(summary.renderable)
            app.action_cycle_window()
            assert app._window == "7d"
            app.action_cycle_repo_filter()
            assert app._repo_filter == "o/repo-a"
            active.focus()
            active.move_cursor(row=0, column=0, animate=False)
            app.action_show_detail()
            assert calls["issue_history"] >= 1
            app._detail_target = tui._DetailTarget(kind="pr", number=101)
            app._refresh_history_table()
            assert calls["pr_history"] >= 1
            # Defensive selection paths when table/model counts are out of sync.
            app._active_rows = ()
            assert app._active_row_selection() is None
            app._tracked_rows = ()
            assert app._tracked_row_selection() is None
            active.clear(columns=False)
            tracked.clear(columns=False)
            app._restore_active_selection(None)
            app._restore_tracked_selection(None)
            app.action_cycle_focus()
            app.action_refresh()

    asyncio.run(run_app())


def test_action_show_detail_uses_tracked_focus_branch(tmp_path: Path) -> None:
    class FocusApp(tui.ObservabilityApp):
        def __init__(self, *, db_path: Path) -> None:
            super().__init__(
                db_path=db_path, refresh_seconds=60, default_window="24h", row_limit=20
            )
            self._forced_focus = DataTable(id="tracked-table")
            self.refreshed = False

        @property
        def focused(self):  # type: ignore[override]
            return self._forced_focus

        def _tracked_row_selection(self) -> TrackedOrBlockedRow | None:
            return TrackedOrBlockedRow(
                repo_full_name="o/repo-a",
                pr_number=101,
                issue_number=7,
                status="blocked",
                branch="agent/design/7",
                last_seen_head_sha=None,
                blocked_reason="x",
                pending_event_count=0,
                updated_at="now",
            )

        def _refresh_history_table(self) -> None:
            self.refreshed = True

    app = FocusApp(db_path=tmp_path / "state.db")
    app.action_show_detail()
    assert app._detail_target is not None
    assert app._detail_target.kind == "pr"
    assert app._detail_target.number == 101
    assert app.refreshed is True
