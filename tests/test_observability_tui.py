from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

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
    TerminalIssueOutcomeRow,
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

    active = ActiveAgentRow(
        run_id="run-1",
        repo_full_name="o/repo-a",
        run_kind="issue_flow",
        issue_number=7,
        pr_number=101,
        flow="design_doc",
        branch="agent/design/7",
        started_at="2026-02-24T00:00:00.000Z",
        elapsed_seconds=12.0,
        prompt="Prompt for active run",
        codex_mode="bugfix",
        codex_session_id="thread-123",
        codex_invocation_started_at="2026-02-24T00:00:10.000Z",
    )
    tracked_pr = TrackedOrBlockedRow(
        repo_full_name="o/repo-a",
        pr_number=101,
        issue_number=7,
        status="blocked",
        branch="agent/design/7",
        last_seen_head_sha="head",
        blocked_reason="boom",
        pending_event_count=1,
        updated_at="2026-02-24T00:00:00.000Z",
    )
    tracked_issue = TrackedOrBlockedRow(
        repo_full_name="o/repo-a",
        pr_number=None,
        issue_number=8,
        status="awaiting_issue_followup",
        branch="agent/small/8",
        last_seen_head_sha=None,
        blocked_reason="needs details",
        pending_event_count=0,
        updated_at="2026-02-24T00:00:00.000Z",
    )

    assert tui._url_for_active_row(active, 2) == "https://github.com/o/repo-a/issues/7"
    assert tui._url_for_active_row(active, 3) == "https://github.com/o/repo-a/pull/101"
    assert tui._url_for_active_row(active, 5) == "https://github.com/o/repo-a/tree/agent/design/7"
    assert tui._url_for_active_row(active, 0) is None
    assert tui._url_for_tracked_row(tracked_pr, 1) == "https://github.com/o/repo-a/pull/101"
    assert tui._url_for_tracked_row(tracked_pr, 2) == "https://github.com/o/repo-a/issues/7"
    assert (
        tui._url_for_tracked_row(tracked_pr, 4) == "https://github.com/o/repo-a/tree/agent/design/7"
    )
    assert tui._url_for_tracked_row(tracked_issue, 1) is None
    assert tui._url_for_tracked_row(tracked_issue, 2) == "https://github.com/o/repo-a/issues/8"
    assert tui._tracked_row_key(tracked_pr) == ("o/repo-a", 101, 7, "blocked")
    assert tui._tracked_row_key(tracked_issue) == ("o/repo-a", None, 8, "awaiting_issue_followup")
    assert tui._render_context_snippet(None, max_chars=10) == "-"
    assert tui._render_context_snippet("x", max_chars=10) == "x"
    assert tui._render_context_snippet("abcdefghijklmnopqrstuvwxyz", max_chars=10) == "abcdefg..."
    assert tui._render_context_snippet("abcdefghijklmnopqrstuvwxyz", max_chars=2) == ".."
    assert tui._render_context_snippet("abc", max_chars=0) == ""
    assert tui._render_context_snippet("   ", max_chars=10) == "-"

    active_context = tui._active_row_context(active)
    assert "Issue: 7" in active_context
    assert "Codex Mode: bugfix" in active_context
    assert "Codex Session ID: thread-123" in active_context
    assert "Last Prompt:" in active_context
    assert "Prompt for active run" in active_context
    tracked_context = tui._tracked_row_context(tracked_pr)
    assert "Repo URL: https://github.com/o/repo-a" in tracked_context
    assert "Blocked Reason:" in tracked_context
    assert "boom" in tracked_context
    active_fields = tui._active_row_detail_fields(active)
    assert active_fields[0].label == "Repo"
    assert active_fields[0].url == "https://github.com/o/repo-a"
    active_field_map = {field.label: field for field in active_fields}
    assert active_field_map["Codex Mode"].value == "bugfix"
    assert active_field_map["Codex Session ID"].value == "thread-123"
    tracked_fields = tui._tracked_row_detail_fields(tracked_pr)
    tracked_field_map = {field.label: field for field in tracked_fields}
    assert tracked_field_map["Repo"].url == "https://github.com/o/repo-a"
    assert tracked_field_map["Repo URL"].url == "https://github.com/o/repo-a"
    assert tracked_field_map["PR"].url == "https://github.com/o/repo-a/pull/101"
    assert tracked_field_map["Issue"].url == "https://github.com/o/repo-a/issues/7"
    assert tracked_field_map["Branch"].url == "https://github.com/o/repo-a/tree/agent/design/7"
    assert tracked_field_map["Last Seen Head SHA"].url == "https://github.com/o/repo-a/commit/head"
    tracked_issue_fields = tui._tracked_row_detail_fields(tracked_issue)
    tracked_issue_field_map = {field.label: field for field in tracked_issue_fields}
    assert tracked_issue_field_map["PR"].value == "-"
    assert tracked_issue_field_map["PR"].url is None
    assert tracked_issue_field_map["Last Seen Head SHA"].value == "-"
    assert tracked_issue_field_map["Last Seen Head SHA"].url is None
    merged_outcome = TerminalIssueOutcomeRow(
        repo_full_name="o/repo-a",
        issue_number=7,
        pr_number=101,
        status="merged",
        branch="agent/design/7",
        updated_at="2026-02-24T00:00:00.000Z",
    )
    closed_outcome = TerminalIssueOutcomeRow(
        repo_full_name="o/repo-a",
        issue_number=8,
        pr_number=102,
        status="closed",
        branch="agent/design/8",
        updated_at="2026-02-24T00:01:00.000Z",
    )
    other_outcome = TerminalIssueOutcomeRow(
        repo_full_name="o/repo-a",
        issue_number=9,
        pr_number=103,
        status="blocked",
        branch="agent/design/9",
        updated_at="2026-02-24T00:02:00.000Z",
    )
    no_pr_outcome = TerminalIssueOutcomeRow(
        repo_full_name="o/repo-a",
        issue_number=10,
        pr_number=None,
        status="closed",
        branch=None,
        updated_at="2026-02-24T00:03:00.000Z",
    )
    assert tui._terminal_issue_outcome_label(merged_outcome) == "completed"
    assert tui._terminal_issue_outcome_label(closed_outcome) == "abandoned"
    assert tui._terminal_issue_outcome_label(other_outcome) == "blocked"
    assert tui._history_row_key(merged_outcome) == (
        "o/repo-a",
        7,
        101,
        "merged",
        "2026-02-24T00:00:00.000Z",
    )
    assert tui._terminal_history_title(merged_outcome) == "Issue #7 Completed"
    history_context = tui._terminal_history_context(merged_outcome)
    assert "Repo URL: https://github.com/o/repo-a" in history_context
    assert "Issue URL: https://github.com/o/repo-a/issues/7" in history_context
    assert "PR URL: https://github.com/o/repo-a/pull/101" in history_context
    history_fields = tui._terminal_history_detail_fields(merged_outcome)
    history_field_map = {field.label: field for field in history_fields}
    assert history_field_map["Repo"].url == "https://github.com/o/repo-a"
    assert history_field_map["Issue"].url == "https://github.com/o/repo-a/issues/7"
    assert history_field_map["PR"].url == "https://github.com/o/repo-a/pull/101"
    assert history_field_map["Branch"].url == "https://github.com/o/repo-a/tree/agent/design/7"
    closed_history_fields = tui._terminal_history_detail_fields(closed_outcome)
    closed_history_field_map = {field.label: field for field in closed_history_fields}
    assert closed_history_field_map["Outcome"].value == "abandoned"
    no_pr_context = tui._terminal_history_context(no_pr_outcome)
    assert "PR: -" in no_pr_context
    no_pr_history_fields = tui._terminal_history_detail_fields(no_pr_outcome)
    no_pr_history_field_map = {field.label: field for field in no_pr_history_fields}
    assert no_pr_history_field_map["PR"].value == "-"
    assert no_pr_history_field_map["PR"].url is None
    sort_stack: tuple[tui._TrackedSortKey, ...] = ()
    sort_stack = tui._next_tracked_sort_stack(sort_stack, 1)
    assert sort_stack == (tui._TrackedSortKey(column_index=1, descending=True),)
    sort_stack = tui._next_tracked_sort_stack(sort_stack, 1)
    assert sort_stack == (tui._TrackedSortKey(column_index=1, descending=False),)
    sort_stack = tui._next_tracked_sort_stack(sort_stack, 5)
    assert sort_stack == (
        tui._TrackedSortKey(column_index=5, descending=True),
        tui._TrackedSortKey(column_index=1, descending=False),
    )
    sort_stack = tui._next_tracked_sort_stack(sort_stack, 1)
    assert sort_stack == (
        tui._TrackedSortKey(column_index=1, descending=True),
        tui._TrackedSortKey(column_index=5, descending=True),
    )
    rows_for_sort = (
        TrackedOrBlockedRow(
            repo_full_name="o/repo-a",
            pr_number=100,
            issue_number=1,
            status="blocked",
            branch="agent/a",
            last_seen_head_sha=None,
            blocked_reason="a",
            pending_event_count=1,
            updated_at="2026-02-24T00:00:01.000Z",
        ),
        TrackedOrBlockedRow(
            repo_full_name="o/repo-b",
            pr_number=50,
            issue_number=2,
            status="blocked",
            branch="agent/b",
            last_seen_head_sha=None,
            blocked_reason="b",
            pending_event_count=1,
            updated_at="2026-02-24T00:00:02.000Z",
        ),
        TrackedOrBlockedRow(
            repo_full_name="o/repo-c",
            pr_number=75,
            issue_number=3,
            status="blocked",
            branch="agent/c",
            last_seen_head_sha=None,
            blocked_reason="c",
            pending_event_count=2,
            updated_at="2026-02-24T00:00:03.000Z",
        ),
    )
    sorted_rows = tui._sort_tracked_rows(
        rows_for_sort,
        (
            tui._TrackedSortKey(column_index=5, descending=True),
            tui._TrackedSortKey(column_index=1, descending=True),
        ),
    )
    assert [row.repo_full_name for row in sorted_rows] == ["o/repo-c", "o/repo-a", "o/repo-b"]
    sorted_rows = tui._sort_tracked_rows(
        rows_for_sort,
        (
            tui._TrackedSortKey(column_index=5, descending=False),
            tui._TrackedSortKey(column_index=1, descending=True),
        ),
    )
    assert [row.repo_full_name for row in sorted_rows] == ["o/repo-a", "o/repo-b", "o/repo-c"]

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
            prompt="Current active prompt",
            codex_mode="small-job",
            codex_session_id="thread-123",
            codex_invocation_started_at="2026-02-24T00:00:10.000Z",
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
    terminal_history = (
        TerminalIssueOutcomeRow(
            repo_full_name="o/repo-a",
            issue_number=8,
            pr_number=102,
            status="closed",
            branch="agent/design/8",
            updated_at="2026-02-24T00:00:01.000Z",
        ),
        TerminalIssueOutcomeRow(
            repo_full_name="o/repo-a",
            issue_number=7,
            pr_number=101,
            status="merged",
            branch="agent/design/7",
            updated_at="2026-02-24T00:00:00.000Z",
        ),
    )

    monkeypatch.setattr(tui, "load_overview", lambda *args, **kwargs: overview)
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: active_rows)
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: tracked_rows)
    monkeypatch.setattr(tui, "load_metrics", lambda *args, **kwargs: metrics)
    monkeypatch.setattr(
        tui,
        "load_terminal_issue_outcomes",
        lambda *args, **kwargs: terminal_history[
            kwargs["offset"] : kwargs["offset"] + kwargs["limit"]
        ],
    )
    opened_urls: list[str] = []
    monkeypatch.setattr(tui, "_open_external_url", lambda url: opened_urls.append(url) or True)

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )
    shown_details: list[tuple[str, str, tuple[object, ...]]] = []
    app._show_detail_context = (  # type: ignore[method-assign]
        lambda *, title, body, fields=(): shown_details.append((title, body, fields))
    )

    async def run_app() -> None:
        async with app.run_test() as _pilot:
            await _pilot.pause()
            active = app.query_one("#active-table", DataTable)
            tracked = app.query_one("#tracked-table", DataTable)
            history = app.query_one("#history-table", DataTable)
            summary = app.query_one("#summary", Static)
            assert active.row_count == 1
            assert tracked.row_count == 1
            assert history.row_count == 2
            assert str(history.get_row_at(0)[1]) == "abandoned"
            assert str(history.get_row_at(1)[1]) == "completed"
            assert "window=24h" in str(summary.renderable)
            app.action_cycle_window()
            assert app._window == "7d"
            app.action_cycle_repo_filter()
            assert app._repo_filter == "o/repo-a"
            active.focus()
            await _pilot.pause()
            active.move_cursor(row=0, column=0, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert shown_details[-1][0] == "Issue #7 Context"
            assert "Current active prompt" in shown_details[-1][1]
            active.move_cursor(row=0, column=2, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert opened_urls[-1] == "https://github.com/o/repo-a/issues/7"
            tracked.focus()
            await _pilot.pause()
            tracked.move_cursor(row=0, column=1, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert opened_urls[-1] == "https://github.com/o/repo-a/pull/101"
            tracked.move_cursor(row=0, column=4, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert opened_urls[-1] == "https://github.com/o/repo-a/tree/agent/design/7"
            tracked.move_cursor(row=0, column=6, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert shown_details[-1][0] == "PR #101 Context"
            assert "Repo URL: https://github.com/o/repo-a" in shown_details[-1][1]
            assert "Blocked Reason:" in shown_details[-1][1]
            app.action_refresh()
            assert tracked.cursor_column == 6
            history.focus()
            await _pilot.pause()
            history.move_cursor(row=1, column=3, animate=False)
            await _pilot.press("enter")
            await _pilot.pause()
            assert shown_details[-1][0] == "Issue #7 Completed"
            assert "Repo URL: https://github.com/o/repo-a" in shown_details[-1][1]
            assert "PR URL: https://github.com/o/repo-a/pull/101" in shown_details[-1][1]
            app.action_refresh()
            assert history.cursor_row == 1
            assert history.cursor_column == 3
            app._refresh_history_table(reset=True)
            assert history.row_count == 2
            # Defensive selection paths when table/model counts are out of sync.
            app._active_rows = ()
            assert app._active_row_selection() is None
            app._tracked_rows = ()
            assert app._tracked_row_selection() is None
            app._history_rows = ()
            assert app._history_row_selection() is None
            active.clear(columns=False)
            tracked.clear(columns=False)
            history.clear(columns=False)
            app._restore_active_selection(None, 0)
            app._restore_tracked_selection(None, 0)
            app._restore_history_selection(None, 1, 2)
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

    app = FocusApp(db_path=tmp_path / "state.db")
    app.action_show_detail()
    assert app._detail_target is not None
    assert app._detail_target.kind == "pr"
    assert app._detail_target.number == 101


def test_action_show_detail_uses_issue_detail_for_tracked_row_without_pr(tmp_path: Path) -> None:
    class FocusApp(tui.ObservabilityApp):
        def __init__(self, *, db_path: Path) -> None:
            super().__init__(
                db_path=db_path, refresh_seconds=60, default_window="24h", row_limit=20
            )
            self._forced_focus = DataTable(id="tracked-table")

        @property
        def focused(self):  # type: ignore[override]
            return self._forced_focus

        def _tracked_row_selection(self) -> TrackedOrBlockedRow | None:
            return TrackedOrBlockedRow(
                repo_full_name="o/repo-a",
                pr_number=None,
                issue_number=8,
                status="awaiting_issue_followup",
                branch="agent/small/8",
                last_seen_head_sha=None,
                blocked_reason="needs follow-up",
                pending_event_count=0,
                updated_at="now",
            )

    app = FocusApp(db_path=tmp_path / "state.db")
    app.action_show_detail()
    assert app._detail_target is not None
    assert app._detail_target.kind == "issue"
    assert app._detail_target.number == 8


def test_open_external_url_uses_open_command_on_macos(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    class FakeCompletedProcess:
        def __init__(self, returncode: int) -> None:
            self.returncode = returncode

    monkeypatch.setattr(tui.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        tui.subprocess,
        "run",
        lambda args, check, capture_output: (
            calls.append((args, check, capture_output)) or FakeCompletedProcess(0)
        ),
    )
    monkeypatch.setattr(
        tui.webbrowser,
        "open_new_tab",
        lambda url: (_ for _ in ()).throw(RuntimeError(f"unexpected fallback: {url}")),
    )

    assert tui._open_external_url("https://example.com") is True
    assert calls == [(["open", "https://example.com"], False, True)]


def test_open_external_url_falls_back_to_webbrowser(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tui.platform, "system", lambda: "Linux")
    opened: list[str] = []
    monkeypatch.setattr(tui.webbrowser, "open_new_tab", lambda url: opened.append(url) or True)

    assert tui._open_external_url("https://example.com/next") is True
    assert opened == ["https://example.com/next"]


def test_open_external_url_handles_oserror_then_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tui.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        tui.subprocess,
        "run",
        lambda args, check, capture_output: (_ for _ in ()).throw(OSError("missing open")),
    )
    opened: list[str] = []
    monkeypatch.setattr(tui.webbrowser, "open_new_tab", lambda url: opened.append(url) or True)

    assert tui._open_external_url("https://example.com/fallback") is True
    assert opened == ["https://example.com/fallback"]


def test_open_external_url_handles_webbrowser_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tui.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        tui.webbrowser,
        "open_new_tab",
        lambda url: (_ for _ in ()).throw(tui.webbrowser.Error("boom")),
    )

    assert tui._open_external_url("https://example.com/error") is False


def test_app_open_external_url_shows_manual_fallback(tmp_path: Path) -> None:
    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )
    shown: list[tuple[str, str]] = []
    app._show_detail_context = (  # type: ignore[method-assign]
        lambda *, title, body, fields=(): shown.append((title, body))
    )
    original_open = tui._open_external_url
    try:
        tui._open_external_url = lambda url: False
        app._open_external_url("https://example.com/manual")
    finally:
        tui._open_external_url = original_open

    assert shown == [
        (
            "Open URL Manually",
            "MergeXO could not open the system browser automatically.\n\n"
            "Open this URL manually:\n"
            "https://example.com/manual",
        )
    ]


def test_action_show_detail_noop_when_focus_is_other(tmp_path: Path) -> None:
    class FocusApp(tui.ObservabilityApp):
        def __init__(self, *, db_path: Path) -> None:
            super().__init__(
                db_path=db_path, refresh_seconds=60, default_window="24h", row_limit=20
            )
            self._forced_focus = Static(id="other")

        @property
        def focused(self):  # type: ignore[override]
            return self._forced_focus

    app = FocusApp(db_path=tmp_path / "state.db")
    app._detail_target = tui._DetailTarget(kind="issue", number=7)
    app.action_show_detail()
    assert app._detail_target == tui._DetailTarget(kind="issue", number=7)


def test_show_detail_context_pushes_modal_when_running(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        tui,
        "load_overview",
        lambda *args, **kwargs: OverviewStats(
            active_agents=0,
            blocked_prs=0,
            tracked_prs=0,
            failures=0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        ),
    )
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: ())
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        tui,
        "load_metrics",
        lambda *args, **kwargs: MetricsStats(
            overall=RuntimeMetric(
                repo_full_name="__all__",
                terminal_count=0,
                failed_count=0,
                failure_rate=0.0,
                mean_runtime_seconds=0.0,
                stddev_runtime_seconds=0.0,
            ),
            per_repo=(),
        ),
    )
    monkeypatch.setattr(tui, "load_terminal_issue_outcomes", lambda *args, **kwargs: ())

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )

    async def run_app() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            app._show_detail_context(title="Detail", body="full body")
            await pilot.pause()
            assert str(app.query_one("#detail-title", Static).renderable) == "Detail"
            assert "full body" in str(app.query_one("#detail-body", Static).renderable)
            modal = app.screen
            assert isinstance(modal, tui._DetailModal)
            modal.action_close()
            await pilot.pause()

    asyncio.run(run_app())


def test_detail_modal_field_navigation_opens_urls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    row = TrackedOrBlockedRow(
        repo_full_name="o/repo-a",
        pr_number=101,
        issue_number=7,
        status="blocked",
        branch="agent/design/7",
        last_seen_head_sha="abc123",
        blocked_reason="boom",
        pending_event_count=1,
        updated_at="2026-02-24T00:00:00.000Z",
    )
    monkeypatch.setattr(
        tui,
        "load_overview",
        lambda *args, **kwargs: OverviewStats(
            active_agents=0,
            blocked_prs=0,
            tracked_prs=0,
            failures=0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        ),
    )
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: ())
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        tui,
        "load_metrics",
        lambda *args, **kwargs: MetricsStats(
            overall=RuntimeMetric(
                repo_full_name="__all__",
                terminal_count=0,
                failed_count=0,
                failure_rate=0.0,
                mean_runtime_seconds=0.0,
                stddev_runtime_seconds=0.0,
            ),
            per_repo=(),
        ),
    )
    monkeypatch.setattr(tui, "load_terminal_issue_outcomes", lambda *args, **kwargs: ())
    fields = tui._tracked_row_detail_fields(row)
    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )
    opened: list[str] = []
    app._open_external_url = lambda url: opened.append(url)  # type: ignore[method-assign]

    async def run_app() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            app._show_detail_context(
                title="PR #101 Context", body="Blocked Reason:\nboom", fields=fields
            )
            await pilot.pause()
            assert isinstance(app.screen, tui._DetailModal)
            table = app.query_one("#detail-fields-table", DataTable)
            table.move_cursor(row=4, column=1, animate=False)  # Status has no URL
            await pilot.press("enter")
            await pilot.pause()
            assert opened == []
            table.move_cursor(row=0, column=1, animate=False)  # Repo
            await pilot.press("enter")
            await pilot.pause()
            assert len(opened) == 1
            assert opened[-1] == "https://github.com/o/repo-a"
            table.move_cursor(row=2, column=1, animate=False)  # PR
            await pilot.press("enter")
            await pilot.pause()
            assert len(opened) == 2
            assert opened[-1] == "https://github.com/o/repo-a/pull/101"
            table.move_cursor(row=3, column=1, animate=False)  # Issue
            await pilot.press("enter")
            await pilot.pause()
            assert len(opened) == 3
            assert opened[-1] == "https://github.com/o/repo-a/issues/7"
            table.move_cursor(row=7, column=1, animate=False)  # SHA commit
            await pilot.press("enter")
            await pilot.pause()
            assert len(opened) == 4
            assert opened[-1] == "https://github.com/o/repo-a/commit/abc123"
            assert isinstance(app.screen, tui._DetailModal)

    asyncio.run(run_app())


def test_tracked_header_click_sorts_with_deduped_stack(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        tui,
        "load_overview",
        lambda *args, **kwargs: OverviewStats(
            active_agents=0,
            blocked_prs=0,
            tracked_prs=3,
            failures=0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        ),
    )
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: ())
    tracked_rows = (
        TrackedOrBlockedRow(
            repo_full_name="o/repo-a",
            pr_number=100,
            issue_number=1,
            status="blocked",
            branch="agent/a",
            last_seen_head_sha=None,
            blocked_reason="a",
            pending_event_count=1,
            updated_at="2026-02-24T00:00:01.000Z",
        ),
        TrackedOrBlockedRow(
            repo_full_name="o/repo-b",
            pr_number=50,
            issue_number=2,
            status="blocked",
            branch="agent/b",
            last_seen_head_sha=None,
            blocked_reason="b",
            pending_event_count=1,
            updated_at="2026-02-24T00:00:02.000Z",
        ),
        TrackedOrBlockedRow(
            repo_full_name="o/repo-c",
            pr_number=75,
            issue_number=3,
            status="blocked",
            branch="agent/c",
            last_seen_head_sha=None,
            blocked_reason="c",
            pending_event_count=2,
            updated_at="2026-02-24T00:00:03.000Z",
        ),
    )
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: tracked_rows)
    monkeypatch.setattr(
        tui,
        "load_metrics",
        lambda *args, **kwargs: MetricsStats(
            overall=RuntimeMetric(
                repo_full_name="__all__",
                terminal_count=0,
                failed_count=0,
                failure_rate=0.0,
                mean_runtime_seconds=0.0,
                stddev_runtime_seconds=0.0,
            ),
            per_repo=(),
        ),
    )
    monkeypatch.setattr(tui, "load_terminal_issue_outcomes", lambda *args, **kwargs: ())

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )

    async def run_app() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            table = app.query_one("#tracked-table", DataTable)

            def row_order() -> list[str]:
                return [str(table.get_row_at(index)[0]) for index in range(table.row_count)]

            def click_header(column_index: int) -> None:
                column = table.ordered_columns[column_index]
                event = DataTable.HeaderSelected(table, column.key, column_index, column.label)
                app.on_data_table_header_selected(event)

            assert row_order() == ["o/repo-a", "o/repo-b", "o/repo-c"]
            click_header(1)  # PR desc first click.
            await pilot.pause()
            assert row_order() == ["o/repo-a", "o/repo-c", "o/repo-b"]
            assert app._tracked_sort_stack == (
                tui._TrackedSortKey(column_index=1, descending=True),
            )
            click_header(5)  # Pending desc as new primary key.
            await pilot.pause()
            assert row_order() == ["o/repo-c", "o/repo-a", "o/repo-b"]
            assert app._tracked_sort_stack == (
                tui._TrackedSortKey(column_index=5, descending=True),
                tui._TrackedSortKey(column_index=1, descending=True),
            )
            click_header(5)  # Toggle pending direction.
            await pilot.pause()
            assert row_order() == ["o/repo-a", "o/repo-b", "o/repo-c"]
            assert app._tracked_sort_stack == (
                tui._TrackedSortKey(column_index=5, descending=False),
                tui._TrackedSortKey(column_index=1, descending=True),
            )

    asyncio.run(run_app())


def test_history_table_paginates_when_scrolling_near_bottom(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        tui,
        "load_overview",
        lambda *args, **kwargs: OverviewStats(
            active_agents=0,
            blocked_prs=0,
            tracked_prs=0,
            failures=0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        ),
    )
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: ())
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        tui,
        "load_metrics",
        lambda *args, **kwargs: MetricsStats(
            overall=RuntimeMetric(
                repo_full_name="__all__",
                terminal_count=0,
                failed_count=0,
                failure_rate=0.0,
                mean_runtime_seconds=0.0,
                stddev_runtime_seconds=0.0,
            ),
            per_repo=(),
        ),
    )
    terminal_rows = (
        TerminalIssueOutcomeRow(
            repo_full_name="o/repo-a",
            issue_number=1,
            pr_number=101,
            status="merged",
            branch="agent/design/1",
            updated_at="2026-02-24T00:00:03.000Z",
        ),
        TerminalIssueOutcomeRow(
            repo_full_name="o/repo-a",
            issue_number=2,
            pr_number=102,
            status="closed",
            branch="agent/design/2",
            updated_at="2026-02-24T00:00:02.000Z",
        ),
        TerminalIssueOutcomeRow(
            repo_full_name="o/repo-a",
            issue_number=3,
            pr_number=103,
            status="merged",
            branch="agent/design/3",
            updated_at="2026-02-24T00:00:01.000Z",
        ),
    )
    load_calls: list[tuple[int, int]] = []

    def fake_load_terminal(*args, **kwargs):  # type: ignore[no-untyped-def]
        limit = kwargs["limit"]
        offset = kwargs["offset"]
        load_calls.append((limit, offset))
        return terminal_rows[offset : offset + limit]

    monkeypatch.setattr(tui, "load_terminal_issue_outcomes", fake_load_terminal)

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=2,
    )

    async def run_app() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            history = app.query_one("#history-table", DataTable)
            assert history.row_count == 2
            assert load_calls[0] == (2, 0)
            row_key = history.ordered_rows[1].key
            app.on_data_table_row_highlighted(
                DataTable.RowHighlighted(history, cursor_row=1, row_key=row_key)
            )
            await pilot.pause()
            assert history.row_count == 3
            assert load_calls[1] == (2, 2)

    asyncio.run(run_app())


def test_history_row_highlight_ignores_non_loading_paths(tmp_path: Path) -> None:
    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )
    load_calls: list[bool] = []
    app._load_more_history_rows = lambda: load_calls.append(True)  # type: ignore[method-assign]

    other_table = SimpleNamespace(id="metrics-table", row_count=1)
    app.on_data_table_row_highlighted(SimpleNamespace(data_table=other_table, cursor_row=0))

    history_table = SimpleNamespace(id="history-table", row_count=7)

    app.on_data_table_row_highlighted(SimpleNamespace(data_table=history_table, cursor_row=-1))

    app._history_has_more = False
    app._history_loading = False
    app.on_data_table_row_highlighted(SimpleNamespace(data_table=history_table, cursor_row=0))

    app._history_has_more = True
    app._history_loading = True
    app.on_data_table_row_highlighted(SimpleNamespace(data_table=history_table, cursor_row=0))

    app._history_loading = False
    app.on_data_table_row_highlighted(SimpleNamespace(data_table=history_table, cursor_row=0))
    assert load_calls == []


def test_history_page_size_load_guard_and_placeholder_replacement(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=None,
    )
    assert app._history_page_size() == 200

    load_attempts: list[bool] = []
    monkeypatch.setattr(
        tui,
        "load_terminal_issue_outcomes",
        lambda *args, **kwargs: load_attempts.append(True) or (),
    )
    app._history_loading = True
    app._load_more_history_rows()
    app._history_loading = False
    app._history_has_more = False
    app._load_more_history_rows()
    assert load_attempts == []

    class FakeHistoryTable:
        def __init__(self) -> None:
            self.rows: list[tuple[object, ...]] = []

        @property
        def row_count(self) -> int:
            return len(self.rows)

        def clear(self, *, columns: bool = False) -> None:
            _ = columns
            self.rows.clear()

        def add_row(self, *items: object) -> None:
            self.rows.append(items)

        def get_row_at(self, index: int) -> tuple[object, ...]:
            return self.rows[index]

    fake_table = FakeHistoryTable()
    app._base_table = lambda selector: fake_table  # type: ignore[method-assign]
    app._history_rows = ()
    app._render_history_rows(())
    assert fake_table.row_count == 1
    assert str(fake_table.get_row_at(0)[5]) == "No terminal issue outcomes found"

    new_row = TerminalIssueOutcomeRow(
        repo_full_name="o/repo-a",
        issue_number=10,
        pr_number=110,
        status="merged",
        branch="agent/design/10",
        updated_at="2026-02-24T00:03:00.000Z",
    )
    app._history_rows = (new_row,)
    app._render_history_rows((new_row,))
    assert fake_table.row_count == 1
    assert str(fake_table.get_row_at(0)[5]) == "merged"


def test_refresh_data_while_modal_open_uses_base_screen(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        tui,
        "load_overview",
        lambda *args, **kwargs: OverviewStats(
            active_agents=0,
            blocked_prs=1,
            tracked_prs=2,
            failures=0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        ),
    )
    monkeypatch.setattr(tui, "load_active_agents", lambda *args, **kwargs: ())
    monkeypatch.setattr(tui, "load_tracked_and_blocked", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        tui,
        "load_metrics",
        lambda *args, **kwargs: MetricsStats(
            overall=RuntimeMetric(
                repo_full_name="__all__",
                terminal_count=0,
                failed_count=0,
                failure_rate=0.0,
                mean_runtime_seconds=0.0,
                stddev_runtime_seconds=0.0,
            ),
            per_repo=(),
        ),
    )
    monkeypatch.setattr(tui, "load_terminal_issue_outcomes", lambda *args, **kwargs: ())

    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )

    async def run_app() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            app._show_detail_context(title="Detail", body="full body")
            await pilot.pause()
            assert isinstance(app.screen, tui._DetailModal)
            app.refresh_data()
            await pilot.pause()
            assert isinstance(app.screen, tui._DetailModal)
            base_screen = app.screen_stack[0]
            summary = base_screen.query_one("#summary", Static)
            assert "blocked=1" in str(summary.renderable)
            assert "tracked=2" in str(summary.renderable)

    asyncio.run(run_app())


def test_detail_modal_close_action_calls_dismiss(monkeypatch: pytest.MonkeyPatch) -> None:
    modal = tui._DetailModal(title="Context", body="Body")
    dismissed: list[None] = []
    monkeypatch.setattr(modal, "dismiss", lambda value=None: dismissed.append(value))

    modal.action_close()
    assert dismissed == [None]


def test_detail_modal_activate_without_fields_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    modal = tui._DetailModal(title="Context", body="Body")
    closed: list[bool] = []
    monkeypatch.setattr(modal, "action_close", lambda: closed.append(True))

    modal.action_activate()
    assert closed == [True]


def test_detail_modal_activate_out_of_range_row_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    modal = tui._DetailModal(
        title="Context",
        body="Body",
        fields=(tui._DetailField("Repo", "o/repo-a", "https://github.com/o/repo-a"),),
    )
    fake_table = SimpleNamespace(cursor_row=-1)
    monkeypatch.setattr(modal, "query_one", lambda *args, **kwargs: fake_table)

    modal.action_activate()


def test_detail_modal_cell_selected_event_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    modal = tui._DetailModal(title="Context", body="Body")
    calls: list[str] = []
    monkeypatch.setattr(modal, "action_activate", lambda: calls.append("activate"))

    modal.on_data_table_cell_selected(SimpleNamespace(data_table=SimpleNamespace(id="other")))
    assert calls == []
    modal.on_data_table_cell_selected(
        SimpleNamespace(data_table=SimpleNamespace(id="detail-fields-table"))
    )
    assert calls == ["activate"]


def test_detail_modal_activate_falls_back_to_module_open(monkeypatch: pytest.MonkeyPatch) -> None:
    modal = tui._DetailModal(
        title="Context",
        body="Body",
        fields=(tui._DetailField("Repo", "o/repo-a", "https://github.com/o/repo-a"),),
    )
    fake_table = SimpleNamespace(cursor_row=0)
    monkeypatch.setattr(modal, "query_one", lambda *args, **kwargs: fake_table)
    opened: list[str] = []
    monkeypatch.setattr(tui, "_open_external_url", lambda url: opened.append(url) or True)

    class NonObservabilityApp:
        pass

    monkeypatch.setattr(type(modal), "app", property(lambda self: NonObservabilityApp()))
    modal.action_activate()
    assert opened == ["https://github.com/o/repo-a"]


def test_detail_data_table_ignores_non_target_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    table = tui._DetailDataTable(id="metrics-table")
    monkeypatch.setattr(DataTable, "action_select_cursor", lambda self: None)
    table.action_select_cursor()


def test_header_selected_ignores_non_tracked_table(tmp_path: Path) -> None:
    app = tui.ObservabilityApp(
        db_path=tmp_path / "state.db",
        refresh_seconds=60,
        default_window="24h",
        row_limit=20,
    )
    called: list[int] = []
    app._sort_tracked_by_column = lambda column_index: called.append(column_index)  # type: ignore[method-assign]

    app.on_data_table_header_selected(
        SimpleNamespace(data_table=SimpleNamespace(id="history-table"), column_index=3)
    )
    assert called == []


def test_base_screen_falls_back_to_screen_when_stack_empty(tmp_path: Path) -> None:
    class FakeApp(tui.ObservabilityApp):
        def __init__(self, *, db_path: Path) -> None:
            super().__init__(
                db_path=db_path, refresh_seconds=60, default_window="24h", row_limit=20
            )
            self._fake_screen = SimpleNamespace(name="fallback-screen")

        @property
        def screen_stack(self):  # type: ignore[override]
            return ()

        @property
        def screen(self):  # type: ignore[override]
            return self._fake_screen

    app = FakeApp(db_path=tmp_path / "state.db")
    assert app._base_screen().name == "fallback-screen"


def test_tracked_sort_value_branches() -> None:
    row = TrackedOrBlockedRow(
        repo_full_name="O/Repo-A",
        pr_number=101,
        issue_number=7,
        status="Blocked",
        branch="Agent/Design/7",
        last_seen_head_sha="head",
        blocked_reason="Boom",
        pending_event_count=2,
        updated_at="2026-02-24T00:00:00.000Z",
    )
    row_without_pr = TrackedOrBlockedRow(
        repo_full_name="O/Repo-B",
        pr_number=None,
        issue_number=8,
        status="Awaiting",
        branch="Agent/Design/8",
        last_seen_head_sha=None,
        blocked_reason=None,
        pending_event_count=0,
        updated_at="2026-02-24T00:01:00.000Z",
    )
    assert tui._tracked_sort_value(row, 0) == "o/repo-a"
    assert tui._tracked_sort_value(row, 1) == 101
    assert tui._tracked_sort_value(row_without_pr, 1) == -1
    assert tui._tracked_sort_value(row, 2) == 7
    assert tui._tracked_sort_value(row, 3) == "blocked"
    assert tui._tracked_sort_value(row, 4) == "agent/design/7"
    assert tui._tracked_sort_value(row, 5) == 2
    assert tui._tracked_sort_value(row, 6) == "boom"
    assert tui._tracked_sort_value(row_without_pr, 6) == ""
    assert tui._tracked_sort_value(row, 7) == "2026-02-24T00:00:00.000Z"
    assert tui._tracked_sort_value(row, 999) == ""
