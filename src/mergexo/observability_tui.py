from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import webbrowser

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import DataTable, Footer, Header, Static

from mergexo.observability_queries import (
    ActiveAgentRow,
    IssueHistoryRow,
    MetricsStats,
    OverviewStats,
    PrHistoryRow,
    TrackedOrBlockedRow,
    load_active_agents,
    load_issue_history,
    load_metrics,
    load_overview,
    load_pr_history,
    load_tracked_and_blocked,
)


_WINDOW_OPTIONS: tuple[str, ...] = ("1h", "24h", "7d", "30d")
_ACTIVE_COL_ISSUE = 2
_ACTIVE_COL_PR = 3
_ACTIVE_COL_BRANCH = 5
_TRACKED_COL_PR = 1
_TRACKED_COL_ISSUE = 2
_TRACKED_COL_BRANCH = 4


@dataclass(frozen=True)
class _DetailTarget:
    kind: str
    number: int


class ObservabilityApp(App[None]):
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("f", "cycle_repo_filter", "Repo Filter"),
        Binding("w", "cycle_window", "Window"),
        Binding("tab", "cycle_focus", "Focus"),
        Binding("enter", "show_detail", "Details"),
    ]
    CSS = """
    Screen {
        layout: vertical;
    }
    .panel-title {
        text-style: bold;
        padding-left: 1;
    }
    #summary {
        height: 3;
        padding: 0 1;
    }
    DataTable {
        height: 1fr;
    }
    """

    def __init__(
        self,
        *,
        db_path: Path,
        refresh_seconds: int = 2,
        default_window: str = "24h",
        row_limit: int | None = 200,
    ) -> None:
        super().__init__()
        self._db_path = db_path
        self._refresh_seconds = refresh_seconds
        self._window = _normalize_window(default_window)
        self._row_limit = row_limit
        self._repo_filter: str | None = None
        self._available_repos: tuple[str, ...] = ()
        self._active_rows: tuple[ActiveAgentRow, ...] = ()
        self._tracked_rows: tuple[TrackedOrBlockedRow, ...] = ()
        self._detail_target: _DetailTarget | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical():
            yield Static("", id="summary")
            yield Static("Active Agents", classes="panel-title")
            yield DataTable(id="active-table")
            yield Static("Tracked And Blocked Work", classes="panel-title")
            yield DataTable(id="tracked-table")
            yield Static("History", classes="panel-title")
            yield DataTable(id="history-table")
            yield Static("Metrics", classes="panel-title")
            yield DataTable(id="metrics-table")
        yield Footer()

    def on_mount(self) -> None:
        self._init_tables()
        self.refresh_data()
        self.set_interval(self._refresh_seconds, self.refresh_data)

    def action_refresh(self) -> None:
        self.refresh_data()

    def action_cycle_window(self) -> None:
        self._window = _next_window(self._window)
        self.refresh_data()

    def action_cycle_repo_filter(self) -> None:
        self._repo_filter = _next_repo_filter(self._repo_filter, self._available_repos)
        self.refresh_data()

    def action_cycle_focus(self) -> None:
        self.screen.focus_next()

    def action_show_detail(self) -> None:
        focused = self.focused
        if isinstance(focused, DataTable) and focused.id == "active-table":
            selected = self._active_row_selection()
            if selected is not None:
                active_url = _url_for_active_row(selected, focused.cursor_column)
                if active_url is not None:
                    webbrowser.open(active_url)
                    return
                self._detail_target = _DetailTarget(kind="issue", number=selected.issue_number)
        elif isinstance(focused, DataTable) and focused.id == "tracked-table":
            selected = self._tracked_row_selection()
            if selected is not None:
                tracked_url = _url_for_tracked_row(selected, focused.cursor_column)
                if tracked_url is not None:
                    webbrowser.open(tracked_url)
                    return
                if selected.pr_number is None:
                    self._detail_target = _DetailTarget(kind="issue", number=selected.issue_number)
                else:
                    self._detail_target = _DetailTarget(kind="pr", number=selected.pr_number)
        if self._detail_target is not None:
            self._refresh_history_table()

    def refresh_data(self) -> None:
        overview = load_overview(self._db_path, self._repo_filter, self._window)
        self._active_rows = load_active_agents(
            self._db_path,
            self._repo_filter,
            limit=self._row_limit,
        )
        self._tracked_rows = load_tracked_and_blocked(
            self._db_path,
            self._repo_filter,
            limit=self._row_limit,
        )
        metrics = load_metrics(self._db_path, self._repo_filter, self._window)
        self._available_repos = tuple(sorted({row.repo_full_name for row in metrics.per_repo}))
        self.query_one("#summary", Static).update(
            _summary_text(overview=overview, repo_filter=self._repo_filter, window=self._window)
        )
        self._refresh_active_table()
        self._refresh_tracked_table()
        self._refresh_metrics_table(metrics)
        self._refresh_history_table()

    def _init_tables(self) -> None:
        active = self.query_one("#active-table", DataTable)
        tracked = self.query_one("#tracked-table", DataTable)
        history = self.query_one("#history-table", DataTable)
        metrics = self.query_one("#metrics-table", DataTable)
        active.add_columns("Repo", "Run", "Issue", "PR", "Flow", "Branch", "Started", "Elapsed")
        tracked.add_columns(
            "Repo",
            "PR",
            "Issue",
            "Status",
            "Branch",
            "Pending",
            "Blocked Reason",
            "Updated",
        )
        history.add_columns("Time", "Type", "From", "To", "Detail")
        metrics.add_columns("Repo", "Terminal", "Failed", "Failure Rate", "Mean", "StdDev")

    def _refresh_active_table(self) -> None:
        table = self.query_one("#active-table", DataTable)
        selected = self._active_row_selection()
        previous_run_id = selected.run_id if selected is not None else None
        previous_column = table.cursor_column if table.row_count > 0 else 0
        table.clear(columns=False)
        for row in self._active_rows:
            table.add_row(
                row.repo_full_name,
                row.run_kind,
                str(row.issue_number),
                str(row.pr_number) if row.pr_number is not None else "-",
                row.flow or "-",
                row.branch or "-",
                row.started_at,
                _render_seconds(row.elapsed_seconds),
            )
        self._restore_active_selection(previous_run_id, previous_column)

    def _refresh_tracked_table(self) -> None:
        table = self.query_one("#tracked-table", DataTable)
        selected = self._tracked_row_selection()
        previous_key = _tracked_row_key(selected) if selected is not None else None
        previous_column = table.cursor_column if table.row_count > 0 else 0
        table.clear(columns=False)
        for row in self._tracked_rows:
            table.add_row(
                row.repo_full_name,
                str(row.pr_number) if row.pr_number is not None else "",
                str(row.issue_number),
                row.status,
                row.branch,
                str(row.pending_event_count),
                row.blocked_reason or "-",
                row.updated_at,
            )
        self._restore_tracked_selection(previous_key, previous_column)

    def _refresh_metrics_table(self, metrics: MetricsStats) -> None:
        table = self.query_one("#metrics-table", DataTable)
        table.clear(columns=False)
        overall = metrics.overall
        table.add_row(
            overall.repo_full_name,
            str(overall.terminal_count),
            str(overall.failed_count),
            _render_ratio(overall.failure_rate),
            _render_seconds(overall.mean_runtime_seconds),
            _render_seconds(overall.stddev_runtime_seconds),
        )
        for row in metrics.per_repo:
            table.add_row(
                row.repo_full_name,
                str(row.terminal_count),
                str(row.failed_count),
                _render_ratio(row.failure_rate),
                _render_seconds(row.mean_runtime_seconds),
                _render_seconds(row.stddev_runtime_seconds),
            )

    def _refresh_history_table(self) -> None:
        table = self.query_one("#history-table", DataTable)
        table.clear(columns=False)
        target = self._detail_target
        if target is None:
            table.add_row("-", "hint", "-", "-", "Press Enter on active/tracked rows for details")
            return
        if target.kind == "issue":
            rows = load_issue_history(
                self._db_path,
                self._repo_filter,
                target.number,
                limit=self._row_limit or 200,
            )
            _fill_issue_history(table, rows)
            return
        rows = load_pr_history(
            self._db_path,
            self._repo_filter,
            target.number,
            limit=self._row_limit or 200,
        )
        _fill_pr_history(table, rows)

    def _active_row_selection(self) -> ActiveAgentRow | None:
        table = self.query_one("#active-table", DataTable)
        if table.row_count < 1:
            return None
        index = table.cursor_row
        if index < 0 or index >= len(self._active_rows):
            return None
        return self._active_rows[index]

    def _tracked_row_selection(self) -> TrackedOrBlockedRow | None:
        table = self.query_one("#tracked-table", DataTable)
        if table.row_count < 1:
            return None
        index = table.cursor_row
        if index < 0 or index >= len(self._tracked_rows):
            return None
        return self._tracked_rows[index]

    def _restore_active_selection(self, previous_run_id: str | None, previous_column: int) -> None:
        table = self.query_one("#active-table", DataTable)
        if table.row_count < 1:
            return
        row_index = 0
        if previous_run_id is not None:
            for idx, row in enumerate(self._active_rows):
                if row.run_id == previous_run_id:
                    row_index = idx
                    break
        max_column = max(0, len(table.columns) - 1)
        table.move_cursor(
            row=row_index,
            column=min(max(previous_column, 0), max_column),
            animate=False,
        )

    def _restore_tracked_selection(
        self,
        previous_key: tuple[str, int | None, int, str] | None,
        previous_column: int,
    ) -> None:
        table = self.query_one("#tracked-table", DataTable)
        if table.row_count < 1:
            return
        row_index = 0
        if previous_key is not None:
            for idx, row in enumerate(self._tracked_rows):
                if _tracked_row_key(row) == previous_key:
                    row_index = idx
                    break
        max_column = max(0, len(table.columns) - 1)
        table.move_cursor(
            row=row_index,
            column=min(max(previous_column, 0), max_column),
            animate=False,
        )


def run_observability_tui(
    *,
    db_path: Path,
    refresh_seconds: int,
    default_window: str,
    row_limit: int | None,
) -> None:
    app = ObservabilityApp(
        db_path=db_path,
        refresh_seconds=refresh_seconds,
        default_window=default_window,
        row_limit=row_limit,
    )
    app.run()


def _fill_issue_history(table: DataTable, rows: tuple[IssueHistoryRow, ...]) -> None:
    if not rows:
        table.add_row("-", "issue", "-", "-", "No issue history found")
        return
    for row in rows:
        table.add_row(
            row.finished_at or row.started_at,
            "issue",
            row.run_kind,
            row.terminal_status or "running",
            row.error or "-",
        )


def _fill_pr_history(table: DataTable, rows: tuple[PrHistoryRow, ...]) -> None:
    if not rows:
        table.add_row("-", "pr", "-", "-", "No PR history found")
        return
    for row in rows:
        detail = row.detail or row.reason or "-"
        table.add_row(
            row.changed_at,
            "pr",
            row.from_status or "-",
            row.to_status,
            detail,
        )


def _summary_text(*, overview: OverviewStats, repo_filter: str | None, window: str) -> str:
    return (
        " | ".join(
            [
                f"repo={repo_filter or 'all'}",
                f"window={window}",
                f"active={overview.active_agents}",
                f"blocked={overview.blocked_prs}",
                f"tracked={overview.tracked_prs}",
                f"failures={overview.failures}",
                f"mean={_render_seconds(overview.mean_runtime_seconds)}",
                f"stddev={_render_seconds(overview.stddev_runtime_seconds)}",
            ]
        )
        + "\nKeys: r refresh | f repo filter | w window | tab focus | enter open/details | q quit"
    )


def _normalize_window(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in _WINDOW_OPTIONS:
        return "24h"
    return normalized


def _next_window(current: str) -> str:
    normalized = _normalize_window(current)
    idx = _WINDOW_OPTIONS.index(normalized)
    return _WINDOW_OPTIONS[(idx + 1) % len(_WINDOW_OPTIONS)]


def _next_repo_filter(current: str | None, available: tuple[str, ...]) -> str | None:
    options: tuple[str | None, ...] = (None, *available)
    if current not in options:
        return options[0]
    idx = options.index(current)
    return options[(idx + 1) % len(options)]


def _render_seconds(value: float) -> str:
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60.0:.1f}m"
    return f"{value / 3600.0:.2f}h"


def _render_ratio(value: float) -> str:
    return f"{value * 100.0:.1f}%"


def _tracked_row_key(row: TrackedOrBlockedRow) -> tuple[str, int | None, int, str]:
    return (row.repo_full_name, row.pr_number, row.issue_number, row.status)


def _url_for_active_row(row: ActiveAgentRow, column: int) -> str | None:
    if column == _ACTIVE_COL_ISSUE:
        return f"https://github.com/{row.repo_full_name}/issues/{row.issue_number}"
    if column == _ACTIVE_COL_PR and row.pr_number is not None:
        return f"https://github.com/{row.repo_full_name}/pull/{row.pr_number}"
    if column == _ACTIVE_COL_BRANCH and row.branch and row.branch != "-":
        return f"https://github.com/{row.repo_full_name}/tree/{row.branch}"
    return None


def _url_for_tracked_row(row: TrackedOrBlockedRow, column: int) -> str | None:
    if column == _TRACKED_COL_PR and row.pr_number is not None:
        return f"https://github.com/{row.repo_full_name}/pull/{row.pr_number}"
    if column == _TRACKED_COL_ISSUE:
        return f"https://github.com/{row.repo_full_name}/issues/{row.issue_number}"
    if column == _TRACKED_COL_BRANCH and row.branch and row.branch != "-":
        return f"https://github.com/{row.repo_full_name}/tree/{row.branch}"
    return None
