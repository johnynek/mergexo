from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import platform
from queue import Empty, SimpleQueue
import subprocess
import time
from typing import Any, Callable
import webbrowser

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Static

from mergexo.observability_queries import (
    ActiveAgentRow,
    IssueHistoryRow,
    MetricsStats,
    OverviewStats,
    PrHistoryRow,
    TerminalIssueOutcomeRow,
    TrackedOrBlockedRow,
    load_active_agents,
    load_metrics,
    load_overview,
    load_terminal_issue_outcomes,
    load_tracked_and_blocked,
)
from mergexo.service_runner import ServiceSignal


_WINDOW_OPTIONS: tuple[str, ...] = ("1h", "24h", "7d", "30d")
_ACTIVE_COL_ISSUE = 2
_ACTIVE_COL_PR = 3
_ACTIVE_COL_BRANCH = 5
_TRACKED_COL_ISSUE = 1
_TRACKED_COL_PR = 2
_TRACKED_COL_BRANCH = 4
_BLOCKED_REASON_MAX_CHARS = 36
_HISTORY_SCROLL_LOAD_THRESHOLD = 5
_SERVICE_SIGNAL_DRAIN_SECONDS = 0.2
_SERVICE_SIGNAL_REFRESH_DEBOUNCE_SECONDS = 0.2


@dataclass(frozen=True)
class _DetailTarget:
    kind: str
    number: int


@dataclass(frozen=True)
class _TrackedSortKey:
    column_index: int
    descending: bool


@dataclass(frozen=True)
class _DetailField:
    label: str
    value: str
    url: str | None = None


class _DetailFieldTable(DataTable):
    """Use DataTable Enter to activate the selected detail field."""

    def action_select_cursor(self) -> None:
        screen = self.screen
        if isinstance(screen, _DetailModal):
            screen.action_activate()


class _DetailModal(ModalScreen[None]):
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("enter", "activate", "Open"),
        Binding("q", "close", "Close"),
    ]
    CSS = """
    #detail-dialog {
        width: 90%;
        height: 80%;
        border: round $accent;
        background: $surface;
        padding: 1 2;
    }
    #detail-title {
        text-style: bold;
        margin-bottom: 1;
    }
    #detail-fields-table {
        height: 1fr;
        margin-bottom: 1;
    }
    #detail-body-scroll {
        height: 1fr;
        border: round $boost;
        padding: 0 1;
    }
    #detail-hint {
        margin-top: 1;
    }
    """

    def __init__(
        self,
        *,
        title: str,
        body: str,
        fields: tuple[_DetailField, ...] = (),
    ) -> None:
        super().__init__()
        self._title = title
        self._body = body
        self._fields = fields

    def compose(self) -> ComposeResult:
        with Vertical(id="detail-dialog"):
            yield Static(self._title, id="detail-title")
            if self._fields:
                yield _DetailFieldTable(id="detail-fields-table")
            if self._body:
                with VerticalScroll(id="detail-body-scroll"):
                    yield Static(self._body, id="detail-body")
            hint = (
                "Use arrows to select a field value; Enter opens link. Press Esc or q to close."
                if self._fields
                else "Press Esc, Enter, or q to close."
            )
            yield Static(hint, id="detail-hint")

    def on_mount(self) -> None:
        if not self._fields:
            return
        table = self.query_one("#detail-fields-table", DataTable)
        table.add_columns("Field", "Value")
        for field in self._fields:
            table.add_row(field.label, field.value)
        if table.row_count > 0:
            table.move_cursor(row=0, column=1, animate=False)
        table.focus()

    def action_activate(self) -> None:
        if not self._fields:
            self.action_close()
            return
        table = self.query_one("#detail-fields-table", DataTable)
        row_index = table.cursor_row
        if row_index < 0 or row_index >= len(self._fields):
            return
        selected = self._fields[row_index]
        if selected.url is None:
            return
        app = self.app
        if isinstance(app, ObservabilityApp):
            app._open_external_url(selected.url)
            return
        _open_external_url(selected.url)

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        if event.data_table.id != "detail-fields-table":
            return
        self.action_activate()

    def action_close(self) -> None:
        self.dismiss(None)


class _DetailDataTable(DataTable):
    """Use DataTable's native Enter key binding to drive app detail actions."""

    def action_select_cursor(self) -> None:
        super().action_select_cursor()
        if self.id not in {"active-table", "tracked-table", "history-table"}:
            return
        app = self.app
        if isinstance(app, ObservabilityApp):
            app.action_show_detail()


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
        service_signal_queue: SimpleQueue[ServiceSignal] | None = None,
        on_shutdown: Callable[[], None] | None = None,
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
        self._tracked_sort_stack: tuple[_TrackedSortKey, ...] = ()
        self._history_rows: tuple[TerminalIssueOutcomeRow, ...] = ()
        self._history_offset: int = 0
        self._history_has_more: bool = True
        self._history_loading: bool = False
        self._detail_target: _DetailTarget | None = None
        self._service_signal_queue = service_signal_queue
        self._on_shutdown = on_shutdown
        self._shutdown_notified = False
        self._signal_refresh_pending = False
        self._last_refresh_at_monotonic = 0.0
        self._fatal_service_error: str | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical():
            yield Static("", id="summary")
            yield Static("Active Agents", classes="panel-title")
            yield _DetailDataTable(id="active-table")
            yield Static("Tracked And Blocked Work", classes="panel-title")
            yield _DetailDataTable(id="tracked-table")
            yield Static("History", classes="panel-title")
            yield _DetailDataTable(id="history-table")
            yield Static("Metrics", classes="panel-title")
            yield DataTable(id="metrics-table")
        yield Footer()

    def on_mount(self) -> None:
        self._init_tables()
        self.refresh_data()
        self.set_interval(self._refresh_seconds, self.refresh_data)
        if self._service_signal_queue is not None:
            self.set_interval(_SERVICE_SIGNAL_DRAIN_SECONDS, self._drain_service_signals)

    @property
    def fatal_service_error(self) -> str | None:
        return self._fatal_service_error

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

    async def action_quit(self) -> None:
        self._notify_shutdown()
        await super().action_quit()

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        if event.data_table.id != "tracked-table":
            return
        self._sort_tracked_by_column(event.column_index)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id != "history-table":
            return
        if event.cursor_row < 0:
            return
        if not self._history_has_more or self._history_loading:
            return
        remaining = event.data_table.row_count - event.cursor_row - 1
        if remaining > _HISTORY_SCROLL_LOAD_THRESHOLD:
            return
        self._load_more_history_rows()

    def _sort_tracked_by_column(self, column_index: int) -> None:
        self._tracked_sort_stack = _next_tracked_sort_stack(self._tracked_sort_stack, column_index)
        self._apply_tracked_sort()
        self._refresh_tracked_table()

    def _apply_tracked_sort(self) -> None:
        self._tracked_rows = _sort_tracked_rows(self._tracked_rows, self._tracked_sort_stack)

    def _history_page_size(self) -> int:
        if self._row_limit is None:
            return 200
        return max(1, self._row_limit)

    def action_show_detail(self) -> None:
        focused = self.focused
        if isinstance(focused, DataTable) and focused.id == "active-table":
            selected = self._active_row_selection()
            if selected is not None:
                active_url = _url_for_active_row(selected, focused.cursor_column)
                if active_url is not None:
                    self._open_external_url(active_url)
                    return
                self._detail_target = _DetailTarget(kind="issue", number=selected.issue_number)
                self._show_detail_context(
                    title=f"Issue #{selected.issue_number} Context",
                    body=_active_row_context(selected),
                    fields=_active_row_detail_fields(selected),
                )
                return
        elif isinstance(focused, DataTable) and focused.id == "tracked-table":
            selected = self._tracked_row_selection()
            if selected is not None:
                tracked_url = _url_for_tracked_row(selected, focused.cursor_column)
                if tracked_url is not None:
                    self._open_external_url(tracked_url)
                    return
                if selected.pr_number is None:
                    self._detail_target = _DetailTarget(kind="issue", number=selected.issue_number)
                else:
                    self._detail_target = _DetailTarget(kind="pr", number=selected.pr_number)
                work_label = (
                    f"PR #{selected.pr_number} Context"
                    if selected.pr_number is not None
                    else f"Issue #{selected.issue_number} Context"
                )
                self._show_detail_context(
                    title=work_label,
                    body=_tracked_row_context(selected),
                    fields=_tracked_row_detail_fields(selected),
                )
                return
        elif isinstance(focused, DataTable) and focused.id == "history-table":
            selected = self._history_row_selection()
            if selected is not None:
                self._show_detail_context(
                    title=_terminal_history_title(selected),
                    body=_terminal_history_context(selected),
                    fields=_terminal_history_detail_fields(selected),
                )
                return

    def _open_external_url(self, url: str) -> None:
        if _open_external_url(url):
            return
        self._show_detail_context(
            title="Open URL Manually",
            body=(
                "MergeXO could not open the system browser automatically.\n\n"
                "Open this URL manually:\n"
                f"{url}"
            ),
        )

    def _show_detail_context(
        self,
        *,
        title: str,
        body: str,
        fields: tuple[_DetailField, ...] = (),
    ) -> None:
        if not self.is_running:
            return
        self.push_screen(_DetailModal(title=title, body=body, fields=fields))

    def _base_screen(self) -> Screen[Any]:
        if self.screen_stack:
            return self.screen_stack[0]
        return self.screen

    def _base_table(self, selector: str) -> DataTable:
        return self._base_screen().query_one(selector, DataTable)

    def _base_static(self, selector: str) -> Static:
        return self._base_screen().query_one(selector, Static)

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
        self._apply_tracked_sort()
        metrics = load_metrics(self._db_path, self._repo_filter, self._window)
        self._available_repos = tuple(sorted({row.repo_full_name for row in metrics.per_repo}))
        self._base_static("#summary").update(
            _summary_text(overview=overview, repo_filter=self._repo_filter, window=self._window)
        )
        self._refresh_active_table()
        self._refresh_tracked_table()
        self._refresh_metrics_table(metrics)
        self._refresh_history_table(reset=True)
        self._last_refresh_at_monotonic = time.monotonic()
        self._signal_refresh_pending = False

    def _drain_service_signals(self) -> None:
        if self._service_signal_queue is None:
            return

        requested_refresh = False
        while True:
            try:
                signal = self._service_signal_queue.get_nowait()
            except Empty:
                break

            if signal.kind == "fatal_error":
                detail = signal.detail or "Service runner failed."
                if self._fatal_service_error is None:
                    self._fatal_service_error = detail
                self._notify_shutdown()
                self.exit()
                return
            requested_refresh = True

        if requested_refresh:
            self._request_refresh_from_signal()
            return
        self._maybe_refresh_from_pending_signal()

    def _request_refresh_from_signal(self) -> None:
        elapsed_seconds = time.monotonic() - self._last_refresh_at_monotonic
        if elapsed_seconds >= _SERVICE_SIGNAL_REFRESH_DEBOUNCE_SECONDS:
            self.refresh_data()
            return
        self._signal_refresh_pending = True

    def _maybe_refresh_from_pending_signal(self) -> None:
        if not self._signal_refresh_pending:
            return
        elapsed_seconds = time.monotonic() - self._last_refresh_at_monotonic
        if elapsed_seconds < _SERVICE_SIGNAL_REFRESH_DEBOUNCE_SECONDS:
            return
        self.refresh_data()

    def _notify_shutdown(self) -> None:
        if self._shutdown_notified:
            return
        self._shutdown_notified = True
        if self._on_shutdown is None:
            return
        self._on_shutdown()

    def _init_tables(self) -> None:
        active = self._base_table("#active-table")
        tracked = self._base_table("#tracked-table")
        history = self._base_table("#history-table")
        metrics = self._base_table("#metrics-table")
        active.add_columns("Repo", "Run", "Issue", "PR", "Flow", "Branch", "Started", "Elapsed")
        tracked.add_columns(
            "Repo",
            "Issue",
            "PR",
            "Status",
            "Branch",
            "Pending",
            "Blocked Reason",
            "Updated",
        )
        history.add_columns("Time", "Outcome", "Repo", "Issue", "PR", "Status")
        metrics.add_columns("Repo", "Terminal", "Failed", "Failure Rate", "Mean", "StdDev")

    def _refresh_active_table(self) -> None:
        table = self._base_table("#active-table")
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
        table = self._base_table("#tracked-table")
        selected = self._tracked_row_selection()
        previous_key = _tracked_row_key(selected) if selected is not None else None
        previous_column = table.cursor_column if table.row_count > 0 else 0
        table.clear(columns=False)
        for row in self._tracked_rows:
            table.add_row(
                row.repo_full_name,
                str(row.issue_number),
                str(row.pr_number) if row.pr_number is not None else "",
                row.status,
                row.branch,
                str(row.pending_event_count),
                _render_context_snippet(row.blocked_reason, max_chars=_BLOCKED_REASON_MAX_CHARS),
                row.updated_at,
            )
        self._restore_tracked_selection(previous_key, previous_column)

    def _refresh_metrics_table(self, metrics: MetricsStats) -> None:
        table = self._base_table("#metrics-table")
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

    def _refresh_history_table(self, *, reset: bool = False) -> None:
        table = self._base_table("#history-table")
        previous_key: tuple[str, int, int | None, str, str] | None = None
        previous_row = 0
        previous_column = 0
        if reset:
            selected = self._history_row_selection()
            previous_key = _history_row_key(selected) if selected is not None else None
            previous_row = table.cursor_row if table.row_count > 0 else 0
            previous_column = table.cursor_column if table.row_count > 0 else 0
            table.clear(columns=False)
            self._history_rows = ()
            self._history_offset = 0
            self._history_has_more = True
        self._load_more_history_rows()
        if reset:
            self._restore_history_selection(previous_key, previous_row, previous_column)

    def _load_more_history_rows(self) -> None:
        if self._history_loading or not self._history_has_more:
            return
        self._history_loading = True
        try:
            page_size = self._history_page_size()
            rows = load_terminal_issue_outcomes(
                self._db_path,
                self._repo_filter,
                limit=page_size,
                offset=self._history_offset,
            )
            self._history_rows = (*self._history_rows, *rows)
            self._history_offset += len(rows)
            if len(rows) < page_size:
                self._history_has_more = False
            self._render_history_rows(rows)
        finally:
            self._history_loading = False

    def _render_history_rows(self, rows: tuple[TerminalIssueOutcomeRow, ...]) -> None:
        table = self._base_table("#history-table")
        if not self._history_rows:
            table.clear(columns=False)
            table.add_row("-", "-", "-", "-", "-", "No terminal issue outcomes found")
            return
        if (
            table.row_count == 1
            and str(table.get_row_at(0)[5]) == "No terminal issue outcomes found"
        ):
            table.clear(columns=False)
        for row in rows:
            table.add_row(
                row.updated_at,
                _terminal_issue_outcome_label(row),
                row.repo_full_name,
                str(row.issue_number),
                str(row.pr_number) if row.pr_number is not None else "-",
                row.status,
            )

    def _active_row_selection(self) -> ActiveAgentRow | None:
        table = self._base_table("#active-table")
        if table.row_count < 1:
            return None
        index = table.cursor_row
        if index < 0 or index >= len(self._active_rows):
            return None
        return self._active_rows[index]

    def _tracked_row_selection(self) -> TrackedOrBlockedRow | None:
        table = self._base_table("#tracked-table")
        if table.row_count < 1:
            return None
        index = table.cursor_row
        if index < 0 or index >= len(self._tracked_rows):
            return None
        return self._tracked_rows[index]

    def _history_row_selection(self) -> TerminalIssueOutcomeRow | None:
        table = self._base_table("#history-table")
        if table.row_count < 1:
            return None
        index = table.cursor_row
        if index < 0 or index >= len(self._history_rows):
            return None
        return self._history_rows[index]

    def _restore_active_selection(self, previous_run_id: str | None, previous_column: int) -> None:
        table = self._base_table("#active-table")
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
        table = self._base_table("#tracked-table")
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

    def _restore_history_selection(
        self,
        previous_key: tuple[str, int, int | None, str, str] | None,
        previous_row: int,
        previous_column: int,
    ) -> None:
        table = self._base_table("#history-table")
        if table.row_count < 1:
            return
        row_index = min(max(previous_row, 0), table.row_count - 1)
        if previous_key is not None:
            for idx, row in enumerate(self._history_rows):
                if _history_row_key(row) == previous_key:
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
    service_signal_queue: SimpleQueue[ServiceSignal] | None = None,
    on_shutdown: Callable[[], None] | None = None,
) -> None:
    app = ObservabilityApp(
        db_path=db_path,
        refresh_seconds=refresh_seconds,
        default_window=default_window,
        row_limit=row_limit,
        service_signal_queue=service_signal_queue,
        on_shutdown=on_shutdown,
    )
    app.run()
    if app.fatal_service_error is not None:
        raise RuntimeError(app.fatal_service_error)


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


def _history_row_key(row: TerminalIssueOutcomeRow) -> tuple[str, int, int | None, str, str]:
    return (
        row.repo_full_name,
        row.issue_number,
        row.pr_number,
        row.status,
        row.updated_at,
    )


def _terminal_issue_outcome_label(row: TerminalIssueOutcomeRow) -> str:
    if row.status == "merged":
        return "completed"
    if row.status == "closed":
        return "abandoned"
    return row.status


def _terminal_history_title(row: TerminalIssueOutcomeRow) -> str:
    outcome = _terminal_issue_outcome_label(row).capitalize()
    return f"Issue #{row.issue_number} {outcome}"


def _terminal_history_context(row: TerminalIssueOutcomeRow) -> str:
    repo_url = _repo_url(row.repo_full_name)
    issue_url = _issue_url(row.repo_full_name, row.issue_number)
    lines = [
        f"Repo: {row.repo_full_name}",
        f"Repo URL: {repo_url}",
        f"Issue: {row.issue_number}",
        f"Issue URL: {issue_url}",
    ]
    if row.pr_number is not None:
        pr_url = _pr_url(row.repo_full_name, row.pr_number)
        lines.extend(
            [
                f"PR: {row.pr_number}",
                f"PR URL: {pr_url}",
            ]
        )
    else:
        lines.append("PR: -")
    lines.extend(
        [
            f"Outcome: {_terminal_issue_outcome_label(row)}",
            f"Status: {row.status}",
            f"Branch: {row.branch or '-'}",
            f"Updated At: {row.updated_at}",
        ]
    )
    return "\n".join(lines)


def _terminal_history_detail_fields(row: TerminalIssueOutcomeRow) -> tuple[_DetailField, ...]:
    repo_url = _repo_url(row.repo_full_name)
    issue_url = _issue_url(row.repo_full_name, row.issue_number)
    pr_value = str(row.pr_number) if row.pr_number is not None else "-"
    pr_url = _pr_url(row.repo_full_name, row.pr_number) if row.pr_number is not None else None
    branch_value = row.branch or "-"
    branch_url = _branch_url(row.repo_full_name, row.branch) if row.branch else None
    return (
        _DetailField("Repo", row.repo_full_name, repo_url),
        _DetailField("Repo URL", repo_url, repo_url),
        _DetailField("Issue", str(row.issue_number), issue_url),
        _DetailField("Issue URL", issue_url, issue_url),
        _DetailField("PR", pr_value, pr_url),
        _DetailField("Outcome", _terminal_issue_outcome_label(row)),
        _DetailField("Status", row.status),
        _DetailField("Branch", branch_value, branch_url),
        _DetailField("Updated At", row.updated_at),
    )


def _next_tracked_sort_stack(
    current: tuple[_TrackedSortKey, ...],
    column_index: int,
) -> tuple[_TrackedSortKey, ...]:
    for item in current:
        if item.column_index != column_index:
            continue
        toggled = _TrackedSortKey(column_index=column_index, descending=not item.descending)
        remaining = tuple(entry for entry in current if entry.column_index != column_index)
        return (toggled, *remaining)
    return (_TrackedSortKey(column_index=column_index, descending=True), *current)


def _sort_tracked_rows(
    rows: tuple[TrackedOrBlockedRow, ...],
    sort_stack: tuple[_TrackedSortKey, ...],
) -> tuple[TrackedOrBlockedRow, ...]:
    if len(rows) < 2 or not sort_stack:
        return rows
    sorted_rows = list(rows)
    for sort_key in reversed(sort_stack):
        sorted_rows.sort(
            key=lambda row, column_index=sort_key.column_index: _tracked_sort_value(
                row,
                column_index,
            ),
            reverse=sort_key.descending,
        )
    return tuple(sorted_rows)


def _tracked_sort_value(row: TrackedOrBlockedRow, column_index: int) -> str | int:
    if column_index == 0:
        return row.repo_full_name.casefold()
    if column_index == 1:
        return row.issue_number
    if column_index == 2:
        if row.pr_number is None:
            return -1
        return row.pr_number
    if column_index == 3:
        return row.status.casefold()
    if column_index == 4:
        return row.branch.casefold()
    if column_index == 5:
        return row.pending_event_count
    if column_index == 6:
        return (row.blocked_reason or "").casefold()
    if column_index == 7:
        return row.updated_at
    return ""


def _repo_url(repo_full_name: str) -> str:
    return f"https://github.com/{repo_full_name}"


def _issue_url(repo_full_name: str, issue_number: int) -> str:
    return f"{_repo_url(repo_full_name)}/issues/{issue_number}"


def _pr_url(repo_full_name: str, pr_number: int) -> str:
    return f"{_repo_url(repo_full_name)}/pull/{pr_number}"


def _branch_url(repo_full_name: str, branch: str) -> str:
    return f"{_repo_url(repo_full_name)}/tree/{branch}"


def _commit_url(repo_full_name: str, sha: str) -> str:
    return f"{_repo_url(repo_full_name)}/commit/{sha}"


def _active_row_detail_fields(row: ActiveAgentRow) -> tuple[_DetailField, ...]:
    branch_value = row.branch or "-"
    branch_link = None
    if row.branch and row.branch != "-":
        branch_link = _branch_url(row.repo_full_name, row.branch)
    pr_value = str(row.pr_number) if row.pr_number is not None else "-"
    pr_link = _pr_url(row.repo_full_name, row.pr_number) if row.pr_number is not None else None
    repo_link = _repo_url(row.repo_full_name)
    return (
        _DetailField("Repo", row.repo_full_name, repo_link),
        _DetailField("Repo URL", repo_link, repo_link),
        _DetailField(
            "Issue", str(row.issue_number), _issue_url(row.repo_full_name, row.issue_number)
        ),
        _DetailField("PR", pr_value, pr_link),
        _DetailField("Flow", row.flow or "-"),
        _DetailField("Branch", branch_value, branch_link),
        _DetailField("Started", row.started_at),
        _DetailField("Elapsed", _render_seconds(row.elapsed_seconds)),
        _DetailField("Codex Mode", row.codex_mode or "-"),
        _DetailField("Codex Session ID", row.codex_session_id or "-"),
        _DetailField("Codex Invocation Started", row.codex_invocation_started_at or "-"),
    )


def _tracked_row_detail_fields(row: TrackedOrBlockedRow) -> tuple[_DetailField, ...]:
    branch_value = row.branch or "-"
    branch_link = None
    if row.branch and row.branch != "-":
        branch_link = _branch_url(row.repo_full_name, row.branch)
    pr_value = str(row.pr_number) if row.pr_number is not None else "-"
    pr_link = _pr_url(row.repo_full_name, row.pr_number) if row.pr_number is not None else None
    sha_value = row.last_seen_head_sha or "-"
    sha_link = None
    if row.last_seen_head_sha:
        sha_link = _commit_url(row.repo_full_name, row.last_seen_head_sha)
    repo_link = _repo_url(row.repo_full_name)
    return (
        _DetailField("Repo", row.repo_full_name, repo_link),
        _DetailField("Repo URL", repo_link, repo_link),
        _DetailField(
            "Issue", str(row.issue_number), _issue_url(row.repo_full_name, row.issue_number)
        ),
        _DetailField("PR", pr_value, pr_link),
        _DetailField("Status", row.status),
        _DetailField("Branch", branch_value, branch_link),
        _DetailField("Pending Events", str(row.pending_event_count)),
        _DetailField("Last Seen Head SHA", sha_value, sha_link),
        _DetailField("Updated", row.updated_at),
    )


def _url_for_active_row(row: ActiveAgentRow, column: int) -> str | None:
    if column == _ACTIVE_COL_ISSUE:
        return _issue_url(row.repo_full_name, row.issue_number)
    if column == _ACTIVE_COL_PR and row.pr_number is not None:
        return _pr_url(row.repo_full_name, row.pr_number)
    if column == _ACTIVE_COL_BRANCH and row.branch and row.branch != "-":
        return _branch_url(row.repo_full_name, row.branch)
    return None


def _url_for_tracked_row(row: TrackedOrBlockedRow, column: int) -> str | None:
    if column == _TRACKED_COL_PR and row.pr_number is not None:
        return _pr_url(row.repo_full_name, row.pr_number)
    if column == _TRACKED_COL_ISSUE:
        return _issue_url(row.repo_full_name, row.issue_number)
    if column == _TRACKED_COL_BRANCH and row.branch and row.branch != "-":
        return _branch_url(row.repo_full_name, row.branch)
    return None


def _render_context_snippet(value: str | None, *, max_chars: int) -> str:
    if value is None:
        return "-"
    if max_chars < 1:
        return ""
    text = value.strip()
    if not text:
        return "-"
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return "." * max_chars
    return text[: max_chars - 3] + "..."


def _active_row_context(row: ActiveAgentRow) -> str:
    prompt_value = row.prompt or "-"
    return "\n".join(
        [
            f"Repo: {row.repo_full_name}",
            f"Run Kind: {row.run_kind}",
            f"Issue: {row.issue_number}",
            f"PR: {row.pr_number if row.pr_number is not None else '-'}",
            f"Flow: {row.flow or '-'}",
            f"Branch: {row.branch or '-'}",
            f"Started: {row.started_at}",
            f"Elapsed: {_render_seconds(row.elapsed_seconds)}",
            f"Codex Mode: {row.codex_mode or '-'}",
            f"Codex Session ID: {row.codex_session_id or '-'}",
            f"Codex Invocation Started: {row.codex_invocation_started_at or '-'}",
            "",
            "Last Prompt:",
            prompt_value,
        ]
    )


def _tracked_row_context(row: TrackedOrBlockedRow) -> str:
    repo_url = _repo_url(row.repo_full_name)
    return "\n".join(
        [
            f"Repo: {row.repo_full_name}",
            f"Repo URL: {repo_url}",
            f"Issue: {row.issue_number}",
            f"PR: {row.pr_number if row.pr_number is not None else '-'}",
            f"Status: {row.status}",
            f"Branch: {row.branch}",
            f"Pending Events: {row.pending_event_count}",
            f"Last Seen Head SHA: {row.last_seen_head_sha or '-'}",
            f"Updated: {row.updated_at}",
            "",
            "Blocked Reason:",
            row.blocked_reason or "-",
        ]
    )


def _open_external_url(url: str) -> bool:
    if platform.system().lower() == "darwin":
        try:
            process = subprocess.run(
                ["open", url],
                check=False,
                capture_output=True,
            )
            if process.returncode == 0:
                return True
        except OSError:
            pass
    try:
        return bool(webbrowser.open_new_tab(url))
    except webbrowser.Error:
        return False
