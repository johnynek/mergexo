from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from math import sqrt
from pathlib import Path
import sqlite3


@dataclass(frozen=True)
class OverviewStats:
    active_agents: int
    blocked_prs: int
    tracked_prs: int
    failures: int
    mean_runtime_seconds: float
    stddev_runtime_seconds: float


@dataclass(frozen=True)
class ActiveAgentRow:
    run_id: str
    repo_full_name: str
    run_kind: str
    issue_number: int
    pr_number: int | None
    flow: str | None
    branch: str | None
    started_at: str
    elapsed_seconds: float
    prompt: str | None = None
    codex_mode: str | None = None
    codex_session_id: str | None = None
    codex_invocation_started_at: str | None = None


@dataclass(frozen=True)
class TrackedOrBlockedRow:
    repo_full_name: str
    pr_number: int | None
    issue_number: int
    status: str
    branch: str
    last_seen_head_sha: str | None
    blocked_reason: str | None
    pending_event_count: int
    updated_at: str


@dataclass(frozen=True)
class IssueHistoryRow:
    run_id: str
    repo_full_name: str
    run_kind: str
    issue_number: int
    pr_number: int | None
    flow: str | None
    branch: str | None
    started_at: str
    finished_at: str | None
    terminal_status: str | None
    failure_class: str | None
    error: str | None
    duration_seconds: float | None


@dataclass(frozen=True)
class PrHistoryRow:
    id: int
    repo_full_name: str
    pr_number: int
    issue_number: int
    from_status: str | None
    to_status: str
    reason: str | None
    detail: str | None
    changed_at: str


@dataclass(frozen=True)
class TerminalIssueOutcomeRow:
    repo_full_name: str
    issue_number: int
    pr_number: int | None
    status: str
    branch: str | None
    updated_at: str


@dataclass(frozen=True)
class RuntimeMetric:
    repo_full_name: str
    terminal_count: int
    failed_count: int
    failure_rate: float
    mean_runtime_seconds: float
    stddev_runtime_seconds: float


@dataclass(frozen=True)
class MetricsStats:
    overall: RuntimeMetric
    per_repo: tuple[RuntimeMetric, ...]


def load_overview(
    db_path: Path, repo_filter: str | None = None, window: str = "24h"
) -> OverviewStats:
    with _connect(db_path) as conn:
        repo_clause, repo_params = _repo_filter_sql(repo_filter)
        pr_repo_clause, pr_repo_params = _repo_filter_sql(repo_filter, prefix="p")
        issue_repo_clause, issue_repo_params = _repo_filter_sql(repo_filter, prefix="i")
        active_rows = conn.execute(
            f"""
            SELECT meta_json
            FROM agent_run_history
            WHERE finished_at IS NULL
              {repo_clause}
            """,
            repo_params,
        ).fetchall()
        active_agents = sum(
            1
            for row in active_rows
            if _active_run_meta_from_json(_as_str(row[0], "meta_json")).codex_active
        )
        blocked_prs = _fetch_int(
            conn,
            f"""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM pr_feedback_state AS p
                    LEFT JOIN issue_takeover_state AS t
                        ON t.repo_full_name = p.repo_full_name
                       AND t.issue_number = p.issue_number
                    WHERE (
                        p.status = 'blocked'
                        OR (
                            p.status = 'awaiting_feedback'
                            AND COALESCE(t.ignore_active, 0) = 1
                        )
                    )
                      {pr_repo_clause}
                )
                +
                (
                    SELECT COUNT(*)
                    FROM issue_runs AS i
                    WHERE i.status = 'awaiting_issue_followup'
                      {issue_repo_clause}
                )
            """,
            (*pr_repo_params, *issue_repo_params),
        )
        tracked_prs = _fetch_int(
            conn,
            f"""
            SELECT COUNT(*)
            FROM pr_feedback_state AS p
            LEFT JOIN issue_takeover_state AS t
                ON t.repo_full_name = p.repo_full_name
               AND t.issue_number = p.issue_number
            WHERE p.status = 'awaiting_feedback'
              AND COALESCE(t.ignore_active, 0) = 0
              {pr_repo_clause}
            """,
            pr_repo_params,
        )
        cutoff_modifier = _window_modifier(window)
        failures = _fetch_int(
            conn,
            f"""
            SELECT COUNT(*)
            FROM agent_run_history
            WHERE finished_at IS NOT NULL
              AND terminal_status = 'failed'
              AND finished_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
              {repo_clause}
            """,
            (cutoff_modifier, *repo_params),
        )
        metrics = _load_metrics(conn=conn, repo_filter=repo_filter, window=window)
        return OverviewStats(
            active_agents=active_agents,
            blocked_prs=blocked_prs,
            tracked_prs=tracked_prs,
            failures=failures,
            mean_runtime_seconds=metrics.overall.mean_runtime_seconds,
            stddev_runtime_seconds=metrics.overall.stddev_runtime_seconds,
        )


def load_active_agents(
    db_path: Path,
    repo_filter: str | None = None,
    *,
    limit: int | None = None,
    now: datetime | None = None,
) -> tuple[ActiveAgentRow, ...]:
    now_value = now if now is not None else datetime.now(timezone.utc)
    now_iso = now_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with _connect(db_path) as conn:
        repo_clause, repo_params = _repo_filter_sql(repo_filter)
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be >= 1")
        rows = conn.execute(
            f"""
            SELECT
                run_id,
                repo_full_name,
                run_kind,
                issue_number,
                pr_number,
                flow,
                branch,
                started_at,
                MAX(0.0, (julianday(?) - julianday(started_at)) * 86400.0) AS elapsed_seconds,
                meta_json
            FROM agent_run_history
            WHERE finished_at IS NULL
              {repo_clause}
            ORDER BY started_at ASC, repo_full_name ASC, issue_number ASC
            """,
            (now_iso, *repo_params),
        ).fetchall()
    active_rows = tuple(
        ActiveAgentRow(
            run_id=_as_str(row[0], "run_id"),
            repo_full_name=_as_str(row[1], "repo_full_name"),
            run_kind=_as_str(row[2], "run_kind"),
            issue_number=_as_int(row[3], "issue_number"),
            pr_number=_as_optional_int(row[4], "pr_number"),
            flow=_as_optional_str(row[5], "flow"),
            branch=_as_optional_str(row[6], "branch"),
            started_at=_as_str(row[7], "started_at"),
            elapsed_seconds=_as_float(row[8], "elapsed_seconds"),
            prompt=meta.prompt,
            codex_mode=meta.codex_mode,
            codex_session_id=meta.codex_session_id,
            codex_invocation_started_at=meta.codex_invocation_started_at,
        )
        for row in rows
        for meta in (_active_run_meta_from_json(_as_str(row[9], "meta_json")),)
        if meta.codex_active
    )
    if limit is None:
        return active_rows
    return active_rows[:limit]


def load_tracked_and_blocked(
    db_path: Path,
    repo_filter: str | None = None,
    *,
    limit: int | None = None,
) -> tuple[TrackedOrBlockedRow, ...]:
    with _connect(db_path) as conn:
        pr_repo_clause, pr_repo_params = _repo_filter_sql(repo_filter, prefix="p")
        issue_repo_clause, issue_repo_params = _repo_filter_sql(repo_filter, prefix="i")
        history_repo_clause, history_repo_params = _repo_filter_sql(repo_filter, prefix="h")
        limit_clause = ""
        params: tuple[object, ...]
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be >= 1")
            limit_clause = "LIMIT ?"
            params = (*pr_repo_params, *issue_repo_params, limit)
        else:
            params = (*pr_repo_params, *issue_repo_params)
        rows = conn.execute(
            f"""
            WITH tracked_work AS (
                SELECT
                    p.repo_full_name AS repo_full_name,
                    p.pr_number AS pr_number,
                    p.issue_number AS issue_number,
                    CASE
                        WHEN p.status = 'awaiting_feedback' AND COALESCE(t.ignore_active, 0) = 1
                        THEN 'blocked'
                        ELSE p.status
                    END AS status,
                    COALESCE(p.branch, '-') AS branch,
                    p.last_seen_head_sha AS last_seen_head_sha,
                    CASE
                        WHEN p.status = 'blocked' THEN r.error
                        WHEN p.status = 'awaiting_feedback' AND COALESCE(t.ignore_active, 0) = 1
                        THEN 'ignored (takeover active)'
                        ELSE NULL
                    END AS blocked_reason,
                    COUNT(e.event_key) AS pending_event_count,
                    p.updated_at AS updated_at
                FROM pr_feedback_state AS p
                LEFT JOIN issue_runs AS r
                    ON r.repo_full_name = p.repo_full_name
                   AND r.issue_number = p.issue_number
                LEFT JOIN issue_takeover_state AS t
                    ON t.repo_full_name = p.repo_full_name
                   AND t.issue_number = p.issue_number
                LEFT JOIN feedback_events AS e
                    ON e.repo_full_name = p.repo_full_name
                   AND e.pr_number = p.pr_number
                   AND e.processed_at IS NULL
                WHERE p.status IN ('awaiting_feedback', 'blocked')
                  {pr_repo_clause}
                GROUP BY
                    p.repo_full_name,
                    p.pr_number,
                    p.issue_number,
                    p.status,
                    p.branch,
                    p.last_seen_head_sha,
                    t.ignore_active,
                    r.error,
                    p.updated_at

                UNION ALL

                SELECT
                    i.repo_full_name AS repo_full_name,
                    NULL AS pr_number,
                    i.issue_number AS issue_number,
                    i.status AS status,
                    COALESCE(i.branch, f.branch, '-') AS branch,
                    NULL AS last_seen_head_sha,
                    COALESCE(f.waiting_reason, i.error) AS blocked_reason,
                    0 AS pending_event_count,
                    i.updated_at AS updated_at
                FROM issue_runs AS i
                LEFT JOIN pre_pr_followup_state AS f
                    ON f.repo_full_name = i.repo_full_name
                   AND f.issue_number = i.issue_number
                WHERE i.status = 'awaiting_issue_followup'
                  {issue_repo_clause}
            )
            SELECT
                repo_full_name,
                pr_number,
                issue_number,
                status,
                branch,
                last_seen_head_sha,
                blocked_reason,
                pending_event_count,
                updated_at
            FROM tracked_work
            ORDER BY
                updated_at DESC,
                repo_full_name ASC,
                issue_number ASC,
                COALESCE(pr_number, 0) ASC
            {limit_clause}
            """,
            params,
        ).fetchall()
        active_feedback_rows = conn.execute(
            f"""
            SELECT
                h.repo_full_name,
                h.pr_number,
                h.meta_json
            FROM agent_run_history AS h
            WHERE h.run_kind = 'feedback_turn'
              AND h.finished_at IS NULL
              {history_repo_clause}
            """,
            history_repo_params,
        ).fetchall()
    tracked_rows = tuple(
        TrackedOrBlockedRow(
            repo_full_name=_as_str(row[0], "repo_full_name"),
            pr_number=_as_optional_int(row[1], "pr_number"),
            issue_number=_as_int(row[2], "issue_number"),
            status=_as_str(row[3], "status"),
            branch=_as_str(row[4], "branch"),
            last_seen_head_sha=_as_optional_str(row[5], "last_seen_head_sha"),
            blocked_reason=_as_optional_str(row[6], "error"),
            pending_event_count=_as_int(row[7], "pending_event_count"),
            updated_at=_as_str(row[8], "updated_at"),
        )
        for row in rows
    )
    active_feedback_prs = {
        (_as_str(row[0], "repo_full_name"), _as_int(row[1], "pr_number"))
        for row in active_feedback_rows
        if row[1] is not None
        and _active_run_meta_from_json(_as_str(row[2], "meta_json")).codex_active
    }
    if not active_feedback_prs:
        return tracked_rows
    return tuple(
        row
        for row in tracked_rows
        if row.pr_number is None or (row.repo_full_name, row.pr_number) not in active_feedback_prs
    )


def load_issue_history(
    db_path: Path,
    repo_filter: str | None,
    issue_number: int,
    limit: int,
) -> tuple[IssueHistoryRow, ...]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    with _connect(db_path) as conn:
        repo_clause, repo_params = _repo_filter_sql(repo_filter)
        rows = conn.execute(
            f"""
            SELECT
                run_id,
                repo_full_name,
                run_kind,
                issue_number,
                pr_number,
                flow,
                branch,
                started_at,
                finished_at,
                terminal_status,
                failure_class,
                error,
                duration_seconds
            FROM agent_run_history
            WHERE issue_number = ?
              {repo_clause}
            ORDER BY started_at DESC, run_id DESC
            LIMIT ?
            """,
            (issue_number, *repo_params, limit),
        ).fetchall()
    return tuple(
        IssueHistoryRow(
            run_id=_as_str(row[0], "run_id"),
            repo_full_name=_as_str(row[1], "repo_full_name"),
            run_kind=_as_str(row[2], "run_kind"),
            issue_number=_as_int(row[3], "issue_number"),
            pr_number=_as_optional_int(row[4], "pr_number"),
            flow=_as_optional_str(row[5], "flow"),
            branch=_as_optional_str(row[6], "branch"),
            started_at=_as_str(row[7], "started_at"),
            finished_at=_as_optional_str(row[8], "finished_at"),
            terminal_status=_as_optional_str(row[9], "terminal_status"),
            failure_class=_as_optional_str(row[10], "failure_class"),
            error=_as_optional_str(row[11], "error"),
            duration_seconds=_as_optional_float(row[12], "duration_seconds"),
        )
        for row in rows
    )


def load_pr_history(
    db_path: Path,
    repo_filter: str | None,
    pr_number: int,
    limit: int,
) -> tuple[PrHistoryRow, ...]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    with _connect(db_path) as conn:
        repo_clause, repo_params = _repo_filter_sql(repo_filter)
        rows = conn.execute(
            f"""
            SELECT
                id,
                repo_full_name,
                pr_number,
                issue_number,
                from_status,
                to_status,
                reason,
                detail,
                changed_at
            FROM pr_status_history
            WHERE pr_number = ?
              {repo_clause}
            ORDER BY changed_at DESC, id DESC
            LIMIT ?
            """,
            (pr_number, *repo_params, limit),
        ).fetchall()
    return tuple(
        PrHistoryRow(
            id=_as_int(row[0], "id"),
            repo_full_name=_as_str(row[1], "repo_full_name"),
            pr_number=_as_int(row[2], "pr_number"),
            issue_number=_as_int(row[3], "issue_number"),
            from_status=_as_optional_str(row[4], "from_status"),
            to_status=_as_str(row[5], "to_status"),
            reason=_as_optional_str(row[6], "reason"),
            detail=_as_optional_str(row[7], "detail"),
            changed_at=_as_str(row[8], "changed_at"),
        )
        for row in rows
    )


def load_terminal_issue_outcomes(
    db_path: Path,
    repo_filter: str | None,
    *,
    limit: int,
    offset: int = 0,
) -> tuple[TerminalIssueOutcomeRow, ...]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")
    with _connect(db_path) as conn:
        repo_clause, repo_params = _repo_filter_sql(repo_filter, prefix="i")
        rows = conn.execute(
            f"""
            SELECT
                i.repo_full_name,
                i.issue_number,
                i.pr_number,
                i.status,
                i.branch,
                i.updated_at
            FROM issue_runs AS i
            WHERE i.status IN ('merged', 'closed')
              {repo_clause}
            ORDER BY
                i.updated_at DESC,
                i.repo_full_name ASC,
                i.issue_number ASC,
                COALESCE(i.pr_number, 0) ASC
            LIMIT ?
            OFFSET ?
            """,
            (*repo_params, limit, offset),
        ).fetchall()
    return tuple(
        TerminalIssueOutcomeRow(
            repo_full_name=_as_str(row[0], "repo_full_name"),
            issue_number=_as_int(row[1], "issue_number"),
            pr_number=_as_optional_int(row[2], "pr_number"),
            status=_as_str(row[3], "status"),
            branch=_as_optional_str(row[4], "branch"),
            updated_at=_as_str(row[5], "updated_at"),
        )
        for row in rows
    )


def load_metrics(
    db_path: Path, repo_filter: str | None = None, window: str = "24h"
) -> MetricsStats:
    with _connect(db_path) as conn:
        return _load_metrics(conn=conn, repo_filter=repo_filter, window=window)


def _load_metrics(
    *, conn: sqlite3.Connection, repo_filter: str | None = None, window: str = "24h"
) -> MetricsStats:
    cutoff_modifier = _window_modifier(window)
    repo_clause, repo_params = _repo_filter_sql(repo_filter)
    overall_row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS terminal_count,
            SUM(CASE WHEN terminal_status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
            AVG(duration_seconds) AS mean_runtime_seconds,
            AVG(duration_seconds * duration_seconds) AS mean_runtime_sq
        FROM agent_run_history
        WHERE finished_at IS NOT NULL
          AND run_kind IN ('issue_flow', 'implementation_flow', 'pre_pr_followup')
          AND terminal_status IN ('completed', 'failed', 'blocked', 'interrupted')
          AND finished_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
          {repo_clause}
        """,
        (cutoff_modifier, *repo_params),
    ).fetchone()
    if overall_row is None:
        raise RuntimeError("metrics query unexpectedly returned no rows")
    overall = _build_metric(
        repo_full_name=repo_filter or "__all__",
        terminal_count=_as_int(overall_row[0], "terminal_count"),
        failed_count=_as_int(overall_row[1], "failed_count"),
        mean_runtime_seconds=_as_optional_float(overall_row[2], "mean_runtime_seconds") or 0.0,
        mean_runtime_sq=_as_optional_float(overall_row[3], "mean_runtime_sq") or 0.0,
    )

    grouped_rows = conn.execute(
        f"""
        SELECT
            repo_full_name,
            COUNT(*) AS terminal_count,
            SUM(CASE WHEN terminal_status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
            AVG(duration_seconds) AS mean_runtime_seconds,
            AVG(duration_seconds * duration_seconds) AS mean_runtime_sq
        FROM agent_run_history
        WHERE finished_at IS NOT NULL
          AND run_kind IN ('issue_flow', 'implementation_flow', 'pre_pr_followup')
          AND terminal_status IN ('completed', 'failed', 'blocked', 'interrupted')
          AND finished_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
          {repo_clause}
        GROUP BY repo_full_name
        ORDER BY repo_full_name ASC
        """,
        (cutoff_modifier, *repo_params),
    ).fetchall()
    per_repo = tuple(
        _build_metric(
            repo_full_name=_as_str(row[0], "repo_full_name"),
            terminal_count=_as_int(row[1], "terminal_count"),
            failed_count=_as_int(row[2], "failed_count"),
            mean_runtime_seconds=_as_optional_float(row[3], "mean_runtime_seconds") or 0.0,
            mean_runtime_sq=_as_optional_float(row[4], "mean_runtime_sq") or 0.0,
        )
        for row in grouped_rows
    )
    return MetricsStats(overall=overall, per_repo=per_repo)


def _build_metric(
    *,
    repo_full_name: str,
    terminal_count: int,
    failed_count: int,
    mean_runtime_seconds: float,
    mean_runtime_sq: float,
) -> RuntimeMetric:
    if terminal_count < 1:
        return RuntimeMetric(
            repo_full_name=repo_full_name,
            terminal_count=0,
            failed_count=0,
            failure_rate=0.0,
            mean_runtime_seconds=0.0,
            stddev_runtime_seconds=0.0,
        )
    variance = max(0.0, mean_runtime_sq - (mean_runtime_seconds * mean_runtime_seconds))
    stddev = sqrt(variance) if terminal_count >= 2 else 0.0
    return RuntimeMetric(
        repo_full_name=repo_full_name,
        terminal_count=terminal_count,
        failed_count=failed_count,
        failure_rate=failed_count / terminal_count,
        mean_runtime_seconds=mean_runtime_seconds,
        stddev_runtime_seconds=stddev,
    )


def _window_modifier(window: str) -> str:
    normalized = window.strip().lower()
    if normalized == "1h":
        return "-1 hours"
    if normalized == "24h":
        return "-24 hours"
    if normalized == "7d":
        return "-7 days"
    if normalized == "30d":
        return "-30 days"
    raise ValueError(f"Unsupported window: {window!r}")


def _repo_filter_sql(
    repo_filter: str | None, *, prefix: str | None = None
) -> tuple[str, tuple[str, ...]]:
    if repo_filter is None:
        return "", ()
    normalized = repo_filter.strip()
    if not normalized:
        return "", ()
    column = "repo_full_name" if prefix is None else f"{prefix}.repo_full_name"
    return f"AND {column} = ?", (normalized,)


@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    try:
        yield conn
    finally:
        conn.close()


def _fetch_int(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> int:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        raise RuntimeError("Expected count query to return a row")
    return _as_int(row[0], "count")


def _as_int(value: object, field: str) -> int:
    if isinstance(value, int):
        return value
    if value is None:
        return 0
    raise RuntimeError(f"Invalid {field} value in observability query result")


def _as_str(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Invalid {field} value in observability query result")
    return value


def _as_optional_str(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"Invalid {field} value in observability query result")
    return value


def _as_optional_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        raise RuntimeError(f"Invalid {field} value in observability query result")
    return value


def _as_float(value: object, field: str) -> float:
    if isinstance(value, int | float):
        return float(value)
    raise RuntimeError(f"Invalid {field} value in observability query result")


def _as_optional_float(value: object, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    raise RuntimeError(f"Invalid {field} value in observability query result")


@dataclass(frozen=True)
class _ActiveRunMeta:
    codex_active: bool
    prompt: str | None
    codex_mode: str | None
    codex_session_id: str | None
    codex_invocation_started_at: str | None


def _active_run_meta_from_json(meta_json: str) -> _ActiveRunMeta:
    try:
        payload = json.loads(meta_json)
    except json.JSONDecodeError:
        payload = None
    if not isinstance(payload, dict):
        payload = {}
    return _ActiveRunMeta(
        codex_active=payload.get("codex_active") is True,
        prompt=_normalized_meta_text(payload.get("last_prompt")),
        codex_mode=_normalized_meta_text(payload.get("codex_mode")),
        codex_session_id=_normalized_meta_text(payload.get("codex_session_id")),
        codex_invocation_started_at=_normalized_meta_text(
            payload.get("codex_invocation_started_at")
        ),
    )


def _normalized_meta_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _prompt_from_meta_json(meta_json: str) -> str | None:
    return _active_run_meta_from_json(meta_json).prompt
