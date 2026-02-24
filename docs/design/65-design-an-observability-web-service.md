---
issue: 65
priority: 2
touch_paths:
  - docs/design/65-design-an-observability-web-service.md
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/service_runner.py
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - src/mergexo/observability_queries.py
  - src/mergexo/observability_tui.py
  - pyproject.toml
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_service_runner.py
  - tests/test_observability_queries.py
  - tests/test_observability_tui.py
  - mergexo.toml.example
  - README.md
depends_on:
  - 54
estimated_size: L
generated_at: 2026-02-24T00:00:00Z
---

# Design an observability web service

_Issue: #65 (https://github.com/johnynek/mergexo/issues/65)_

## Summary

Rescope issue #65 to a Python terminal UI (TUI) MVP instead of a browser dashboard. The goal is an `htop`/`btop`-style observability experience that operators can use over SSH without deploying a second service.

This design provides:
1. Active issue/PR agents and runtime duration.
2. Failures and blocked work.
3. Tracked/open PR and issue state.
4. Issue and PR history.
5. Metrics: mean runtime, runtime std-dev, and failure rate (global and per repo).

## Decision from PR feedback

1. Do not ship a separate web backend.
2. Do not require TypeScript frontend for MVP.
3. Build an all-Python terminal experience first.
4. Keep architecture compatible with a future web UI if needed later.

## Which Python TUI libraries fit best?

### Shortlist

1. `textual`
- Best feature fit for `htop`/`btop`-like dashboards.
- Rich layout system, tables, keybindings, periodic refresh, async-friendly.
- Good developer ergonomics and active ecosystem.

2. `rich` + `Live`
- Very good for simple read-only live panels.
- Less suitable for multi-screen navigation and complex interaction.

3. `urwid`
- Mature and capable.
- Older API style; steeper path for modern dashboard UX.

4. `prompt_toolkit`
- Excellent for command-driven shells.
- Weaker fit for dense, panel-based monitoring UI.

### Recommendation

Use `textual` for MVP. It gives the right interaction model for an `htop`/`btop`-style console while keeping implementation in Python.

## Goals

1. Provide a terminal observability console runnable on the same host as MergeXO.
2. Require only Python runtime in production.
3. Show active agents, blocked failures, tracked work, and history.
4. Provide global and per-repo metrics.
5. Keep multi-repo support first-class.

## Non-goals

1. Browser-based dashboard in this phase.
2. Distributed telemetry backend.
3. Write operations from TUI in MVP (read-only first).
4. Replacing GitHub as the system of record.

## Proposed architecture

### 1. Process model

1. Add a new CLI command, for example `mergexo top --config mergexo.toml`.
2. Command opens a local terminal dashboard and reads `state.db` directly.
3. TUI runs independently from `mergexo service` process (same host, same DB).
4. Operator can SSH into host and run the TUI safely in read-only mode.

### 2. Data model additions for durable history

Current tables are mostly snapshot state; we add history tables for timelines and metrics:

`agent_run_history`
1. `run_id TEXT PRIMARY KEY`
2. `repo_full_name TEXT NOT NULL`
3. `run_kind TEXT NOT NULL` (`issue_flow`, `implementation_flow`, `pre_pr_followup`, `feedback_turn`)
4. `issue_number INTEGER NOT NULL`
5. `pr_number INTEGER NULL`
6. `flow TEXT NULL`
7. `branch TEXT NULL`
8. `started_at TEXT NOT NULL`
9. `finished_at TEXT NULL`
10. `terminal_status TEXT NULL` (`completed`, `failed`, `blocked`, `merged`, `closed`, `interrupted`)
11. `failure_class TEXT NULL` (`agent_error`, `tests_failed`, `policy_block`, `github_error`, `history_rewrite`, `unknown`)
12. `error TEXT NULL`
13. `duration_seconds REAL NULL`
14. `meta_json TEXT NOT NULL DEFAULT '{}'`

Indexes:
1. `(repo_full_name, finished_at)`
2. `(repo_full_name, started_at)`
3. `(repo_full_name, terminal_status, started_at)`

`pr_status_history`
1. `id INTEGER PRIMARY KEY AUTOINCREMENT`
2. `repo_full_name TEXT NOT NULL`
3. `pr_number INTEGER NOT NULL`
4. `issue_number INTEGER NOT NULL`
5. `from_status TEXT NULL`
6. `to_status TEXT NOT NULL`
7. `reason TEXT NULL`
8. `detail TEXT NULL`
9. `changed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))`

Indexes:
1. `(repo_full_name, pr_number, changed_at)`
2. `(repo_full_name, changed_at)`

### 3. Instrumentation points in orchestrator/service

1. On enqueue (`issue`, `implementation`, `pre_pr_followup`, `feedback`), write `agent_run_history` start row and keep `run_id` in memory metadata.
2. In `_reap_finished`, finalize `run_id` with terminal status, duration, and failure class.
3. On PR status updates, append `pr_status_history` transitions.
4. On startup, reconcile stale unfinished runs to `interrupted`.

### 4. Query layer

Add `src/mergexo/observability_queries.py` with pure read functions:
1. `load_overview(repo_filter, window)`
2. `load_active_agents(repo_filter)`
3. `load_tracked_and_blocked(repo_filter)`
4. `load_issue_history(repo_filter, issue_number, limit)`
5. `load_pr_history(repo_filter, pr_number, limit)`
6. `load_metrics(repo_filter, window)`

All query outputs are typed dataclasses/tuples and immutable-by-convention.

### 5. TUI layout and interaction model

Implement with Textual in `src/mergexo/observability_tui.py`.

Main panels:
1. Top summary bar: active agents, blocked PRs, tracked PRs, failures (24h), mean/std-dev runtime.
2. Active agents table: repo, run kind, issue, PR, branch, started_at, elapsed.
3. Blocked/tracked panel: blocked reason, pending event count, status.
4. History panel: issue/PR timeline for selected row.
5. Metrics panel: per-repo failure rate and runtime stats.

Keybindings (MVP):
1. `r`: manual refresh.
2. `f`: set repo filter.
3. `w`: set time window (`1h`, `24h`, `7d`, `30d`).
4. `tab`: cycle focused panel.
5. `enter`: open detail view for selected issue/PR.
6. `q`: quit.

Refresh behavior:
1. Auto-refresh every 2s by default (configurable).
2. Full-screen redraw with stable row selection where possible.

### 6. Metrics definitions

1. Terminal sample set: rows with `finished_at IS NOT NULL` and terminal status in (`completed`, `failed`, `blocked`, `interrupted`).
2. Mean runtime: `AVG(duration_seconds)`.
3. Runtime std-dev: `sqrt(AVG(x^2) - AVG(x)^2)`, return `0` when sample size < 2.
4. Failure rate: `failed_count / terminal_count`.
5. Per-repo breakdown: same formulas grouped by `repo_full_name`.

## Implementation plan

1. Add Textual dependency in `pyproject.toml`.
2. Add TUI runtime config fields (refresh interval, default window, optional row limit).
3. Add history tables + indexes in `StateStore._init_schema`.
4. Add run-history/transition write APIs to `StateStore`.
5. Instrument orchestrator/service paths for run lifecycle writes.
6. Implement read query module (`observability_queries.py`).
7. Implement Textual app (`observability_tui.py`) and CLI entrypoint in `cli.py`.
8. Document command usage in README and sample config.
9. Add retention/pruning policy for history tables.

## Testing plan

1. `tests/test_state.py`
- schema creation for history tables.
- run start/finish and stale-run reconciliation.
- PR transition history append behavior.

2. `tests/test_orchestrator.py`
- enqueue writes run start rows.
- terminal completion writes final rows exactly once.
- failure class mapping coverage.

3. `tests/test_observability_queries.py`
- repo/time-window filters.
- metric math correctness (mean, std-dev, failure rate).

4. `tests/test_observability_tui.py`
- panel rendering with fixture datasets.
- keybinding behavior and selection flow.

5. `tests/test_service_runner.py`
- no behavior regressions from instrumentation hooks.

## Acceptance criteria

1. Operators can SSH into host and run the observability TUI with one command.
2. TUI shows all active issue and feedback agents with elapsed runtime.
3. TUI shows blocked and tracked PRs with reasons and pending events.
4. TUI shows issue and PR history timelines from durable history tables.
5. TUI reports mean runtime, std-dev runtime, and failure rate globally and per repo.
6. Repo filter and time-window filter work across all panels.
7. Metrics formulas are documented and match query outputs.
8. Stale unfinished runs are reconciled to `interrupted` after restart.
9. DB changes are additive and do not require destructive reset.
10. README documents the new TUI operation workflow.

## Risks and mitigations

1. Risk: TUI dependency footprint and compatibility.
Mitigation: pin Textual version; keep fallback path for plain CLI summaries.

2. Risk: SQLite read contention with running service.
Mitigation: WAL mode, short read transactions, indexed queries.

3. Risk: History table growth.
Mitigation: retention window and periodic prune.

4. Risk: Terminal rendering differences across SSH environments.
Mitigation: test with common TERM settings; fallback non-interactive summary mode.

5. Risk: Complex TUI interaction increases implementation time.
Mitigation: ship a simple read-only panel set first; defer advanced actions.

## Rollout notes

1. Land state schema + instrumentation first.
2. Add query layer and CLI command returning non-interactive summary output.
3. Add full-screen Textual TUI.
4. Canary with one repo and compare values against direct SQL/CLI checks.
5. Roll out to multi-repo operators after stability window.

## Open questions

1. Should blocked attempts be included in top-level failure KPI or always shown separately?
2. Default history retention window: `30`, `90`, or `180` days?
3. Do we want a read-only MVP only, or minimal operator actions (`unblock`, `restart`) in TUI v1?
