---
issue: 65
priority: 3
touch_paths:
  - docs/design/65-design-an-observability-web-service.md
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/service_runner.py
  - src/mergexo/models.py
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - mergexo.toml.example
  - README.md
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_service_runner.py
  - web/observability/package.json
  - web/observability/tsconfig.json
  - web/observability/src/server/main.ts
  - web/observability/src/server/routes.ts
  - web/observability/src/server/sqlite.ts
  - web/observability/src/server/queries.ts
  - web/observability/src/server/routes.test.ts
  - web/observability/src/client/main.tsx
  - web/observability/src/client/App.tsx
  - web/observability/src/client/components/ActiveAgentsTable.tsx
  - web/observability/src/client/components/MetricsPanel.tsx
  - web/observability/src/client/styles.css
depends_on: []
estimated_size: M
generated_at: 2026-02-24T05:19:28Z
---

# Design an observability web service

_Issue: #65 (https://github.com/johnynek/mergexo/issues/65)_

## Summary

Design for a TypeScript observability web service with durable run-history storage, dashboard APIs/UI, metrics definitions, acceptance criteria, risks, and rollout plan for MergeXO.

---
issue: 65
priority: 2
touch_paths:
  - docs/design/65-design-an-observability-web-service.md
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/service_runner.py
  - src/mergexo/models.py
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - mergexo.toml.example
  - README.md
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_service_runner.py
  - web/observability/package.json
  - web/observability/tsconfig.json
  - web/observability/src/server/main.ts
  - web/observability/src/server/routes.ts
  - web/observability/src/server/sqlite.ts
  - web/observability/src/server/queries.ts
  - web/observability/src/server/routes.test.ts
  - web/observability/src/client/main.tsx
  - web/observability/src/client/App.tsx
  - web/observability/src/client/components/ActiveAgentsTable.tsx
  - web/observability/src/client/components/MetricsPanel.tsx
  - web/observability/src/client/styles.css
depends_on:
  - 33
  - 54
estimated_size: L
generated_at: 2026-02-24T00:00:00Z
---

# Design an observability web service

_Issue: #65 (https://github.com/johnynek/mergexo/issues/65)_

## Summary

Build a TypeScript observability web service that reads MergeXO runtime state and durable run history, then exposes a dashboard for:
1. Active issue/PR agents and elapsed runtime.
2. Failures and blocked work.
3. Open tracked issues/PRs and lifecycle status.
4. Per-issue and per-PR history timelines.
5. Metrics (mean runtime, standard deviation, failure rate), globally and per repo.

The design uses a sidecar TypeScript app (`web/observability`) plus additive SQLite tables written by the Python orchestrator for reliable history and metrics.

## Context

What exists today:
1. `state.db` already tracks current runtime state (`issue_runs`, `pr_feedback_state`, `feedback_events`, `agent_sessions`, `operator_commands`, `runtime_operations`).
2. Structured logs exist in `<runtime.base_dir>/logs/YYYY-MM-DD.log`, but they are best-effort and tied to verbose mode.
3. There is no durable append-only history table for run attempts or PR status transitions.
4. Current state tables are mostly latest-state snapshots, so they cannot reliably answer duration distributions or long-window history.

Implication:
1. A dashboard can show current state today.
2. Accurate history + mean/std-dev/failure-rate requires durable attempt records.

## Goals

1. Provide a local web dashboard with fast visibility into MergeXO operations.
2. Show active agents for issues and PRs, with elapsed runtime.
3. Show failures, blocked PRs, tracked PRs, and pre-PR follow-up waits.
4. Show issue and PR history timelines.
5. Provide mean runtime, runtime std-dev, and failure rate globally and per repo.
6. Keep multi-repo support first-class.
7. Keep architecture simple and mostly functional/immutable in TypeScript.

## Non-goals

1. Replacing GitHub as source of truth for issue/PR content.
2. Building a cross-machine distributed metrics platform.
3. Shipping write actions (unblock/restart) in MVP; read-only is default.
4. Requiring webhooks; polling remains acceptable.

## Framework choice

Chosen stack:
1. Backend: `Hono` (TypeScript, small, functional middleware style).
2. Runtime: Node + `tsx` for dev, standard build for prod.
3. Frontend: React + TypeScript + Vite.
4. Data fetching: TanStack Query.
5. Validation: Zod schemas at API boundaries.

Rationale:
1. Minimal framework overhead.
2. Clear request/response composition and immutable DTO handling.
3. Easy local deployment as sidecar next to current Python service.

## Proposed architecture

### 1. Topology

1. Python `mergexo service` continues to orchestrate work and write `state.db`.
2. New TypeScript service reads `state.db` in read-only mode and serves:
- JSON API (`/api/v1/*`)
- dashboard SPA (`/`)
3. DB access uses SQLite WAL-compatible read-only connections.
4. Dashboard updates with short polling (5s default).

### 2. Durable observability data model

Additive tables in `state.db`:

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
1. `(repo_full_name, finished_at)` for active-runs queries.
2. `(repo_full_name, started_at)` for time-window metrics.
3. `(repo_full_name, terminal_status, started_at)` for failure/blocked trends.

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
1. `(repo_full_name, pr_number, changed_at)` for PR timeline.
2. `(repo_full_name, changed_at)` for activity feeds.

Optional but useful (`service_poll_history`):
1. Snapshot queue and capacity each poll for saturation graphs.

### 3. Python instrumentation changes

Write-path changes in orchestrator/service runner:

1. On enqueue of issue/implementation/pre-PR-follow-up work:
- insert `agent_run_history` row with `started_at`, `run_kind`, `flow`, `issue_number`, `branch`.
- store `run_id` in in-memory metadata.

2. On enqueue of feedback turn:
- insert `agent_run_history` row with `run_kind=feedback_turn`, `issue_number`, `pr_number`.

3. On completion/failure/block in `_reap_finished`:
- finalize matching `run_id` with `finished_at`, `terminal_status`, `duration_seconds`, `failure_class`, `error`.

4. On PR status transitions in `mark_pr_status` call sites:
- append row to `pr_status_history` with reason/detail.

5. On startup poll setup:
- mark stale `agent_run_history` rows (`finished_at IS NULL`) as `interrupted`.

### 4. API surface (read-only MVP)

1. `GET /api/v1/health`
- service health and DB readability.

2. `GET /api/v1/repos`
- configured repo list from DB/config.

3. `GET /api/v1/overview?repo=<repo|all>&window=24h`
- summary counts: active agents, tracked PRs, blocked PRs, awaiting followups, failures in window, pending restart op.

4. `GET /api/v1/active-agents?repo=...`
- active rows from `agent_run_history` (`finished_at IS NULL`) with elapsed seconds.

5. `GET /api/v1/tracked?repo=...`
- tracked PRs from `pr_feedback_state` plus issue status from `issue_runs`.

6. `GET /api/v1/blocked?repo=...`
- blocked PRs including pending feedback events and latest error.

7. `GET /api/v1/history/issues/:issueNumber?repo=...&limit=...`
- issue timeline from `agent_run_history`, `issue_runs`, and related PR transitions.

8. `GET /api/v1/history/prs/:prNumber?repo=...&limit=...`
- PR timeline from `pr_status_history`, feedback runs, and operator command results.

9. `GET /api/v1/metrics?repo=...&window=7d`
- global and per-repo:
  - `mean_work_time_seconds`
  - `stddev_work_time_seconds`
  - `failure_rate`
  - `blocked_rate`
  - counts (`completed`, `failed`, `blocked`, `interrupted`)

### 5. Metric definitions

1. Terminal sample set:
- rows in `agent_run_history` with non-null `finished_at` and `terminal_status` in (`completed`, `failed`, `blocked`, `interrupted`).

2. Mean runtime:
- `AVG(duration_seconds)` on terminal sample.

3. Runtime std-dev:
- `sqrt(AVG(duration_seconds * duration_seconds) - AVG(duration_seconds)^2)`.
- returns `0` when sample size < 2.

4. Failure rate:
- `failed_count / terminal_count` where `failed_count` uses `terminal_status='failed'`.

5. Blocked rate:
- `blocked_count / terminal_count` where `terminal_status='blocked'`.

6. Repo breakdown:
- same metrics grouped by `repo_full_name`.

### 6. Dashboard UI

Views:

1. **Overview**
- cards: active agents, blocked PRs, tracked PRs, failures (24h), mean runtime, std-dev.
- repo filter and time-window selector.

2. **Active Agents**
- table: repo, run kind, issue, PR, branch, started_at, elapsed, current status.
- sort by elapsed descending.

3. **Tracked and Blocked**
- tracked PR table (awaiting feedback).
- blocked PR table with reason and pending event count.

4. **History**
- issue timeline by issue number.
- PR timeline by PR number.
- includes transitions, retries, blocks, merge/close events.

5. **Metrics**
- per-repo comparison table.
- time-series for failure rate and mean runtime.

MVP interactions:

1. Click-through links to GitHub issue/PR URLs.
2. Copy helper commands (`/mergexo unblock ...`, `/mergexo restart ...`) from UI.
3. Filter/sort/search controls.

Post-MVP optional interactions:

1. Action endpoints to submit unblock/restart requests (disabled by default, explicit auth required).

### 7. Functional/immutable coding approach (TypeScript)

1. Query modules return `readonly` DTO arrays.
2. Mapping/aggregation functions are pure and side-effect free.
3. Route handlers perform parse/validate -> pure compute -> serialize response.
4. No mutable singleton state except DB connection pool.

## Key things visible in dashboard

Required by issue plus additions:

1. Which issues and PRs have active agents.
2. How long each active agent has been running.
3. Failures and blocked reasons.
4. Open tracked PRs and issues in MergeXO lifecycle.
5. Per-issue history.
6. Per-PR history.
7. Mean agent work time (global and per repo).
8. Runtime std-dev (global and per repo).
9. Failure rate (global and per repo).
10. Additional high-value signals: queue pressure, pending feedback events, restart operation state, operator command outcomes.

## Implementation plan

1. Add new state schema tables and indexes in `StateStore._init_schema`.
2. Add `StateStore` APIs:
- `start_agent_run(...) -> run_id`
- `finish_agent_run(...)`
- `append_pr_status_transition(...)`
- query helpers used by tests (optional for Python).
3. Extend `_RunningIssueMetadata` and feedback in-memory handles to carry `run_id`.
4. Instrument enqueue and reap paths in `orchestrator.py`.
5. Mark stale unfinished runs as `interrupted` at startup.
6. Add/adjust observability events for correlation IDs (`run_id`).
7. Scaffold `web/observability` TypeScript project and scripts.
8. Implement SQLite query layer and API routes.
9. Implement dashboard UI pages and shared filters.
10. Document startup/run instructions in README.
11. Add config examples (listen host/port, optional auth, retention window).
12. Add retention/pruning for history tables (for example 90 days default).

## Testing plan

Python tests:

1. `tests/test_state.py`
- schema creation for new tables.
- start/finish run behavior.
- stale-run interruption logic.
- PR transition append behavior.

2. `tests/test_orchestrator.py`
- each enqueue path creates one run history row.
- terminal outcomes finalize same row exactly once.
- blocked/failed classification mapping.

3. `tests/test_service_runner.py`
- startup interruption reconciliation.
- optional poll history snapshots if implemented.

TypeScript tests:

1. `routes.test.ts`
- endpoint contract validation.
- repo/time-window filtering.
- metrics aggregation correctness.

2. UI smoke tests
- overview renders with mock API payload.
- active-agent elapsed-time rendering.

Manual canary checks:

1. Active agent starts; row appears within 5s.
2. Failure appears in blocked/failures views.
3. Issue and PR timeline shows ordered transitions.
4. Metrics match expected values for known synthetic dataset.

## Acceptance criteria

1. Dashboard lists all currently active issue and feedback agents with repo, issue/PR id, and elapsed runtime.
2. Dashboard lists tracked PRs and blocked PRs with reasons and pending event counts.
3. Dashboard exposes issue history and PR history timelines from durable data (not only in-memory state).
4. Dashboard reports mean runtime and runtime std-dev for selected window, globally and per repo.
5. Dashboard reports failure rate for selected window, globally and per repo.
6. Metrics use deterministic formulas documented in this design.
7. Multi-repo filtering works across all views.
8. SQLite migration is additive and does not require destructive reset.
9. If MergeXO restarts mid-run, unfinished runs are eventually marked `interrupted` and not left forever active.
10. README documents how to run the observability web service.

## Risks and mitigations

1. Risk: SQLite read/write contention under concurrent polling.
Mitigation: read-only DB connections in web service, WAL mode, short queries, proper indexes.

2. Risk: Incomplete or inconsistent history due to missed finalize calls.
Mitigation: finalize in `_reap_finished` single path, startup stale-run reconciliation, unit tests for exactly-once semantics.

3. Risk: Metric confusion (what counts as failure).
Mitigation: explicit definitions in API response (`failure_rate`, `blocked_rate`, denominator counts).

4. Risk: Dashboard introduces another runtime/toolchain (Node).
Mitigation: isolate under `web/observability`, minimal dependencies, documented one-command startup.

5. Risk: Sensitive error text exposure.
Mitigation: local bind default (`127.0.0.1`), optional auth gate, avoid rendering raw HTML, sanitize output.

6. Risk: History tables grow indefinitely.
Mitigation: retention policy + periodic prune job + indexes aligned to windowed queries.

## Rollout notes

1. Land additive DB schema + Python instrumentation first.
2. Verify no behavior regression in core orchestration paths.
3. Deploy web service in read-only mode on one canary repo.
4. Compare dashboard counts against CLI/state queries for one week.
5. Enable broader multi-repo rollout after parity is stable.
6. Keep action endpoints disabled until read-only reliability is proven.
7. Introduce optional action endpoints (unblock/restart) behind explicit auth and config flag in a follow-up.

## Open questions

1. Should blocked attempts count toward "failure rate" in the top-level KPI or remain separate only?
2. What default retention window is acceptable for local disk usage (30/90/180 days)?
3. Should action endpoints ever bypass GitHub comment-command flow, or always emit commands through GitHub for audit parity?
