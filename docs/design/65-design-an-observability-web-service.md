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
  - src/mergexo/observability_web.py
  - src/mergexo/observability_api.py
  - src/mergexo/static/observability/index.html
  - src/mergexo/static/observability/assets/*
  - web/observability-frontend/package.json
  - web/observability-frontend/tsconfig.json
  - web/observability-frontend/vite.config.ts
  - web/observability-frontend/src/main.tsx
  - web/observability-frontend/src/App.tsx
  - web/observability-frontend/src/components/ActiveAgentsTable.tsx
  - web/observability-frontend/src/components/MetricsPanel.tsx
  - web/observability-frontend/src/styles.css
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_service_runner.py
  - tests/test_observability_web.py
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

Add an observability dashboard that is hosted by the existing Python MergeXO process. TypeScript is used only for the frontend UI. The backend API stays in Python and reads/writes the existing SQLite state, so operators deploy one service, not two.

The dashboard provides:
1. Active issue/PR agents and elapsed runtime.
2. Failures and blocked work.
3. Open tracked PRs/issues and queue state.
4. History for issues and PRs.
5. Metrics: mean runtime, runtime std-dev, and failure rate (global and per repo).

## Key decision from PR feedback

1. No standalone TypeScript backend service.
2. No Hono sidecar process.
3. Python hosts API and static frontend assets.
4. TypeScript is frontend-only.

This addresses deployment overhead and keeps all observability data access in the same runtime that already owns state transitions.

## Context

Current MergeXO state already includes real-time operational data:
1. `issue_runs` for issue lifecycle status.
2. `pr_feedback_state` for tracked and blocked PRs.
3. `feedback_events`, `operator_commands`, `runtime_operations`, and `agent_sessions`.

Gap:
1. Existing tables are mainly current-state snapshots.
2. We need durable run-history records for accurate timeline and metrics (mean/std-dev/failure rate).

## Goals

1. Ship a local web dashboard for observability.
2. Keep deployment as a single Python service.
3. Keep frontend in TypeScript.
4. Preserve multi-repo support.
5. Add durable history for issue/PR run analytics.

## Non-goals

1. Building a second deployable backend service.
2. Replacing GitHub as source of truth.
3. Building distributed telemetry infrastructure.
4. Enabling write actions in MVP (read-only first).

## Proposed architecture

### 1. Process model

1. `mergexo service` starts an optional observability HTTP server in-process (background thread).
2. HTTP server exposes:
- JSON API endpoints under `/api/observability/v1/*`
- static frontend bundle under `/observability`.
3. Server reads SQLite in WAL-safe read mode.
4. ServiceRunner shutdown stops the HTTP server cleanly.

Outcome:
1. One deployed process.
2. One state DB.
3. No extra backend runtime to operate.

### 2. Python web/API implementation

New Python modules:
1. `src/mergexo/observability_api.py`
- pure query/aggregation functions returning immutable DTOs.
2. `src/mergexo/observability_web.py`
- HTTP routing and static file serving.

Framework approach:
1. Prefer Python stdlib (`http.server` + `ThreadingHTTPServer`) for MVP to avoid new runtime dependencies.
2. Endpoints return JSON only.
3. Frontend bundle served from `src/mergexo/static/observability/`.

### 3. TypeScript frontend implementation

Frontend lives in `web/observability-frontend`:
1. React + TypeScript + Vite.
2. Polls Python JSON endpoints every 5s (configurable).
3. Build output copied into `src/mergexo/static/observability/`.

Deployment model:
1. Node toolchain required for frontend development/build.
2. Runtime production service is Python-only.

### 4. Durable observability data model additions

Additive SQLite tables in `StateStore`:

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

### 5. Instrumentation points in orchestrator/service

1. On enqueue (`issue`, `implementation`, `pre_pr_followup`, `feedback`): insert `agent_run_history` start row and store `run_id` in in-memory running metadata.
2. On terminal reap in `_reap_finished`: finalize `run_id` with terminal status, error class, and duration.
3. On PR status changes: append `pr_status_history` transition rows.
4. On startup: mark stale unfinished runs as `interrupted`.

### 6. API endpoints (MVP)

1. `GET /api/observability/v1/health`
2. `GET /api/observability/v1/repos`
3. `GET /api/observability/v1/overview?repo=<repo|all>&window=24h`
4. `GET /api/observability/v1/active-agents?repo=...`
5. `GET /api/observability/v1/tracked?repo=...`
6. `GET /api/observability/v1/blocked?repo=...`
7. `GET /api/observability/v1/history/issues/:issueNumber?repo=...&limit=...`
8. `GET /api/observability/v1/history/prs/:prNumber?repo=...&limit=...`
9. `GET /api/observability/v1/metrics?repo=...&window=7d`

### 7. Metrics definitions

1. Terminal sample set: `agent_run_history` rows with `finished_at IS NOT NULL` and terminal status in (`completed`, `failed`, `blocked`, `interrupted`).
2. Mean runtime: `AVG(duration_seconds)`.
3. Std-dev runtime: `sqrt(AVG(x^2) - AVG(x)^2)`, with `0` for sample size < 2.
4. Failure rate: `failed_count / terminal_count`.
5. Repo breakdown: same formulas grouped by `repo_full_name`.

## Dashboard scope

### 1. Overview page

1. Active agents count.
2. Blocked PR count.
3. Tracked PR count.
4. Recent failures.
5. Mean/std-dev runtime.

### 2. Active agents page

1. Repo, run kind, issue, PR, branch.
2. Start timestamp and elapsed runtime.
3. Sort by longest-running.

### 3. Tracked and blocked page

1. Tracked PR table.
2. Blocked PR table with reason and pending event count.

### 4. History page

1. Issue history timeline.
2. PR history timeline.
3. Operator/restart events where relevant.

### 5. Metrics page

1. Global metrics.
2. Per-repo breakdown.
3. Time window filters.

## Implementation plan

1. Clean and keep one frontmatter/document body for this design doc.
2. Add observability config block (`enabled`, `host`, `port`, `poll_seconds`, `retention_days`).
3. Add new history tables + indexes in `StateStore._init_schema`.
4. Add `StateStore` methods for run start/finish and PR status transition append.
5. Extend orchestrator running metadata with `run_id`.
6. Instrument enqueue/reap/status transition paths.
7. Add Python HTTP server lifecycle into `ServiceRunner`.
8. Add frontend project and static build output wiring.
9. Add README and config docs.
10. Add retention/pruning of history rows.

## Testing plan

Python tests:
1. `tests/test_state.py`: schema, run start/finish, stale-run interruption, transition history.
2. `tests/test_orchestrator.py`: enqueue creates run rows; reap finalizes exactly once; classification correctness.
3. `tests/test_service_runner.py`: observability server start/stop integration.
4. `tests/test_observability_web.py`: endpoint payloads, filtering, metrics formula correctness.

Frontend tests:
1. API contract tests against fixture payloads.
2. UI smoke tests for active agents and metrics rendering.

## Acceptance criteria

1. Observability dashboard is reachable from the Python MergeXO service process.
2. No second backend service is required in deployment.
3. TypeScript is used only for frontend UI assets.
4. Dashboard shows active issue/PR agents with elapsed runtime.
5. Dashboard shows tracked and blocked PRs with reasons and pending event counts.
6. Dashboard shows issue and PR history from durable run/transition records.
7. Dashboard reports mean runtime, std-dev runtime, and failure rate globally and per repo.
8. Metrics formulas are explicitly documented and match API output.
9. Multi-repo filters work across all dashboard views.
10. Startup reconciles stale unfinished runs to `interrupted`.
11. DB changes are additive and do not require destructive reset.

## Risks and mitigations

1. Risk: HTTP server complexity in existing service loop.
Mitigation: isolate in `observability_web.py` with explicit start/stop lifecycle and tests.

2. Risk: SQLite contention.
Mitigation: WAL mode, short read queries, query indexes, read-only endpoint semantics.

3. Risk: History table growth.
Mitigation: retention policy and periodic prune.

4. Risk: Frontend/backend API drift.
Mitigation: typed response schemas and contract tests.

5. Risk: Sensitive local data exposure.
Mitigation: default bind `127.0.0.1`, optional auth token for non-local binds, explicit redaction rules.

## Rollout notes

1. Land DB and Python instrumentation first.
2. Enable observability server behind config flag.
3. Canary on one repo and validate parity with CLI/state queries.
4. Roll out across multi-repo deployments after one stable week.
5. Keep write actions out of MVP; evaluate later behind auth and explicit config.

## Open questions

1. Default retention window (`30`, `90`, `180` days)?
2. Should blocked attempts be part of top-level failure KPI or shown as separate KPI only?
3. Should non-local observability access be allowed at all in MVP, or strictly localhost-only?
