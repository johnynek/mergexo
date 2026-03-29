---
issue: 169
priority: 3
touch_paths:
  - src/mergexo/observability_tui.py
  - src/mergexo/observability_queries.py
  - src/mergexo/state.py
  - tests/test_observability_tui.py
  - tests/test_observability_queries.py
  - tests/test_state.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-03-29T01:59:32Z
---

# Design roadmap observability for the `top` command

_Issue: #169 (https://github.com/johnynek/mergexo/issues/169)_

## Summary

Add roadmap-specific observability to `mergexo top` with a dedicated roadmap panel on the main dashboard and a recursive drill-down explorer backed by existing roadmap state APIs, so operators can inspect frontier state, blockers, revisions, and nested child roadmaps without leaving the TUI.

## Context

`mergexo top` currently reads from `src/mergexo/observability_queries.py` and renders four read-only panels in `src/mergexo/observability_tui.py`: active agents, tracked or blocked work, history, and metrics. That gives good issue and PR visibility, but it does not surface the roadmap DAG that lives in `roadmap_state` and `roadmap_nodes`.

Roadmap execution state already exists in the local DB and in `StateStore` read APIs:

- active roadmap records: `list_active_roadmaps(...)`
- node state: `list_roadmap_nodes(...)` and `list_roadmap_status_snapshot(...)`
- ready frontier: `list_ready_roadmap_frontier(...)`
- blockers: `list_roadmap_blockers_oldest_first(...)`
- revision lineage: `list_roadmap_revisions(...)`

`observability_queries.py` also already exposes lifecycle counts and blocker-age queries, but `top` never renders them. Operators currently have to fall back to `/roadmap status`, raw DB inspection, or issue threads to understand roadmap progress. That is too slow for debugging roadmap rollout problems.

## Problem

1. The main dashboard can show roadmap authoring or revision PR activity indirectly, but it cannot show the state of an activated roadmap.
2. The current TUI has no way to inspect the ready frontier, node-by-node milestone progress, blocked nodes, or pending revision PRs.
3. Nested roadmap nodes are opaque. Once a parent roadmap fans out into a child roadmap, the operator cannot follow that subtree from the dashboard.
4. Re-implementing roadmap scheduling rules inside the UI would risk drift from the orchestrator's actual behavior.

## Goals

1. Add first-class roadmap visibility to `top` without regressing existing issue and PR panels.
2. Let operators drill into a roadmap and see the current frontier, blocker state, revision state, and per-node progress.
3. Support recursive navigation into activated child roadmaps and a clear back path to the parent view.
4. Keep the dashboard read-only and sourced from `state.db`.
5. Reuse existing roadmap semantics from the state layer wherever possible.

## Non-goals

1. Adding roadmap write actions such as revise, abandon, or retry from the TUI.
2. Redesigning roadmap scheduling, milestone semantics, or `/roadmap status` comment behavior.
3. Building a full historical browser for completed and superseded roadmaps in this issue.

## Proposed Design

### 1. Add a dedicated roadmap observability read model

Add a read-only roadmap snapshot layer that the TUI can call without duplicating roadmap logic. The snapshot should be built from the existing roadmap state APIs rather than from fresh UI-specific interpretations of dependency state.

Implementation shape:

1. In `src/mergexo/state.py`, add a small read-only helper or helpers that compose:
   - `list_active_roadmaps(...)`
   - `list_roadmap_nodes(...)`
   - `list_ready_roadmap_frontier(...)`
   - `list_roadmap_blockers_oldest_first(...)`
   - `list_roadmap_revisions(...)`
2. In `src/mergexo/observability_queries.py`, expose TUI-friendly dataclasses such as a top-level `RoadmapOverviewRow` plus a detail payload for one roadmap.
3. The detail payload should include:
   - roadmap issue number, graph version, status, adjustment state, pending revision PR, last error, and updated timestamp
   - ready frontier node ids
   - topologically ordered node rows
   - blockers ordered oldest first
   - recent revision records
4. For roadmap nodes with `kind = roadmap`, enrich the row with child roadmap state when `child_issue_number` matches an activated child roadmap.
5. When a child roadmap issue exists but its roadmap state is not yet activated, surface that as an explicit not-yet-activated display state instead of throwing.

This keeps the TUI aligned with the same frontier and blocker semantics the orchestrator already uses.

### 2. Extend the main dashboard with a compact `Roadmaps` panel

Add a new `Roadmaps` table to `src/mergexo/observability_tui.py`. It should sit on the main screen with the other overview panels and show active roadmaps only.

Recommended columns:

- `Repo`
- `Roadmap`
- `State`
- `Version`
- `Frontier`
- `Blocked`
- `Updated`

Display rules:

1. `State` should distinguish ordinary active execution from `awaiting_revision_merge`.
2. `Frontier` should show a compact summary of the ready node ids, with truncation when needed.
3. `Blocked` should show blocked node count, not just a boolean.
4. `Roadmap` should open the parent roadmap issue URL with `o`.
5. `Enter` on a roadmap row should open a dedicated roadmap explorer instead of the existing generic detail modal.

The summary line should also incorporate the existing roadmap lifecycle counts from `load_roadmap_lifecycle_counts(...)` so the operator can see active, revision-requested, superseded, abandoned, and completed totals at a glance.

### 3. Add a full-screen roadmap explorer with recursive navigation

The roadmap view should not try to fit all DAG state into the top-level dashboard. Add a dedicated `RoadmapExplorerScreen` in `src/mergexo/observability_tui.py` and push it from the main `Roadmaps` table.

The explorer should render:

1. A header block with roadmap metadata:
   - roadmap issue number
   - graph version
   - status and adjustment state
   - pending revision PR, if present
   - last note or last error, if present
2. A compact frontier block that shows the current ready node ids.
3. A node table ordered in stable topological order, not raw `node_id` order.
4. A blockers table ordered oldest first.
5. A small recent-revisions section or table.

Recommended node table columns:

- `Node`
- `Kind`
- `Status`
- `Depends`
- `Child`
- `Frontier`
- `Progress`
- `Blocked`

Interaction model:

1. `Enter` on a non-roadmap node opens a detail modal with the child issue link, timestamps, dependency summary, and body text.
2. `Enter` on a roadmap node with an activated child roadmap pushes another `RoadmapExplorerScreen` for that child roadmap.
3. `Escape`, `Backspace`, or `b` pops back to the parent explorer or the main dashboard.
4. `o` continues to open the selected issue or PR URL when the focused column has one.
5. Refresh continues to re-read the open explorer from `state.db` so the operator can watch fan-out and revision transitions live.

Using a screen stack instead of a single modal gives enough space for DAG state and makes recursive navigation natural.

### 4. Preserve current semantics and fail safely during partial roadmap state

Roadmap activation and revision are multi-step flows. The explorer must handle transitional states without crashing:

1. If a roadmap disappears from the active set while an explorer is open, reload the latest record and show its current status rather than assuming it is still active.
2. If a roadmap node has a child issue but no child roadmap state yet, show that the child roadmap is not activated yet and do not allow drill-in.
3. If dependency JSON or related state is malformed, render a clear placeholder row and keep the TUI responsive.
4. No schema migration is required for this issue. The design reads existing roadmap tables and existing query surfaces.

### 5. Testing

Cover the feature at three layers.

`tests/test_state.py`:

1. Snapshot helpers return ready frontier, blockers, and child roadmap linkage correctly.
2. Nested child roadmap states are surfaced only when the child roadmap has actually activated.
3. Transitional missing-child-state cases return a safe display shape instead of raising.

`tests/test_observability_queries.py`:

1. Roadmap overview rows honor repo filtering and ordering.
2. Lifecycle counts continue to work and feed the dashboard summary.
3. Detail payloads include frontier, blocker ordering, revision info, and nested child roadmap state.

`tests/test_observability_tui.py`:

1. Main dashboard renders the `Roadmaps` panel and summary counts.
2. `Enter` on a roadmap row opens the explorer.
3. `Enter` on a nested roadmap node drills into the child roadmap.
4. `Escape`, `Backspace`, or `b` returns to the previous screen without losing the prior selection.
5. URL opening and periodic refresh continue to work in both the main screen and explorer.

## Implementation Plan

1. Add read-only roadmap snapshot helpers in `src/mergexo/state.py` so frontier and nested-child semantics stay owned by the state layer.
2. Add roadmap overview and detail loaders in `src/mergexo/observability_queries.py`, reusing existing lifecycle-count and blocker-age queries where possible.
3. Extend `src/mergexo/observability_tui.py` with:
   - a compact main-screen `Roadmaps` table
   - an explorer screen
   - back-stack navigation
   - node detail handling for roadmap rows
4. Add or update tests in `tests/test_state.py`, `tests/test_observability_queries.py`, and `tests/test_observability_tui.py`.
5. Update `README.md` so the dashboard section documents the roadmap panel and explorer navigation.

## Acceptance Criteria

1. `mergexo top` shows active roadmaps in a dedicated `Roadmaps` panel without removing existing active, tracked, history, or metrics panels.
2. The dashboard summary shows roadmap lifecycle counts using the existing roadmap observability data.
3. Selecting a roadmap opens a read-only explorer that shows graph version, current adjustment state, ready frontier, blockers, and per-node progress.
4. The explorer orders nodes deterministically in DAG order and marks which nodes are currently on the ready frontier.
5. Activated child roadmaps can be opened recursively from their parent roadmap nodes, and the operator can return to the parent view without quitting the app.
6. Roadmap nodes whose child roadmap has not activated yet show a safe, explicit placeholder state instead of crashing or opening a broken view.
7. Repo filtering and refresh behavior continue to apply to roadmap data.
8. The feature ships without a new sqlite migration and without changing roadmap execution semantics.

## Risks

1. The biggest correctness risk is duplicating frontier logic in the TUI and drifting from the orchestrator. Mitigation: keep roadmap snapshot semantics in `state.py` and consume that from the UI.
2. Adding another main-screen panel can reduce usable vertical space. Mitigation: keep the top-level roadmap table compact and move full DAG inspection into a dedicated explorer screen.
3. Nested roadmap activation is eventually consistent across issue creation, PR merge, and roadmap activation. Mitigation: explicitly model not-yet-activated and other partial states in the read model instead of assuming every roadmap node is immediately drillable.

## Rollout Notes

1. Ship this as a read-only observability enhancement. Do not combine it with roadmap control actions in the same PR.
2. Canary on a repo that has:
   - one active roadmap with a non-empty ready frontier
   - one roadmap waiting on a revision PR
   - one nested child roadmap
3. During rollout, compare the explorer output with `/roadmap status` comments for the same roadmap to confirm the dashboard is showing the same frontier and blocker state.
4. If the roadmap table feels too noisy on small terminals, keep the explorer behavior and reduce only the top-level row density. Do not cut the recursive drill-in behavior, since that is the main operator value for this issue.
