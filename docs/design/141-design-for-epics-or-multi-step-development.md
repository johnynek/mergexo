---
issue: 141
status: proposed
priority: 2
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/github_gateway.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/observability_queries.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_codex_adapter.py
  - tests/test_github_gateway.py
  - tests/test_feedback_loop.py
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_observability_queries.py
  - src/mergexo/roadmap_parser.py
  - tests/test_roadmap_parser.py
depends_on: []
estimated_size: L
generated_at: 2026-03-07T03:30:00Z
---

# Design for epics or multi step development

_Issue: #141 (https://github.com/johnynek/mergexo/issues/141)_

## Summary

Add a first-class `roadmap` flow that lets one issue define a bounded DAG (3 to 7 nodes), then opens child issues in dependency order. Node kinds include `roadmap`, `design_doc`, and `small_job`, and dependencies can target either `planned` or `implemented` milestones. The roadmap graph is committed in-repo as canonical JSON, mirrored in sqlite for runtime scheduling, and the parent roadmap issue closes only when all nodes reach terminal implementation outcomes. The design includes revision/abandon handling and an agent escalation path that pauses further fan-out when foundational assumptions fail.

## Context

MergeXO currently routes issues into `design_doc`, `bugfix`, and `small_job` flows, with design-to-implementation promotion. That supports single-thread work, but not epics where multiple steps must be sequenced or parallelized.

Issue #141 needs:
1. A roadmap artifact distinct from a design doc.
2. A DAG of less than 7 nodes.
3. Child work opened as issues (not PRs).
4. Automatic labels to kick off downstream work.
5. Parent-child linking and dependency-ordered execution.
6. PRs tied only to direct issue owners.
7. Orchestrator closure of parent roadmap after all children are implemented.
8. A roadmap revision and abandonment lifecycle.
9. Agent escalation to interrupt a flawed roadmap.

## Goals

1. Add a new `roadmap` intake flow.
2. Support roadmap node kinds `design_doc`, `small_job`, and `roadmap`.
3. Support dependency edges with explicit milestone requirements (`planned` or `implemented`).
4. Keep roadmap relationship state reviewable in the repository.
5. Persist runtime scheduling and progress state in sqlite.
6. Open child issues only when dependencies are satisfied.
7. Close roadmap issue only when all roadmap nodes are implemented or explicitly abandoned.
8. Support revision/abandon transitions without losing lineage.
9. Allow agent escalations to pause fan-out and request roadmap revision.

## Non-goals

1. Graphs larger than 7 nodes.
2. Cyclic dependency support.
3. Cross-repo dependency resolution.
4. Replacing existing issue/PR feedback loop behavior.
5. Webhook migration.

## Proposed architecture

### 1. New flow and config

Add:
1. `runtime.enable_roadmaps` default `false`.
2. `repo.roadmap_label` default `agent:roadmap`.
3. `repo.roadmap_docs_dir` default `docs/roadmap`.
4. `repo.roadmap_revision_label` default `agent:roadmap-revise`.
5. `repo.roadmap_abandon_label` default `agent:roadmap-abandon`.

Flow precedence:
1. `ignore_label`
2. `roadmap_label`
3. `bugfix_label`
4. `small_job_label`
5. `trigger_label`

`IssueFlow` adds `roadmap` with branch prefix `agent/roadmap/<issue>-<slug>`.

### 2. Canonical roadmap graph in repo

Each roadmap PR must include two files:
1. Narrative roadmap markdown: `docs/roadmap/<issue>-<slug>.md`.
2. Canonical machine-readable graph: `docs/roadmap/<issue>-<slug>.graph.json`.

`.graph.json` is source of truth for node relationships and must be human-reviewable in PR. It includes:
1. `roadmap_issue_number`.
2. `version`.
3. `nodes` array.
4. each node has `node_id`, `kind`, `title`, `body_markdown`, and `depends_on`.
5. each dependency entry has `node_id` and `requires` where `requires` is `planned` or `implemented`.
6. node references are always internal `node_id`s, not pre-existing issue numbers.

MergeXO parses this file with `roadmap_parser.py` and stores a checksum in sqlite. If sqlite view and repo graph diverge, MergeXO blocks fan-out and posts a deterministic correction comment.

This keeps roadmap relationship state in-repo while sqlite remains runtime state for progress, claims, and idempotency. Active, completed, superseded, and abandoned roadmaps stay auditable by scanning `docs/roadmap/*.graph.json` on default branch and git history.

### 3. Node kinds and milestone semantics

Node kinds:
1. `design_doc`: opens a design-flow issue (`repo.trigger_label`).
2. `small_job`: opens a small-job issue (`repo.small_job_label`).
3. `roadmap`: opens another roadmap issue (`repo.roadmap_label`).

Milestones exposed by each node kind:
1. `design_doc`
- `planned`: design PR merged.
- `implemented`: implementation PR merged.
2. `small_job`
- `implemented`: small-job PR merged.
- `planned` is treated as `implemented` for dependency checks.
3. `roadmap`
- `planned`: child roadmap PR merged and graph activated.
- `implemented`: child roadmap reaches completed status.

Default dependency requirement is `implemented` if omitted.

This supports chains like “design A -> design B using A doc -> design C using B doc” by setting `requires=planned` on those edges.

### 4. State model

Add new sqlite tables:

1. `roadmap_state`
- key: `(repo_full_name, roadmap_issue_number)`
- fields: `roadmap_pr_number`, `roadmap_doc_path`, `graph_path`, `graph_checksum`, `status`, `parent_roadmap_issue_number`, `superseding_roadmap_issue_number`, `revision_requested_at`, `last_error`, `updated_at`
- status: `active`, `revision_requested`, `superseded`, `abandoned`, `completed`

2. `roadmap_nodes`
- key: `(repo_full_name, roadmap_issue_number, node_id)`
- fields: `kind`, `title`, `body_markdown`, `dependencies_json`, `child_issue_number`, `child_issue_url`, `status`, `planned_at`, `implemented_at`, `updated_at`
- status: `pending`, `issued`, `completed`, `blocked`, `abandoned`

3. indexes
- by roadmap/node status for poll scans
- by `child_issue_number` for reverse lookup

State APIs include:
1. `upsert_roadmap_graph(...)`
2. `list_roadmap_activation_candidates(...)`
3. `list_active_roadmaps(...)`
4. `list_ready_roadmap_nodes(...)`
5. `mark_roadmap_node_issue_created(...)`
6. `record_roadmap_node_milestone(...)`
7. `mark_roadmap_revision_requested(...)`
8. `mark_roadmap_abandoned(...)`
9. `mark_roadmap_completed(...)`
10. `find_roadmap_by_child_issue(...)`

### 5. Orchestrator DAG progression

Add poll steps:
1. `enqueue_roadmap_work`
- intake roadmap-labeled source issues and open roadmap PRs.

2. `activate_merged_roadmaps`
- detect merged roadmap PRs by branch prefix `agent/roadmap/`.
- load `*.graph.json` from default branch.
- validate DAG and persist roadmap/node rows.
- mark roadmap as `planned` once graph is activated, even before all nodes are issued.

3. `advance_roadmap_nodes`
- compute achieved node milestones.
- create child issues for nodes where all dependencies are satisfied.
- map node kind to trigger label:
  - `design_doc` -> `repo.trigger_label`
  - `small_job` -> `repo.small_job_label`
  - `roadmap` -> `repo.roadmap_label`

Child issues are created only when ready, so unresolved downstream nodes have no issue yet.

### 6. Linking and milestone detection

Each child issue includes:
1. `Parent roadmap: #<roadmap_issue_number>`.
2. `Roadmap node: <node_id>`.
3. dependency context copied from graph.

Milestone detection:
1. design node `planned`: child issue has merged design PR state.
2. design node `implemented`: child issue has merged implementation state (`agent/impl/` merge).
3. small-job node `implemented`: child issue merged with `agent/small/` branch flow.
4. roadmap node `planned`: child roadmap activated.
5. roadmap node `implemented`: child roadmap completed.

When all nodes satisfy their terminal implementation milestone, MergeXO posts summary and closes parent roadmap issue.

### 7. Revision and abandonment

Revision flow:
1. roadmap transitions to `revision_requested` (via label or escalation).
2. no new child issues are created while revision is pending.
3. a superseding roadmap issue is opened and linked.
4. on superseding roadmap activation, prior roadmap becomes `superseded`; unresolved prior nodes become `abandoned`.

Abandon flow:
1. `repo.roadmap_abandon_label` sets roadmap `abandoned`.
2. unresolved nodes become `abandoned`.
3. open unresolved child issues created by this roadmap are closed with abandonment comment.
4. parent roadmap issue is closed with abandonment reason.

### 8. Agent escalation

Extend direct/feedback agent outputs with optional escalation object:
1. `kind = roadmap_revision`.
2. `summary`.
3. `details`.

On escalation:
1. resolve parent roadmap using child-issue link.
2. mark roadmap `revision_requested`.
3. post tokenized escalation comment on roadmap issue linking source issue/PR.
4. pause further node fan-out pending revision/abandon action.

### 9. PR ownership invariant

1. Roadmap PR uses `Refs #<roadmap_issue_number>`.
2. Child PRs reference only their own child issue.
3. No child PR automatically closes roadmap parent.
4. Parent closes only by roadmap orchestrator completion/abandon logic.

### 10. Observability

Add roadmap metrics/events:
1. active/revision_requested/superseded/abandoned/completed roadmap counts.
2. `roadmap_node_issued` and milestone progression events.
3. drift and validation failures between sqlite graph checksum and in-repo `.graph.json`.

## Implementation plan

1. Extend config and model types for roadmap flow, labels, and node milestones.
2. Add roadmap prompt and adapter output schema for `.graph.json` generation.
3. Add `roadmap_parser.py` with DAG and milestone-dependency validation.
4. Add roadmap tables/APIs in `StateStore` with additive migrations.
5. Add orchestrator roadmap intake worker and PR creation path.
6. Add roadmap activation step that parses merged `.graph.json`.
7. Add DAG advancement step that opens ready child issues by node kind.
8. Add milestone detection for design/small-job/roadmap child nodes.
9. Add parent roadmap close path when all nodes implemented.
10. Add revision/supersede and abandon transitions.
11. Add escalation handling in direct and feedback processing.
12. Add GitHub gateway issue-close helper and tests.
13. Add observability query support and tests.
14. Update README and sample config for roadmap authoring and lifecycle.

## Acceptance criteria

1. With `runtime.enable_roadmaps = true`, an issue with `repo.roadmap_label` opens a roadmap PR.
2. Merged roadmap PR must include valid `*.graph.json`; invalid graphs block fan-out with deterministic guidance.
3. Graph validation enforces 3 to 7 nodes, unique IDs, no cycles, valid node kinds, and valid dependency milestone requirements.
4. Default branch always contains human-reviewable roadmap relationship files for active and completed roadmaps (`docs/roadmap/*.graph.json`), while runtime completion state stays in sqlite.
5. `small_job` is accepted as a roadmap node kind and creates child issues with `repo.small_job_label`.
6. Child issues are created only when dependency milestones are satisfied; roadmap merge does not require creating all node issues at once.
7. Dependencies can require `planned` or `implemented`, and scheduler behavior reflects that distinction.
8. Design-node dependents with `requires=planned` can start after design doc merge without waiting for implementation merge.
9. Design-node dependents with `requires=implemented` wait for implementation merge.
10. Roadmap-node dependents can separately depend on child roadmap `planned` or `implemented` milestones.
11. Child issue bodies include parent roadmap issue and node ID linkage.
12. Child PR references remain scoped to direct child issues; roadmap issue is not auto-closed by child PR text.
13. Parent roadmap closes only when all nodes reach terminal implementation outcomes or roadmap is abandoned.
14. Agent escalation marks roadmap `revision_requested` and pauses further fan-out.
15. Revision and abandonment transitions preserve lineage and do not duplicate already-issued node issues.
16. Existing non-roadmap flows remain unchanged when feature flag is off.

## Risks and mitigations

1. Risk: repo graph file and sqlite mirror drift.
Mitigation: store graph checksum, validate on poll, block fan-out on mismatch, and post deterministic repair guidance.

2. Risk: milestone detection errors unlock nodes too early.
Mitigation: explicit per-kind milestone mapping and tests for `planned` vs `implemented` gating.

3. Risk: frequent revisions create noisy issue churn.
Mitigation: revision_requested pauses fan-out immediately and enforces single active superseding roadmap.

4. Risk: agent escalations become spammy.
Mitigation: tokenized idempotent roadmap escalation comments and single active revision-request state.

5. Risk: additional poll work increases GitHub API load.
Mitigation: only scan active roadmaps and roadmap-linked child issues, with incremental state-driven checks.

## Rollout notes

1. Ship behind `runtime.enable_roadmaps = false`.
2. Land schema/parser and no-op orchestration wiring first.
3. Canary with one roadmap that mixes:
- `design_doc` and `small_job` nodes
- `planned` and `implemented` dependency edges
- parallel and sequential branches
4. Validate canary outcomes:
- ready-node issue creation order
- milestone-gated unlock behavior
- parent close only on full completion
- revision-request pause behavior
- abandon transition behavior
5. Enable escalation handling after baseline DAG progression is stable.
6. Promote feature to broader repos once canary is stable and drift/duplication incidents remain zero.
