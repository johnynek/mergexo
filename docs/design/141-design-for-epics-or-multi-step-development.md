---
issue: 141
priority: 3
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
estimated_size: M
generated_at: 2026-03-06T22:56:16Z
---

# Design for epics or multi step development

_Issue: #141 (https://github.com/johnynek/mergexo/issues/141)_

## Summary

Introduce a roadmap DAG flow that decomposes epic work into dependency-ordered child issues, supports nested roadmaps, and closes the parent roadmap issue only after all child nodes are implemented, with explicit revision/abandon and escalation handling.

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
generated_at: 2026-03-06T00:00:00Z
---

# Design for epics or multi step development

_Issue: #141 (https://github.com/johnynek/mergexo/issues/141)_

## Summary

Add a first-class `roadmap` flow that lets one issue describe a bounded DAG (3 to 7 nodes) of dependent work, then automatically fans out child issues in dependency order. Child issues are labeled so existing MergeXO flows execute them, and the orchestrator closes the parent roadmap issue when all roadmap nodes complete. The design includes a revision and abandonment model plus an agent escalation path that pauses further fan-out when a fundamental flaw is discovered.

## Context

Today MergeXO supports three issue intake flows (`design_doc`, `bugfix`, `small_job`) and a design-to-implementation promotion path. This is effective for single-PR or single-issue scope, but it does not model multi-step initiatives where work must be decomposed and sequenced.

Issue #141 asks for:
1. A roadmap artifact that is different from a design doc.
2. A roadmap DAG of less than 7 nodes, with sequential and parallel edges.
3. Child work to be opened as issues, not PRs.
4. Correct labels on child issues so next-layer work starts automatically.
5. Parent-child linkage and dependency-aware activation.
6. PRs referencing only their direct issue owners.
7. Parent roadmap issue closure only after children are implemented.
8. A roadmap revision and abandonment story.
9. Agent-triggered escalation when implementation discovers a fundamental roadmap flaw.

## Goals

1. Introduce a `roadmap` issue flow with deterministic intake routing.
2. Support a machine-validated roadmap DAG with node types `design_doc` and `roadmap`.
3. Fan out child issues only when node dependencies are satisfied.
4. Keep existing child issue execution in existing flows and labels.
5. Persist roadmap graph and node lifecycle state in SQLite.
6. Close parent roadmap issue automatically once all nodes are complete.
7. Support revision and abandonment as explicit lifecycle transitions.
8. Allow agents to raise a structured roadmap revision escalation that pauses additional fan-out.
9. Keep behavior restart-safe and idempotent.

## Non-goals

1. Arbitrary graph size or graph cycles.
2. Replacing existing design-doc and feedback loop behavior.
3. Building cross-repository dependency graphs.
4. Full semantic merge of old and new roadmap graphs during revision.
5. Webhook migration.

## Proposed architecture

### 1. New intake flow and config surface

Add a new issue flow `roadmap` with these config additions:
1. `runtime.enable_roadmaps` default `false`.
2. `repo.roadmap_label` default `agent:roadmap`.
3. `repo.roadmap_docs_dir` default `docs/roadmap`.
4. `repo.roadmap_revision_label` default `agent:roadmap-revise`.
5. `repo.roadmap_abandon_label` default `agent:roadmap-abandon`.

Flow precedence becomes:
1. `ignore_label`
2. `roadmap_label`
3. `bugfix_label`
4. `small_job_label`
5. `trigger_label`

`IssueFlow` expands to include `roadmap`, with branch prefix `agent/roadmap/<issue>-<slug>`.

### 2. Roadmap document contract and parser

Roadmap docs are generated in `repo.roadmap_docs_dir` and include both narrative text and one machine-readable graph payload. The payload is validated by a new parser module (`roadmap_parser.py`) and must satisfy:
1. Node count between 3 and 7.
2. Unique stable node IDs.
3. Node kind in `{design_doc, roadmap}`.
4. Dependency IDs refer to existing nodes.
5. DAG is acyclic.

Parser output is a normalized `RoadmapGraph` model consumed by orchestrator and persisted in state. If parsing or validation fails after roadmap PR merge, fan-out is blocked and a deterministic parent-issue comment is posted requesting roadmap revision.

### 3. State model for roadmap lifecycle

Add normalized roadmap tables in `state.py`:
1. `roadmap_state`
- key: `(repo_full_name, roadmap_issue_number)`
- fields: `roadmap_pr_number`, `roadmap_doc_path`, `status`, `parent_roadmap_issue_number`, `superseding_roadmap_issue_number`, `revision_requested_at`, `last_error`, `updated_at`
- status: `active`, `revision_requested`, `superseded`, `abandoned`, `completed`

2. `roadmap_nodes`
- key: `(repo_full_name, roadmap_issue_number, node_id)`
- fields: `node_kind`, `title`, `body_markdown`, `dependency_ids_json`, `child_issue_number`, `child_issue_url`, `status`, `updated_at`
- node status: `pending`, `issued`, `completed`, `blocked`, `abandoned`

3. indexes
- by `status` for active scans
- by `child_issue_number` for reverse lookup during escalation/completion sync

State APIs include:
1. `upsert_roadmap_graph(...)`
2. `list_roadmap_activation_candidates(...)`
3. `list_active_roadmaps(...)`
4. `list_ready_roadmap_nodes(...)`
5. `mark_roadmap_node_issue_created(...)`
6. `mark_roadmap_node_completed(...)`
7. `mark_roadmap_revision_requested(...)`
8. `mark_roadmap_abandoned(...)`
9. `mark_roadmap_completed(...)`
10. `find_roadmap_by_child_issue(...)`

### 4. Roadmap activation and DAG progression

Add orchestrator poll steps:
1. `enqueue_roadmap_work`
- intake labeled roadmap issues and open roadmap doc PRs.

2. `activate_merged_roadmaps`
- detect merged roadmap PRs from `issue_runs` branch prefix `agent/roadmap/`.
- load merged roadmap doc from default branch.
- parse/validate graph.
- persist `roadmap_state=active` plus `roadmap_nodes=pending`.

3. `advance_roadmap_nodes`
- for each active roadmap, compute node completion and readiness.
- create child issues for nodes where all dependencies are `completed` and no child issue exists.
- child labels:
  - node kind `design_doc` => `repo.trigger_label`
  - node kind `roadmap` => `repo.roadmap_label`
- child issue body includes parent roadmap issue link, roadmap node ID, and node-local specification.

This design intentionally creates child issues when ready, not all at once, so label mutation APIs are not required for dependency gating.

### 5. Parent-child linkage and completion semantics

Each created child issue is linked in both places:
1. child issue body includes `Parent roadmap: #<roadmap_issue_number>` and `Roadmap node: <node_id>`.
2. `roadmap_nodes.child_issue_number` stores the reverse link.

Node completion semantics:
1. `design_doc` node completes only when the child issue reaches merged implementation state (`issue_runs.status = merged` and branch prefix `agent/impl/`).
2. `roadmap` node completes when the child roadmap state reaches `completed`.

When all nodes in a roadmap are complete:
1. post completion summary comment on roadmap issue.
2. close roadmap issue via new `GitHubGateway.close_issue(...)`.
3. set `roadmap_state.status = completed`.

### 6. Revision and abandonment model

Roadmap revision is a lifecycle state, not a special-case failure.

Revision path:
1. `roadmap_state.active -> revision_requested` pauses creation of new child issues.
2. a revision roadmap issue is created (label `repo.roadmap_label`) referencing superseded roadmap and reason.
3. when revision roadmap merges and activates, old roadmap transitions to `superseded`.
4. old roadmap pending nodes become `abandoned`.

Abandonment path:
1. `repo.roadmap_abandon_label` on active roadmap issue transitions roadmap to `abandoned`.
2. pending nodes mark `abandoned`.
3. open child issues that were created by this roadmap and not completed are closed with an abandonment comment.
4. parent roadmap issue is closed with abandonment reason.

This treats abandonment as a terminal revision choice.

### 7. Agent escalation for roadmap flaws

Add structured escalation output for direct and feedback agent contracts:
1. optional escalation object with `kind = roadmap_revision`, `summary`, and `details`.
2. prompts instruct agents to use escalation when they detect a fundamental flaw that invalidates downstream roadmap assumptions.

Orchestrator behavior on escalation:
1. resolve parent roadmap via `find_roadmap_by_child_issue(...)`.
2. mark roadmap `revision_requested`.
3. post tokenized comment on parent roadmap issue summarizing escalation and source child issue/PR.
4. pause fan-out of additional pending nodes until roadmap is revised or abandoned.

If no parent roadmap link is found, orchestrator posts a warning on the source issue and does not alter unrelated roadmap state.

### 8. PR ownership and closure invariants

No child PR should close a roadmap issue automatically.

Invariants:
1. roadmap PR body uses `Refs #<roadmap_issue_number>`.
2. design child PRs and implementation PRs continue to reference only their direct child issue (`Refs` for design PR, `Fixes` for implementation PR).
3. parent roadmap issue closes only from roadmap completion/abandonment logic in orchestrator.

### 9. Observability

Add roadmap visibility to observability queries:
1. count active/revision_requested/abandoned roadmaps.
2. include roadmap progression events (`roadmap_activated`, `roadmap_node_issued`, `roadmap_node_completed`, `roadmap_revision_requested`, `roadmap_completed`, `roadmap_abandoned`).

## Implementation plan

1. Extend config and model types for roadmap labels, dirs, flags, and `IssueFlow = roadmap`.
2. Add roadmap prompt builder and adapter contract/result type for roadmap generation.
3. Extend `CodexAdapter` with roadmap output schema validation.
4. Add `roadmap_parser.py` for machine-readable graph extraction and DAG validation.
5. Add roadmap schema tables and APIs in `StateStore` with additive migrations.
6. Add orchestrator roadmap intake path (`_process_roadmap_issue`) that opens roadmap PRs.
7. Add orchestrator activation and DAG advancement poll steps.
8. Add child issue creation templates and parent-child link persistence.
9. Add completion sync and parent roadmap auto-close behavior.
10. Add revision/abandon controls and transitions.
11. Add optional agent escalation handling in direct + feedback paths.
12. Add GitHub gateway method for closing issues and tests.
13. Add observability query updates and tests.
14. Update README and sample config with roadmap and revision lifecycle docs.

## Acceptance criteria

1. With `runtime.enable_roadmaps = true`, an open issue labeled `repo.roadmap_label` is routed to roadmap flow and opens a roadmap PR writing into `repo.roadmap_docs_dir`.
2. A merged roadmap PR with a valid DAG (3 to 7 nodes, acyclic) activates roadmap state and persists nodes.
3. Nodes with no dependencies create child issues immediately; dependent nodes do not create child issues until dependencies are complete.
4. Child issue labels are correct by node kind (`design_doc` -> `repo.trigger_label`, `roadmap` -> `repo.roadmap_label`).
5. Each child issue body contains a link to the parent roadmap issue and the roadmap node ID.
6. No child PR body closes the roadmap issue; PR references remain scoped to direct child issues.
7. `design_doc` nodes are not marked complete at design-PR merge time; they complete only after implementation merge.
8. `roadmap` nodes complete only when the child roadmap reaches `completed`.
9. Parent roadmap issue closes automatically only when all nodes are complete.
10. Invalid merged roadmap graphs (cycle, bad dependency, node count out of bounds) do not fan out child issues and post deterministic revision-needed guidance.
11. A roadmap revision request pauses additional node fan-out and records `revision_requested` state.
12. Agent escalation with `kind = roadmap_revision` marks parent roadmap revision-needed and posts a roadmap-level escalation comment.
13. Abandoning a roadmap transitions remaining pending/issued nodes to abandoned semantics and closes parent roadmap issue with an explicit reason.
14. Restart/retry does not duplicate child issue creation for the same roadmap node.
15. Existing non-roadmap flows are unchanged when roadmap feature flag is disabled.

## Risks and mitigations

1. Risk: roadmap parser brittleness due free-form markdown edits.
Mitigation: require a strict machine-readable graph payload plus explicit validation errors with revision guidance.

2. Risk: dependency deadlocks or silent stalls.
Mitigation: DAG validation at activation, explicit blocked node status, and roadmap-level observability events.

3. Risk: premature dependency unlock (for example at design PR merge before implementation).
Mitigation: complete design nodes only on merged implementation branch criteria.

4. Risk: excessive issue churn on frequent revisions.
Mitigation: revision pauses fan-out first, then creates one superseding roadmap issue with explicit lineage, and marks superseded nodes abandoned.

5. Risk: false-positive agent escalations could halt roadmap progress.
Mitigation: escalation is advisory; maintainers can clear revision state by revising or explicitly resuming/abandoning.

6. Risk: GitHub API and comment noise from roadmap orchestration.
Mitigation: scope scans to active roadmaps, use idempotent action tokens, and keep comments transition-based.

## Rollout notes

1. Ship behind `runtime.enable_roadmaps = false` by default.
2. Land schema + parser + orchestrator no-op wiring first, then enable intake in canary.
3. Canary with one roadmap containing both parallel and sequential nodes.
4. Validate canary scenarios:
- parallel roots create multiple child issues in one poll
- dependency gating holds back downstream nodes until parent completion
- parent roadmap closes only when all nodes complete
- revision request pauses fan-out
- abandonment closes remaining roadmap work cleanly
5. Enable agent escalation handling after basic roadmap fan-out is stable.
6. After successful canary window, enable roadmap flow by default and document operational guidance in README.
