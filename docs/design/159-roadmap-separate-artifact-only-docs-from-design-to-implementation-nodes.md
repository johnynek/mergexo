---
issue: 159
priority: 3
touch_paths:
  - src/mergexo/models.py
  - src/mergexo/roadmap_parser.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/prompts.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/roadmap_transition_validator.py
  - tests/test_agent_adapter.py
  - tests/test_models_and_prompts.py
  - tests/test_roadmap_parser.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_roadmap_transition_validator.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-03-28T21:11:21Z
---

# Design `reference_doc` roadmap nodes for artifact-only docs

_Issue: #159 (https://github.com/johnynek/mergexo/issues/159)_

## Summary

Add a first-class `reference_doc` roadmap node kind for reviewed, durable documentation artifacts that downstream roadmap nodes can consult without spawning the existing design-to-implementation lane. Keep `design_doc` as the explicit two-stage design-then-implementation flow, preserve the current `planned`/`implemented` dependency vocabulary, and route roadmap-created reference docs through a terminal doc-only path so issue #151 / PR #154 can model `review_design` truthfully.

## Context

The roadmap model introduced in issue #141 currently overloads `design_doc` with two different meanings:
- a reviewed document artifact that other roadmap nodes can consult
- a full MergeXO lane that opens a design issue, merges a design PR, and then later turns that same issue into implementation work

That mismatch is already visible in the first real roadmap PR for issue #151. In PR #154, the `review_design` node is authored as `kind = design_doc`, and downstream implementation nodes depend on `review_design` with `requires = planned`. That matches the human intent of “wait for the reviewed design artifact,” but it does not match current runtime behavior. In the current orchestrator, merged `agent/design/*` work is treated as an implementation candidate, and the roadmap prompt explicitly tells agents to prefer `design_doc` when downstream work should wait for a reviewed design artifact.

The result is a structurally valid roadmap that still encodes the wrong execution semantics.

## Goals

1. Add a truthful roadmap concept for artifact-only documentation work.
2. Make roadmap node kinds map cleanly to what MergeXO actually does at runtime.
3. Preserve the existing dependency vocabulary of `planned` and `implemented`.
4. Keep downstream dependency authoring simple for nodes that need a reviewed document.
5. Preserve backward compatibility for already-merged roadmaps while giving open roadmaps a clean migration path.

## Non-goals

1. Introducing richer artifact handoff between child workers.
2. Reworking roadmap markdown generation from issue #155.
3. Splitting documentation storage into a second directory such as `docs/reference` in this issue.
4. Adding a new public repo label unless later experience shows the internal routing approach is insufficient.

## Decision

Add a new roadmap node kind named `reference_doc`.

A new kind is preferable to a mode or flag on `design_doc` for four reasons:
- roadmap graphs stay readable without interpreting combinations like `design_doc + artifact_only`
- prompt guidance can be truthful and short for authors in non-MergeXO repositories
- runtime dispatch remains explicit because MergeXO already switches behavior on node kind
- migration is safer because existing `design_doc` nodes keep their old meaning instead of silently changing under a new defaulted flag

`design_doc` remains the explicit “design now, implementation later from the same issue” lane. `reference_doc` means “produce a reviewed, durable document artifact and stop there.”

## Proposed Semantics

### Node-kind contract

| Kind | Runtime behavior | `planned` milestone | `implemented` milestone | Recommended downstream dependency |
| --- | --- | --- | --- | --- |
| `reference_doc` | Open a doc-only child issue, author and merge a reviewed document PR, then stop. No implementation candidate is created from that child issue. | The reference PR has merged to the default branch. | The same event as `planned`. | Use `planned` when downstream work needs the reviewed artifact. |
| `design_doc` | Open a design child issue, author and merge a design PR, then allow the existing implementation-from-design flow to continue on the same child issue. | The design PR has merged. | The implementation PR from that design has merged. | Use `planned` when downstream work only needs the merged design; use `implemented` when it depends on the shipped implementation outcome. |
| `small_job` | Existing direct implementation lane. | The child issue has been created and labeled. | The small-job PR has merged. | Unchanged. |
| `roadmap` | Existing nested-roadmap lane. | The child roadmap PR has merged and its graph is activated. | The child roadmap reaches completed status. | Unchanged. |

The important constraint is that `reference_doc` does not add a third dependency milestone. Roadmaps continue to use only `planned` and `implemented`.

### Why `reference_doc.planned` and `reference_doc.implemented` are the same event

Downstream nodes care about a reviewed artifact that exists on the default branch, not merely that the issue was created. Reusing issue creation as `planned` would preserve the current `small_job` convention, but it would reintroduce the same ambiguity this issue is trying to remove.

For `reference_doc`, issue creation should still move node status from `pending` to `issued`, but dependency satisfaction should not happen until the PR merges. Recording both `planned_at` and `implemented_at` at merge time keeps the scheduler simple, keeps the dependency schema unchanged, and makes status reporting explicit that this node kind is terminal once the artifact exists.

### Downstream dependency convention

Downstream nodes that need the reviewed artifact should depend on `reference_doc` with `requires = planned`.

`implemented` remains valid because the milestones collapse, but prompt text and examples should prefer `planned` so the graph communicates “wait until the artifact exists” rather than “wait for some later phase that does not actually exist.”

## Architecture

### 1. Extend the roadmap schema and prompt surface

Update the roadmap type system, parser, and Codex schemas so `reference_doc` is a valid `RoadmapNodeKind` anywhere roadmap graphs are validated or rendered:
- `src/mergexo/models.py`
- `src/mergexo/roadmap_parser.py`
- `src/mergexo/codex_adapter.py`
- `src/mergexo/prompts.py`

Prompt guidance should change from “prefer `design_doc` when downstream work should wait for a reviewed design artifact” to:
- use `reference_doc` for durable reviewed docs that other nodes should consult
- use `design_doc` only when the roadmap intentionally wants the existing design-to-implementation lane

The roadmap adjustment and requested-revision prompts should use the same distinction so revisions do not regress back into the overloaded meaning.

### 2. Route `reference_doc` through a terminal doc-only issue flow

`reference_doc` needs runtime behavior that is distinct from `design_doc`, otherwise merged doc PRs will still feed `list_implementation_candidates(...)`.

The recommended implementation is:
- keep roadmap-created `reference_doc` child issues on the existing design-style label path by mapping the node kind to `repo.trigger_label`
- add an explicit child-issue marker such as `Roadmap node kind: reference_doc` in `_render_roadmap_child_issue_body(...)`
- teach issue-flow resolution to interpret `repo.trigger_label` plus that marker as a separate internal flow, `reference_doc`
- factor the current design-doc authoring path so `design_doc` and `reference_doc` share document-authoring machinery but diverge in branch prefix and PR semantics

The `reference_doc` flow should:
- write its artifact into `repo.design_docs_dir` for now
- use a distinct branch prefix such as `agent/reference/<issue>-<slug>`
- open a PR/body combination that closes the child issue when the doc PR merges
- never enter the implementation-candidate query, which can remain keyed to merged `agent/design/*` branches

This keeps the fix focused on execution semantics and avoids adding a second documentation directory in the same change.

### 3. Preserve roadmap scheduler behavior with one new milestone case

Roadmap scheduling already separates node status from dependency milestones. That should remain true.

Required runtime changes:
- `_roadmap_child_label_for_kind(...)` must accept `reference_doc` and map it to `repo.trigger_label`
- `_roadmap_node_milestones(...)` must add a `reference_doc` branch-prefix case that records `planned = True` and `implemented = True` only when the merged reference-doc PR exists
- issue creation for `reference_doc` should set node status to `issued`, but should not set `planned_at`
- `state._roadmap_node_dependencies_satisfied(...)` can remain unchanged because it already understands only `planned_at` and `implemented_at`

No sqlite schema migration should be required for this issue. Roadmap node kind is already stored as text, and the existing `planned_at` and `implemented_at` columns are sufficient for the collapsed `reference_doc` milestone model.

### 4. Make revision and migration rules explicit

Existing merged roadmaps must keep current `design_doc` meaning. Silent reinterpretation would change runtime behavior after the fact.

Migration rules:
- already-merged roadmaps: no automatic migration and no backfill
- open roadmap PRs: authors should update the graph before merge
- active roadmaps on `main`: same-roadmap revisions may convert unstarted nodes from `design_doc` to `reference_doc`
- started nodes: once a child issue has been issued, the kind remains behaviorally significant and should not change in place

To make active-roadmap migration practical, `roadmap_transition_validator.py` should allow an in-place kind change between `design_doc` and `reference_doc` only when the node has not started work yet. After issue creation or completion, the current immutability rules should continue to hold.

## Issue #151 / PR #154 Example

The current `review_design` node in PR #154 should be modeled as:
- `node_id`: `review_design`
- `kind`: `reference_doc`
- `title`: `Design optional automated code review flow`
- `depends_on`: `[]`

The downstream nodes `review_config` and `review_contract` should continue to depend on `review_design` with `requires = planned`.

That keeps the roadmap intent intact:
- there is one reviewed design/reference artifact that several later nodes consult
- MergeXO does not open a second implementation lane from that artifact-only doc
- the roadmap still uses the existing dependency language without inventing a special-case edge type

As of March 28, 2026, PR #154 is still open, so it should be updated before merge rather than relying on backward compatibility after activation.

## Implementation Plan

1. Extend `RoadmapNodeKind` and all roadmap JSON schemas to include `reference_doc`, then add parser and prompt tests that reject stale allowed-kind lists.
2. Add explicit node-kind markers to roadmap child issue bodies and extend issue-flow resolution so a roadmap-created `reference_doc` child issue routes into a separate internal terminal doc flow.
3. Factor the current design-doc authoring path into shared document-authoring logic used by both `design_doc` and `reference_doc`, with distinct branch prefixes and PR-closing behavior.
4. Update roadmap milestone detection so `reference_doc` records both `planned_at` and `implemented_at` when its doc PR merges.
5. Relax roadmap transition validation only for not-yet-started `design_doc` to `reference_doc` migrations, and cover the allowed and rejected cases in tests.
6. Update roadmap authoring and revision prompts, status text, and README guidance so the semantics shown to operators and agents match the runtime.

## Risks

- If any allowed-kind list is missed, the system will drift between prompt, parser, adapter, and orchestrator behavior. The implementation should treat cross-file enum coverage as a first-class test target.
- If the child-issue marker or branch-prefix split is inconsistent, `reference_doc` work could still leak into the implementation-candidate queue. End-to-end orchestration tests should assert that merged `agent/reference/*` work never appears in `list_implementation_candidates(...)`.
- Collapsing `reference_doc.planned` and `reference_doc.implemented` is semantically correct but non-obvious. Status output and prompt text must say this explicitly so roadmap authors do not assume `planned` means issue creation for every kind.
- Issue #155 is still a separate concern. When graph-first roadmap markdown lands, the renderer must also learn the new node kind so reviewable markdown stays aligned with the graph schema.

## Rollout Notes

- Ship schema, prompt, orchestrator, and tests in the same change. Do not land the new prompt wording before the runtime understands `reference_doc`.
- Do not reinterpret existing merged `design_doc` nodes. Backward compatibility should mean “old graphs keep old behavior,” not “old graphs change meaning under the same spelling.”
- Update open roadmap PRs, starting with PR #154, before they merge.
- After rollout, roadmap authoring examples should treat `reference_doc` as the default answer for shared reviewed documentation artifacts and reserve `design_doc` for lanes that intentionally continue into implementation.

## Acceptance Criteria

1. Roadmap graphs can represent artifact-only documentation work with a first-class `reference_doc` node kind.
2. The design doc, prompts, and runtime all agree on the milestone semantics for `reference_doc`, `design_doc`, `small_job`, and `roadmap`.
3. Downstream roadmap nodes can depend on a reviewed artifact-only doc using the existing `requires = planned` convention.
4. Merged `reference_doc` work does not create an implementation candidate, while merged `design_doc` work still does.
5. Existing merged roadmaps keep current behavior, and open or revisable roadmaps have a documented migration path.
6. Issue #151 / PR #154 is explicitly covered, and `review_design` is specified as `kind = reference_doc` rather than `design_doc`.
