# Roadmap Revision System

## Goal

Turn roadmap revision into the normal control loop for multi-step execution.
Before issuing the next ready roadmap frontier, MergeXO should evaluate whether
the roadmap still fits what we learned from completed dependency work. If the
answer is yes, it should issue the next child work immediately. If the answer
is no, it should create a same-roadmap revision PR, pause fan-out, and resume
only after that PR merges and the updated graph passes transition validation.

The target control loop is:

```text
(roadmap, ready_frontier, dependency_artifacts) ->
    Proceed
  | Revise(updated_markdown, updated_graph, rationale)
  | Abandon(reason)
```

## Current Starting Point

The current branch already contains the enabling foundation:

1. Roadmap graphs are versioned and stored non-destructively.
2. Roadmap revisions are applied to the same roadmap instead of opening a
   superseding roadmap issue for the normal revision path.
3. Graph transition validation exists and protects basic invariants.
4. Roadmap advancement now pauses at an adjustment gate before issuing ready
   nodes.
5. The adjustment decision can currently return `proceed`,
   `request_revision`, or `abandon`.

This is enough to support manual same-roadmap revision, but it is not yet the
full continuous-adjustment system.

## Complete Steps

- Checkpoint 1 completed: lifecycle state and coordination state are now
  separated in the implementation. `revision_requested` is no longer a roadmap
  lifecycle status. Active roadmaps waiting on revision are represented with
  `adjustment_state = awaiting_revision_merge`, the state schema now carries
  pending revision PR metadata fields, older `revision_requested` rows are
  migrated forward during state initialization, and orchestration plus
  observability now key off coordination state instead of a synthetic lifecycle
  status.

## Current Job

No active checkpoint is recorded here after the latest checkpoint commit.

## Remaining Work

- Automatic same-roadmap revision PR creation is still missing. A
  `request_revision` decision pauses the roadmap and comments on the roadmap
  issue, but it does not create or update the roadmap revision PR.
- The adjustment agent does not yet consume dependency PR artifacts. It sees
  the roadmap markdown, graph, ready frontier, and a status snapshot, but not
  merged PR bodies, changed files, review threads, or upstream issue comments.
- The adjustment decision does not yet produce a concrete revision payload. It
  returns a coarse decision plus rationale, not a PR-ready roadmap markdown
  and graph update.
- Active roadmaps still treat unsolicited merged graph changes as drift. A
  graph update is only accepted when the roadmap has already entered the
  revision flow.
- The adjustment lock is only partially realized operationally. We have claim
  APIs and adjustment state, but not the full "worker owns the adjustment until
  the revision PR is resolved" lifecycle with pending PR metadata.
- Transition validation still enforces the minimal safety rules, not the full
  lineage policy. In particular, we do not yet strictly enforce semantic
  `node_id` stability versus replacement across all revision cases.
- The effective adjustment input is still weaker than the intended
  `(roadmap, ready_frontier, dependency_artifacts)` interface.
- We do not yet have the full recomputation loop around an auto-generated
  revision PR: create PR, wait for merge, apply revision, recompute frontier,
  then resume issuing work.
- Status and observability are still thin. The current status output shows
  roadmap version and adjustment state, but not pending revision PR details,
  adjustment rationale history, or revision history.
- We do not yet persist learned adjustment context or a basis digest strong
  enough to skip redundant no-op adjustment runs.

## Target End State

The finished system should satisfy these properties:

1. Every ready roadmap frontier passes through exactly one adjustment decision
   before new child work is issued.
2. Adjustment decisions use both the current roadmap and the concrete artifacts
   produced by the dependency frontier.
3. Revision is a same-roadmap update, not a replacement roadmap, unless the
   operator explicitly requests a hard reset.
4. A revision PR is created automatically when the adjustment decision requests
   it.
5. The roadmap remains locked for further fan-out until that revision PR is
   merged or otherwise resolved.
6. Merged roadmap updates are validated as graph transitions, then applied in a
   single transactional step.
7. Node identity is stable across revisions for the same work, while replaced
   work receives new node identifiers.
8. Status reporting makes it obvious whether the roadmap is idle, evaluating,
   waiting on a revision PR, or abandoned.

## Checkpoint Commit Plan

Each checkpoint commit should leave `./scripts/test.sh` green. The goal is not
to hide partial behavior, but to keep each commit logically self-contained and
fully tested.

### Commit 1: Separate lifecycle state from coordination state

Purpose:
Make the state model match the target execution model before adding more
behavior.

Changes:

- Move the "paused for revision" concept out of the main roadmap lifecycle and
  into explicit coordination state.
- Extend roadmap state with pending revision PR metadata:
  - PR number
  - PR URL
  - head SHA or commit marker
  - requested version
  - adjustment owner
  - lock timestamps
  - last adjustment basis digest
- Update row parsers, migrations, status helpers, and test fixtures.

Primary files:

- `src/mergexo/state.py`
- `src/mergexo/orchestrator.py`
- `tests/test_state.py`
- `tests/test_orchestrator.py`

Exit criteria:

- Lifecycle values describe only long-lived roadmap state such as draft,
  active, completed, abandoned, and superseded.
- Coordination values describe only adjustment ownership and revision waiting.
- Existing roadmap behavior is preserved.

### Commit 2: Capture dependency artifacts for the ready frontier

Purpose:
Upgrade the adjustment input from a status snapshot to the real dependency
frontier context.

Changes:

- Add pure helper functions to collect dependency artifacts for all nodes in the
  ready frontier.
- Summarize, at minimum:
  - merged PR title/body
  - changed files
  - review summaries
  - key issue comments
  - resolution markers from dependency child issues
- Thread that data through the orchestrator into the adjustment request.
- Keep the behavior decision-compatible with the current `proceed |
  request_revision | abandon` shape.

Primary files:

- `src/mergexo/orchestrator.py`
- `src/mergexo/github_types.py` or adjacent typed models if needed
- `src/mergexo/state.py` for frontier queries if needed
- `tests/test_orchestrator.py`

Testing:

- Example-based tests for dependency artifact collection.
- Property-style invariants for frontier completeness where practical.

Exit criteria:

- Adjustment input is a real `(roadmap, ready_frontier, dependency_artifacts)`
  request.
- No operator-visible behavior changes yet beyond richer internal context.

### Commit 3: Make adjustment return a concrete revision payload

Purpose:
Teach the agent to propose the actual roadmap update, not just ask for one.

Changes:

- Extend the adjustment decision type from:

  ```text
  Proceed | RequestRevision | Abandon
  ```

  to:

  ```text
  Proceed
  | Revise(updated_markdown, updated_graph, rationale)
  | Abandon(reason)
  ```

- Update prompts, typed models, and adapter parsing.
- Ensure the Codex adapter validates that a `Revise` payload is internally
  consistent before returning it to the orchestrator.

Primary files:

- `src/mergexo/models.py`
- `src/mergexo/prompts.py`
- `src/mergexo/agent_adapter.py`
- `src/mergexo/codex_adapter.py`
- `tests/test_models_and_prompts.py`
- `tests/test_codex_adapter.py`
- `tests/test_agent_adapter.py`

Exit criteria:

- The adjustment agent can produce a concrete roadmap revision proposal.
- Existing `Proceed` and `Abandon` paths continue to work.

### Commit 4: Auto-create the same-roadmap revision PR

Purpose:
Close the loop from `Revise(...)` to an actual pending revision PR.

Changes:

- Add orchestrator logic that takes a `Revise` payload and creates or updates a
  same-roadmap revision branch and PR.
- Persist pending PR metadata in roadmap state.
- Emit deterministic comments that explain why the roadmap is paused and point
  at the revision PR.
- Keep ownership of the roadmap adjustment lock while waiting on the PR.

Primary files:

- `src/mergexo/orchestrator.py`
- `src/mergexo/state.py`
- `src/mergexo/github.py` or the existing PR creation layer
- `tests/test_orchestrator.py`
- `tests/test_state.py`

Exit criteria:

- A `Revise` decision results in exactly one tracked pending revision PR.
- Further roadmap fan-out remains blocked until that PR is resolved.

### Commit 5: Apply merged revision PRs as the normal active-roadmap path

Purpose:
Stop treating valid merged roadmap updates as drift when they correspond to the
tracked pending revision PR.

Changes:

- Generalize the revision-apply path so it works from pending PR metadata, not
  only from an explicit `revision_requested` lifecycle status.
- Allow a merged update to the canonical roadmap files when it matches the
  tracked pending revision PR and requested version.
- Recompute the ready frontier immediately after successful apply.
- Keep rejecting unrelated graph edits as drift.

Primary files:

- `src/mergexo/orchestrator.py`
- `src/mergexo/state.py`
- `tests/test_orchestrator.py`
- `tests/test_state.py`

Exit criteria:

- Active roadmaps accept the expected pending revision merge as a normal path.
- Unexpected graph changes are still blocked.

### Commit 6: Strengthen graph transition validation and lineage rules

Purpose:
Make roadmap revision safe enough to be routine.

Changes:

- Expand transition validation to enforce:
  - semantic `node_id` stability for unchanged work
  - new `node_id` for replaced work
  - no silent rewrite of issued or completed work
  - no dependency rewrites that invalidate already-achieved milestones
  - retirement rules for pending but unissued nodes
  - monotonic versioning and graph identity checks
- Add more property-based tests for transition invariants.

Primary files:

- `src/mergexo/roadmap_transition_validator.py`
- `tests/test_roadmap_transition_validator.py`

Exit criteria:

- Transition validation covers the full lineage policy needed for living
  roadmaps.
- The validator remains pure and independently testable.

### Commit 7: Finish lock ownership, crash recovery, and no-op short-circuiting

Purpose:
Make the adjustment loop robust under retries, crashes, and multiple workers.

Changes:

- Keep explicit roadmap adjustment ownership from evaluation through pending PR
  resolution.
- Add recovery behavior for stale locks and interrupted workers.
- Persist a stronger adjustment basis digest so the orchestrator can skip
  redundant no-op re-evaluations when neither the frontier nor dependency
  artifacts changed.
- Re-run the frontier after recovery in a deterministic way.

Primary files:

- `src/mergexo/state.py`
- `src/mergexo/orchestrator.py`
- `tests/test_state.py`
- `tests/test_orchestrator.py`

Testing:

- Example-based crash recovery scenarios.
- Property-style invariants around exclusive ownership and idempotent recovery.

Exit criteria:

- At most one worker owns adjustment for a roadmap at a time.
- Repeated polls do not create duplicate revision PRs or duplicate issue fan-out.

### Commit 8: Improve status, revision history, and operator UX

Purpose:
Make continuous adjustment legible to operators and reviewers.

Changes:

- Extend `/roadmap status` output to include:
  - current version
  - coordination state
  - pending revision PR link and version
  - latest adjustment rationale
  - recent revision history
- Add concise comments around adjustment start, revision creation, revision
  merge, and resume.
- Update docs so the operator workflow matches the new living-roadmap model.

Primary files:

- `src/mergexo/orchestrator.py`
- `src/mergexo/observability_queries.py` if needed
- `README.md`
- `docs/design/141-design-for-epics-or-multi-step-development.md`
- tests for status output

Exit criteria:

- Operators can tell exactly why a roadmap is paused and what needs to happen
  next.
- The docs describe continuous adjustment as the normal roadmap flow.

## Notes On Testing Strategy

The style guide requires 100% line coverage and prefers property-based tests for
core logic. The checkpoints above should follow that policy:

1. Keep state transitions and graph validation pure where possible.
2. Add `hypothesis` tests for transition invariants and lock ownership
   invariants where they provide better coverage than example-based tests.
3. Use example-based tests for GitHub integration boundaries, PR creation, and
   user-visible comments.
4. Run `./scripts/test.sh` after each checkpoint commit.

## Recommended Working Rule For The Big PR

Do not merge a checkpoint commit that introduces a new state concept without
also adding:

1. migration coverage,
2. parser coverage,
3. orchestrator integration coverage, and
4. an explicit failure-mode test for the new branch.

That rule should keep the big PR reviewable even though it spans the full
continuous-adjustment system.
