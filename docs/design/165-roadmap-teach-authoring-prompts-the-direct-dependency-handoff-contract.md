---
issue: 165
priority: 3
touch_paths:
  - src/mergexo/prompts.py
  - tests/test_models_and_prompts.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-03-29T00:47:32Z
---

# Teach roadmap authoring prompts the direct dependency handoff contract

_Issue: #165 (https://github.com/johnynek/mergexo/issues/165)_

## Summary

Define the post-#159/#162 roadmap authoring contract so initial roadmap creation and same-roadmap revision prompts explicitly teach direct-only worker handoff, explicit edge modeling, and milestone choice based on the earliest truthful artifact handoff.

## Context

Issue #159 and PR #161 made roadmap node kinds truthful by separating `reference_doc` from `design_doc`. Issue #162 and PR #164 then made the runtime truthful for downstream execution: each roadmap-created child issue now carries a durable direct-dependency handoff snapshot, and workers receive that same direct dependency handoff in their prompt, including exact doc or roadmap artifact paths plus GitHub and git provenance.

The remaining gap is on the authoring side. `src/mergexo/prompts.py` already teaches roadmap authors about node kinds and milestone ordering, but it still frames dependencies mostly as sequencing. It does not clearly tell the roadmap-authoring agent what a downstream worker will actually receive from a dependency edge.

That mismatch matters because the runtime contract is now stronger than the authoring contract:
- workers receive direct dependency artifacts only
- the handoff is projected into both the child issue body and the worker prompt
- doc-like dependencies include exact artifact paths and provenance
- workers are not expected to reconstruct artifact locations from transitive graph structure or git history

When the authoring prompt omits that contract, an authored roadmap can still be structurally valid while being based on the wrong execution model.

## Goals

1. Define a single truthful author-facing contract for roadmap dependency modeling after #159 and #162.
2. Update roadmap authoring and revision prompts so they explicitly promise the direct-only worker handoff behavior that already exists at runtime.
3. Teach roadmap authors to add explicit direct edges whenever a downstream worker needs another upstream artifact.
4. Reframe `planned` versus `implemented` around the earliest milestone at which MergeXO can hand the needed input to the downstream worker truthfully.
5. Keep the guidance short enough for routine prompt use while still being concrete enough to prevent the older dependency misunderstandings.

## Non-goals

1. Redesigning runtime dependency handoff again after #162.
2. Changing roadmap graph schema, roadmap node kinds, or dependency field names.
3. Adding a new artifact-discovery runtime for workers.
4. Building a heuristic roadmap linter that tries to infer every missing dependency edge.

## Decision

Make the roadmap authoring prompts explicitly treat dependencies as both sequencing constraints and worker-input declarations.

The core authoring rule should be:

A roadmap edge exists when the downstream worker must receive that upstream artifact directly in its child issue body and worker prompt.

That rule aligns the authoring mental model with the current runtime behavior. A dependency is no longer just "what should happen before this other thing." It is also "what MergeXO will hand to the downstream worker as direct input."

This issue should be implemented as a prompt-contract update, not a runtime change. The runtime contract already exists in the child issue body marker, worker prompt rendering, and README execution docs. The missing work is to teach roadmap authors and revisers the same contract in the prompts that create or revise roadmap graphs.

## Prompt Surfaces

The new contract should be carried by all roadmap-authoring surfaces that can create or revise dependency edges:

1. `build_roadmap_prompt(...)`
   This is the main initial-authoring surface and should carry the full contract.
2. `build_roadmap_adjustment_prompt(...)`
   This prompt can decide `proceed`, but when it decides `revise` it is authoring a same-roadmap revision. It needs the same dependency-modeling contract.
3. `build_requested_roadmap_revision_prompt(...)`
   This prompt always authors or declines a same-roadmap revision and therefore also needs the same contract.

A short consistency reminder should also be considered for `build_roadmap_feedback_prompt(...)`. That prompt edits roadmap artifacts during review feedback, so it can otherwise regress dependency wording back to the older sequencing-only model. The shorter reminder can reuse the same helper text but does not need the full explanatory block if prompt size becomes a concern.

The contract text should be produced from one shared helper in `src/mergexo/prompts.py` so the three required prompts cannot drift from one another.

## Required Prompt Contract

The shared prompt block should make these promises explicitly and in plain language:

- Downstream workers receive direct dependency artifacts only, not the transitive closure of the roadmap graph.
- The direct dependency handoff appears in both the child issue body and the worker prompt.
- For doc-like dependencies, MergeXO hands off exact artifact paths on the default branch plus stable GitHub and git provenance when available.
- Workers should not be expected to walk the roadmap graph, infer artifact locations from node ids, search the repository heuristically, or reconstruct intent from git history.
- If a downstream worker needs another upstream artifact, the roadmap author must model that as an explicit direct dependency edge.

This wording should be presented as a truthful statement about the current system, not as a suggestion or best effort. Issue #165 exists because the runtime already guarantees these behaviors.

## How Prompts Should Teach Edge Modeling

The prompts should move from "dependencies express milestone ordering" to "dependencies express the direct artifacts the worker will receive when unblocked."

The authoring guidance should be:

1. Ask what concrete upstream artifact or state the downstream worker must have on its first turn.
2. Add a direct edge to every upstream node whose artifact must appear in that worker's handoff.
3. Choose `requires = planned` or `requires = implemented` based on the earliest milestone at which MergeXO can hand that required input over truthfully.
4. If the worker needs two upstream artifacts, add two direct edges. Do not rely on one upstream node to transitively carry another node's artifact unless the roadmap intentionally created a new direct artifact that summarizes it.

The prompts should also include concise anti-pattern guidance:

- Do not rely on transitive dependencies to get artifacts into a worker prompt.
- Do not assume the worker will walk git history or search for the right file.
- Do not choose `planned` merely to preserve ordering if the needed artifact does not exist yet at that milestone.

## Per-Kind Handoff Guidance

The initial roadmap prompt should include short per-kind examples of what downstream workers will receive. Full JSON examples are unnecessary; one-line milestone-oriented bullets are sufficient and keep the prompt practical.

The recommended guidance is:

- `reference_doc`
  Downstream workers receive the merged doc artifact itself, including its exact path and provenance. If a node only needs that reviewed document, model a direct edge to the `reference_doc` node.
- `design_doc`
  For `planned`, downstream workers receive the merged design artifact path and design PR provenance. For `implemented`, they still receive the design doc path, but the dependency is satisfied by the later implementation outcome from that same child issue.
- `small_job`
  For `planned`, the worker can rely only on the child issue existing and its issue text; merged code and changed files are not available yet. For `implemented`, the worker receives merged PR provenance and changed-file context.
- `roadmap`
  For `planned`, the worker receives the child roadmap markdown and graph artifact paths plus activation provenance. For `implemented`, the worker can assume the child roadmap completed rather than merely activated.

These examples matter because the right edge and the right `requires` value depend on what the future worker will actually see.

## Teaching `planned` Versus `implemented`

The prompts should stop teaching `planned` and `implemented` as generic ordering milestones and instead teach them as worker-input milestones.

The decision rule should be:

Choose the earliest milestone at which MergeXO can truthfully hand the downstream worker the concrete direct input it needs.

That yields the following authoring guidance:

- `reference_doc`
  Usually use `planned` when downstream work needs the merged document artifact. `implemented` remains technically valid because `reference_doc` is terminal, but it should not be the default teaching because it hides the real artifact handoff rule.
- `design_doc`
  Use `planned` when the merged design artifact is enough for downstream work. Use `implemented` only when the downstream node depends on the shipped implementation outcome from that same design issue.
- `small_job`
  Use `planned` only when the downstream node can begin once the upstream issue exists and no merged artifact is required. If the downstream node needs code, files, or behavior from the upstream job, use `implemented`.
- `roadmap`
  Use `planned` when downstream work only needs the activated child roadmap artifacts. Use `implemented` when it depends on outputs that exist only after the nested roadmap completes.

This reframing is the main behavior change for the authoring prompts. It turns `planned` into a truthful promise about what the worker will receive, not just a topological convenience.

## Issue #151 / PR #154 Example

Issue #151 and PR #154 should be the motivating example in the prompt contract update.

The prompt should teach that:

- `review_design` is a `reference_doc` because its purpose is to produce a reviewed durable design/reference artifact.
- `review_config` needs a direct edge to `review_design` with `requires = planned` because its worker should receive the exact `review_design` artifact path and accepted design constraints in its direct dependency handoff.
- `review_contract` also needs a direct edge to `review_design` with `requires = planned` for the same reason.
- If a later node needs both the reviewed design artifact and a merged output from `review_config`, that later node should depend directly on both nodes. It should not assume the `review_design` artifact will arrive transitively through `review_config`.

This is the concrete example that turns the new rule into graph-authoring behavior.

## Validation And Warning Strategy

Prompt text plus tests are sufficient for this issue. A new graph validator is not required.

The reason is scope and signal quality:

- The runtime contract is already implemented and deterministic.
- The remaining gap is the author's mental model, which prompt language directly addresses.
- Detecting missing direct edges statically is heuristic and repo-specific. A hard validator would create false positives and expand the issue beyond the prompt-contract mismatch.

The reinforcement for this issue should therefore be:

- one shared prompt-contract helper in `src/mergexo/prompts.py`
- prompt tests that assert the same direct-handoff language appears in every required roadmap authoring surface
- a README wording update so operator-facing docs and agent-facing prompts say the same thing

If later roadmaps still show repeated under-modeled edges, a future issue can add non-blocking lint or review hints. That should not block this prompt-alignment change.

## Architecture

The architecture change is small but important: centralize the roadmap authoring contract into one reusable prompt fragment and project it into each authoring surface that can emit or revise dependency edges.

Recommended structure inside `src/mergexo/prompts.py`:

- add a helper that returns the shared direct-dependency handoff contract text
- optionally add a second helper for the short per-kind handoff bullets so the initial prompt can include the fuller version while revision prompts use a shorter form if needed
- call that helper from `build_roadmap_prompt(...)`, `build_roadmap_adjustment_prompt(...)`, and `build_requested_roadmap_revision_prompt(...)`
- optionally call the shorter form from `build_roadmap_feedback_prompt(...)`

No changes to response schemas in `src/mergexo/codex_adapter.py` are expected, because this issue changes prompt content rather than JSON shape.

## Implementation Plan

1. Add a shared roadmap-authoring contract helper to `src/mergexo/prompts.py` that states the direct-only handoff rule, the child-issue-body plus worker-prompt projection, the doc-path plus provenance promise, and the explicit-edge requirement.
2. Insert that helper into `build_roadmap_prompt(...)` before the existing node-kind and milestone guidance so the downstream worker model is established before the prompt asks the agent to choose dependencies.
3. Insert the same contract into `build_roadmap_adjustment_prompt(...)` and `build_requested_roadmap_revision_prompt(...)` so same-roadmap revisions are authored against the same mental model as initial roadmaps.
4. Update the milestone guidance in those prompts so `planned` and `implemented` are taught as earliest truthful handoff milestones rather than generic sequencing markers.
5. Add concise per-kind handoff bullets and anti-pattern bullets, with special emphasis on `reference_doc`, `design_doc`, and the direct-only rule.
6. Add or update tests in `tests/test_models_and_prompts.py` so the initial roadmap prompt and both revision prompts assert the new contract text and issue #151 style guidance.
7. Update `README.md` roadmap documentation so the operator-facing wording about roadmap authoring matches the prompt contract already used at runtime.

## Risks

- Prompt drift
  If each roadmap authoring prompt gets its own manually written version of the contract, the wording will diverge again. A shared helper and prompt-level tests reduce that risk.
- Prompt bloat
  Adding too much explanatory text could make the roadmap prompts harder to use. The contract should stay compact and use one-line per-kind examples instead of large narrative examples or JSON payloads.
- Overcorrection toward `implemented`
  Authors may respond to the new contract by overusing `implemented` for safety. The milestone guidance must explicitly say to choose the earliest truthful handoff milestone, not the latest one.
- Review-path regression
  If roadmap review-feedback prompts keep the older sequencing-only framing, reviewers can accidentally reintroduce weak edges while editing the roadmap PR. A short reminder there is recommended even if it is not strictly required for the initial landing.

## Rollout Notes

- Land the prompt text changes, README update, and prompt tests in the same PR.
- No runtime or state migration is required; this issue aligns agent-facing wording with behavior that already shipped in #164.
- Existing already-issued child issues do not need backfill. The new contract affects future roadmap authoring turns and future same-roadmap revision turns.
- Open roadmap work should adopt the new guidance the next time it is authored or revised. For issue #151 / PR #154 specifically, the next revision should keep `review_design` as a direct `reference_doc` dependency of both `review_config` and `review_contract`.

## Acceptance Criteria

1. The design doc defines the authoritative roadmap authoring contract after #159 and #162.
2. The design doc specifies the exact promises that roadmap authoring and same-roadmap revision prompts should make about worker handoff behavior.
3. The design doc states explicitly that workers receive direct dependency artifacts only, not a transitive closure, and explains how roadmap authors should encode any additional required upstream inputs.
4. The design doc explains how prompt guidance for `reference_doc`, `design_doc`, `small_job`, and `roadmap` should change in light of the direct-handoff contract.
5. The design doc explains `planned` versus `implemented` using the earliest truthful handoff rule rather than generic milestone ordering.
6. The design doc uses issue #151 / PR #154 as a motivating example and states that `review_config` and `review_contract` should each depend directly on `review_design` with `requires = planned`.
7. The design doc identifies likely implementation touch paths in `src/mergexo/prompts.py`, `tests/test_models_and_prompts.py`, and `README.md`.
8. The design leaves schemas and runtime handoff behavior unchanged and scopes the implementation to prompt, test, and documentation alignment.
