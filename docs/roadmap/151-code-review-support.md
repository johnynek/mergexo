# Code review support

_Issue: #151 (https://github.com/johnynek/mergexo/issues/151)_

## Overview

Add an optional pre-PR automated review loop for code-producing flows. The reviewer should run as a separate agent with a dedicated prompt, should accept repo-specific review guidance, and should feed structured findings back into a bounded author repair loop before MergeXO opens a PR. The rollout is design-first because the feature crosses config, prompt contracts, orchestrator control flow, and operator-visible behavior. Each child PR should keep scope tight and land with full test coverage for the touched behavior.

## DAG notes

- `review_design` merges first and fixes the control loop, config surface, finding schema, and stop conditions.
- After the design lands, `review_config` and `review_contract` can proceed in parallel.
- `review_handoff` waits for the implemented reviewer finding schema so the author-side context format is stable.
- `review_direct` is the first end-to-end slice and covers `bugfix` plus `small_job`.
- `review_impl` extends the same gate to implementation-from-design after the direct-flow path is proven.
- `review_visibility` closes the loop with status comments, docs, and any small observability gaps once both flow integrations exist.

## Nodes

### `review_design`
Kind: `design_doc`
Depends on: none
Title: Design optional automated code review flow

Produce the design doc for issue #151. It should define:
- when automated review runs in relation to author turns, required tests, and PR creation
- repo config for opt-in, reviewer guidance, and bounded repair rounds
- the reviewer output schema and what counts as a blocking finding
- the author handoff strategy for replaying review feedback into a fresh authoring turn
- failure handling, stop conditions, and user-visible status messages when review cannot converge

Completion outcome:
A merged design doc that downstream implementation issues can follow without reopening the control-loop questions.

### `review_config`
Kind: `small_job`
Depends on: `review_design` (`planned`)
Title: Add review config and guidance resolution

Add the repo-level configuration and checkout-time guidance plumbing needed for automated review.

Scope:
- extend config with review enablement or policy, reviewer guidance path, and any review-loop limits chosen in the design
- resolve the optional review guidance file from the checkout similarly to `coding_guidelines_path`
- thread resolved values through the relevant call sites without enabling review behavior yet

Completion outcome:
Config defaults, validation, and path-resolution behavior are implemented and fully covered by tests, with no behavior change when review is disabled.

### `review_contract`
Kind: `small_job`
Depends on: `review_design` (`planned`)
Title: Add reviewer prompt and structured findings contract

Implement the dedicated reviewer-agent contract.

Scope:
- add typed models for reviewer results and findings
- add a prompt builder that includes code-review instructions, repo coding guidance, and repo review guidance
- add Codex adapter schema validation for the reviewer response shape

Completion outcome:
MergeXO can run a review-only agent turn and obtain deterministic structured findings, but no flow uses it yet.

### `review_handoff`
Kind: `small_job`
Depends on: `review_contract` (`implemented`)
Title: Build deterministic author handoff from reviewer findings

Implement the author-side context builder for automated review follow-up.

Scope:
- convert reviewer findings into compact, ordered follow-up context for the next author turn
- preserve file references, round metadata, and truncation rules deterministically
- reuse the existing pre-PR repair style so author retry turns stay bounded and testable

Completion outcome:
The next author turn can consume reviewer findings through a stable handoff format, independent of GitHub PR comments.

### `review_direct`
Kind: `small_job`
Depends on: `review_config` (`implemented`), `review_contract` (`implemented`), `review_handoff` (`implemented`)
Title: Run automated review loops for bugfix and small-job intake

Integrate the automated reviewer into direct PR-producing flows.

Scope:
- after author changes and local required tests succeed, run the reviewer when repo config enables it
- when findings are returned, rerun the author flow with reviewer handoff context and repeat up to the configured limit
- open the PR only after review passes; otherwise post a clear blocking comment on the source issue

Completion outcome:
`bugfix` and `small_job` support optional automated review with deterministic pass, repair, and max-round blocking behavior, while preserving existing behavior when review is disabled.

### `review_impl`
Kind: `small_job`
Depends on: `review_direct` (`implemented`)
Title: Apply automated review to implementation-from-design flow

Extend the same review gate to implementation PRs created from merged design docs.

Scope:
- reuse the shared review loop for `_process_implementation_candidate`
- preserve design-doc context alongside reviewer follow-up context
- keep post-PR human feedback behavior unchanged once the PR is opened

Completion outcome:
Implementation-flow PRs get the same optional second-agent review path without regressing the existing design-to-implementation workflow.

### `review_visibility`
Kind: `small_job`
Depends on: `review_direct` (`implemented`), `review_impl` (`implemented`)
Title: Document and surface automated review outcomes

Finish the operator-facing surfaces for the new behavior.

Scope:
- add deterministic source-issue or PR messaging for review-passed, review-blocked, and review-skipped outcomes as defined by the design
- update `README.md` and `mergexo.toml.example` with the new config keys and guidance-file expectations
- fill any small logging or observability gaps needed to make review rounds and outcomes legible

Completion outcome:
Maintainers can tell when automated review ran, why it blocked if it failed, and how to configure repo-specific reviewer guidance.

## Decomposition notes

This roadmap intentionally stays at seven nodes: one design doc and six bounded implementation jobs. That keeps the DAG within the repo's recommended size and avoids a nested roadmap while still giving MergeXO incremental, reviewable work items.