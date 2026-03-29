# Roadmap #168

> Generated from roadmap graph JSON.
> Edit the `.graph.json` file, not this `.md` file.
> Regenerate with `python roadmap_json_to_md.py <path/to.graph.json> [--output <path/to.md>]`.

## Metadata

- Roadmap issue: `#168`
- Graph version: `2`
- Node count: `5`

## Dependency Overview

1. `review_design` (`reference_doc`): none
2. `review_contracts` (`small_job`): `review_design` (`planned`)
3. `review_pr_plumbing` (`small_job`): `review_design` (`planned`)
4. `review_runtime` (`small_job`): `review_contracts` (`implemented`), `review_design` (`planned`), `review_pr_plumbing` (`implemented`)
5. `review_docs_runtime` (`small_job`): `review_contracts` (`implemented`), `review_design` (`planned`), `review_runtime` (`implemented`)

## Nodes

### `review_design`

- Kind: `reference_doc`
- Title: Write the reviewed design for standard-flow code review support
- Depends on: none

#### Body

## Summary
Write a durable design/reference doc for optional code-review support on MergeXO's code-producing flows. This artifact should settle architecture questions before implementation work starts.

## Scope
- Cover `bugfix`, `small_job`, and `implementation` flows explicitly, and state whether design-doc or roadmap PRs stay on the existing human-review path.
- Define the repo configuration surface for enabling review, locating repo-supplied review guidance, and bounding any review/repair rounds.
- Define the structured reviewer output contract and the author-repair handoff contract.
- Define how the author session remains usable for later PR feedback after pre-PR review completes.
- Define sequencing around required tests, push/PR creation, blocked outcomes, and rollout defaults.

## Likely touch paths
- `docs/design/168-code-review-support-as-part-of-the-standard-flow.md`

## Acceptance criteria
- The design names the concrete artifacts, prompts, and adapter/orchestrator responsibilities.
- The design explains how reviewer findings are passed back to the author agent for action without losing later feedback-loop continuity.
- The design states the opt-in/default-off behavior and the expected test plan for each implementation slice.

### `review_contracts`

- Kind: `small_job`
- Title: Implement reviewer and author-repair contracts for pre-PR review
- Depends on: `review_design` (`planned`)

#### Body

## Summary
Implement the reviewer and author-repair agent contracts needed for pre-PR review without exposing repo config yet.

## Scope
- Add reviewer result dataclasses and strict structured-output validation for `approved`, `changes_requested`, and `escalate`.
- Add prompt builders for the reviewer turn and the author-repair turn, with optional review-guidance content passed in by callers when available.
- Add adapter entrypoints for reviewer execution and author repair that resume the saved author session first and fall back to a fresh thread when needed.
- Keep the direct-turn output contract unchanged for author-repair turns, and do not wire repo config or orchestrator flow control in this job.

## Likely touch paths
- `src/mergexo/agent_adapter.py`
- `src/mergexo/codex_adapter.py`
- `src/mergexo/prompts.py`
- `tests/test_models_and_prompts.py`
- `tests/test_codex_adapter.py`

## Acceptance criteria
- Reviewer output is machine-consumable, with valid `approved`, `changes_requested`, and `escalate` cases plus rejection of invalid payloads.
- Reviewer prompts include coding guidelines and optional repo review guidance content when provided by the caller, while remaining usable when that guidance is absent.
- Author-repair turns preserve `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`, with tests for resume success and fresh-thread fallback.
- No repo-level review config is parsed or exposed in this slice.

### `review_pr_plumbing`

- Kind: `small_job`
- Title: Add draft PR plumbing for escalated pre-PR review outcomes
- Depends on: `review_design` (`planned`)

#### Body

## Summary
Add the draft-PR creation plumbing needed when pre-PR review escalates instead of clearing.

## Scope
- Extend GitHub PR creation support to request draft PRs while preserving the current non-draft default.
- Thread the draft flag through PR creation helpers and outbox serialization so later flow work can open draft PRs intentionally.
- Keep this slice focused on PR-creation plumbing; reviewer decisions and orchestration policy land separately.

## Likely touch paths
- `src/mergexo/github_gateway.py`
- `src/mergexo/orchestrator.py`
- `tests/test_github_gateway.py`
- `tests/test_orchestrator.py`

## Acceptance criteria
- PR creation can request draft state and preserves current behavior when draft is not requested.
- Outbox persistence and rehydration round-trip the draft flag correctly.
- Tests cover both draft and non-draft PR creation paths.

### `review_runtime`

- Kind: `small_job`
- Title: Ship the pre-PR review runtime and repo config for standard flows
- Depends on: `review_contracts` (`implemented`), `review_design` (`planned`), `review_pr_plumbing` (`implemented`)

#### Body

## Summary
Ship the end-to-end pre-PR review gate for `bugfix`, `small_job`, and `implementation`, landing repo config only together with the runtime that consumes it.

## Scope
- Parse `pre_pr_review_*` repo keys into `RepoConfig`, support keyed `[repo.<id>]` and legacy `[repo]` shapes, and resolve optional review-guidance paths in the checkout.
- Apply the review gate to `bugfix`, `small_job`, and `implementation` after the author has produced a local test-green candidate.
- On `changes_requested`, run bounded author-repair rounds, rerun `required_tests`, and rerun review whenever local or push-time repair changes `HEAD`.
- On `escalate` or exhausted repair rounds, push the current test-green head, open a draft PR, and post the final reviewer concerns without overwriting the saved author session.
- Preserve exact existing behavior when review is disabled.

## Likely touch paths
- `src/mergexo/config.py`
- `src/mergexo/orchestrator.py`
- `src/mergexo/state.py`
- `tests/test_config.py`
- `tests/test_orchestrator.py`
- `tests/test_state.py`

## Acceptance criteria
- A repo can opt into review for any subset of `bugfix`, `small_job`, and `implementation`, and config errors for invalid flow names, duplicate flows, negative limits, or wrong types stay precise.
- Disabled repos behave exactly as before, and missing review-guidance files degrade cleanly without aborting unrelated flows.
- Approved review opens a normal PR; escalated or exhausted review opens a draft PR with reviewer concerns attached; only hard-stop conditions prevent PR creation.
- The saved issue-scoped session remains the author session across initial authoring, repair turns, and later PR feedback.
- Tests cover no-review behavior, clean approval, findings-triggered repair, exhausted-round escalation, explicit reviewer escalation, push-time mutation re-review, and later feedback continuing from the author session.

### `review_docs_runtime`

- Kind: `small_job`
- Title: Document review guidance and operator configuration
- Depends on: `review_contracts` (`implemented`), `review_design` (`planned`), `review_runtime` (`implemented`)

#### Body

## Summary
Document the shipped pre-PR review feature and provide operator-facing guidance only after the runtime behavior is merged.

## Scope
- Update `README.md` with the eligible flows, opt-in config surface, review lifecycle, draft-PR escalation behavior, and author-repair handoff.
- Update `mergexo.toml.example` with the new `pre_pr_review_*` repo settings.
- Add or update a concrete example review-guidance document under `docs/` if the shipped runtime includes a repository-supplied guidance example.
- Keep docs explicit that `reference_doc`, `design_doc`, `roadmap`, and same-roadmap revisions stay on the human-review path.

## Likely touch paths
- `README.md`
- `mergexo.toml.example`
- `docs/`

## Acceptance criteria
- Docs explain that review is optional, default-off, and configurable per repo for `bugfix`, `small_job`, and `implementation`.
- Docs identify how repo review guidance is supplied, when missing guidance is tolerated, and how reviewer findings are handed back to the author.
- Docs explain that escalated review opens a draft PR with reviewer concerns, while true hard stops can still prevent PR creation.
- Docs reflect shipped behavior only.
