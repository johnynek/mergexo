# Roadmap #168

> Generated from roadmap graph JSON.
> Edit the `.graph.json` file, not this `.md` file.
> Regenerate with `python roadmap_json_to_md.py <path/to.graph.json> [--output <path/to.md>]`.

## Metadata

- Roadmap issue: `#168`
- Graph version: `1`
- Node count: `6`

## Dependency Overview

1. `review_design` (`reference_doc`): none
2. `review_config` (`small_job`): `review_design` (`planned`)
3. `review_reviewer` (`small_job`): `review_config` (`implemented`), `review_design` (`planned`)
4. `review_repair` (`small_job`): `review_design` (`planned`), `review_reviewer` (`implemented`)
5. `review_flow` (`small_job`): `review_config` (`implemented`), `review_design` (`planned`), `review_repair` (`implemented`), `review_reviewer` (`implemented`)
6. `review_docs` (`small_job`): `review_config` (`implemented`), `review_design` (`planned`), `review_flow` (`implemented`)

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

### `review_config`

- Kind: `small_job`
- Title: Add repo configuration for optional pre-PR review
- Depends on: `review_design` (`planned`)

#### Body

## Summary
Add the repo-level configuration and path-resolution plumbing needed to turn standard-flow review on selectively and to locate repo-supplied review guidance. Do not wire the review execution loop yet.

## Scope
- Extend `RepoConfig` and config parsing for the review settings defined by the accepted design.
- Support both keyed `[repo.<id>]` and legacy `[repo]` config shapes.
- Add helper(s) that resolve the configured review-guidance path in a checkout and degrade cleanly when the file is absent.
- Keep existing flows unchanged when review is not enabled.

## Likely touch paths
- `src/mergexo/config.py`
- `src/mergexo/orchestrator.py`
- `tests/test_config.py`

## Acceptance criteria
- Valid review config loads correctly in both repo config shapes.
- Invalid types or invalid combinations fail with precise config errors.
- Missing review-guidance files are detectable without breaking unrelated flows.
- Tests cover defaults, overrides, legacy compatibility, and path resolution.

### `review_reviewer`

- Kind: `small_job`
- Title: Implement the reviewer prompt and structured findings contract
- Depends on: `review_config` (`implemented`), `review_design` (`planned`)

#### Body

## Summary
Add the dedicated review-agent invocation used to inspect locally authored code changes before PR creation. The output must be structured so the orchestrator can decide whether to continue, request author repairs, or block.

## Scope
- Add the review result dataclasses and any schema helpers needed for structured findings.
- Add a prompt builder that gives the reviewer the issue context, changed-file context, coding guidelines, and repo review guidance when configured.
- Add the Codex adapter entrypoint and strict output validation for the reviewer turn.
- Keep this job focused on the review-agent contract; author remediation and orchestration land separately.

## Likely touch paths
- `src/mergexo/agent_adapter.py`
- `src/mergexo/codex_adapter.py`
- `src/mergexo/prompts.py`
- `tests/test_models_and_prompts.py`
- `tests/test_codex_adapter.py`

## Acceptance criteria
- The reviewer output is structured and machine-consumable, with clear empty-findings and findings-present cases.
- The prompt includes repo review guidance when available and stays usable when that file is absent.
- Invalid reviewer payloads are rejected by tests instead of flowing into orchestration.

### `review_repair`

- Kind: `small_job`
- Title: Resume the author agent from reviewer findings
- Depends on: `review_design` (`planned`), `review_reviewer` (`implemented`)

#### Body

## Summary
Teach MergeXO to hand structured reviewer findings back to the author agent before PR creation so the author can make targeted fixes in the same workstream.

## Scope
- Add a dedicated prompt/adapter path for a pre-PR author-repair turn that consumes reviewer findings.
- Reuse the direct-turn output contract so repaired author turns still return PR metadata, commit intent, and blocked reasons consistently.
- Preserve author-session continuity when resume works, and define/test the fallback behavior when a fresh thread is required.
- Keep this job limited to the author-side repair contract; the orchestrator loop that calls it lands separately.

## Likely touch paths
- `src/mergexo/agent_adapter.py`
- `src/mergexo/codex_adapter.py`
- `src/mergexo/prompts.py`
- `tests/test_models_and_prompts.py`
- `tests/test_codex_adapter.py`

## Acceptance criteria
- Structured reviewer findings can be injected into an author follow-up turn without dropping required direct-turn fields.
- Resume and fresh-thread fallback behavior are both covered by tests.
- Blocked or ambiguous repair outcomes stay explicit in the returned contract.

### `review_flow`

- Kind: `small_job`
- Title: Wire the pre-PR review loop into standard code flows
- Depends on: `review_config` (`implemented`), `review_design` (`planned`), `review_repair` (`implemented`), `review_reviewer` (`implemented`)

#### Body

## Summary
Integrate optional review into the standard code-producing flows so MergeXO can run authoring, review, and author repair before opening a PR.

## Scope
- Apply the review loop to `bugfix`, `small_job`, and `implementation` flows.
- After the initial author turn, run the reviewer, and when findings are returned, run bounded author-repair rounds before PR creation.
- Re-run required-tests checks after each repair commit and only open the PR when the configured review gate is satisfied.
- Preserve the saved author session for the later human PR-feedback loop.
- Post clear issue comments and logs when review blocks, round limits are exceeded, or review is skipped because the feature is disabled.

## Likely touch paths
- `src/mergexo/orchestrator.py`
- `src/mergexo/state.py`
- `tests/test_orchestrator.py`
- `tests/test_state.py`

## Acceptance criteria
- When review is disabled, the existing flows behave exactly as before.
- When review is enabled and the reviewer returns no findings, PR creation proceeds normally.
- When findings are returned, author repair runs before PR creation and required tests are rechecked.
- When review cannot be cleared safely, no PR is opened and the issue gets a clear blocking comment.
- Tests cover success, findings-triggered repair, round-limit or blocked outcomes, and later feedback-loop session continuity.

### `review_docs`

- Kind: `small_job`
- Title: Document review guidance and operator configuration
- Depends on: `review_config` (`implemented`), `review_design` (`planned`), `review_flow` (`implemented`)

#### Body

## Summary
Add the operator-facing docs and example artifacts for the new review capability so repositories have a clear way to configure it and supply repo-specific review guidance.

## Scope
- Update `README.md` with the standard-flow review lifecycle, config knobs, and how reviewer findings are handed back to the author agent before PR creation.
- Update `mergexo.toml.example` with the new review-related repo settings.
- Add a concrete example review-guidance document under `docs/` that MergeXO itself can use as a repository-supplied reviewer brief.
- Keep the docs aligned to shipped behavior, not aspirational behavior.

## Likely touch paths
- `README.md`
- `mergexo.toml.example`
- `docs/`

## Acceptance criteria
- The docs explain that review is optional and how to enable it per repo.
- The docs identify where review guidance lives in-repo and how it influences the reviewer prompt.
- The docs explain the author-repair handoff and any important limits or failure modes operators should expect.
