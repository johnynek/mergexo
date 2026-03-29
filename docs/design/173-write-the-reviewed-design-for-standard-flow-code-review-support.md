---
issue: 173
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_models_and_prompts.py
  - tests/test_codex_adapter.py
  - tests/test_orchestrator.py
  - tests/test_state.py
depends_on: []
estimated_size: M
generated_at: 2026-03-29T18:27:03Z
---

# Design optional pre-PR code review for standard flows

_Issue: #173 (https://github.com/johnynek/mergexo/issues/173)_

## Summary

Define an opt-in pre-PR reviewer and author-repair loop for `bugfix`, `small_job`, and `implementation` that preserves the author session for later PR feedback, keeps design and roadmap PRs on the human-review path, and sequences review ahead of push and PR creation.

## Context

MergeXO currently has three code-producing pre-PR flows:
- `bugfix` and `small_job` start from an issue, let an author agent edit the checkout, run `required_tests`, push, and open a PR.
- `implementation` does the same, but it also passes the merged design-doc context into the author turn.
- After PR creation, the existing feedback loop resumes the saved author session from `agent_sessions` and uses it to answer PR review comments, issue comments, and CI failures.

`reference_doc`, `design_doc`, `roadmap`, and same-roadmap revision PRs already exist to gather human review on design or planning artifacts. Adding an automatic pre-PR reviewer there would mostly duplicate the existing human review surface and slow the planning loop.

Issue #173 asks for optional pre-PR review on code-producing flows only. The design needs to define the repo configuration surface, the reviewer and author-repair contracts, and the sequencing around required tests, push, and PR creation without breaking the existing post-PR feedback loop.

## Goals

1. Add an opt-in, default-off pre-PR review gate for `bugfix`, `small_job`, and `implementation`.
2. Let repositories provide reviewer-specific guidance without making that file mandatory.
3. Reuse the existing author session for author-repair turns and later PR feedback turns.
4. Keep the runtime bounded and deterministic with explicit review and repair limits plus clear blocked outcomes.
5. Preserve current behavior exactly when review is disabled.

## Non-goals

1. Running pre-PR automated review on `design_doc`, `reference_doc`, `roadmap`, or same-roadmap revision PRs.
2. Replacing human PR review after the PR is opened.
3. Introducing multiple reviewer agents or a persisted reviewer discussion history.
4. Changing the existing post-PR feedback-loop contract except where it consumes the saved author session.

## Decision

Standard-flow review is an optional pre-PR gate that runs after the author has produced a locally test-green candidate and before MergeXO pushes or opens a PR.

Eligible flows:
- `bugfix`
- `small_job`
- `implementation`

Flows that stay on the existing human-review path:
- `reference_doc`
- `design_doc`
- `roadmap`
- same-roadmap revision PRs

The core invariants are:
- Review is repo-scoped and opt-in.
- Review never overwrites the saved author session with a reviewer session.
- A PR is created only after the exact local head to be pushed has both passed `required_tests` and cleared the reviewer gate.
- Any post-review code mutation invalidates the earlier review approval and re-enters the gate.

## Configuration Surface

Add a small `PrePrReviewConfig` value on `RepoConfig`, but load it from flat repo keys so keyed `[repo.<id>]` and legacy `[repo]` shapes can share the same parser structure.

Proposed repo keys:
- `pre_pr_review_flows`: array of `bugfix`, `small_job`, and/or `implementation`. Absent or empty means review is disabled.
- `pre_pr_review_guidance_path`: optional repo-relative or absolute path to reviewer-specific guidance.
- `pre_pr_review_max_repair_rounds`: integer `>= 0`; bounds how many author-repair cycles may happen after the initial review.

Semantics:
- Default-off: if `pre_pr_review_flows` is absent or `[]`, all flows behave exactly as they do today.
- Missing `pre_pr_review_guidance_path` is non-fatal. MergeXO logs the miss, omits the extra guidance from the prompt, and continues with the base review instructions.
- `design_doc`, `reference_doc`, and `roadmap` flows ignore these keys because they do not enter the pre-PR review gate.
- `pre_pr_review_max_repair_rounds = 0` means: review once and do not run author repair automatically.
- When review keys are present, invalid flow names, duplicate values, negative limits, or wrong types should fail config loading with precise errors.

## Reviewer Contract

Add a dedicated reviewer prompt and schema for pre-PR review. This is distinct from the post-PR feedback prompt.

Concrete artifacts:
- `build_pre_pr_review_prompt(...)` in `src/mergexo/prompts.py`
- `ReviewFinding` dataclass
- `PrePrReviewResult` dataclass
- `review_pre_pr_candidate(...)` adapter entrypoint

Prompt inputs should include:
- issue title/body and flow name
- repository/default-branch context
- changed file list and current local diff context
- `coding_guidelines_path` when present
- repo-supplied `pre_pr_review_guidance_path` contents when present
- for `implementation`, the merged design doc path and design provenance already available to the author flow

Structured reviewer output:
- `outcome`: `approved | changes_requested | blocked`
- `summary`: non-empty string
- `findings`: array of blocking findings
- `blocked_reason`: non-empty string or `null`

Each finding should include:
- `finding_id`: stable within the response, for example `R1`
- `path`: repo-relative file path
- `line`: integer or `null`
- `title`: short actionable headline
- `details`: concrete explanation of the defect and expected repair

Contract rules:
- `approved` requires `findings = []` and `blocked_reason = null`.
- `changes_requested` requires at least one finding and `blocked_reason = null`.
- `blocked` requires `blocked_reason` and does not open a PR.
- Findings are blocking findings only. Non-blocking style nits should not appear in this gate.

## Author Repair Contract

When the reviewer returns `changes_requested`, MergeXO hands those structured findings back to the author agent instead of opening a PR.

Concrete artifacts:
- `build_pre_pr_author_repair_prompt(...)` in `src/mergexo/prompts.py`
- `repair_from_pre_pr_review(...)` adapter entrypoint, or equivalent author-resume method
- reuse of the existing `DirectStartResult` output contract

The author-repair prompt should contain:
- the original issue context for the active flow
- the reviewer summary and structured findings JSON
- the current branch and head context
- flow-specific context already used by the base author prompt
- the unchanged direct-turn output contract: `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`

This keeps author-repair turns compatible with the existing direct-flow machinery:
- `commit_message` still signals that MergeXO may commit the new edits
- `blocked_reason` still stops the flow with a clear issue comment
- `escalation` remains available for roadmap-assumption failures in the current issue/design lane

## Session Continuity

The saved issue-scoped agent session must remain the author session, not the reviewer session.

Design decisions:
- After the initial author turn, MergeXO saves the returned author session exactly as it does today.
- Pre-PR reviewer sessions are treated as ephemeral process-local state; they are not written to `agent_sessions`.
- When author repair is needed, MergeXO first tries to resume the saved author thread.
- If author resume fails because the underlying agent cannot continue the thread, the adapter starts a fresh author thread seeded with the original flow context plus the structured reviewer findings.
- After either resume or fresh-thread fallback, MergeXO immediately overwrites the saved issue session with the newest author session.

This preserves the later PR feedback loop:
- once a PR is opened, `respond_to_feedback(...)` continues from the most recent author thread stored for that issue
- pre-PR reviewer activity cannot steal or invalidate the thread that post-PR feedback expects to resume

No new sqlite table is required for reviewer conversational state. The durable state continues to be the existing issue-scoped author session plus the normal pre-PR blocked and follow-up state.

## Orchestration Sequence

The orchestration change belongs in the direct-flow path, not in the post-PR feedback loop.

For an eligible flow with review enabled, the sequence is:

1. Run the normal author turn for `bugfix`, `small_job`, or `implementation`.
2. Commit the author's local edits and run `required_tests` using the existing repair loop.
3. Once the local head is test-green, invoke the reviewer on that exact local head.
4. If the reviewer returns `approved`, the candidate is eligible for push.
5. If the reviewer returns `changes_requested`, resume the author session with the findings, commit the repair, rerun `required_tests`, and rerun the reviewer.
6. If the reviewer returns `blocked`, or if the author returns `blocked_reason`, stop before push and PR creation and leave a clear source-issue comment.
7. Only after review approval do push and PR creation run.

Two additional sequencing rules keep the gate truthful:
- If required-tests repair changes the branch, the reviewer must run again on the repaired head.
- If push-time conflict repair or automatic remote-race reconciliation changes the branch, `required_tests` and the reviewer must run again before the PR is opened.

This means the reviewer approval applies to the exact head that is about to become the PR head, not an earlier local state.

## Blocked Outcomes

Blocked outcomes should stay explicit and should not produce a PR.

Cases that block before PR creation:
- reviewer returns `blocked`
- author returns `blocked_reason` during initial authoring or repair
- `pre_pr_review_max_repair_rounds` is exhausted while the reviewer still returns findings
- required tests cannot be satisfied after the existing repair limit
- push-time merge conflict or remote-race repair changes cannot be cleared safely
- new source-issue comments arrive and the existing pre-PR ordering gate defers the run

Behavior:
- post a clear source-issue comment describing why no PR was opened
- reuse the existing recoverable pre-PR blocked and follow-up path when the failure is recoverable
- do not synthesize a draft PR just to hold reviewer findings; the feature is specifically a pre-PR gate

## Adapter And Orchestrator Responsibilities

`src/mergexo/config.py`
- Parse flat `pre_pr_review_*` repo keys and normalize them into `RepoConfig.pre_pr_review`.
- Keep keyed and legacy repo shapes behaviorally equivalent.

`src/mergexo/prompts.py`
- Build the reviewer prompt.
- Build the author-repair prompt.
- Inject optional repo review guidance when present.
- Keep the direct-turn output contract unchanged for author-repair turns.

`src/mergexo/agent_adapter.py`
- Define the reviewer result dataclasses and any new adapter entrypoints.

`src/mergexo/codex_adapter.py`
- Add strict JSON schemas for reviewer output.
- Validate reviewer payloads before orchestration sees them.
- Implement author-resume first, with fresh-thread fallback for repair turns.

`src/mergexo/orchestrator.py`
- Resolve review guidance paths in the checkout.
- Decide whether the current flow enters the review gate.
- Run the author -> tests -> reviewer -> author-repair loop.
- Preserve only the author session in durable state.
- Rerun the gate whenever push-time mutations change the candidate head.
- Emit clear blocked comments and keep disabled flows unchanged.

`src/mergexo/state.py`
- Ideally remain unchanged structurally.
- If implementation convenience warrants new helper methods, they must preserve the current issue-scoped author-session contract and avoid a schema migration unless truly necessary.

## Implementation Plan

1. Config slice
- Add `pre_pr_review_*` repo keys and `RepoConfig` plumbing in `src/mergexo/config.py`.
- Add path resolution helpers in `src/mergexo/orchestrator.py`.
- Test in `tests/test_config.py` for keyed and legacy config, invalid flow names, empty-flow default-off behavior, and missing-guidance handling.

2. Reviewer contract slice
- Add reviewer dataclasses and a prompt builder in `src/mergexo/agent_adapter.py` and `src/mergexo/prompts.py`.
- Add strict schema parsing in `src/mergexo/codex_adapter.py`.
- Test in `tests/test_models_and_prompts.py` and `tests/test_codex_adapter.py` for prompt content, guidance inclusion, `approved` vs `changes_requested` vs `blocked`, and invalid payload rejection.

3. Author-repair slice
- Add a dedicated author-repair adapter path that resumes the author session first and falls back to a fresh thread when needed.
- Keep the direct-turn JSON contract unchanged.
- Test in `tests/test_codex_adapter.py` for resume success, fresh-thread fallback, and preservation of `pr_title`, `pr_summary`, `commit_message`, and `blocked_reason`.

4. Flow-integration slice
- Integrate the review gate into `bugfix`, `small_job`, and `implementation` orchestration only.
- Ensure `required_tests`, push repair, and remote-race reconciliation rerun review when they change `HEAD`.
- Test in `tests/test_orchestrator.py` for:
  - disabled behavior unchanged
  - reviewer pass with normal PR creation
  - findings-triggered author repair
  - exhausted repair rounds
  - reviewer-blocked outcome
  - push-time mutation forcing re-review
  - later PR feedback using the saved author session after pre-PR review

5. Docs slice
- Update `README.md` and `mergexo.toml.example` with the new config and lifecycle.
- Add an example repo review-guidance document under `docs/` only if the implementation lands with a concrete in-repo example.
- Keep operator docs explicit that design and roadmap PRs remain human-reviewed only.

## Acceptance Criteria

1. A repo can opt into pre-PR review for any subset of `bugfix`, `small_job`, and `implementation`, and the default behavior remains off.
2. `design_doc`, `reference_doc`, `roadmap`, and same-roadmap revision PRs stay on the existing human-review path and do not enter the automated pre-PR reviewer gate.
3. The design names the concrete artifacts, prompt builders, and adapter/orchestrator responsibilities for reviewer turns and author-repair turns.
4. Reviewer findings are passed back to the author as structured data without overwriting the saved author session used by later PR feedback.
5. A PR is created only after the exact candidate head has passed `required_tests` and the reviewer has returned `approved`.
6. When review cannot clear safely, no PR is opened and the source issue gets a clear blocked comment.
7. The implementation plan calls out the expected tests for config, reviewer contract, author repair, and orchestration sequencing slices.

## Risks and Mitigations

1. Reviewer false positives or prompt drift could slow delivery.
Mitigation: default-off rollout, repo-supplied guidance, and a contract that only returns blocking findings.

2. Reviewer or repair turns could break later PR feedback by overwriting the wrong session.
Mitigation: never persist reviewer sessions in `agent_sessions`; always persist the newest author session immediately after author turns.

3. Push-time branch changes could make the approved review stale.
Mitigation: any code-changing required-tests repair, merge-conflict repair, or remote-race reconciliation must rerun review before PR creation.

4. Config misconfiguration or missing guidance files could cause surprising failures.
Mitigation: strict config validation for types and flow names; missing guidance files degrade cleanly and are logged instead of aborting unrelated flows.

## Rollout Notes

- Ship this behind repo-level opt-in only; there is no global default-on rollout.
- Land config parsing, reviewer contract, and orchestration changes together so a repo cannot enable review before the runtime understands it.
- Start operational rollout on repositories that already have stable `required_tests` and clear coding guidelines, since the reviewer gate assumes those local checks are meaningful.
- Do not backfill or reinterpret existing PRs. The feature applies only to new pre-PR direct-flow runs after the repo opts in.
- Documentation should explicitly say that this feature augments, but does not replace, normal human PR review.
