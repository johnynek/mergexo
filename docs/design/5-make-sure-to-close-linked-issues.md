---
issue: 5
priority: 3
touch_paths:
  - src/mergexo/orchestrator.py
  - tests/test_orchestrator.py
  - docs/design.md
depends_on: []
estimated_size: M
generated_at: 2026-02-22T01:18:03Z
---

# Make sure to close linked issues

_Issue: #5 (https://github.com/johnynek/mergexo/issues/5)_

## Summary

Change issue-driven PR body generation to include GitHub closing syntax (`Closes #<issue_number>`) so merged design PRs automatically close their source issues, with regression tests and docs aligned to the new contract.

## Context
Phase 1 creates a design PR from an issue in `Phase1Orchestrator._process_issue`. The PR body currently includes `Refs #<issue_number>`, which links the issue but does not auto-close it on merge. Issue #5 requires the PR body to include a closing keyword so GitHub closes the linked issue when the PR merges.

## Goals
1. Ensure every issue-triggered design PR body includes `Closes #<issue_number>`.
2. Preserve existing behavior for branch creation, commit/push flow, PR title, and issue comment posting.
3. Add test coverage that prevents regressions in the PR body contract.
4. Align repository design documentation with runtime behavior.

## Non-Goals
1. Editing existing open PR descriptions retroactively.
2. Adding cross-repository close syntax support beyond `#<issue_number>`.
3. Changing feedback-loop logic, state schema, or GitHub gateway APIs.

## Proposed Design
### PR body contract
Use a deterministic body template for issue-driven design PRs:
1. Intro: `Design doc for issue #<issue_number>.`
2. Close directive: `Closes #<issue_number>`
3. Traceability: `Source issue: <issue_url>`

This replaces the current `Refs #<issue_number>` line with `Closes #<issue_number>`.

### Composition location
Keep composition in orchestrator scope but move it to a private helper (for example, `_build_design_pr_body(issue: Issue) -> str`) to avoid string drift and make the contract easy to test.

### Behavioral impact
1. On merge of the generated PR into the default branch, GitHub auto-closes the referenced issue.
2. No change to orchestration state transitions (`running`, `awaiting_feedback`, etc.).
3. No change to GitHub API surface; only the PR `body` payload content changes.

## Implementation Plan
1. Add a small helper in `src/mergexo/orchestrator.py` that renders the design PR body with `Closes #<issue_number>`.
2. Update `_process_issue` to call that helper when invoking `create_pull_request`.
3. Extend `tests/test_orchestrator.py` to assert that the recorded created PR body includes `Closes #7` for the happy path fixture and still includes the source issue URL.
4. Update `docs/design.md` references that currently prescribe `Refs #...` so the architecture/design guidance matches actual behavior.

## Acceptance Criteria
1. For input issue number `N`, orchestrator sends a PR body containing exact token `Closes #N`.
2. The PR body still contains `Source issue: <issue.html_url>`.
3. Existing `_process_issue` happy-path assertions remain valid (PR opened, comment posted, git cleanup executed, state/session persistence intact).
4. Tests fail if the close directive is removed or points to a different issue number.
5. `docs/design.md` no longer describes `Refs #...` as the required issue-linking directive for generated PRs.
6. Manual verification on one merged generated PR confirms GitHub auto-closes the linked issue.

## Risks
1. Incorrect issue number in the closing token could close the wrong issue.
Mitigation: derive the token directly from `issue.number` (same source already used for branch and title) and assert it in tests.

2. GitHub auto-close behavior depends on merge target semantics.
Mitigation: keep base branch behavior unchanged (`default_branch`) and document expected merge behavior.

3. Future PR creation paths may reintroduce non-closing references.
Mitigation: centralize PR body formatting helper and reuse it for any new issue-driven PR flows.

## Rollout Notes
1. No feature flag required; this is a targeted template update.
2. Ship with unit test coverage in the same change.
3. After deploy, observe first few merged design PRs to confirm linked issues auto-close as expected.
4. If unintended closures are observed, revert the template helper change quickly (no schema/data migration needed).
