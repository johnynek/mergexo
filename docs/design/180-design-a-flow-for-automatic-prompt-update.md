---
issue: 180
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/github_gateway.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_codex_adapter.py
  - tests/test_github_gateway.py
  - tests/test_orchestrator.py
  - tests/test_state.py
depends_on: []
estimated_size: M
generated_at: 2026-03-30T00:30:34Z
---

# Design a style-update flow for automatic prompt updates

_Issue: #180 (https://github.com/johnynek/mergexo/issues/180)_

## Summary

Add a label-triggered `style_update` direct flow that mines recent merged agent PR feedback, updates `repo.coding_guidelines_path`, and opens a normal PR only when repeated reviewer guidance warrants a durable prompt-doc change.

## Context

MergeXO's code-producing flows already pass `repo.coding_guidelines_path` into the `bugfix`, `small_job`, and `implementation` prompts. That file is the repo-specific prompt surface that can change future agent behavior without editing MergeXO's built-in prompt templates.

Issue #180 asks for a new one-step workflow that mines recent PR review history, finds repeated maintainer feedback given to MergeXO-authored PRs, and proposes concrete updates to that style guide. The desired UX is an issue label, similar to `agent:bugfix` or `agent:small-job`, that opens a PR with the prompt-doc updates.

## Problem

Today, repeated reviewer guidance is trapped in historical PR comments:
- Maintainers restate the same feedback across multiple agent-authored PRs.
- The durable lesson often never makes it back into `coding_guidelines_path`.
- A manual `small_job` can edit the style guide, but it does not deterministically gather the last `N` relevant PRs, filter noise, or explain why a guideline change is justified.
- When there is no durable pattern, the current direct-flow contract has no clean no-op outcome.

## Goals

1. Add a dedicated `style_update` issue flow for prompt-doc maintenance.
2. Gather the most recent merged MergeXO-authored PR feedback in a deterministic, bounded way.
3. Update `repo.coding_guidelines_path` from that feedback and open a normal reviewable PR.
4. Preserve the existing direct-flow lifecycle: issue status comments, saved author session, pre-PR follow-up recovery, push handling, and post-PR feedback continuity.
5. Support a caller-controlled review window size without requiring code changes.
6. Allow a graceful no-op outcome when the corpus does not support a durable style-guide change.

## Non-goals

1. Automatically editing repository code or tests beyond what is needed to create or update the style-guide document.
2. Mining human-authored PRs, open PRs, or unmerged PRs.
3. Updating MergeXO's built-in prompt text in `src/mergexo/prompts.py` for target repositories; this flow updates the repo-supplied guidance file instead.
4. Replacing normal human PR review after the style-update PR is opened.
5. Solving generalized historical analytics for every workflow; this issue is specific to prompt/style-guide maintenance.

## Proposed Architecture

### Triggering and config

Add a new repo label config:
- `repo.style_update_label`, default `agent:style-update`.

Add a new repo window config:
- `repo.style_update_recent_pr_limit`, integer `>= 1`, default `20`.

Add a deterministic per-issue override:
- Parse an optional `recent_pr_limit: <int>` line from the issue body.
- The override is bounded to `1..50` by code so the GitHub scan and prompt stay bounded.
- Invalid override syntax blocks the run with a clear issue comment instead of silently falling back.

Flow precedence becomes:
1. `ignore_label`
2. `roadmap_label`
3. `bugfix_label`
4. `small_job_label`
5. `style_update_label`
6. `trigger_label`

This keeps existing urgent code flows ahead of style maintenance while still ensuring a style-update issue does not accidentally fall through to design-doc flow.

Internal flow details:
- internal flow name: `style_update`
- branch prefix: `agent/style/<issue_number>-<slug>`
- PR body footer: `Fixes #<issue_number>`

The update target is always `repo.coding_guidelines_path`:
- If the configured path resolves outside the checkout, the flow blocks because the resulting change would not be reviewable in a PR.
- If the path resolves inside the checkout and the file exists, its current contents are included in the prompt.
- If the path resolves inside the checkout but the file is missing, the agent may create it at that exact path.

### Review corpus collection

Add a GitHub gateway helper that lists recent closed PRs for the repo's default branch and returns enough metadata to filter locally:
- PR number, title, body, URL
- head ref and base ref
- merged timestamp
- author login

The orchestrator then builds the style-update corpus by scanning newest-first until it finds `N` eligible merged PRs or hits a fixed scan budget. Eligible PRs are:
- merged into `repo.default_branch`
- opened from MergeXO-managed code-producing branches
- branch prefixes limited to `agent/bugfix/`, `agent/small/`, and `agent/impl/`

For each selected PR, reuse existing GitHub reads:
- `list_pull_request_files`
- `list_pull_request_review_comments`
- `list_pull_request_review_summaries`
- `list_pull_request_issue_comments`

Filter the raw feedback before it reaches the prompt:
- exclude bot authors
- exclude authors not in `repo.allowed_users`
- exclude MergeXO action-token/status noise
- keep only review summaries, inline review comments, and PR-thread issue comments
- truncate very long comment bodies and cap total prompt corpus size to a fixed internal budget

The rendered corpus should preserve evidence, not just conclusions. Each PR entry should include:
- PR number/title/URL
- changed files
- timestamp ordering
- normalized reviewer comment excerpts with author, location, and source URL when available

If the scan finds no eligible reviewed PRs, or only noise after filtering, the flow should end with a no-op issue comment and no PR.

### Agent contract

Add a dedicated prompt and adapter entrypoint:
- `build_style_update_prompt(...)` in `src/mergexo/prompts.py`
- `start_style_update_from_issue(...)` on `AgentAdapter` and `CodexAdapter`

Prompt inputs:
- issue title/body and requested PR window
- repo/default-branch context
- resolved `coding_guidelines_path`
- current style-guide contents when present
- structured review corpus built from recent merged PRs

Prompt rules:
- extract only durable, future-facing guidance
- prefer patterns that appear in multiple review signals, ideally across multiple PRs
- do not encode one-off bug explanations or issue-specific implementation details as global style rules
- keep edits focused on `coding_guidelines_path` unless a tiny adjacent doc change is required to keep links or references correct
- include motivating PR numbers in `pr_summary` so reviewers can audit the evidence

Extend the direct-turn result contract with an optional `no_change_reason` field:
- `no_change_reason` means the corpus did not justify a durable doc update
- `blocked_reason` remains for operational blockers such as invalid config, unreadable repository state, or insufficient checkout safety
- existing `bugfix`, `small_job`, and `implementation` flows keep `no_change_reason = null`

### Orchestration sequence

`style_update` reuses the direct-flow skeleton wherever possible:

1. Claim the issue run, post the standard start comment, prepare the checkout, and create/reset `agent/style/...`.
2. Resolve the target style-guide path and requested PR window.
3. Collect the recent merged-PR feedback corpus.
4. Invoke `start_style_update_from_issue(...)` with the corpus and current guide contents.
5. Save the returned author session exactly as other direct flows do.
6. If `no_change_reason` is set, post a deterministic issue comment explaining that no durable update was found and record the run as completed without opening a PR.
7. If `blocked_reason` is set, use the existing blocked pre-PR path and issue comment behavior.
8. If edits exist, commit them, run `required_tests` when configured, reuse the existing required-test repair loop, reuse the existing push/merge-conflict repair path, run the pre-PR ordering gate, and open the PR.
9. After PR creation, let the normal feedback loop continue from the saved author session.

This keeps `style_update` aligned with the rest of MergeXO instead of inventing a separate workflow engine.

### State and feedback continuity

No new sqlite tables are required.

Required state changes are small and additive:
- extend `IssueFlow`, `PrePrFlow`, and `PrePrFollowupFlow` literals to include `style_update`
- teach branch parsing, recovery helpers, and start-comment rendering about the new flow
- add a `completed without PR` helper in `StateStore`, or generalize `mark_completed`, so a style-update no-op is recorded as `completed` rather than `failed` or `blocked`
- keep using the existing `agent_sessions` table so PR feedback resumes the same author thread

Because the post-PR feedback prompt is already generic, no specialized feedback-loop prompt is required for style-update PRs.

## Implementation Plan

1. Extend config parsing and docs for `style_update_label` and `style_update_recent_pr_limit`, including README and `mergexo.toml.example`.
2. Extend flow enums, label precedence, branch naming, recovery helpers, and start-comment rendering to recognize `style_update`.
3. Add a GitHub gateway method for recent merged PR discovery, plus model types for recent-PR metadata if needed.
4. Build orchestrator helpers to parse `recent_pr_limit`, collect the recent merged-PR corpus, filter noise, and render a bounded prompt section.
5. Add `build_style_update_prompt(...)` and tests that verify the prompt includes the target path, current guide contents, and corpus instructions.
6. Add `start_style_update_from_issue(...)` to the adapter layer and implement Codex JSON parsing for `no_change_reason`.
7. Reuse the existing direct-flow commit/test/push/PR machinery, adding only the no-op completion path that skips commit/PR creation.
8. Add end-to-end orchestrator tests for routing, corpus selection, noise filtering, no-op completion, guide creation/update, and post-PR feedback continuity.

## Acceptance Criteria

1. An open issue labeled `repo.style_update_label` is routed to a new `style_update` flow and uses an `agent/style/...` branch.
2. The flow updates only `repo.coding_guidelines_path`; paths outside the checkout are rejected with a clear issue comment.
3. `repo.style_update_recent_pr_limit` defaults the review window, and an issue-body line `recent_pr_limit: <N>` overrides it within the allowed bounds.
4. The collected corpus includes only recent merged MergeXO-authored PRs from `bugfix`, `small_job`, and `implementation` branches on the default branch.
5. Reviewer corpus filtering excludes bots, unauthorized authors, MergeXO tokens, and status noise while preserving useful review summaries, inline comments, and PR-thread comments.
6. The style-update prompt includes the current style-guide contents when present, or instructs the agent to create the file at the configured path when it is missing inside the repo.
7. When the corpus supports durable guidance updates, MergeXO opens a PR whose summary cites the motivating PRs and whose saved session can later handle PR review feedback normally.
8. When the corpus does not support a durable update, MergeXO posts a no-update issue comment, records the run as completed, and does not open a PR.
9. Existing `design_doc`, `roadmap`, `bugfix`, `small_job`, `implementation`, and feedback-loop behavior is unchanged when the style-update label is absent.
10. Tests cover flow routing, request-window parsing, recent-PR discovery, feedback filtering, no-op completion, normal PR creation, and follow-up/feedback continuity.

## Risks and Mitigations

1. Risk: the agent overfits to one-off review comments.
Mitigation: restrict the corpus to recent merged agent PRs, require repeated evidence in the prompt, and keep the result on the normal human-review path.

2. Risk: GitHub history collection becomes expensive or prompt-heavy.
Mitigation: default to a small recent-PR window, cap override size, stop scanning after a fixed budget, and truncate comment excerpts.

3. Risk: the flow edits the wrong document or an out-of-repo path.
Mitigation: bind the flow strictly to `repo.coding_guidelines_path` and reject paths that resolve outside the checkout.

4. Risk: `repo.allowed_users` is not the right proxy for trusted reviewers in some repos.
Mitigation: reuse the existing allowlist in v1 for consistency, and treat broader reviewer-selection controls as follow-up work only if needed.

5. Risk: graceful no-op handling adds edge cases to direct-flow bookkeeping.
Mitigation: implement it as a small additive completion path instead of a separate workflow family or a new state table.

## Rollout Notes

- This feature is naturally opt-in because it only runs when a maintainer applies `repo.style_update_label`.
- Start with one canary repo and keep the default review window small so the first few PRs are easy to audit.
- Dogfood it in `johnynek/mergexo` against `docs/python_style.md` before advertising it broadly.
- After a few successful runs, document recommended issue-body syntax and reviewer expectations in the README.
