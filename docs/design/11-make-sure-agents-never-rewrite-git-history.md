---
issue: 11
priority: 3
touch_paths:
  - src/mergexo/orchestrator.py
  - src/mergexo/git_ops.py
  - src/mergexo/github_gateway.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/prompts.py
  - tests/test_orchestrator.py
  - tests/test_git_ops.py
  - tests/test_github_gateway.py
  - tests/test_feedback_loop.py
  - tests/test_models_and_prompts.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T02:25:43Z
---

# Enforce Linear PR History in Agent Feedback Turns

_Issue: #11 (https://github.com/johnynek/mergexo/issues/11)_

## Summary

Add deterministic history-guard checks so feedback agents can only append commits to the current PR head, never rewrite existing PR commit history via rebase/amend/reset/force-push behavior.

# Make sure agents never rewrite git history

## Summary
Enforce a linear-history invariant for tracked PR branches in the feedback loop. The system will detect and block any non-fast-forward history transition, whether it happens locally during an agent turn or remotely between polling cycles. Prompt guidance will be strengthened, but correctness will come from deterministic git/GitHub checks in orchestrator code.

## Context
Today, feedback agents run inside a real checkout and can execute arbitrary edits before returning structured output. We already verify that local checkout head matches PR head before agent execution, but we do not enforce that head evolution remains append-only after the agent runs.

If an agent rebases, amends, resets, or force-pushes, PR commit history can be rewritten. That breaks incremental review in GitHub UI and makes review context harder to follow.

## Goals
1. Guarantee that MergeXO never accepts agent turns that rewrite existing PR history.
2. Keep PR history append-only: old head must remain an ancestor of new head.
3. Detect rewrites both within a turn and across polling cycles.
4. Fail safely: block automation on violation and notify humans.
5. Preserve existing happy-path behavior for normal append-only commits.

## Non-goals
1. Preventing maintainers from manually rewriting history outside MergeXO.
2. Automatically repairing rewritten history.
3. Building a full manual-unblock CLI in this issue.

## Proposed architecture

## 1. Linear-history invariant
For every tracked PR, define `H_prev` as last accepted head SHA and `H_now` as current head SHA.

Allowed transitions:
1. `H_prev == H_now`.
2. `H_prev` is an ancestor of `H_now`.

Disallowed transitions:
1. `H_now` is behind `H_prev`.
2. `H_prev` and `H_now` diverged.

A disallowed transition is treated as history rewrite.

## 2. Enforcement layers
### 2.1 Prompt-level prevention (soft guard)
Update feedback prompt rules in `src/mergexo/prompts.py`:
1. Explicitly forbid `git rebase`, `git commit --amend`, `git reset`, `git push --force`, and `git push --force-with-lease`.
2. Require additive commits only.
3. If rewrite seems required, ask for human guidance via `general_comment` and do not request commit.

This reduces violations but is not trusted as enforcement.

### 2.2 Local lineage guard (hard guard)
Add ancestry helpers in `src/mergexo/git_ops.py`:
1. `is_ancestor(checkout_path, older_sha, newer_sha) -> bool` using `git merge-base` semantics.
2. Optional fetch helper for branch refresh before ancestry checks.

In `Phase1Orchestrator._process_feedback_turn`:
1. Capture `turn_start_head = pr.head_sha` before calling agent.
2. After agent returns, compute local `HEAD`.
3. If `turn_start_head` is not ancestor of local `HEAD`, treat as local rewrite attempt.
4. On violation: do not commit, do not push, mark PR blocked, post one operator-facing comment.

### 2.3 Remote lineage guard (hard guard)
Add commit comparison support in `src/mergexo/github_gateway.py`:
1. `compare_commits(base_sha, head_sha)` backed by GitHub compare API.
2. Normalize status to `ahead`, `identical`, `behind`, `diverged`.

Use it in orchestrator at two points:
1. Start-of-turn drift check: if `tracked.last_seen_head_sha` exists and differs from `pr.head_sha`, require `last_seen -> current` to be `ahead` or `identical`; otherwise block as rewrite detected between cycles.
2. Pre-finalize check: refresh PR snapshot before finalization and require `turn_start_head -> refreshed_head` to be `ahead` or `identical`. If not, block.

This catches force-push/rewrite even if it happened outside the exact local checkout state.

### 2.4 Violation handling and idempotency
On rewrite detection:
1. Mark PR state as `blocked` with explicit reason in `issue_runs.error` via existing `mark_pr_status` path.
2. Post one comment to the PR explaining block reason and expected remediation.
3. Reuse action-token pattern (`src/mergexo/feedback_loop.py`) for violation comments to avoid duplicate spam on retry/crash windows.
4. Leave pending feedback events unprocessed so manual recovery can replay context if unblocked later.

## Implementation plan
1. Add git ancestry helper methods in `src/mergexo/git_ops.py` and unit tests in `tests/test_git_ops.py`.
2. Add GitHub compare wrapper in `src/mergexo/github_gateway.py` and parsing tests in `tests/test_github_gateway.py`.
3. Add violation token helper in `src/mergexo/feedback_loop.py` with tests in `tests/test_feedback_loop.py`.
4. Update `src/mergexo/orchestrator.py` feedback-turn flow:
1. Validate cross-cycle head transition.
2. Validate local post-agent lineage.
3. Validate pre-finalize remote transition.
4. Block and comment on violations.
5. Update feedback prompt text in `src/mergexo/prompts.py` and assertions in `tests/test_models_and_prompts.py`.
6. Expand orchestrator tests in `tests/test_orchestrator.py` for local rewrite, remote rewrite, and allowed fast-forward drift paths.

## Testing plan
1. `tests/test_git_ops.py`: ancestry helper returns true for ancestor/equal and false for non-ancestor.
2. `tests/test_github_gateway.py`: compare endpoint parsing and error handling.
3. `tests/test_orchestrator.py`: block when agent rewrites local history in a turn.
4. `tests/test_orchestrator.py`: block when remote head transition is `behind` or `diverged` relative to last seen head.
5. `tests/test_orchestrator.py`: allow `ahead` remote transition and continue/retry safely.
6. `tests/test_orchestrator.py`: normal append-only feedback commit still commits, pushes, and finalizes.
7. `tests/test_models_and_prompts.py`: feedback prompt includes explicit no-history-rewrite contract.

## Acceptance criteria
1. If an agent rebases/amends/resets so local post-turn `HEAD` no longer descends from PR head at turn start, MergeXO blocks the PR and performs no push.
2. If PR head changed since last seen and transition is non-fast-forward (`behind` or `diverged`), MergeXO blocks the PR.
3. If PR head changed by fast-forward (`ahead`), MergeXO does not block and continues safely on the latest head.
4. Normal additive feedback commits continue to work and produce incremental PR history.
5. Violation comments are idempotent and not duplicated across retries/crashes.
6. Feedback prompt explicitly instructs agents to avoid history-rewrite commands.
7. Existing non-feedback issue-to-design flow remains unchanged.

## Risks and mitigations
1. Risk: false positives from transient API/git inconsistency.
Mitigation: refresh PR snapshot before decisive checks; only block on explicit non-fast-forward classification.

2. Risk: blocked PRs require manual intervention.
Mitigation: post clear remediation comment and preserve pending events for replay after manual unblocking.

3. Risk: extra API calls add latency.
Mitigation: comparisons happen only on active feedback turns and add low overhead relative to agent turn time.

4. Risk: maintainers intentionally rewrite history and still want automation.
Mitigation: this is an intentional safety policy; follow-up can add explicit operator override workflow.

## Rollout notes
1. Ship with enforcement enabled whenever `runtime.enable_feedback_loop` is enabled.
2. Roll out to one repository first and monitor blocked-turn rate.
3. Verify that blocked events include clear reasons in state and PR comments.
4. After canary confidence, enable broadly with no behavior change to append-only workflows.
