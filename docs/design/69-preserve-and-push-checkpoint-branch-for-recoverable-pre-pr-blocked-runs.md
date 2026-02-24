---
issue: 69
priority: 3
touch_paths:
  - docs/design/69-preserve-and-push-checkpoint-branch-for-recoverable-pre-pr-blocked-runs.md
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/git_ops.py
  - src/mergexo/feedback_loop.py
  - README.md
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_git_ops.py
  - tests/test_feedback_loop.py
depends_on: []
estimated_size: M
generated_at: 2026-02-24T18:46:54Z
---

# Preserve and push checkpoint branch for recoverable pre-PR blocked runs

_Issue: #69 (https://github.com/johnynek/mergexo/issues/69)_

## Summary

Design to persist recoverable pre-PR blocked work by checkpointing and pushing the branch before cleanup, publishing detailed checkpoint status on the source issue, and resuming retries from that checkpoint branch head.

---
issue: 69
priority: 2
touch_paths:
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/git_ops.py
  - src/mergexo/feedback_loop.py
  - README.md
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_git_ops.py
  - tests/test_feedback_loop.py
depends_on: [51]
estimated_size: M
generated_at: 2026-02-24T00:00:00Z
---

# Preserve and push checkpoint branch for recoverable pre-PR blocked runs

_Issue: #69 (https://github.com/johnynek/mergexo/issues/69)_

## Summary
When a pre-PR run ends in a recoverable blocked state, persist the exact working tree to the remote branch before worker cleanup, emit a structured source-issue checkpoint comment, and resume follow-up retries from that checkpointed branch head instead of rebuilding from a clean default-branch checkout.

## Context
Current behavior:
1. Pre-PR recoverable failures are routed to `awaiting_issue_followup`.
2. Worker cleanup resets and cleans the checkout.
3. If no commit was pushed before the block, the in-progress tree is lost.
4. Follow-up retries can recreate branch names but may effectively restart from default branch content.

Consequence: users cannot inspect or comment on the exact blocked tree in GitHub, and retries lose momentum.

## Goals
1. Guarantee a recoverable pre-PR blocked outcome leaves a pushed remote checkpoint branch and commit.
2. Publish one detailed source-issue status comment with blocked reason and checkpoint references.
3. Resume comment-driven follow-up retries from checkpoint branch head.
4. Keep behavior scoped to pre-PR flows only.
5. Add a durable last-checkpoint SHA marker in state for observability.

## Non-goals
1. Changing PR feedback-loop semantics for existing PRs.
2. Introducing new operator commands.
3. Redesigning branch naming strategy.
4. Replacing existing recoverable and non-recoverable error classification.

## Proposed Architecture

### 1. Pre-cleanup checkpoint capture in pre-PR workers
For pre-PR flows (`design_doc`, `bugfix`, `small_job`, `implementation`), add a recoverable-blocked interception path before `cleanup_slot` runs:
1. Detect recoverable blocked exceptions in worker context when `runtime.enable_issue_comment_routing` is enabled.
2. Stage all tracked and untracked changes.
3. Create a checkpoint commit when staged diff exists.
4. Push the flow branch to origin.
5. Resolve final checkpoint SHA from local `HEAD` after push.
6. Raise a typed recoverable exception carrying waiting reason, checkpoint branch, and checkpoint SHA.

This ensures checkpoint persistence happens before checkout reset and clean.

### 2. Durable checkpoint marker in follow-up state
Extend `pre_pr_followup_state` with nullable `last_checkpoint_sha`:
1. Persist `last_checkpoint_sha` alongside existing `flow`, `branch`, `context_json`, and `waiting_reason`.
2. Keep `issue_runs.status='awaiting_issue_followup'` as lifecycle authority.
3. Use additive migration (`ALTER TABLE ... ADD COLUMN`) for existing DBs.

This adds direct observability for the latest recoverable checkpoint.

### 3. Structured source-issue checkpoint comment
After successful checkpoint push, post one source-issue comment containing:
1. blocked reason,
2. checkpoint branch name,
3. checkpoint commit SHA,
4. tree link: `https://github.com/<repo_full_name>/tree/<checkpoint_sha>`,
5. compare link: `https://github.com/<repo_full_name>/compare/<default_branch>...<checkpoint_sha>`,
6. concise next-step instructions for the reporter.

Use an action token keyed by issue and checkpoint SHA to avoid duplicate checkpoint comments on retry and crash replay.

### 4. Resume semantics from checkpoint branch head
On `awaiting_issue_followup` retry enqueue:
1. Continue reading `pre_pr_followup_state.branch` as canonical resume branch.
2. Pass this stored branch explicitly through worker entry points so resume does not depend on recomputed branch naming.
3. Worker branch reset must target `origin/<stored_branch>` when it exists.
4. Follow-up context remains comment-ordered and unchanged from issue #51 behavior.

This guarantees retries continue from the pushed checkpoint branch head.

### 5. Scope guard: pre-PR only
Apply this feature only when no PR has been created for the run:
1. Paths that end in `awaiting_feedback`, `blocked` PR state, `merged`, or `closed` remain unchanged.
2. Existing PR feedback state machine and commands are unaffected.

## Data Model Changes

### `pre_pr_followup_state` table
Add column:
1. `last_checkpoint_sha TEXT NULL`.

### `PrePrFollowupState` dataclass and APIs
Update:
1. dataclass field `last_checkpoint_sha: str | None`,
2. `mark_awaiting_issue_followup(..., last_checkpoint_sha: str | None = None)`,
3. `list_pre_pr_followups()` selection and parsing to include the new column.

No destructive migration is required.

## Orchestrator Changes

### New recoverable checkpoint exception
Add a dedicated exception type for recoverable pre-PR blocked runs that includes checkpoint metadata:
1. `waiting_reason`,
2. `checkpoint_branch`,
3. `checkpoint_sha`.

### Worker checkpoint helper
Add helper(s) in orchestrator that:
1. stage and optionally commit local changes for checkpointing,
2. push checkpoint branch,
3. compute final SHA,
4. render and post structured checkpoint status comment.

### Reaper integration
In `_reap_finished`:
1. detect typed checkpointed recoverable exception,
2. write `awaiting_issue_followup` with `last_checkpoint_sha`,
3. preserve current cursor advancement behavior for consumed source comments.

### Follow-up resume plumbing
Update follow-up enqueue and worker call signatures to carry stored branch explicitly so retries always restore from checkpoint branch state. A follow-up retry is triggered only by qualifying source-issue comments from allowed users (consistent with `repo.allowed_users` and existing non-bot filtering).

## Git and Comment Helper Changes

### `git_ops.py`
Add a focused helper for checkpoint persistence, for example:
1. stage changes,
2. commit only when there is diff,
3. push branch,
4. return final head SHA.

### `feedback_loop.py`
Add token helper for checkpoint comments, for example:
1. `compute_pre_pr_checkpoint_token(issue_number, checkpoint_sha)`.

## Failure Handling Policy
If checkpoint persistence fails:
1. do not silently continue as recoverable follow-up state,
2. mark run as failed with explicit checkpoint persistence error details,
3. include enough detail in logs and issue comment for manual recovery.

This avoids false "recoverable" state that cannot actually resume from a checkpoint.

## Implementation Plan
1. Extend state schema and `PrePrFollowupState` with `last_checkpoint_sha` plus additive migration.
2. Add checkpoint comment token helper in `feedback_loop.py`.
3. Add orchestrator checkpoint helper and typed recoverable checkpoint exception.
4. Intercept recoverable pre-PR blocked outcomes in worker paths before cleanup to checkpoint and push.
5. Post structured checkpoint status comment with branch, SHA, tree, compare, and next steps.
6. Persist checkpoint SHA through `_reap_finished -> mark_awaiting_issue_followup`.
7. Plumb stored branch through follow-up worker submission and worker execution for deterministic resume.
8. Update README section on source-issue routing to document checkpoint behavior and links.
9. Add or extend tests in state, orchestrator, git ops, and feedback-loop modules.

## Testing Plan
1. Validate `pre_pr_followup_state` migration adds `last_checkpoint_sha` for existing DBs.
2. Validate `mark_awaiting_issue_followup` persists and overwrites checkpoint SHA correctly.
3. Validate git checkpoint helper commits when diff exists.
4. Validate git checkpoint helper skips commit when clean but still pushes and returns SHA.
5. Validate git checkpoint helper surfaces push failures.
6. Validate checkpoint token stability and uniqueness.
7. Validate recoverable pre-PR blocked worker path checkpoints before cleanup.
8. Validate checkpoint comment includes blocked reason, branch, SHA, tree, compare, and instructions.
9. Validate `_reap_finished` stores `awaiting_issue_followup` with `last_checkpoint_sha`.
10. Validate follow-up retry uses stored branch for resume and does not restart from default branch.
11. Validate PR feedback-loop behavior remains unchanged.

## Acceptance Criteria
1. A recoverable pre-PR blocked run leaves a pushed branch and checkpoint commit visible on GitHub.
2. Source issue receives a detailed checkpoint status comment with blocked reason, branch, SHA, tree link, compare link, and next-step guidance.
3. A follow-up retry triggered from source-issue comments resumes from checkpoint branch head.
4. Existing `awaiting_issue_followup` routing and comment-cursor behavior continue to work.
5. Worker cleanup no longer causes data loss for recoverable pre-PR blocked outcomes.
6. State exposes the latest checkpoint SHA for each `awaiting_issue_followup` row.

## Risks and Mitigations
1. Risk: checkpoint push can fail due auth, network, or branch race.
Mitigation: explicit error path, clear logs, no silent recoverable transition without checkpoint.
2. Risk: duplicate checkpoint comments across retries or crash windows.
Mitigation: tokenized idempotent checkpoint comments.
3. Risk: schema drift on existing state DBs.
Mitigation: additive migration guarded by column-existence checks.
4. Risk: branch mismatch on resume if branch is recomputed from mutable issue title.
Mitigation: pass stored branch explicitly through follow-up worker execution.
5. Risk: unresolved merge state may prevent committing all local changes.
Mitigation: best-effort checkpoint of committed HEAD, explicit warning or error path, and clear operator visibility.

## Rollout Notes
1. No new feature flag is required; behavior is active only when `runtime.enable_issue_comment_routing = true`.
2. Roll out first on one repository with issue-comment routing enabled.
3. Run canary scenarios for blocked-before-PR with unpushed changes, checkpoint link inspection, and comment-driven resume from checkpoint branch.
4. Verify repeated blocked retries update checkpoint SHA while keeping checkpoint comment posting idempotent.
5. Monitor verbose events and `pre_pr_followup_state.last_checkpoint_sha` for correctness.
6. After canary stability, keep as default behavior for pre-PR recoverable blocked flows under issue-comment routing.
