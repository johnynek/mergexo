---
issue: 2
status: implemented
priority: 3
touch_paths:
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/github_gateway.py
  - src/mergexo/git_ops.py
  - src/mergexo/models.py
  - src/mergexo/prompts.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/config.py
  - src/mergexo/feedback_loop.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_github_gateway.py
  - tests/test_codex_adapter.py
  - tests/test_feedback_loop.py
depends_on: []
estimated_size: M
generated_at: 2026-02-21T02:31:29Z
implemented_at: 2026-02-21T02:47:27Z
---

# Design the feedback loop

_Issue: #2 (https://github.com/johnynek/mergexo/issues/2)_

## Summary

Implement a crash safe PR feedback loop that polls review and issue comments, routes turns to the saved agent session and branch, restores git state before execution, and uses deterministic GitHub action tokens so retries do not duplicate replies while GitHub remains source of truth.

# Design the feedback loop

## Context
Today the worker creates a design PR once and stops. PR review comments and PR issue comments are not consumed, so humans cannot iterate with the agent in the same PR lifecycle.

## Goals
1. Collect new feedback from open MergeXO design PRs on every poll.
2. Route each feedback turn to the correct issue, PR branch, and saved agent session.
3. Restore checkout and head state before invoking the agent.
4. Prevent processing the same comment more than once, including restart and crash cases.
5. Keep GitHub as source of truth and SQLite as local cache plus runtime coordinator.
6. Make outbound GitHub writes as atomic as practical.

## Non goals
1. Webhooks.
2. Perfect cross API transactionality.
3. Implementation phase scheduling changes.

## Architecture

### Feedback collection
For each open tracked PR:
1. Read PR snapshot.
2. Read review comments from `pulls/{pr}/comments`.
3. Read PR issue comments from `issues/{pr}/comments`.
4. Read changed files from `pulls/{pr}/files`.
5. Read source issue body for turn context.

Normalize inbound comments to event keys:
`{pr_number}:{kind}:{comment_id}:{updated_at}`
where `kind` is `review` or `issue`.

Persist all seen events in SQLite table `feedback_events` with nullable `processed_at`.
Ignore bot authored comments and comments that already contain MergeXO action tokens.

### Routing to agent state
Routing key is `pr_number`.
1. Resolve `pr_number -> issue_number, branch` from local state.
2. Resolve `issue_number -> adapter, thread_id` from `agent_sessions`.
3. Build `FeedbackTurn` from unprocessed events only.
4. If session is missing, mark run `blocked` and post one operator message.

### Restoring git state before agent call
Before `respond_to_feedback`:
1. Acquire worker slot.
2. Clean checkout to default branch state.
3. Checkout tracked PR branch.
4. Hard reset to `origin/{branch}`.
5. Verify local `HEAD` equals GitHub PR `head_sha`.
6. If still mismatched after one refetch, skip this turn and retry next poll.

### Executing feedback turn
1. Call `agent.respond_to_feedback(session, turn, cwd)`.
2. If `commit_message` exists, commit and push.
3. Refresh PR snapshot after push.
4. Persist updated session handle from adapter result.

### Idempotent outbound actions
Each outbound action appends hidden token marker:
`<!-- mergexo-action:{token} -->`

Deterministic token design:
1. `turn_key = sha256(pr_number + head_sha + sorted(unprocessed_event_keys))`
2. Review reply token: `sha256(turn_key + review_reply + review_comment_id)`
3. General PR comment token: `sha256(turn_key + general_comment)`

Before posting action:
1. Re read remote comments.
2. If token exists remotely, treat action as already applied.
3. Else post action with token marker.

This covers crash window where GitHub write succeeds but SQLite write does not.
Retry recomputes same token and detects existing remote action.

### Finalization and crash handling
Order of operations:
1. Ingest events.
2. Execute git and GitHub side effects with token checks.
3. Confirm all expected tokens exist on GitHub.
4. In one SQLite transaction:
   - mark events processed,
   - update session,
   - update PR head cache.

If crash happens before step 4, events stay unprocessed and are retried safely.
If crash happens mid post, next run posts only missing actions.

### Atomicity model
GitHub does not expose one transaction that can atomically reply to many existing review comments and also post a PR issue comment.
We provide practical atomicity by action group semantics:
1. One feedback turn equals one `turn_key` group.
2. Group is complete only when every expected token is visible on GitHub.
3. Events are marked processed only after group completion.

## Data model changes
Add `feedback_events`:
- `event_key` TEXT PRIMARY KEY
- `pr_number` INTEGER NOT NULL
- `issue_number` INTEGER NOT NULL
- `kind` TEXT NOT NULL
- `comment_id` INTEGER NOT NULL
- `updated_at` TEXT NOT NULL
- `processed_at` TEXT NULL
- `created_at` TEXT NOT NULL default now

Add `pr_feedback_state`:
- `pr_number` INTEGER PRIMARY KEY
- `issue_number` INTEGER NOT NULL
- `branch` TEXT NOT NULL
- `status` TEXT NOT NULL
- `last_seen_head_sha` TEXT NULL
- `updated_at` TEXT NOT NULL default now

Extend `issue_runs.status` with `awaiting_feedback`, `merged`, `closed`, `blocked`.

## Implementation plan
1. Add new state APIs for event ingest, pending event query, and finalization transaction.
2. Extend GitHub gateway with issue fetch and PR state fields needed for merged or closed detection.
3. Add git helper to restore branch and validate expected head SHA.
4. Add feedback loop worker path in orchestrator with per PR running guard.
5. Add token generation and remote token reconciliation helper module.
6. Extend feedback prompt to include `turn_key` for traceability.
7. Add unit and failure injection tests for crash and retry windows.

## Acceptance criteria
1. New review comment on an open tracked PR causes one logical feedback turn.
2. Re polling with no new comments produces no new replies and no new commits.
3. Crash after GitHub post and before SQLite finalize does not create duplicate replies after restart.
4. `processed_at` is set only after all outbound tokens are confirmed on GitHub.
5. Agent is invoked only when local checkout head matches GitHub PR `head_sha`.
6. Feedback routes by `pr_number` to the matching branch and saved session.
7. Merged or closed PR stops receiving scheduled feedback turns.
8. Tests cover dedupe, restart recovery, and head mismatch retry behavior.

## Risks
1. More polling can hit API limits.
Mitigation: poll interval floor, per cycle cap, and fair scheduling.

2. User edits or deletes tokenized bot comments.
Mitigation: reconciliation logs mismatch and requires operator attention.

3. Agent output can differ across retries.
Mitigation: idempotency keys are deterministic and independent from wording.

4. Feedback bursts can starve issue intake.
Mitigation: bounded feedback queue and round robin between new issue work and feedback work.

## Rollout notes
1. Ship behind `runtime.enable_feedback_loop` default false.
2. Land schema first, then enable behavior by flag.
3. Canary with one repository and one worker slot.
4. Add structured logs for `issue_number`, `pr_number`, `turn_key`, `event_key`, `action_token`.
5. Promote to default on after one week without duplicate action incidents.
