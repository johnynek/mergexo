---
issue: 51
priority: 3
touch_paths:
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/config.py
  - mergexo.toml.example
  - README.md
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_feedback_loop.py
  - tests/test_config.py
depends_on: []
estimated_size: M
generated_at: 2026-02-23T19:35:56Z
---

# Handle blocked issues before PR creation

_Issue: #51 (https://github.com/johnynek/mergexo/issues/51)_

## Summary

Introduce a recoverable pre-PR follow-up loop that queues source-issue comments, retries work before opening a PR, and redirects source-issue comments to the PR once a PR exists. This removes the current terminal failed-before-PR corner case and documents clear user behavior in README.

## Context
Today, direct flows can end before PR creation with a blocked-style outcome (for example, environment limits or required test constraints). The orchestrator posts a comment on the source issue, raises, and the run is persisted as `failed`.

That causes three problems:
1. The issue has no PR, so normal PR feedback automation cannot run.
2. Replies on the source issue are not routed back to agent execution.
3. Existing blocked-before-PR incidents are effectively dead-ended unless an operator intervenes manually.

Issue #51 asks for a robust, documented behavior where comments on the issue before PR creation are actionable, queued if needed, and processed before a PR is opened. It also asks for explicit behavior after PR creation.

## Goals
1. Eliminate terminal pre-PR dead-end behavior for recoverable blocked outcomes.
2. Make source-issue comments actionable before PR creation.
3. Queue comments made while an issue run is active and consume them after that run completes.
4. Ensure comments made on the source issue are not silently ignored after a PR exists.
5. Keep idempotent, crash-safe behavior consistent with existing feedback token patterns.
6. Add explicit README usage guidance for before-PR vs after-PR comment paths.

## Non-goals
1. Redesigning the PR feedback loop itself.
2. Webhook migration (polling remains the trigger source).
3. New operator command grammar for this flow.
4. Perfect semantic intent parsing of issue comments.

## Proposed Architecture
### 1. Pre-PR lifecycle state
Add a recoverable issue state for pre-PR waiting:
1. `running` -> `awaiting_issue_followup` for recoverable pre-PR blocks.
2. `awaiting_issue_followup` -> `running` when queued source-issue comments are available.
3. `running` -> `awaiting_feedback` when PR is created.

`awaiting_issue_followup` replaces terminal handling for expected pre-PR block scenarios (agent `blocked_reason`, repeated required-test impossibility, and similar recoverable constraints).

### 2. Durable state additions
Keep `issue_runs` as the top-level lifecycle row and add dedicated pre-PR follow-up metadata:

`pre_pr_followup_state`
1. `repo_full_name` TEXT NOT NULL
2. `issue_number` INTEGER NOT NULL
3. `flow` TEXT NOT NULL (`design_doc`, `bugfix`, `small_job`, `implementation`)
4. `branch` TEXT NOT NULL
5. `context_json` TEXT NOT NULL (flow-specific resume context)
6. `waiting_reason` TEXT NOT NULL
7. `updated_at` TEXT NOT NULL
8. Primary key `(repo_full_name, issue_number)`

`issue_comment_cursors`
1. `repo_full_name` TEXT NOT NULL
2. `issue_number` INTEGER NOT NULL
3. `pre_pr_last_consumed_comment_id` INTEGER NOT NULL DEFAULT 0
4. `post_pr_last_redirected_comment_id` INTEGER NOT NULL DEFAULT 0
5. `updated_at` TEXT NOT NULL
6. Primary key `(repo_full_name, issue_number)`

No destructive migration is required. New tables are additive.

### 3. Queue semantics before PR creation
Queueing is comment-id based against source issue comments:
1. At run start, record `run_start_comment_id` as the current max source-issue comment id.
2. If run ends in recoverable pre-PR waiting, persist `awaiting_issue_followup` plus `run_start_comment_id` in cursor state.
3. Pending follow-ups are comments with `comment_id > pre_pr_last_consumed_comment_id`, filtered by:
- non-bot author
- allowlisted author
- no MergeXO action token marker
4. Follow-ups are consumed in ascending `comment_id` order, batched per retry turn.
5. Comments posted while a retry is active remain queued and are picked up next turn.

### 4. Retry execution flow
Add a dedicated orchestrator enqueue path for pre-PR follow-ups:
1. Query issues in `awaiting_issue_followup`.
2. Fetch source-issue comments and materialize pending follow-up batch from cursor.
3. If batch is empty, do not enqueue.
4. If batch exists, enqueue one retry worker for that issue.

Retry worker behavior:
1. Build augmented issue context by appending:
- previous waiting reason
- ordered follow-up comments (author, URL, timestamp, body)
2. Re-run the same flow indicated by `pre_pr_followup_state.flow`.
3. On completion (success or another recoverable wait), advance `pre_pr_last_consumed_comment_id` to the max consumed id.
4. On successful PR creation, clear `pre_pr_followup_state` row.

Pre-PR ordering gate:
1. Right before PR creation, run one final source-issue comment check.
2. If new pending follow-up comments exist, do not open PR yet.
3. Transition back to `awaiting_issue_followup` with reason `new_issue_comments_pending`.

This enforces that issue comments made before PR creation are processed first.

### 5. Source-issue routing after PR creation
After a PR exists for an issue, source-issue comments are no longer fed to pre-PR retries.

Routing behavior:
1. For issue rows with `pr_number` and open feedback states (`awaiting_feedback`, PR-level `blocked`), scan source-issue comments with `comment_id > post_pr_last_redirected_comment_id`.
2. For each qualifying comment, post one deterministic redirect reply on the source issue:
- includes PR number and URL
- states that comments on the issue are no longer actioned
- instructs user to comment on the PR thread
3. Use tokenized idempotency (`compute_source_issue_redirect_token`) so retries/restarts do not duplicate replies.
4. Advance `post_pr_last_redirected_comment_id` only after redirect token reconciliation.

### 6. Backward compatibility for already-failed rows
Adopt historical pre-PR blocked failures so existing incidents become recoverable:
1. During state init/startup, detect legacy `issue_runs` rows with:
- `status = failed`
- `pr_number IS NULL`
- known pre-PR blocked error signatures
2. Convert to `awaiting_issue_followup` and seed `pre_pr_followup_state`.
3. This allows a new issue comment to restart work without manual DB edits.

### 7. Observability
Add structured events:
1. `pre_pr_followup_waiting`
2. `pre_pr_followup_comments_detected`
3. `pre_pr_followup_enqueued`
4. `pre_pr_followup_resumed`
5. `pre_pr_followup_consumed`
6. `source_issue_comment_redirected`

Include `repo_full_name`, `issue_number`, optional `pr_number`, and comment id ranges.

## Implementation Plan
1. Add runtime flag `runtime.enable_issue_comment_routing` in config parsing and sample config.
2. Add schema tables and APIs in `StateStore` for pre-PR follow-up metadata and comment cursors.
3. Add helper token function in `feedback_loop.py` for source-issue redirect replies.
4. Refactor orchestrator pre-PR block handling:
- classify recoverable pre-PR wait outcomes
- persist `awaiting_issue_followup` instead of terminal `failed`
- persist/advance comment cursors
5. Add pre-PR follow-up enqueue/worker path in orchestrator.
6. Add pre-PR ordering gate before PR creation.
7. Add post-PR source-issue redirect scan path in orchestrator.
8. Add legacy row adoption logic for historical failed-before-PR incidents.
9. Update README with explicit operator/user workflow.
10. Expand tests across orchestrator, state, feedback helpers, and config.

## README Usage Plan
Add a dedicated section describing:
1. Before PR exists: reply on the issue thread to unblock/resume; comments are queued and consumed.
2. While worker is active: new comments are queued for the next retry turn.
3. After PR exists: issue comments get a redirect response and are not actioned; comment on the PR instead.
4. Recovery note for legacy blocked-before-PR incidents after upgrade.
5. Feature flag enablement and rollout recommendation.

## Acceptance Criteria
1. A recoverable pre-PR block no longer ends as terminal `failed`; it transitions to `awaiting_issue_followup`.
2. A comment on a pre-PR waiting issue triggers exactly one follow-up retry enqueue.
3. Multiple comments are consumed in order and passed to agent context in the next retry.
4. Comments posted during an active retry are queued and consumed in a subsequent retry.
5. If new source-issue comments appear before PR creation, PR creation is deferred until those comments are processed.
6. Once PR exists, new source-issue comments receive one deterministic redirect message with the PR link.
7. Source-issue comments after PR creation are not passed into pre-PR retry context.
8. Redirect replies are idempotent across process restarts (no duplicate redirect for same comment).
9. Unauthorized or bot comments do not trigger retries and do not trigger redirects.
10. Existing PR feedback loop behavior remains unchanged.
11. Legacy failed-before-PR rows matching blocked signatures become retryable after upgrade.
12. README documents before-PR and after-PR comment behavior with concrete examples.

## Risks and Mitigations
1. Risk: extra GitHub API load from source-issue comment polling.
Mitigation: poll only tracked issues with relevant states and use cursor-based incremental scans.

2. Risk: noisy retries from low-signal comments.
Mitigation: batch comments per retry, keep one in-flight retry per issue, and log reason/comment counts for tuning.

3. Risk: cursor bugs can skip or replay comments.
Mitigation: deterministic ordering by comment id, transactional cursor updates, idempotent token checks.

4. Risk: legacy failure classification misses some historical rows.
Mitigation: explicit signature set plus conservative fallback to leave unknown failures untouched.

5. Risk: user confusion about where to comment after PR creation.
Mitigation: automatic redirect replies and clear README documentation.

## Rollout Notes
1. Ship behind `runtime.enable_issue_comment_routing = false` by default.
2. Land schema and tests first, then enable on one repository.
3. Verify with canary scenarios:
- blocked before PR, then comment, then retry
- comment during active run
- comment on source issue after PR open
4. Enable legacy failed-row adoption in canary and verify known incidents recover.
5. Turn feature on by default after stable canary window and no duplicate redirect incidents.
6. Keep existing manual/operator recovery paths as temporary fallback during rollout.
