---
issue: 117
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_orchestrator.py
  - tests/test_state.py
depends_on: []
estimated_size: M
generated_at: 2026-02-26T21:47:38Z
---

# PR takeover via ignore label

_Issue: #117 (https://github.com/johnynek/mergexo/issues/117)_

## Summary

Add a human-takeover control label (`agent:ignore`) that pauses all automation for an issue/PR, suppresses comment processing while active, and resumes safely after label removal with deterministic comment boundaries.

---
issue: 117
status: proposed
priority: 2
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_orchestrator.py
  - tests/test_state.py
depends_on: []
estimated_size: M
generated_at: 2026-02-26T22:00:00Z
---

# Design a PR takeover story

_Issue: #117 (https://github.com/johnynek/mergexo/issues/117)_

## Summary

Introduce a human-takeover control label (`agent:ignore` by default) that preempts MergeXO automation for a source issue and its linked PR work. While the label is present, MergeXO does not process issue comments, PR comments, CI-triggered feedback, or enqueue new/retry work for that issue. When the label is removed, MergeXO resumes normally but only processes comments newer than stored takeover boundaries, so comments made during takeover are not replayed.

## Context

Today MergeXO has rich automation for:
1. Issue intake and flow routing (`design_doc`, `bugfix`, `small_job`, and implementation follow-up).
2. Pre-PR source-issue follow-up routing.
3. PR feedback automation and CI-triggered remediation.

Issue #117 asks for a natural human override path when the bot is not helping. The requested behavior is label-driven and explicit:
1. A label like `agent:ignore` should preempt automation.
2. Comments during takeover should not be processed.
3. After label removal, new comments should resume processing.
4. README should document the operator flow.

## Goals

1. Provide a single, explicit control surface for human takeover using an issue label.
2. Preempt all automation for that issue while takeover is active.
3. Ensure comments made during takeover are not replayed after resume.
4. Keep behavior crash-safe and restart-safe via persisted state.
5. Document an operator-friendly flow in README.

## Non-goals

1. Hard-canceling already running worker subprocesses mid-turn.
2. Adding a new command grammar for takeover control.
3. Reworking existing feedback token/idempotency design.
4. Replacing current polling architecture with webhooks.

## Proposed architecture

### 1. Config: explicit takeover label

Add a new repo config key:
1. `repo.ignore_label` with default `"agent:ignore"`.

Behavior:
1. If `ignore_label` is present on the source issue, takeover is active.
2. Intake precedence becomes: `ignore_label` preempts `bugfix_label`, `small_job_label`, and `trigger_label`.

This keeps control per repo and avoids hardcoding label names.

### 2. Persisted takeover state and resume boundaries

Add durable state to track whether takeover is currently active for an issue:
1. New table `issue_takeover_state` keyed by `(repo_full_name, issue_number)`.
2. Column `ignore_active` (`0|1`) plus `updated_at`.

Extend PR feedback state for comment-floor boundaries:
1. Add `takeover_review_floor_comment_id` to `pr_feedback_state` (default `0`).
2. Add `takeover_issue_floor_comment_id` to `pr_feedback_state` (default `0`).

Reuse existing issue cursor state for source-issue boundaries:
1. `issue_comment_cursors.pre_pr_last_consumed_comment_id`.
2. `issue_comment_cursors.post_pr_last_redirected_comment_id`.

Boundary model:
1. While takeover is active, MergeXO continuously advances these floors/cursors to latest observed comment IDs.
2. On transition from active to inactive (label removed), MergeXO performs one final boundary snapshot before resuming.
3. Normal processing only accepts comments newer than those floors.

This gives deterministic “do not replay takeover comments” behavior with additive schema changes only.

### 3. Queue gating: preempt anything else

Gate all issue/PR automation on takeover state:
1. `_enqueue_new_work`: skip issue intake if takeover label present.
2. `_enqueue_implementation_work`: skip implementation enqueue for taken-over issue.
3. `_enqueue_pre_pr_followup_work`: skip follow-up retries while takeover active.
4. `_enqueue_feedback_work`: do not schedule PR feedback turns while takeover active.
5. `_monitor_pr_actions`: do not enqueue actions feedback for taken-over issues.
6. `_scan_post_pr_source_issue_comment_redirects`: do not post redirects while takeover active.

Defensive runtime check:
1. `_process_feedback_turn` re-checks takeover state at start to avoid race conditions if label was added after enqueue.

### 4. Event and comment handling changes

PR feedback comments:
1. In `_normalize_review_events` and `_normalize_issue_events`, ignore comments with `comment_id <= takeover_*_floor_comment_id`.
2. When takeover activates, mark pending feedback events for that PR processed to drop backlog and prevent replay.

Source issue comments:
1. Pre-PR follow-up flow already filters by `pre_pr_last_consumed_comment_id`; takeover updates this cursor to suppress in-takeover comments.
2. Post-PR source redirects already filter by `post_pr_last_redirected_comment_id`; takeover updates this cursor similarly.

Net effect:
1. Comments posted during takeover are intentionally skipped.
2. Comments posted after takeover ends can trigger normal follow-up/feedback processing.

### 5. Poll-time takeover synchronizer

Add a centralized helper in orchestrator (single authority for takeover transitions):
1. Reads current issue labels.
2. Reads persisted `ignore_active` state.
3. Detects transitions (`inactive -> active`, `active -> inactive`, steady-state).
4. Applies boundary updates and queue gating decisions.

Use per-poll in-memory cache for issue snapshots to reduce duplicate `get_issue` calls across enqueue/monitor phases.

### 6. README flow update

Add a dedicated “PR takeover” story:
1. Add `agent:ignore` (or configured `repo.ignore_label`) to source issue.
2. MergeXO pauses automation for that issue/PR.
3. Human takes over changes and review manually.
4. Remove label to resume automation.
5. Leave a fresh comment after removal to trigger next turn.

Also update configuration and label rules sections:
1. Document `repo.ignore_label` default and semantics.
2. Document that ignore preempts all other flow labels.

## Implementation plan

1. Extend `RepoConfig` and config parsing with `ignore_label` defaulting to `agent:ignore`.
2. Update `mergexo.toml.example` and README configuration table for `ignore_label`.
3. Add `issue_takeover_state` table and migration-safe initialization in `StateStore`.
4. Add `pr_feedback_state` columns for takeover PR comment floors and migration-safe `ALTER TABLE` guards.
5. Add `StateStore` APIs to read/write takeover active state, advance PR floors, and clear pending feedback events for a PR.
6. Add orchestrator takeover sync helper(s) and per-poll issue label cache.
7. Integrate takeover gating into enqueue paths, actions monitor, post-PR redirect scan, and defensive check in feedback worker.
8. Apply floor-based filtering in PR comment normalization paths.
9. Update README with explicit human takeover operator flow and precedence rules.
10. Add tests for config defaults/overrides, state migrations and floor behavior, and orchestrator gating/resume semantics.

## Acceptance criteria

1. `repo.ignore_label` is configurable and defaults to `agent:ignore`.
2. Issue intake is skipped when an issue has `ignore_label`, even if trigger labels are present.
3. While `ignore_label` is present, MergeXO does not enqueue pre-PR follow-up retries for that issue.
4. While `ignore_label` is present, MergeXO does not enqueue PR feedback turns for linked PRs.
5. While `ignore_label` is present, MergeXO does not enqueue CI/actions feedback turns for linked PRs.
6. Comments posted on source issue during takeover do not trigger follow-up processing after resume.
7. Comments posted on PR during takeover do not trigger feedback processing after resume.
8. After removing `ignore_label`, a new PR or issue comment posted after resume boundary is processed normally.
9. Existing non-takeover behavior for issues without `ignore_label` remains unchanged.
10. README documents takeover behavior, resume behavior, and config key clearly.

## Risks and mitigations

1. Risk: extra GitHub API load due additional issue-label checks and boundary snapshots.
Mitigation: cache issue snapshots per poll and only snapshot comment boundaries for issues currently in takeover transitions or active takeover.

2. Risk: ambiguity near label-removal timing can cause user confusion about which comments are resumed.
Mitigation: define deterministic boundary behavior and document “remove label, then leave a fresh comment to resume”.

3. Risk: stale pending feedback events could replay old context after takeover.
Mitigation: mark pending feedback events processed when takeover is active and enforce PR comment floors on normalization.

4. Risk: regression in feedback loop filtering logic.
Mitigation: add targeted tests for floor filtering, takeover transitions, and unchanged default behavior.

## Rollout notes

1. Ship schema and orchestrator changes together (additive migration only).
2. Enable in one canary repo by using default `agent:ignore` label.
3. Validate canary scenarios:
1. active PR receives `agent:ignore` and automation pauses
2. comments during takeover are ignored
3. label removed and fresh comment resumes automation
4. no replay of takeover-period comments
4. Monitor logs for new takeover transition events and skipped enqueue reasons.
5. After canary confidence, document as recommended operator control path in README examples.
