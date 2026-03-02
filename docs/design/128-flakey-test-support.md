---
issue: 128
priority: 3
touch_paths:
  - docs/design/128-flakey-test-support.md
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/prompts.py
  - src/mergexo/orchestrator.py
  - src/mergexo/github_gateway.py
  - src/mergexo/state.py
  - src/mergexo/models.py
  - src/mergexo/feedback_loop.py
  - tests/test_orchestrator.py
  - tests/test_github_gateway.py
  - tests/test_state.py
  - tests/test_codex_adapter.py
  - tests/test_models_and_prompts.py
  - tests/test_feedback_loop.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-03-02T20:21:54Z
---

# flakey test support

_Issue: #128 (https://github.com/johnynek/mergexo/issues/128)_

## Summary

Design for issue #128 adding agent-signaled flaky CI classification, automatic flaky issue filing, one-shot rerun, and second-failure PR blocking with linked PR messaging.

---
issue: 128
priority: 2
touch_paths:
  - docs/design/128-flakey-test-support.md
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/prompts.py
  - src/mergexo/orchestrator.py
  - src/mergexo/github_gateway.py
  - src/mergexo/state.py
  - src/mergexo/models.py
  - src/mergexo/feedback_loop.py
  - tests/test_orchestrator.py
  - tests/test_github_gateway.py
  - tests/test_state.py
  - tests/test_codex_adapter.py
  - tests/test_models_and_prompts.py
  - tests/test_feedback_loop.py
  - README.md
depends_on:
  - 68
  - 93
estimated_size: L
generated_at: 2026-03-02T00:00:00Z
---

# flakey test support

_Issue: #128 (https://github.com/johnynek/mergexo/issues/128)_

## Summary

Add a deterministic flaky-CI handling path on top of the existing Actions feedback loop. When the feedback agent identifies an Actions failure as an unrelated flaky test, MergeXO files a repository issue with full CI evidence, posts a PR comment linking that issue, reruns the failed CI once, and then either resumes normally if rerun is green or blocks the PR with an explicit message if rerun fails again.

## Context

Today MergeXO already:

1. Detects failing GitHub Actions runs and turns them into `actions` feedback events.
2. Injects failed-action names and log tails into feedback context.
3. Lets the feedback agent decide whether to edit code, request git operations, or report a blocker.

Issue #128 adds a specific workflow for a common case: CI failed due to a flaky unrelated test, not a regression from current PR changes.

## Goals

1. Allow the feedback agent to explicitly report that a CI failure is likely an unrelated flaky test.
2. Capture enough structured evidence from that report to open a reproducible bug issue automatically.
3. Create the issue with no labels.
4. Post a PR comment that links the created flaky-test issue.
5. Rerun the failed CI one time automatically.
6. If rerun fails again, mark the PR as blocked and post a blocking message.
7. Keep the flow restart-safe and idempotent enough to avoid duplicate issue/comment spam.

## Non-goals

1. Building a generic probabilistic flake detector independent of agent judgment.
2. Retrying CI more than once per flaky incident in this issue.
3. Supporting CI providers beyond GitHub Actions.
4. Replacing existing manual unblock/operator controls.

## Proposed architecture

### 1. Extend feedback contract with structured flaky-test report

Add an optional structured field to feedback results, returned only when the agent judges the failure as unrelated flake.

Proposed shape:

1. `flaky_test_report.run_id: int`.
2. `flaky_test_report.title: str`.
3. `flaky_test_report.summary: str`.
4. `flaky_test_report.relevant_log_excerpt: str`.

Contract rules:

1. Agent sets `flaky_test_report` only when CI context is present and classification is “unrelated flaky test”.
2. When `flaky_test_report` is set, `commit_message` must be null.
3. The report must include log evidence copied from provided CI context and concrete reproduction details (test target, command, and assumptions) sufficient for a human to reproduce locally.
4. If uncertain, the agent should not set the field.

Implementation impact:

1. Add a `FlakyTestReport` dataclass in `agent_adapter.py` or `models.py`.
2. Add `flaky_test_report: FlakyTestReport | None` to `FeedbackResult`.
3. Update `build_feedback_prompt` JSON contract and rules.
4. Update `codex_adapter.py` parser validation for optional `flaky_test_report`.

### 2. Persist flaky incident state per PR

Add a new SQLite table dedicated to flaky incident lifecycle, one active incident per PR.

Proposed table: `pr_flake_state`.

Key columns:

1. `repo_full_name`, `pr_number` primary key.
2. `issue_number` (source issue).
3. `head_sha` (PR head when flake was reported).
4. `run_id` and `initial_run_updated_at`.
5. `status` with values `awaiting_rerun_result`, `resolved_after_rerun`, `blocked_after_second_failure`.
6. `flake_issue_number` and `flake_issue_url`.
7. `report_title`, `report_summary`, `report_excerpt`.
8. `full_log_context_markdown` (serialized CI context used in filed issue).
9. `rerun_requested_at`, `updated_at`, `created_at`.

StateStore additions:

1. Upsert flake state when incident is accepted.
2. Read active flake state for a PR.
3. Transition to resolved/blocked terminal states.
4. Clear stale flake state when PR head changes or PR closes/merges.
5. Include migration-safe schema creation and tests.

### 3. GitHubGateway write APIs for issue creation and CI rerun

Add explicit gateway methods:

1. `create_issue(title, body, labels=None)`.
2. `rerun_workflow_run_failed_jobs(run_id)`.

Behavior:

1. `create_issue` posts to `/repos/{owner}/{repo}/issues`.
2. MergeXO calls it with no labels.
3. `rerun_workflow_run_failed_jobs` posts to `/repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs`.
4. Gateway gets a non-JSON POST helper path for endpoints that return empty response bodies.

### 4. Flake handling path inside `_process_feedback_turn`

Add a branch after feedback result is produced and before commit/push path.

Flow:

1. Only consider flake handling when pending events for this turn are actions-only.
2. Validate reported `run_id` is in actionable actions events for this turn.
3. Build issue body from three sources.
4. Source one: agent report summary and relevant excerpt.
5. Source two: PR/source-issue metadata and run URL.
6. Source three: full Actions context text already built by `_render_actions_failure_context_comment`.
7. File flaky issue via gateway with no labels.
8. Persist `pr_flake_state` row.
9. Post a tokenized PR comment saying a flake was detected and linking the created issue.
10. Trigger one rerun for the failed run.
11. Mark this turn’s pending actions events processed and finalize feedback turn.
12. Return `completed` without entering commit/push logic.

### 5. Rerun outcome reconciliation inside `_monitor_pr_actions`

Enhance actions monitor to reconcile any active `pr_flake_state` row.

For each tracked PR with active flake state:

1. Fetch current runs for PR head SHA.
2. If PR head SHA changed from stored `head_sha`, clear stale flake state and continue normal monitoring.
3. Locate stored `run_id`.
4. If run has not updated since `initial_run_updated_at`, keep waiting.
5. If run is updated but not terminal, keep waiting.
6. If run is terminal and green, mark `resolved_after_rerun`, optionally post a short informational PR comment, and resume normal behavior.
7. If run is terminal and non-green, mark PR `blocked`, mark flake state `blocked_after_second_failure`, and post a blocking PR comment that links the flaky-test issue.

This enforces the requested “one rerun only, second failure blocks PR” policy in orchestrator logic without another agent turn.

### 6. PR/issue message templates and idempotency

Add deterministic comment/issue rendering helpers in orchestrator.

PR comment on first flake detection includes:

1. Run ID and run URL.
2. Link to created flaky issue.
3. Statement that MergeXO is rerunning CI once.

Flaky issue body includes:

1. Meaningful title from agent report with fallback if empty.
2. Why failure appears unrelated to current PR.
3. Relevant log excerpt.
4. Full captured CI context for reproduction.
5. PR URL, source issue, run metadata.

PR comment on second failure includes:

1. Link to same flaky issue.
2. Statement that rerun failed again.
3. Statement that PR is now blocked.

Use action tokens for PR comments to prevent duplicates on retries/restarts.

### 7. Observability additions

Add dedicated events:

1. `flake_report_detected`.
2. `flake_issue_created`.
3. `flake_pr_comment_posted`.
4. `flake_rerun_requested`.
5. `flake_rerun_resolved_green`.
6. `flake_rerun_failed_blocked`.
7. `flake_report_rejected_invalid_context`.

## Implementation plan

1. Add `FlakyTestReport` type and extend `FeedbackResult` in `src/mergexo/agent_adapter.py` and `src/mergexo/models.py`.
2. Update feedback prompt contract in `src/mergexo/prompts.py`.
3. Add flake report parsing and validation in `src/mergexo/codex_adapter.py`.
4. Add helper token(s) in `src/mergexo/feedback_loop.py` for deduped PR flake comments.
5. Add `pr_flake_state` schema and state APIs in `src/mergexo/state.py`.
6. Add `create_issue` and rerun API methods in `src/mergexo/github_gateway.py`.
7. Add orchestrator helpers to render flaky issue title/body and PR comments.
8. Add orchestrator branch in `_process_feedback_turn` to execute flaky workflow.
9. Add orchestrator reconciliation logic in `_monitor_pr_actions` for rerun outcomes and second-failure blocking.
10. Update README with the new flaky CI handling behavior.
11. Add tests in orchestrator/state/gateway/codex/prompt/feedback modules.

## Testing plan

1. `tests/test_models_and_prompts.py` verifies feedback prompt contains `flaky_test_report` contract and rules.
2. `tests/test_codex_adapter.py` verifies parsing of valid and invalid `flaky_test_report` payloads.
3. `tests/test_state.py` verifies `pr_flake_state` schema, transitions, and migration behavior.
4. `tests/test_github_gateway.py` verifies `create_issue` payload and rerun endpoint behavior with empty response body.
5. `tests/test_orchestrator.py` verifies first flake detection files issue, posts PR comment, requests rerun, and does not block immediately.
6. `tests/test_orchestrator.py` verifies rerun green path resolves state and keeps PR in `awaiting_feedback`.
7. `tests/test_orchestrator.py` verifies rerun non-green path blocks PR and posts blocking comment linking issue.
8. `tests/test_orchestrator.py` verifies idempotency of comments/issue links across repeated polls.

## Acceptance criteria

1. When feedback agent returns `flaky_test_report` for an actionable Actions failure, MergeXO opens a new repository issue with no labels.
2. Created issue title is meaningful and derived from report content with fallback logic.
3. Created issue body includes why failure looks flaky and includes full CI context captured by MergeXO plus relevant excerpt.
4. MergeXO posts a PR comment that a flake was encountered and links the created issue.
5. MergeXO reruns the failed CI once after posting the issue.
6. If rerun completes green, PR is not blocked and normal monitoring continues.
7. If rerun completes non-green, PR is marked `blocked` and a blocking PR comment is posted with issue link.
8. Existing non-flake Actions feedback behavior remains unchanged when no `flaky_test_report` is provided.
9. Flow is restart-safe enough that repeated polls do not repeatedly post duplicate flake PR comments.

## Risks and mitigations

1. Risk: false positives from agent classification may open unnecessary flaky-test issues.
Mitigation: require explicit structured report and clear prompt rule to report only when confidence is high.

2. Risk: rerun API may fail due permissions or transient GitHub errors.
Mitigation: log explicit failure, keep flake state visible, and mark PR blocked with actionable message when rerun cannot be performed.

3. Risk: issue bodies can become too large due log context.
Mitigation: reuse existing bounded log tails controlled by `runtime.pr_actions_log_tail_lines` and label truncation clearly.

4. Risk: duplicate issue/comment side effects around retries.
Mitigation: persist `pr_flake_state`, use tokenized PR comments, and gate side effects on stored state.

5. Risk: head SHA drift makes rerun result ambiguous.
Mitigation: tie flake state to `head_sha`; clear stale incident on head change and fall back to normal Actions monitoring.

## Rollout notes

1. Roll out on repos already using Actions monitoring (`repo.pr_actions_feedback_policy != "never"`).
2. Canary on one repository first and observe 1-2 weeks of incidents.
3. Validate canary scenarios: first flake detection, issue creation, PR link comment, rerun green, rerun second-failure block.
4. Monitor new observability events and verify there is no duplicate comment/issue spam.
5. After canary confidence, enable across remaining repos using Actions feedback.
