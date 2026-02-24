---
issue: 68
priority: 3
touch_paths:
  - docs/design/68-add-a-doc-to-monitor-github-actions-for-prs.md
  - src/mergexo/orchestrator.py
  - src/mergexo/github_gateway.py
  - src/mergexo/models.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/state.py
  - src/mergexo/prompts.py
  - src/mergexo/config.py
  - tests/test_orchestrator.py
  - tests/test_github_gateway.py
  - tests/test_feedback_loop.py
  - tests/test_state.py
  - tests/test_models_and_prompts.py
  - tests/test_config.py
  - README.md
  - mergexo.toml.example
depends_on: []
estimated_size: M
generated_at: 2026-02-24T18:41:04Z
---

# Monitor GitHub Actions for PR feedback wakeups

_Issue: #68 (https://github.com/johnynek/mergexo/issues/68)_

## Summary

Design for issue #68: add poll-time GitHub Actions monitoring, deterministic CI-failure feedback events, failure-log context injection, and rollout controls.

---
issue: 68
priority: 2
touch_paths:
  - docs/design/68-add-a-doc-to-monitor-github-actions-for-prs.md
  - src/mergexo/orchestrator.py
  - src/mergexo/github_gateway.py
  - src/mergexo/models.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/state.py
  - src/mergexo/prompts.py
  - src/mergexo/config.py
  - tests/test_orchestrator.py
  - tests/test_github_gateway.py
  - tests/test_feedback_loop.py
  - tests/test_state.py
  - tests/test_models_and_prompts.py
  - tests/test_config.py
  - README.md
  - mergexo.toml.example
depends_on:
  - 2
estimated_size: M
generated_at: 2026-02-24T00:00:00Z
---

# Monitor GitHub Actions for PR feedback wakeups

_Issue: #68 (https://github.com/johnynek/mergexo/issues/68)_

## Summary

Add a CI monitoring path for active tracked PRs. While GitHub Actions runs are active for the PR head SHA, MergeXO keeps monitoring. When all runs complete and one or more are non-green, MergeXO enqueues deterministic feedback events, wakes the feedback agent with failed run context plus failed job log excerpts, and asks the agent to reproduce failures locally and fix them. A non-null `commit_message` remains the ready-to-push signal, and existing required pre-push tests remain the final push gate.

## Context

Current behavior wakes feedback turns only from PR review comments and PR issue comments. CI failures that happen without new comments can leave a PR stalled until a human intervenes.

## Goals

1. Detect GitHub Actions run state for each tracked `awaiting_feedback` PR head.
2. Wait until all relevant runs for that head are terminal before triggering remediation.
3. Wake the agent exactly once per failed run update, with dedupe across restarts.
4. Include actionable failure data and log excerpts in the feedback prompt.
5. Keep behavior crash-safe and idempotent using existing feedback event machinery.
6. Keep scope limited to GitHub Actions for this issue.

## Non-goals

1. Supporting non-GitHub CI providers.
2. Replacing required pre-push local tests with remote CI status.
3. Auto-merging PRs after CI recovery.
4. Webhook-based push events in this phase.

## Proposed architecture

### 1. Poll-time Actions monitor step

Add a new orchestrator poll step before feedback enqueue.

1. Load tracked PRs in `awaiting_feedback`.
2. For each tracked PR, read the current PR snapshot and head SHA.
3. Query GitHub Actions workflow runs filtered to the current head SHA and PR event.
4. If any run is active (`queued`, `in_progress`, `waiting`, `requested`, `pending`), do nothing except observability logging.
5. If all runs are terminal and at least one run is non-green, emit feedback events for failed runs.

Green conclusions for this feature are `success`, `neutral`, and `skipped`. Any other completed conclusion is treated as non-green.

This step is lightweight and does not consume worker slots.

### 2. Reuse feedback_events for CI wakeups

Reuse `feedback_events` instead of adding new tables.

1. Extend `FeedbackKind` to include `actions`.
2. Encode run updates as deterministic keys: `{pr_number}:actions:{run_id}:{updated_at}`.
3. Store `run_id` in `comment_id` for `actions` events.
4. Ingest via existing `StateStore.ingest_feedback_events` dedupe and WAL durability.

This keeps crash behavior aligned with existing comment feedback processing.

### 3. GitHub gateway extensions

Add GitHub Actions read methods to `GitHubGateway`.

1. `list_workflow_runs_for_head(pr_number, head_sha)` returning run id, name, status, conclusion, url, and timestamps.
2. `list_workflow_jobs(run_id)` returning job id, name, status, conclusion, and url.
3. `get_failed_run_log_excerpt(run_id, max_chars)` returning truncated failed-job logs.

If job logs are unavailable, continue with metadata-only context and include a placeholder in prompt context.

### 4. Feedback-turn integration

In `_process_feedback_turn`:

1. Partition pending events into `review`, `issue`, and `actions`.
2. Revalidate `actions` events against current head SHA and current run conclusion before agent call.
3. Mark stale `actions` events processed when the run is now green or no longer relevant to current head.
4. Build synthetic system issue comments containing failed workflow run summary, failed job names and conclusions, and truncated failed-job log excerpt.
5. Append these synthetic comments to the existing issue comment context passed to the agent.

If there are no actionable pending events after revalidation, return without agent invocation.

### 5. Prompt updates for CI remediation

Update `build_feedback_prompt` rules.

1. When CI failure context is present, reproduce failures locally before finalizing.
2. Repair code and tests until local required test command passes.
3. Treat non-null `commit_message` as the readiness signal for push.
4. If blocked, explain concretely in `general_comment` with minimal ambiguity.

No schema change to feedback result is required.

### 6. Config and safety limits

Add runtime config knobs.

1. `runtime.enable_pr_actions_monitoring` with default `false` for canary rollout.
2. `runtime.pr_actions_log_excerpt_chars` with default `4000` and bounded integer validation.

These control rollout and prompt-size safety.

### 7. Observability events

Add structured events.

1. `actions_monitor_scan_started`
2. `actions_monitor_active_runs_detected`
3. `actions_failure_events_enqueued`
4. `actions_failure_context_loaded`
5. `actions_failure_logs_unavailable`
6. `actions_failure_event_stale_resolved`

## Implementation plan

1. Extend models for workflow run and job snapshots in `src/mergexo/models.py`.
2. Extend feedback event kind typing in `src/mergexo/feedback_loop.py`.
3. Add config fields and validation in `src/mergexo/config.py`.
4. Add GitHub Actions query and log methods in `src/mergexo/github_gateway.py`.
5. Add poll-time Actions monitor step in `Phase1Orchestrator.poll_once`.
6. Add helper methods in orchestrator to convert failed runs to `FeedbackEventRecord`.
7. Add orchestrator revalidation path for pending `actions` events.
8. Add synthetic CI failure context builder for feedback turns.
9. Update feedback prompt wording in `src/mergexo/prompts.py`.
10. Add and adjust tests for gateway parsing, orchestrator event ingestion and revalidation, prompt text, and config parsing.
11. Document feature flag and operator behavior in `README.md` and `mergexo.toml.example`.

## Testing plan

1. `tests/test_github_gateway.py`: parse workflow runs and jobs payloads, active vs terminal classification, failed-log truncation behavior, and missing-log fallback.
2. `tests/test_orchestrator.py`: no event enqueue while runs are active, enqueue on terminal non-green runs, dedupe on repeated polls, stale-event resolution, and CI context injection in feedback turns.
3. `tests/test_feedback_loop.py`: `actions` event key formatting and stability.
4. `tests/test_state.py`: ingest, list, and finalize behavior for `actions` feedback events.
5. `tests/test_models_and_prompts.py`: feedback prompt includes CI remediation instructions.
6. `tests/test_config.py`: new runtime fields parse and validate bounds.

## Acceptance criteria

1. For an open tracked PR with active GitHub Actions runs, MergeXO logs monitoring state and does not enqueue CI remediation events yet.
2. When all runs for the tracked head complete and at least one is non-green, MergeXO records pending `actions` feedback events.
3. The same failed run update is not re-enqueued on subsequent polls unless the run `updated_at` changes.
4. A feedback turn triggered by `actions` events includes failed run metadata and failed-job log excerpts in agent context.
5. If no review or issue comments exist, CI-failure context alone can trigger an agent feedback turn.
6. If a queued `actions` event becomes stale because the run or head is now green or mismatched, MergeXO resolves it without unnecessary remediation output.
7. Existing comment-driven feedback behavior remains unchanged.
8. Existing required pre-push local test gate remains enforced before push.
9. Feature behavior can be enabled and disabled through config without impacting unrelated flows.
10. Unit tests cover polling, dedupe, context construction, stale resolution, and config parsing.

## Risks and mitigations

1. Risk: GitHub API volume increases with per-PR Actions polling.
Mitigation: ETag-based GET reuse, per-poll bounded queries, and feature-flag rollout.
2. Risk: Prompt bloat from large job logs.
Mitigation: hard truncation by configurable char limit and per-run excerpt limits.
3. Risk: Stale failures from reruns trigger unnecessary agent work.
Mitigation: revalidate pending `actions` events against current head and current run conclusion before invoking agent.
4. Risk: Non-standard conclusions are misclassified.
Mitigation: explicit green conclusion allowlist with unit tests.
5. Risk: Log endpoint failures hide useful context.
Mitigation: proceed with run and job metadata and include explicit missing-log markers.

## Rollout notes

1. Ship behind `runtime.enable_pr_actions_monitoring = false`.
2. Canary on one repository with a short poll interval and known CI workflows.
3. Validate canary scenarios: long-running workflows stay monitor-only, terminal failures enqueue exactly one remediation turn, rerun-to-green stale events auto-resolve.
4. Watch verbose logs for Actions API failures and prompt-size anomalies.
5. After stable canary, enable by default in config templates.
