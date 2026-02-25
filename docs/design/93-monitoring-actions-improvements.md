---
issue: 93
priority: 3
touch_paths:
  - docs/design/93-monitoring-actions-improvements.md
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/orchestrator.py
  - tests/test_config.py
  - tests/test_orchestrator.py
  - README.md
  - mergexo.toml.example
depends_on: []
estimated_size: M
generated_at: 2026-02-25T16:19:41Z
---

# Monitoring actions improvements

_Issue: #93 (https://github.com/johnynek/mergexo/issues/93)_

## Summary

Design for issue #93 introducing per-repo Actions monitoring policy (`never`, `first_fail`, `all_complete`) with backward-compatible fallback from the existing runtime flag, plus implementation, testing, acceptance criteria, risks, and rollout plan.

---
issue: 93
priority: 2
touch_paths:
  - docs/design/93-monitoring-actions-improvements.md
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/orchestrator.py
  - tests/test_config.py
  - tests/test_orchestrator.py
  - README.md
  - mergexo.toml.example
depends_on:
  - 68
estimated_size: M
generated_at: 2026-02-25T00:00:00Z
---

# Monitoring actions improvements

_Issue: #93 (https://github.com/johnynek/mergexo/issues/93)_

## Summary
Introduce a per-repo GitHub Actions feedback policy with three modes: `never`, `first_fail`, and `all_complete`.

This solves two gaps:
1. Actions monitoring can be configured per repository instead of only globally.
2. Repos with fail-fast/dependency-gated workflows can trigger remediation on first failure instead of waiting for all runs to become terminal.

## Problem statement
1. Current monitoring is controlled by `runtime.enable_pr_actions_monitoring`, which is global and not repo-specific.
2. Current monitor logic enqueues `actions` feedback only when all runs for the PR head SHA are terminal.
3. Some repos can leave downstream runs pending/waiting after an early failure, so remediation never starts.
4. Operators need explicit control over when failures should wake the feedback agent.

## Goals
1. Add per-repo Actions monitoring behavior.
2. Support clear policies: `never`, `first_fail`, `all_complete`.
3. Preserve deterministic event dedupe (`{pr}:actions:{run_id}:{updated_at}`).
4. Preserve stale-event revalidation during feedback turns.
5. Keep existing deployments working without forced immediate config migration.

## Non-goals
1. Supporting non-GitHub CI providers.
2. Replacing local required-tests gates with remote CI status.
3. Redesigning prompt/result schemas.
4. Moving to webhook/event-stream CI ingestion in this issue.

## Proposed architecture

### 1. Per-repo policy in config
Add a new repo-level key in both `[repo.<id>]` and legacy `[repo]`:

- `pr_actions_feedback_policy = never | first_fail | all_complete`

Policy semantics:
1. `never`: disable Actions monitoring for this repo.
2. `first_fail`: enqueue failed completed runs even when other runs are still active.
3. `all_complete`: enqueue failed runs only after all runs are terminal (current behavior).

### 2. Backward-compatible fallback
Keep `runtime.enable_pr_actions_monitoring` as compatibility fallback.

Effective policy resolution:
1. If `repo.pr_actions_feedback_policy` is set, use it.
2. Else if `runtime.enable_pr_actions_monitoring` is `true`, use `all_complete`.
3. Else use `never`.

This preserves existing behavior while enabling per-repo override.

### 3. Orchestrator polling by policy
In poll-time monitoring for tracked PRs:
1. Fetch PR snapshot and workflow runs for `(pr_number, head_sha)`.
2. Partition runs into:
   - `failed_runs`: `status == completed` and conclusion not in green allowlist.
   - `active_runs`: `status != completed`.
3. Apply policy:
   - `never`: skip monitor for this repo.
   - `all_complete`: if `active_runs` exists, wait; else enqueue events for `failed_runs`.
   - `first_fail`: enqueue events for `failed_runs` regardless of `active_runs`.
4. If `failed_runs` is empty, enqueue nothing in all modes.

Event key format and dedupe remain unchanged.

### 4. Feedback-turn correctness remains unchanged
Keep `_resolve_actions_feedback_events` as the revalidation gate:
1. Drop stale events if run id/head/update no longer matches.
2. Drop events that are now green or non-terminal.
3. Build synthetic failure context only from actionable runs.

This keeps `first_fail` safe when reruns or head changes invalidate prior failures.

### 5. Observability updates
Reuse existing events and include policy context fields:
1. `actions_monitor_scan_started` includes `policy`.
2. `actions_monitor_active_runs_detected` includes `policy` and `failed_run_count`.
3. `actions_failure_events_enqueued` includes `policy`, `failed_run_count`, and `active_run_count`.

No new database tables are required.

## Implementation plan
1. Add policy literal type (or equivalent validator) and `RepoConfig` field in `src/mergexo/config.py` (and `src/mergexo/models.py` if type alias is shared there).
2. Parse and validate `repo.pr_actions_feedback_policy` in config loader.
3. Add orchestrator helper to compute effective policy with runtime fallback.
4. Replace global monitor gating in `poll_once` with effective per-repo policy.
5. Update `_monitor_pr_actions` branch logic for `first_fail` vs `all_complete`.
6. Keep `actions` event-key format, ingestion, and stale-resolution paths unchanged.
7. Update documentation in `README.md` and `mergexo.toml.example`.
8. Add tests for config parsing/precedence and monitor behavior by policy.

## Testing plan
1. `tests/test_config.py`
   - accepts `never`, `first_fail`, `all_complete`.
   - rejects invalid policy strings.
   - verifies runtime fallback when repo policy is omitted.
2. `tests/test_orchestrator.py`
   - `never`: no monitor enqueue.
   - `all_complete`: wait while active runs exist.
   - `first_fail`: enqueue when failed completed run exists even with active runs.
   - dedupe remains one event per run `updated_at`.
   - stale-event revalidation remains unchanged.
3. Existing monitor-order and poll-step tests are updated to assert policy-driven gating.

## Acceptance criteria
1. One repo can disable Actions monitoring without disabling it for other repos in the same service.
2. With `first_fail`, a failed completed run enqueues `actions` feedback even when other runs are active/pending.
3. With `all_complete`, failures enqueue only when no runs for the head SHA remain active.
4. With `never`, Actions monitor does not enqueue `actions` feedback events.
5. Legacy config behavior remains compatible: runtime flag only still maps to `true -> all_complete`, `false -> never`.
6. Event dedupe key format is unchanged.
7. Stale `actions` events are still auto-resolved before agent invocation.
8. README and sample config clearly document policy values and precedence.

## Risks and mitigations
1. Risk: `first_fail` may trigger extra remediation turns before all failures are visible.
Mitigation: retain deterministic dedupe and stale revalidation; document latency-vs-completeness tradeoff.
2. Risk: confusion about runtime-vs-repo precedence.
Mitigation: explicit precedence rules in docs and tests.
3. Risk: behavior regressions for existing deployments.
Mitigation: compatibility fallback plus dedicated regression tests.
4. Risk: unexpected Actions statuses.
Mitigation: conservative terminal/active classification with unit tests.

## Rollout notes
1. Ship with runtime fallback enabled.
2. Canary `first_fail` on repos with dependency-gated CI behavior (for example bosatsu).
3. Watch for faster first remediation wakeups, absence of duplicate enqueue churn, and correct stale-event cleanup after reruns.
4. After canary success, set explicit `pr_actions_feedback_policy` for each repo.
5. In a follow-up issue, remove deprecated runtime fallback once all repos have explicit policies.
