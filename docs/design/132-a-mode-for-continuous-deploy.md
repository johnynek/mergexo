---
issue: 132
priority: 3
touch_paths:
  - docs/design/132-a-mode-for-continuous-deploy.md
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/state.py
  - src/mergexo/service_runner.py
  - tests/test_config.py
  - tests/test_state.py
  - tests/test_service_runner.py
  - README.md
  - mergexo.toml.example
depends_on: []
estimated_size: M
generated_at: 2026-03-03T07:10:24Z
---

# A mode for continuous deploy

_Issue: #132 (https://github.com/johnynek/mergexo/issues/132)_

## Summary

Design for an optional service-mode continuous deploy loop that updates MergeXO from upstream main when idle, with startup health checks and automatic rollback/quarantine.

---
issue: 132
status: proposed
priority: 2
touch_paths:
  - docs/design/132-a-mode-for-continuous-deploy.md
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/state.py
  - src/mergexo/service_runner.py
  - tests/test_config.py
  - tests/test_state.py
  - tests/test_service_runner.py
  - README.md
  - mergexo.toml.example
depends_on: []
estimated_size: L
generated_at: 2026-03-03T00:00:00Z
---

# A mode for continuous deploy

_Issue: #132 (https://github.com/johnynek/mergexo/issues/132)_

## Summary

Add an optional continuous-deploy mode for `mergexo service` that automatically updates the local MergeXO checkout when upstream `main` moves forward, but only when no agent work is in flight. The mode reuses existing restart-drain mechanics, adds boot-health confirmation, and adds rollback/quarantine for bad revisions.

## Context

Today MergeXO supports runtime updates through manual operator commands (`/mergexo restart`). That path already provides:

1. Single-flight restart coordination via sqlite `runtime_operations`.
2. Drain semantics that stop new enqueue work and wait for active futures to finish.
3. `git_checkout` update mode (`git pull --ff-only` + `uv sync`) and process re-exec.

Issue #132 asks for the same update behavior to happen automatically without waiting for manual commands, with rollback when a new revision crashes or fails health.

## Goals

1. Periodically check whether the configured deploy branch has advanced.
2. Only trigger auto-update when there are no outstanding agent runs.
3. Reuse the existing restart drain/re-exec path instead of adding a second update pipeline.
4. Add rollback for unhealthy upgrades and prevent endless retry loops on the same bad revision.
5. Provide a local health endpoint to support health reporting and operator checks.

## Non-goals

1. Rolling updates across multiple hosts.
2. Zero-downtime hot reload.
3. Supporting continuous deploy for `mode=pypi` in the first increment.
4. Replacing existing manual `/mergexo restart` controls.

## Proposed architecture

### 1. Continuous deploy controller inside `ServiceRunner`

Add a periodic controller in `ServiceRunner.run` that executes only when all conditions are true:

1. `runtime.continuous_deploy_enabled` is `true`.
2. Service is in continuous mode (`once=False`).
3. No restart operation is currently pending/running.
4. GitHub auth shutdown is not pending.
5. `self._total_pending_futures(orchestrators) == 0`.
6. Current wall time is beyond the next deploy-check deadline.

This keeps all deployment decisions in the same process that already owns restart draining and update execution.

### 2. Update detection by git remote-head comparison

For `git_checkout` deployments, add a helper that checks for a new target revision without mutating local state:

1. `git -C <git_checkout_root> fetch origin <continuous_deploy_branch> --prune --tags`.
2. Read local head via `git rev-parse HEAD`.
3. Read remote head via `git rev-parse origin/<continuous_deploy_branch>`.
4. If equal: no deploy.
5. If local is not an ancestor of remote (`merge-base != local`): mark as diverged and skip auto-deploy.
6. If remote equals the last quarantined bad target: skip auto-deploy.
7. Otherwise schedule deploy from `from_sha=local` to `to_sha=remote`.

### 3. Reuse existing restart operation and drain path

Do not add a second runtime operation type.

1. Continuous deploy requests the existing `restart` runtime operation using a synthetic command key (for example `continuous:<target_sha>:<timestamp>`).
2. Existing `_process_global_restart_drain` behavior handles “stop enqueue + wait for zero pending futures”.
3. Existing `_handle_restart_requested` applies update and re-execs.

This minimizes behavioral drift between manual and automatic updates.

### 4. Persist deploy-attempt state for rollback and quarantine

Add a dedicated sqlite table for deploy lifecycle state (service-scoped, not repo-scoped), for example `continuous_deploy_state` with one logical row.

Proposed fields:

1. `status`: `idle | awaiting_health | healthy | rolled_back | failed`.
2. `previous_sha`: revision before update.
3. `target_sha`: revision being deployed.
4. `active_sha`: last known healthy running revision.
5. `blocked_target_sha`: most recent auto-deploy target that was rolled back.
6. `boot_attempt_count`: number of starts seen while `awaiting_health`.
7. `requested_at`, `updated_at`, `healthy_at`, `last_error`.

State transitions:

1. `idle/healthy -> awaiting_health` when auto-update is scheduled.
2. `awaiting_health -> healthy` after startup health is confirmed.
3. `awaiting_health -> rolled_back` when health checks fail repeatedly.
4. `rolled_back/failed -> awaiting_health` only when remote target differs from `blocked_target_sha`.

### 5. Boot-health confirmation and crash-loop rollback

Add startup reconciliation before normal polling:

1. If no pending deploy attempt: continue normally.
2. If `awaiting_health` exists:
   - increment `boot_attempt_count`.
   - if count exceeds `runtime.continuous_deploy_max_boot_failures`, perform rollback to `previous_sha` and re-exec.

Mark an attempt healthy after service proves liveness:

1. At least one full poll cycle over configured repos completed.
2. No fatal exception occurred.
3. GitHub auth shutdown is not active.

This design supports crash-loop rollback when the process is restarted by an external supervisor (for example launchd/systemd).

### 6. Local health endpoint

Add a small localhost HTTP endpoint in service mode:

1. Bind host default: `127.0.0.1`.
2. Configurable port; `0` disables endpoint.
3. `GET /healthz` returns:
   - `200` with JSON (`status=healthy`, `active_sha`, `uptime_seconds`) when healthy.
   - `503` with JSON reason (`starting`, `auth_shutdown_pending`, `rollback_in_progress`, or `fatal_error`) otherwise.

The endpoint is for local checks and external watchdog integration, not public traffic.

### 7. Rollback behavior

Rollback is `git_checkout` only in v1:

1. `git -C <git_checkout_root> fetch origin <branch> --prune --tags`.
2. `git -C <git_checkout_root> checkout -B <branch> <previous_sha>`.
3. `uv sync`.
4. Attempt to open a GitHub issue (no labels) describing the rollback event, including:
   - failed target revision SHA (`target_sha`)
   - previous healthy revision SHA (`previous_sha`)
   - stack trace and/or failure details captured from the failed boot attempt
5. If issue creation fails (for example due to missing issue-write permission), continue rollback, emit a structured warning event, and persist the issue-creation failure detail in deploy state.
6. Mark state `rolled_back`, set `blocked_target_sha=target_sha`, set error/detail.
7. Re-exec service.

If rollback itself fails, mark state `failed`, emit fatal log event, and require operator intervention.

### 8. Interaction with manual restart commands

1. `/mergexo restart` remains unchanged.
2. Manual and continuous deploy share the same single-flight restart gate.
3. Continuous deploy requests do not create or update `operator_commands` records.
4. If manual restart is pending/running, continuous checks do nothing.

## Configuration changes

Add runtime keys:

1. `continuous_deploy_enabled` (bool, default `false`).
2. `continuous_deploy_check_interval_seconds` (int, default `300`, min `10`).
3. `continuous_deploy_branch` (string, default `"main"`).
4. `continuous_deploy_healthcheck_host` (string, default `"127.0.0.1"`).
5. `continuous_deploy_healthcheck_port` (int, default `8765`, allow `0` to disable).
6. `continuous_deploy_max_boot_failures` (int, default `2`, min `1`).

Validation rules:

1. Continuous deploy requires restart mode `git_checkout` to be present in `restart_supported_modes`.
2. `git_checkout_root` must be set or resolvable to an existing git checkout.
3. Healthcheck port must be `0` or in valid TCP range.

## Implementation plan

1. Extend `RuntimeConfig` and config parsing/validation in `src/mergexo/config.py`.
2. Add state model dataclass(es) in `src/mergexo/models.py` for continuous deploy status.
3. Add `continuous_deploy_state` schema and migration-safe init in `src/mergexo/state.py`.
4. Add state-store APIs for start attempt, boot-attempt increment, mark healthy, mark rollback, and read blocked target.
5. Add service-runner timer/check logic to detect updates only when idle.
6. Add git revision comparison helpers in `ServiceRunner` for remote-head detection and divergence checks.
7. Add helper to request restart operation for continuous deploy using synthetic command keys.
8. Add startup reconciliation path that processes pending deploy attempts and triggers rollback on repeated failed boots.
9. Add localhost health endpoint lifecycle in `src/mergexo/service_runner.py`.
10. Add rollback execution path and quarantine semantics for bad target revisions.
11. Add best-effort rollback issue reporting via `GitHubGateway.create_issue(labels=None)` with failure-tolerant handling.
12. Update `mergexo.toml.example` and README operational docs.
13. Add tests for config parsing, state transitions, idle gating, deploy scheduling, healthy completion, rollback, rollback issue reporting, and quarantine behavior.

## Testing plan

1. `tests/test_config.py`
   - parses new continuous deploy keys and defaults.
   - rejects invalid intervals/ports/boot-failure thresholds.
2. `tests/test_state.py`
   - creates/migrates `continuous_deploy_state`.
   - verifies status transitions and blocked-target persistence.
3. `tests/test_service_runner.py`
   - does not schedule deploy when pending work exists.
   - schedules deploy when idle and remote head advances.
   - does not schedule when remote equals blocked target.
   - marks deploy healthy after successful poll cycle.
   - rolls back after repeated failed boots.
   - opens an unlabeled rollback issue that includes `target_sha` and captured stack trace details.
   - continues rollback when rollback issue creation fails (for example unauthorized issue write).
   - exposes `/healthz` with expected healthy/unhealthy statuses.
4. regression checks
   - manual `/mergexo restart` behavior unchanged.
   - multi-repo service polling cadence unchanged when continuous deploy disabled.

## Acceptance criteria

1. Continuous deploy is disabled by default and does not change current behavior when unset.
2. When enabled, MergeXO checks for upstream branch movement at configured intervals.
3. Continuous deploy never starts while any agent run is in flight.
4. When branch head advances and service is idle, MergeXO performs update + restart via existing drain mechanics.
5. A successful post-restart poll cycle marks the new revision healthy.
6. If updated revision repeatedly fails before becoming healthy, MergeXO rolls back to the previous healthy revision.
7. After rollback, the failed target revision is quarantined and not retried until branch head changes.
8. `/healthz` reports unhealthy during startup/rollback and healthy after successful startup.
9. Manual `/mergexo restart` remains functional and single-flight with continuous deploy.
10. Rollback opens an unlabeled GitHub issue containing failed `target_sha` plus captured stack trace/failure details.
11. Rollback still completes when rollback issue creation fails, with warnings/logging and persisted diagnostics.
12. README and example config document enablement, requirements, and rollback semantics.

## Risks and mitigations

1. Risk: false-positive rollbacks due transient startup failures.
Mitigation: require repeated failed boots before rollback (`continuous_deploy_max_boot_failures`).

2. Risk: update loop on permanently bad upstream commit.
Mitigation: persist `blocked_target_sha` quarantine and retry only when target changes.

3. Risk: local checkout drift or diverged branch.
Mitigation: enforce fast-forward-only eligibility; skip auto-deploy on divergence with explicit log events.

4. Risk: no rollback if process is not externally restarted after crash.
Mitigation: document requirement to run service under OS supervisor for crash-loop recovery.

5. Risk: localhost health endpoint misconfiguration.
Mitigation: bind localhost by default, allow disabling with port `0`, and validate config strictly.

## Rollout notes

1. Land schema/config/service changes behind `continuous_deploy_enabled = false`.
2. Canary on MergeXO’s own deployment with:
   - short check interval (for example 60-120s),
   - health endpoint enabled,
   - supervisor restart policy enabled.
3. Validate canary scenarios:
   - idle auto-update after merge to `main`,
   - no update while work in flight,
   - forced bad revision rolls back and quarantines.
4. After canary stability, document as recommended mode for self-hosted MergeXO instances that track `main`.
