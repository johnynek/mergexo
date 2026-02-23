---
issue: 54
priority: 3
touch_paths:
  - docs/design/54-improve-concurrency-in-multi-repo-mode.md
  - src/mergexo/service_runner.py
  - src/mergexo/orchestrator.py
  - tests/test_service_runner.py
  - tests/test_orchestrator.py
depends_on: []
estimated_size: M
generated_at: 2026-02-23T20:25:17Z
---

# Design doc for issue #54: improve concurrency in multi-repo mode

_Issue: #54 (https://github.com/johnynek/mergexo/issues/54)_

## Summary

Proposes a non-blocking multi-repo service scheduler with a shared thread pool, shared global capacity limiter, and process-global restart drain logic.

---
issue: 54
priority: 2
touch_paths:
  - src/mergexo/service_runner.py
  - src/mergexo/orchestrator.py
  - tests/test_service_runner.py
  - tests/test_orchestrator.py
depends_on:
  - 33
estimated_size: M
generated_at: 2026-02-23T00:00:00Z
---

# Improve concurrency in multi-repo mode

_Issue: #54 (https://github.com/johnynek/mergexo/issues/54)_

## Summary

Move multi-repo service scheduling from blocking per-repo `run(once=True)` calls to non-blocking per-repo `poll_once(pool)` calls on a shared process-wide worker pool. Keep round-robin polling, enforce one global in-flight limit, and move restart drain decisions to process scope.

## Context

Current behavior in code:
1. `ServiceRunner.run` creates a new `Phase1Orchestrator` per repo turn and calls `orchestrator.run(once=True)` in multi-repo mode.
2. `Phase1Orchestrator.run(once=True)` waits for all local workers via `_wait_for_all` before returning.
3. Result: one long repo turn blocks polling of the next repo.
4. Worker capacity is enforced per orchestrator via local `_running` and `_running_feedback`, not across repos.
5. Restart drain in `_drain_for_pending_restart_if_needed` is also per orchestrator, so drain decisions are not process-global.

## Goals

1. Keep existing round-robin poll order and sleep cadence in service mode.
2. Ensure a long-running repo does not block polling and enqueue opportunities for other repos.
3. Treat `runtime.worker_count` as a process-global active-agent cap across all repos.
4. Make restart drain global so restart waits for all repos.
5. Preserve existing behavior for non-service flows unless required.

## Non-goals

1. Introducing strict per-repo quotas or weighted fairness.
2. Changing GitHub API polling cadence semantics.
3. Redesigning issue-selection priority inside each repo.
4. Schema migration work.

## Review of proposed idea

1. One long-lived `ThreadPoolExecutor` for the whole service process: adopt.
2. One long-lived `Phase1Orchestrator` per repo: adopt.
3. Add `poll_once(pool)` that reaps, scans, enqueues, and returns immediately: adopt, with an `allow_enqueue` switch for drain phases.
4. Add process-global capacity limiter: adopt.
5. Move restart drain logic to process-global scope: adopt.

## Proposed architecture

### 1. Service-level scheduler ownership

`ServiceRunner` will own:
1. One shared `ThreadPoolExecutor(max_workers=runtime.worker_count)` for entire service lifetime.
2. One shared `GlobalWorkLimiter` with capacity `runtime.worker_count`.
3. One long-lived orchestrator instance per repo.
4. One process-global restart drain timer (`_restart_drain_started_at_monotonic`).

### 2. Orchestrator non-blocking poll API

Add `Phase1Orchestrator.poll_once(pool, *, allow_enqueue=True) -> None`.

Behavior per call:
1. Ensure git layout once, guarded by internal boolean state.
2. Reap finished issue and feedback futures and persist state updates.
3. Scan operator commands when enabled.
4. Enqueue new work only if `allow_enqueue` is true and restart is not pending.
5. Return immediately and never wait for all futures.

`run(once=...)` remains for compatibility, but service mode will use `poll_once`.

### 3. Shared global capacity limiter

Add a thread-safe limiter used by all orchestrators in service mode:
1. `try_acquire()` returns false when process-wide capacity is full.
2. `release()` returns one global slot.
3. `in_flight()` exposes current process-wide active count.

Enqueue flow updates:
1. Acquire token before submitting each future.
2. On submit success, attach `future.add_done_callback` to release token.
3. On submit failure, release token immediately.
4. Local per-orchestrator maps (`_running`, `_running_feedback`) remain for state reconciliation and idempotent completion handling.

### 4. ServiceRunner loop changes

Continuous mode:
1. Poll one repo per iteration in current round-robin order.
2. Call `orchestrator.poll_once(pool, allow_enqueue=not global_restart_draining)`.
3. Evaluate process-global restart drain state.
4. Sleep `poll_interval_seconds` exactly as today.

`--once` mode:
1. Poll each repo once with `allow_enqueue=True`.
2. Enter a global drain loop polling all repos with `allow_enqueue=False` until all pending futures across all orchestrators are reaped.
3. Exit.

### 5. Process-global restart drain

Move drain decisions to `ServiceRunner`:
1. Read `restart` runtime operation from `StateStore`.
2. If no pending or running operation, clear global drain timer.
3. If operation is pending or running, disable enqueue in all repos.
4. Continue polling repos to reap and reconcile futures.
5. If total pending futures across all repos is zero, mark runtime operation `running` and trigger existing `_handle_restart_requested(...)` path.
6. If timeout exceeds `runtime.restart_drain_timeout_seconds`, mark operation and command failed, post operator reply in the originating repo, and clear drain timer.

Important detail:
Restart completion must key off zero pending futures after reap, not only limiter `in_flight`, to avoid restarting before completion state is written.

### 6. Observability updates

Add service-level events:
1. `service_repo_polled` with repo and queue counts.
2. `service_global_capacity` with in-flight count.
3. `service_restart_drain_started`.
4. `service_restart_drain_progress`.
5. `service_restart_drain_completed`.
6. `service_restart_drain_timeout`.

Existing issue and PR lifecycle events remain unchanged.

## Implementation plan

1. Extend `Phase1Orchestrator` constructor to accept optional shared capacity limiter.
2. Add `poll_once(pool, allow_enqueue=True)` and wire existing enqueue paths through it.
3. Update enqueue methods to claim and release global tokens around future submission.
4. Keep `run(once)` available for compatibility and tests.
5. Refactor `ServiceRunner.run` to build orchestrators once and hold one shared thread pool across the entire run.
6. Replace multi-repo `run(once=True)` calls with round-robin `poll_once(...)` calls.
7. Add process-global restart drain helper methods in `ServiceRunner`.
8. Update `tests/test_service_runner.py` for non-blocking multi-repo polling and global restart drain.
9. Update `tests/test_orchestrator.py` for `poll_once` and shared limiter behavior.

## Testing plan

1. Verify non-blocking multi-repo polling by simulating a long-running future in repo A and asserting repo B still gets polled on the next turn.
2. Verify service creates one shared executor for all repos.
3. Verify process-wide active futures never exceed `worker_count` across two active repos.
4. Verify completed futures release global capacity immediately via callback.
5. Verify restart requested in repo A waits for repo B active work before restart handling.
6. Verify restart timeout marks operation and command as failed and posts one operator reply.
7. Verify `service --once` polls each repo once, drains started work, and exits.
8. Verify single-repo service behavior remains functionally equivalent.

## Acceptance criteria

1. In multi-repo service mode, polling repo B is not blocked by repo A running workers.
2. `ServiceRunner` reuses one orchestrator instance per repo across polls.
3. `ServiceRunner` uses one process-wide thread pool for all repos.
4. Process-wide active work never exceeds `runtime.worker_count`.
5. Work from different repos can execute concurrently in the shared pool.
6. Restart drain halts new enqueue across all repos once restart is pending.
7. Restart is executed only after all repos have zero pending futures.
8. Restart timeout handling remains correct and posts status exactly once.
9. `service --once` still returns after one global poll cycle plus drain.
10. Existing single-repo service behavior remains functionally unchanged.

## Risks and mitigations

1. Risk: capacity token leaks can deadlock enqueue.
Mitigation: release in done callback, release on submit failure, and invariant tests.

2. Risk: restart before state reconciliation if drain checks only running-token count.
Mitigation: restart gate requires zero pending futures after reap across all orchestrators.

3. Risk: one hot repo can still capture most slots under first-come enqueue.
Mitigation: round-robin poll removes hard blocking starvation; strict fair-share is a follow-up item.

4. Risk: service loop complexity increases.
Mitigation: isolate restart-drain and once-drain helpers with dedicated unit tests.

## Rollout notes

1. No config or schema changes required.
2. Deploy to a two-repo canary with `worker_count > 1`.
3. Validate cross-repo polling while one repo runs long tasks.
4. Validate total active agents never exceeds configured `worker_count`.
5. Validate restart command drains globally before update and reexec.
6. Monitor for drain timeout anomalies and limiter invariant failures during canary window.
7. Roll out broadly after stable canary.
