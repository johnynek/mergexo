---
issue: 77
priority: 3
touch_paths:
  - docs/design/77-tui-should-be-the-default-mode.md
  - src/mergexo/cli.py
  - src/mergexo/default_mode.py
  - src/mergexo/service_runner.py
  - src/mergexo/observability_tui.py
  - src/mergexo/observability.py
  - tests/test_cli.py
  - tests/test_service_runner.py
  - tests/test_observability_tui.py
  - tests/test_default_mode.py
  - tests/test_observability.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-02-24T20:13:02Z
---

# Design doc for issue #77: TUI should be the default mode

_Issue: #77 (https://github.com/johnynek/mergexo/issues/77)_

## Summary

Defines a default `mergexo` console mode that runs service + TUI + file logging together, with a thread-safe bridge between the synchronous service loop and Textual asyncio UI.

---
issue: 77
priority: 2
touch_paths:
  - docs/design/77-tui-should-be-the-default-mode.md
  - src/mergexo/cli.py
  - src/mergexo/default_mode.py
  - src/mergexo/service_runner.py
  - src/mergexo/observability_tui.py
  - src/mergexo/observability.py
  - tests/test_cli.py
  - tests/test_service_runner.py
  - tests/test_observability_tui.py
  - tests/test_default_mode.py
  - tests/test_observability.py
  - README.md
depends_on:
  - 54
  - 65
estimated_size: M
generated_at: 2026-02-24T00:00:00Z
---

# TUI should be the default mode

_Issue: #77 (https://github.com/johnynek/mergexo/issues/77)_

## Summary

Running `mergexo` with no subcommand should launch a combined console mode that starts the service loop, shows the observability TUI, and writes runtime logs to file by default.

## Problem statement

1. Today `mergexo` requires a subcommand and exits with argparse usage text.
2. Operators need two commands for normal operations: `mergexo service` and `mergexo top`.
3. The service loop is synchronous and thread-pool based, while `top` runs on Textual asyncio.
4. File logging is only enabled when verbose mode is explicitly set.
5. Issue #77 asks for a default mode that runs service, top, and file logging in one entrypoint, and asks whether loops should be unified.

## Goals

1. Bare `mergexo` starts the production supervisor behavior and the TUI in one invocation.
2. Default mode enables file logging without requiring `--verbose`.
3. Service and TUI run concurrently without blocking each other.
4. Keep `state.db` as the authoritative data source for dashboard panels.
5. Preserve existing explicit commands (`init`, `run`, `service`, `top`, `feedback`) for scripts and operators.
6. Keep restart behavior in service mode working with the new default command.

## Non-goals

1. Rewriting orchestrator and service internals to full asyncio in this issue.
2. Replacing SQLite polling with a full pub/sub event architecture.
3. Changing existing state schema for this issue.
4. Adding write actions to the TUI.

## Current behavior and constraints

1. `src/mergexo/cli.py` requires a subcommand via `add_subparsers(required=True)`.
2. `src/mergexo/service_runner.py` runs a blocking poll loop with `ThreadPoolExecutor` and `time.sleep`.
3. `src/mergexo/observability_tui.py` runs a Textual app on an asyncio loop and refreshes from SQLite on an interval.
4. `src/mergexo/observability.py` only adds file logging when verbose mode is enabled.
5. SQLite is already in WAL mode in `StateStore`, which supports concurrent readers and writers.

## Proposed architecture

### 1. CLI default entrypoint

1. Add a new explicit command `console` that means service + TUI + default file logging.
2. Make bare `mergexo` equivalent to `mergexo console`.
3. Keep existing subcommands unchanged.
4. `console` accepts `--config` and `--verbose` with the same meanings as current commands.
5. If `--verbose` is omitted in `console`, default to `low` so logs are written to stderr and `<runtime.base_dir>/logs/YYYY-MM-DD.log`.

### 2. Combined runtime supervisor

1. Add `src/mergexo/default_mode.py` with a coordinator function `run_default_mode(...)`.
2. Run the Textual app on the main thread.
3. Run `run_service(...)` on a background thread.
4. Share one `state.db` path and existing runtime config.
5. Add a `threading.Event` shutdown signal passed into service runtime so TUI quit cleanly stops the service loop.
6. Ensure the coordinator joins the service thread on exit and propagates service exceptions as process failure.

### 3. Service to UI event integration

1. Keep SQLite as the source of truth for all UI tables.
2. Add an optional thread-safe signal sink in service runner that emits coarse lifecycle hints:
   - poll completed
   - work reaped
   - restart drain state changed
   - fatal service error
3. In `ObservabilityApp`, add optional queue draining on a short interval (for example 200 ms) and trigger debounced refresh when hints arrive.
4. Keep existing periodic refresh as fallback so UI stays correct even if hint delivery is delayed or dropped.
5. Do not stream full internal event payloads into the UI. Hints only request refresh or surface fatal errors.

### 4. Event loop strategy decision

1. Do not unify service loop and TUI loop in this issue.
2. Keep service synchronous and threaded because GitHub and git paths are currently blocking and stable.
3. Keep the Textual asyncio loop isolated on the main thread.
4. Bridge loops with thread-safe primitives (`Event` and `SimpleQueue`) instead of cross-loop await patterns.
5. Revisit full asyncio unification only if profiling shows a real bottleneck after rollout.

### 5. Logging behavior

1. `console` mode computes effective verbosity as:
   - `args.verbose` when provided
   - otherwise `low`
2. Existing explicit commands keep current behavior (no logging unless user requested `--verbose`).
3. `configure_logging(..., state_dir=...)` remains the owner of file handler setup and UTC daily rotation.
4. No new log format is introduced.

### 6. Shutdown and restart semantics

1. Pressing `q` in TUI initiates coordinator shutdown: set stop event, wait for service thread, then exit.
2. Service fatal errors are forwarded to UI via queue. UI shows a modal or summary and exits non-zero.
3. Runtime restart operations continue to re-exec process argv through existing service runner logic.
4. When launched as bare `mergexo`, restart re-execs the same default mode command so service and TUI both come back.

## Implementation plan

1. CLI and dispatch:
   - update `src/mergexo/cli.py` to support `console` and map empty argv to `console`
   - add `_cmd_console(...)` that initializes state and invokes the default mode runner
2. Default mode coordinator:
   - add `src/mergexo/default_mode.py`
   - implement thread lifecycle, queue wiring, and exit or error propagation
3. Service loop hooks:
   - extend `src/mergexo/service_runner.py` with optional stop-event checks and optional UI signal callback
   - replace fixed sleeps with stop-aware waits
4. TUI hooks:
   - extend `src/mergexo/observability_tui.py` to accept optional signal queue and optional shutdown callback
   - add debounced refresh trigger from queue hints
5. Logging defaults:
   - adjust `src/mergexo/cli.py` and, if needed, small helper paths in `src/mergexo/observability.py` to apply `console` default verbosity
6. Docs:
   - update `README.md` quickstart and command docs for new default behavior

## Test plan

1. `tests/test_cli.py`
   - bare `mergexo` dispatches to `console`
   - `mergexo --config <path>` dispatches to `console`
   - explicit subcommands still dispatch unchanged
   - `console` default verbosity is `low` when omitted
2. `tests/test_service_runner.py`
   - stop event breaks the continuous loop without waiting a full poll interval
   - UI signal callback receives poll and drain hints
3. `tests/test_observability_tui.py`
   - queue hints trigger refresh
   - fatal-error hint path exits app flow deterministically
4. `tests/test_default_mode.py` (new)
   - coordinator starts service thread and TUI
   - TUI exit sets stop event and joins thread
   - service exception propagates as failure
5. `tests/test_observability.py`
   - file logging still rotates correctly
   - `low` mode writes the expected event subset in default mode

## Acceptance criteria

1. Running `uv run mergexo` starts service activity and opens the TUI without requiring a subcommand.
2. Default mode writes logs to `<runtime.base_dir>/logs/YYYY-MM-DD.log` even when `--verbose` is not passed.
3. Existing commands `init`, `run`, `service`, `top`, and `feedback` continue to work as before.
4. TUI remains responsive while service polls and workers execute.
5. TUI data stays consistent with SQLite state while service is active.
6. Quitting the TUI shuts down the service loop cleanly and exits process.
7. Service fatal errors are visible to operators and return non-zero exit status.
8. Restart command behavior remains functional when started through default mode.
9. Tests cover CLI dispatch, coordinator lifecycle, stop behavior, and TUI refresh hint handling.
10. README documents the new default invocation and explicit alternatives.

## Risks and mitigations

1. Risk: thread lifecycle bugs can leave background service running after UI exit.  
Mitigation: explicit stop event, bounded join timeout, and coordinator lifecycle tests.
2. Risk: frequent UI refresh from hints can increase SQLite read load.  
Mitigation: debounce hint-triggered refresh and keep bounded periodic fallback.
3. Risk: restart re-exec from a background thread can produce terminal edge cases.  
Mitigation: preserve existing restart path and add integration tests around startup argv and restart signaling.
4. Risk: bare command behavior change may surprise automation that relied on parse failure.  
Mitigation: keep explicit `mergexo service` documented for non-interactive scripts.
5. Risk: non-TTY environments may not support TUI.  
Mitigation: detect non-interactive terminal in coordinator and fail fast with guidance to use `mergexo service`.

## Rollout notes

1. Land in two steps:
   - step 1: coordinator, CLI default alias, and stop-aware service hooks
   - step 2: event-hint queue and TUI reactive refresh
2. Keep `mergexo service` and `mergexo top` available during rollout as fallback operations.
3. Canary on one repo with normal operator workflow for at least one day.
4. Validate log file creation, UI responsiveness under active work, clean shutdown on `q`, and restart command path.
5. After canary, update team runbooks to treat bare `mergexo` as the standard operator entrypoint.
