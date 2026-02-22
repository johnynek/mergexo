---
issue: 8
status: implemented
priority: 3
touch_paths:
  - src/mergexo/cli.py
  - src/mergexo/observability.py
  - src/mergexo/orchestrator.py
  - src/mergexo/git_ops.py
  - src/mergexo/github_gateway.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/shell.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_git_ops.py
  - tests/test_github_gateway.py
  - tests/test_shell.py
  - tests/test_observability.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-02-22T01:29:53Z
implemented_at: 2026-02-22T02:27:46Z
---

# Add Verbose Runtime Mode with Structured Event Logging

_Issue: #8 (https://github.com/johnynek/mergexo/issues/8)_

## Summary

Introduce a `--verbose` CLI flag that enables structured stderr logs for poll events and side-effect actions (git, GitHub, and agent turns), while keeping default behavior quiet via a null logger and preserving existing command semantics.

## Context
`mergexo run` currently performs polling, queueing, git operations, and GitHub writes with little to no runtime visibility. This makes it hard to debug whether the system is idle, blocked, retrying, or actively taking actions.

Issue #8 requests a composable `--verbose` mode that prints events and actions as they happen, with a design similar to standard Python logging.

## Goals
1. Add `--verbose` mode to CLI execution paths (`run`, and optionally `init`) so runtime behavior is visible in real time.
2. Keep default behavior quiet (null logger style) to avoid changing current UX for normal operation.
3. Keep logging composable and centralized so future phases can add events without ad hoc `print` calls.
4. Emit useful correlation fields (issue, PR, branch, worker slot) for debugging concurrent runs.
5. Avoid logging sensitive free-form content (issue bodies, prompts, command input payloads).

## Non-goals
1. File-based log persistence and rotation.
2. Multiple verbosity tiers (`-vv`) or a full debug/trace mode in this issue.
3. TOML-configured log levels.
4. Retrofitting structured JSON logs for external log ingestion.

## Proposed architecture
### 1. Central observability module
Add `src/mergexo/observability.py` with two responsibilities:
1. `configure_logging(verbose: bool) -> None`
2. `log_event(logger: logging.Logger, event: str, **fields: object) -> None`

`configure_logging` behavior:
1. Configure the `mergexo` logger tree (not global root) to avoid unrelated third-party log noise.
2. `verbose=False`: attach `NullHandler`, set non-emitting level, disable propagation.
3. `verbose=True`: attach `StreamHandler(sys.stderr)` with stable format including timestamp, level, logger name, and thread name.
4. Reconfiguration is idempotent so repeated CLI entry in tests does not duplicate handlers.

`log_event` behavior:
1. Emits a single INFO line with stable event name plus sorted `key=value` fields.
2. Keeps call sites concise and consistent.
3. Rejects multiline or complex payloads by string-normalizing to short scalar values.

### 2. CLI integration
Update `src/mergexo/cli.py`:
1. Add `-v`/`--verbose` flag to subcommands where runtime actions occur (`run`; `init` can share for consistency).
2. Call `configure_logging(args.verbose)` once during startup before command dispatch.
3. Preserve existing `init` summary prints so scripted workflows remain unchanged.

### 3. Event taxonomy
Use stable event names so logs can be searched and asserted in tests.

Core event groups:
1. Polling and queueing: `poll_started`, `poll_completed`, `issues_fetched`, `issue_enqueued`, `issue_skipped`.
2. Worker lifecycle: `slot_acquired`, `slot_released`, `issue_processing_started`, `issue_processing_completed`, `issue_processing_failed`.
3. Feedback loop: `feedback_scan_started`, `feedback_events_pending`, `feedback_turn_started`, `feedback_turn_completed`, `feedback_turn_blocked`.
4. Side effects: `git_prepare_checkout`, `git_branch_reset`, `git_commit`, `git_push`, `github_pr_created`, `github_issue_comment_posted`, `github_review_reply_posted`.
5. Agent boundary: `design_turn_started`, `design_turn_completed`, `feedback_agent_call_started`, `feedback_agent_call_completed`.

Common correlation fields where available:
1. `issue_number`
2. `pr_number`
3. `branch`
4. `slot`
5. `turn_key`

### 4. Instrumentation points
`src/mergexo/orchestrator.py`:
1. Poll cycle start/end and fetched counts.
2. Per-issue enqueue decisions and skip reasons.
3. Issue processing milestones from checkout prep through PR creation/comment.
4. Feedback path milestones: pending event counts, missing session block, head mismatch retry, action emission, finalize status.

`src/mergexo/git_ops.py`:
1. Log high-level mutating operations with branch/path/slot context.
2. Keep command internals compact; no raw command output in INFO path.

`src/mergexo/github_gateway.py`:
1. Log outbound writes (PR create, comments, review replies).
2. Log read operations as endpoint + count only.

`src/mergexo/codex_adapter.py`:
1. Log turn boundaries and session/thread identifiers.
2. Never log raw prompts or raw event stream content.

`src/mergexo/shell.py`:
1. On non-zero exit, emit one ERROR record with command and exit code before raising.
2. Do not log `input_text` to prevent token/prompt leakage.

## Implementation plan
1. Add `src/mergexo/observability.py` with logger configuration and event helper.
2. Add CLI `--verbose` wiring and startup logging configuration in `src/mergexo/cli.py`.
3. Instrument orchestrator event boundaries first (highest operator value).
4. Add side-effect instrumentation in git/GitHub/adapter/shell modules.
5. Extend tests for parser behavior and log emission/no-emission cases.
6. Update `README.md` with `--verbose` usage guidance.

## Testing plan
1. `tests/test_cli.py`: parser accepts `--verbose`; dispatch configures logging with expected boolean.
2. `tests/test_observability.py`: `configure_logging` idempotency and `log_event` formatting contract.
3. `tests/test_orchestrator.py`: verbose mode emits expected lifecycle events in happy and failure paths.
4. `tests/test_git_ops.py`, `tests/test_github_gateway.py`, `tests/test_shell.py`: targeted assertions for key action/error log events without changing existing behavior.

## Acceptance criteria
1. `uv run mergexo run --config mergexo.toml --once --verbose` emits INFO logs showing poll activity and at least one action event.
2. Running the same command without `--verbose` emits no new INFO logs from `mergexo` logger.
3. Issue processing logs include `issue_number` and branch context for enqueue/start/completion or failure.
4. Feedback processing logs include `pr_number` and indicate whether a turn was skipped, blocked, retried, or completed.
5. GitHub write actions (PR create/comment/reply) are visible in verbose mode.
6. Command failures still raise existing exceptions and also emit one ERROR log with command + exit code.
7. No verbose log line includes raw issue bodies, PR comment bodies, Codex prompts, or command input payloads.
8. Existing non-logging behavior remains unchanged (same control flow and existing tests still pass with expected updates).

## Risks and mitigations
1. Risk: log volume becomes noisy during continuous mode.
Mitigation: keep INFO logs at lifecycle boundaries, reserve low-level details for future debug tier.

2. Risk: sensitive content leaks into logs.
Mitigation: log identifiers and counts only; explicitly avoid body/prompt/input payload logging.

3. Risk: concurrent workers interleave output and reduce readability.
Mitigation: include thread name and correlation fields (`issue_number`, `pr_number`, `slot`) on every major event.

4. Risk: logger setup becomes brittle across tests or repeated entrypoint calls.
Mitigation: centralize setup in `configure_logging` and make it deterministic/idempotent.

## Rollout notes
1. Ship with default silent mode; only `--verbose` enables output.
2. Merge in two steps if needed: logging bootstrap + CLI flag first, then instrumentation coverage.
3. Validate with one full `--once --verbose` issue-to-PR run and one feedback-loop run.
4. After rollout, use verbose logs as the primary operator workflow for debugging stuck or blocked runs.
