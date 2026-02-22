---
issue: 23
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/github_gateway.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/models.py
  - src/mergexo/service_runner.py
  - mergexo.toml.example
  - README.md
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_github_gateway.py
  - tests/test_feedback_loop.py
  - tests/test_service_runner.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T17:49:02Z
---

# Design approach for full GitHub operation

_Issue: #23 (https://github.com/johnynek/mergexo/issues/23)_

## Summary

Add a GitHub-driven operator control plane so maintainers can unblock blocked PRs and request MergeXO restarts from comments, backed by durable command state, idempotent acknowledgements, and a supervisor-based restart/update path that is explicit about today's supported `uv`-from-checkout deployment while keeping a clear extension point for future PyPI installs.

## Context
MergeXO currently needs local shell access for two operational actions:
1. Unblocking PRs that entered `blocked` feedback state (`mergexo feedback blocked reset ...`).
2. Restarting MergeXO after updating code or package version.

Issue #23 asks for full GitHub operation for both actions. The near-term environment is a git checkout deployment, but the design must also accommodate a future PyPI deployment model.

## Goals
1. Allow maintainers to unblock blocked PRs from GitHub comments without SSH/shell access.
2. Allow maintainers to request a safe MergeXO restart from GitHub.
3. Add a `/mergexo help` command that returns the current command set and a README link.
4. Ensure every `/mergexo ...` command receives a deterministic success/failure/rejection reply.
5. Make restart flow explicit for today's supported `uv` checkout runtime, with a future-compatible `pypi` mode.
6. Keep command handling idempotent across retries/crashes.
7. Keep an auditable state trail of who requested what and when.

## Non-goals
1. Arbitrary remote command execution from GitHub comments.
2. Replacing the current feedback-loop logic or state machine.
3. Webhook migration in this issue (polling remains the event source).
4. Automatic rollback/version pinning beyond basic failure reporting.

## Proposed Architecture

### 1. GitHub Operator Command Channel
Introduce a narrow command grammar in issue comments:
1. `/mergexo unblock`
2. `/mergexo unblock head_sha=<sha>`
3. `/mergexo unblock pr=<number> [head_sha=<sha>]`
4. `/mergexo restart`
5. `/mergexo restart mode=git_checkout|pypi`
6. `/mergexo help`

Command sources:
1. Blocked PR threads: primary place for `unblock`.
2. A configured operations issue (`repo.operations_issue_number`): place for global ops (`restart`, cross-PR `unblock pr=...`).

Command key for dedupe: `issue_number:comment_id:updated_at`.

Command reply contract:
1. Every recognized `/mergexo ...` command produces exactly one idempotent result comment in the same issue/PR conversation.
2. Result comment includes: normalized command, status (`applied`, `rejected`, `failed`, or `help`), and brief detail.
3. `/mergexo help` always responds with the supported commands and a link to `README.md#github-operator-commands`.
4. Parse failures (unknown subcommand or bad args) also receive a usage reply so operators are never silent-failed.
5. Each result comment includes a reference to the originating command comment URL so the response is traceable as a thread-level reply.

### 2. Authorization Model
1. Add explicit operator allowlist in config: `repo.operator_logins`.
2. Ignore bot users and non-allowlisted users.
3. For unauthorized commands, post one idempotent rejection comment and mark command as rejected in state.
4. Treat command parsing and auth as deterministic control-plane logic (never delegated to agent output).

### 3. State and Idempotency
Add durable tables in `StateStore`:
1. `operator_commands`:
- `command_key` primary key
- `issue_number`
- `pr_number` nullable
- `comment_id`
- `author_login`
- `command`
- `args_json`
- `status` (`applied`, `rejected`, `failed`)
- `result`
- `created_at`, `updated_at`

2. `runtime_operations`:
- `op_name` (initially only `restart`)
- `status` (`pending`, `running`, `failed`, `completed`)
- `requested_by`
- `request_command_key`
- `mode` (`git_checkout` or `pypi`)
- `detail`
- `created_at`, `updated_at`

Use deterministic action tokens on command-result comments so crash windows do not duplicate acknowledgements.

### 4. Unblock Operation Flow
1. On every poll, orchestrator scans blocked PR comment streams plus the operations issue comment stream.
2. Parse unprocessed command comments into operator command records.
3. Validate target PR is currently blocked.
4. Execute `state.reset_blocked_pull_requests(...)` with optional `last_seen_head_sha_override` from `head_sha`.
5. Post success/failure result on the target PR with an action token.
6. Persist command final status in `operator_commands`.

Key detail: this scan is independent of `_enqueue_feedback_work`, so blocked PRs remain operable even though feedback scheduling skips blocked rows.

### 5. Restart Operation Flow
1. Command handler creates/updates a single pending restart record in `runtime_operations`.
2. Orchestrator enters drain mode:
- stop enqueueing new issue/implementation/feedback work
- continue reaping existing futures until zero active workers
3. When drained, orchestrator raises a typed restart signal (`RestartRequested`) with requested mode.
4. A new supervisor runtime (`mergexo service`) catches that signal and performs update+restart.

If update fails, supervisor marks operation `failed`, posts a failure comment reply to the originating command thread, and keeps the current process running.

### 6. UV-First Update and Restart Strategy
Add updater modes in config:
1. `git_checkout` mode (supported now, default):
- Assume MergeXO is launched from the mergexo repository checkout root.
- Service launch command is explicitly:
  `uv run mergexo service --config <path-to-mergexo.toml>`
- Update commands are explicitly:
  `git -C <checkout_root> pull --ff-only origin <default_branch>`
  `uv sync`
- Restart is explicit re-exec of the same service command and args used at boot (for example, `uv run mergexo service --config <path-to-mergexo.toml>` plus flags such as `--verbose` when present).

2. `pypi` mode (future deployments):
- Not enabled in the first rollout, but reserved in the command grammar and state model.
- When enabled, use a configured interpreter path and upgrade without assuming system-wide install, for example:
  `uv pip install --python <service_python> --upgrade mergexo`
- Restart still re-execs the same recorded service command line.

The command-level `mode=` argument can override default mode per restart request, but only to supported configured modes.

### 7. CLI and Process Model
1. Keep `mergexo run` for direct local execution.
2. Add `mergexo service` as the production entrypoint for GitHub-operated restarts.
3. `service` wraps orchestrator lifecycle, performs update strategy, and re-execs itself on successful restart operation using the exact startup command line captured at process boot.
4. Document that GitHub restart commands require `service` mode for fully automatic update+restart behavior.

### 8. Observability and Audit
Add structured events:
1. `operator_command_seen`
2. `operator_command_applied`
3. `operator_command_rejected`
4. `operator_command_failed`
5. `restart_requested`
6. `restart_drain_started`
7. `restart_drain_completed`
8. `restart_update_started`
9. `restart_update_failed`
10. `restart_completed`

All events include command key, actor, issue/PR target, and operation mode when applicable.

## Implementation Plan
1. Extend config models and sample config for operator channels, allowlist, and updater mode/commands.
2. Add state schema and APIs for operator commands and runtime operations.
3. Extend GitHub gateway with generic issue-comment listing needed for operations issue scanning.
4. Add operator command parser and token helpers.
5. Add orchestrator operation scan stage and unblock handler.
6. Add restart request ingestion and orchestrator drain behavior.
7. Add `service_runner` module and `mergexo service` CLI command.
8. Implement updater executors for `git_checkout` and `pypi` modes.
9. Update README with GitHub ops workflow and deployment requirements.
10. Add/expand tests across config, state, gateway, orchestrator, feedback tokening, CLI, and service runner.

## Acceptance Criteria
1. A blocked PR can be unblocked by an allowlisted maintainer via `/mergexo unblock` comment on that PR.
2. `/mergexo unblock head_sha=<sha>` writes the override to `last_seen_head_sha` and returns PR to `awaiting_feedback`.
3. `/mergexo unblock pr=<number>` works from the operations issue for blocked PRs.
4. Duplicate processing of the same command comment does not produce duplicate state mutations or duplicate acknowledgement comments.
5. Unauthorized command authors do not mutate state and receive a deterministic rejection response.
6. `/mergexo restart` creates a pending restart operation visible in state and logs.
7. During restart drain, no new issue/design/implementation/feedback work is enqueued.
8. After all active workers finish, supervisor executes configured update commands and restarts MergeXO.
9. In `git_checkout` mode, restart path runs configured checkout update/sync commands before re-exec.
10. `/mergexo help` returns the supported command list and `README.md#github-operator-commands`.
11. Every `/mergexo` command (including parse failures and unauthorized requests) receives one deterministic result reply in the same issue/PR conversation.
12. In future `pypi` mode, restart path runs configured package upgrade command through `uv` against a configured interpreter before re-exec.
13. If update command fails, restart operation is marked failed, a GitHub result reply is posted for that command, and current process continues.
14. Existing issue intake and feedback behavior is unchanged when GitHub operations are disabled.

## Risks and Mitigations
1. Risk: command abuse from public comments.
Mitigation: strict `operator_logins` allowlist, bot filtering, and auditable command table.

2. Risk: restart thrash from repeated commands.
Mitigation: single-flight restart operation in state; additional restart commands collapse into one pending request.

3. Risk: update command failures leave automation unavailable.
Mitigation: fail-safe behavior keeps current process alive and reports failure on GitHub.

4. Risk: drain can take too long if workers hang.
Mitigation: configurable drain timeout with explicit failure result and no forced kill in MVP.

5. Risk: extra polling API load from operations scanning.
Mitigation: only scan blocked PRs plus one operations issue; cap fetched comments per poll and reuse existing poll interval.

## Rollout Notes
1. Ship behind `runtime.enable_github_operations = false` by default.
2. Canary unblock-only first on one repository and one or two operator accounts.
3. Validate idempotency by forcing process restarts mid-command handling.
4. Enable restart in `git_checkout` mode after unblock is stable.
5. Add a README section `github-operator-commands` and move production launch instructions to `mergexo service` for GitHub-operated instances.
6. Add `pypi` mode only after package publishing pipeline is stable.
7. Keep local CLI unblock command as break-glass fallback during rollout.
