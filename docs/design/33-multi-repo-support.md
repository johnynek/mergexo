---
issue: 33
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/cli.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/git_ops.py
  - src/mergexo/service_runner.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_service_runner.py
  - tests/test_git_ops.py
depends_on: []
estimated_size: L
generated_at: 2026-02-22T00:00:00Z
---

# Multi repo support

_Issue: #33 (https://github.com/johnynek/mergexo/issues/33)_

## Summary

Allow one MergeXO process to monitor multiple repositories by supporting keyed repo tables in config, making auth and operator settings repo-local, and scoping all work and state rows by repository. Restart and update remain global operations for the single running process.

## Context

Current code assumes exactly one repo in `AppConfig.repo`, one `GitHubGateway`, one `GitRepoManager`, and state rows keyed only by issue/PR numbers. That does not scale to multi-repo operation because issue/PR numbers collide across repositories and auth policy needs to be repo-specific.

## Goals

1. Support one process monitoring N repositories from one config file.
2. Keep per-repo configuration for `operations_issue_number`, `operator_logins`, and `allowed_users`.
3. Keep a single internal runtime/data model regardless of one-repo or multi-repo config input.
4. Keep restart and update behavior global to the process.

## Non-goals

1. Cross-repo dependency scheduling.
2. Webhook redesign.
3. Per-repo worker-count limits in this issue (worker count remains global).
4. In-place migration from old single-repo DB schema.

## Proposed Architecture

### 1. Config contract and normalization

Update `src/mergexo/config.py` to normalize all config inputs into one internal representation: `AppConfig.repos`.

1. `RepoConfig` adds `repo_id` (local config identifier) and `allowed_users` (normalized allowlist).
2. Supported TOML shapes:
- Legacy single repo: `[repo]` + existing fields.
- Multi repo: `[repo.<id>]` entries.
3. Internal normalization:
- Legacy `[repo]` config is translated after parsing into one `RepoConfig` entry in `AppConfig.repos`.
- Multi repo config directly populates multiple `RepoConfig` entries.
4. `name` inference for keyed sections:
- In `[repo.<id>]`, when `name` is omitted, use `<id>` as GitHub repo name.
- When `name` is set, `<id>` remains only a local config identifier.
5. `allowed_users` placement:
- In multi-repo mode, `allowed_users` is optional per repo and defaults to `[owner]` when omitted.
- Global `[auth]` is not used in multi-repo mode.
- In legacy single-repo mode, `[auth].allowed_users` can be read as a compatibility fallback and translated into the single normalized repo entry.
6. Default values:
- `default_branch` defaults to `"main"`.
- `trigger_label` defaults to `"agent:design"`.
- `bugfix_label` defaults to `"agent:bugfix"`.
- `small_job_label` defaults to `"agent:small-job"`.
- `coding_guidelines_path` is required (no default).
- `design_docs_dir` defaults to `"docs/design"`.
- `operator_logins` defaults to an empty list.
- `operations_issue_number` defaults to unset.
- `allowed_users` rules:
  - Multi-repo mode: if omitted in a repo section, default to `[owner]`.
  - Legacy single-repo mode: if `repo.allowed_users` and `[auth].allowed_users` are both absent, default to `[owner]`.

Example multi-repo config shape:

```toml
[repo.mergexo]
owner = "johnynek"
# name omitted, resolves to "mergexo"
# default_branch defaults to "main"
# trigger_label defaults to "agent:design"
# bugfix_label defaults to "agent:bugfix"
# small_job_label defaults to "agent:small-job"
# coding_guidelines_path is required
# design_docs_dir defaults to "docs/design"
allowed_users = ["alice", "bob"]

[repo.bosatsu]
owner = "johnynek"
name = "bosatsu"
# default_branch defaults to "main"
# trigger_label defaults to "agent:design"
# coding_guidelines_path is required
# design_docs_dir defaults to "docs/design"
# allowed_users omitted => defaults to ["johnynek"]
```

### 2. Repo runtime contexts

Create one runtime context per configured repo:

- `RepoRuntime` contains `RepoConfig`, `GitHubGateway`, `GitRepoManager`, and normalized operator allowlist.
- `Phase1Orchestrator` operates on a map/list of repo runtimes.
- All repo-scoped logs include `repo_full_name`.

### 3. Global worker scheduling and split polling cadence

Keep `runtime.worker_count` as one global capacity, and adjust polling to preserve global API rate expectations.

1. Poll one repo per scheduler tick in round-robin order.
2. With `runtime.poll_interval_seconds = P` and repo count `R`, each repo is polled every `P * R` seconds.
3. This makes `poll_interval_seconds` approximate overall GitHub request cadence at the process level, rather than per-repo cadence.
4. Work dispatch still uses global worker capacity and fair cross-repo scheduling.

### 4. Git layout and lazy clone strategy

Keep per-repo mirrors and checkout roots, and make slot checkout cloning fully lazy.

1. `init` / startup ensures mirror and directory layout only.
2. Worker slot checkout for a repo is cloned only on first use of that `(repo, slot)` pair.
3. Initial clone uses `--reference-if-able` against the per-repo mirror.
4. Result: no `worker_count * repo_count` network clones at startup, and no repeated network fetch per slot clone.

### 5. State schema: single repo-scoped form, no in-place migration

Use one schema keyed by `repo_full_name` for all installs:

- `issue_runs`: primary key `(repo_full_name, issue_number)`
- `agent_sessions`: primary key `(repo_full_name, issue_number)`
- `feedback_events`: primary key `(repo_full_name, event_key)`
- `pr_feedback_state`: primary key `(repo_full_name, pr_number)`
- `operator_commands`: primary key `(repo_full_name, command_key)`
- `runtime_operations`: global single-flight rows, plus `request_repo_full_name` for restart reply routing.

For this issue, we intentionally do not implement in-place DB migration from the old schema.

1. Upgrade behavior: require reinitializing state DB after deploying this change.
2. Operator flow: stop process, remove prior state DB, run `mergexo init`, restart service.
3. Startup should detect obvious legacy-schema mismatch and fail with a clear reinit instruction.

### 6. Operator commands and auth behavior

Per-repo behavior:

1. `allowed_users` gates issue intake and PR feedback for that repo only.
2. `operations_issue_number` and `operator_logins` are resolved from the same repo config.
3. `/mergexo unblock` only affects blocked PRs in that repo context.

Global behavior:

1. `/mergexo restart` remains process-global single-flight.
2. Restart status replies post back to the originating repo issue thread via stored `request_repo_full_name`.

### 7. CLI changes

`src/mergexo/cli.py` updates:

1. `init` initializes mirrors/layout for all configured repos (without eagerly cloning slot checkouts).
2. `run` and `service` construct multi-repo runtimes and use round-robin polling.
3. `feedback blocked list` output includes `repo_full_name`.
4. `feedback blocked reset` adds `--repo` filter and requires it with `--pr` when multiple repos are configured.

## Implementation Plan

1. Refactor config models and parser in `src/mergexo/config.py` to always produce normalized `AppConfig.repos`.
2. Add `repo_id` and repo-local `allowed_users` handling to `RepoConfig` and related types in `src/mergexo/models.py` as needed.
3. Update orchestrator setup in `src/mergexo/cli.py` and `src/mergexo/service_runner.py` to create one runtime context per repo.
4. Implement round-robin per-tick repo polling and global worker fairness in `src/mergexo/orchestrator.py`.
5. Update `src/mergexo/git_ops.py` so slot checkout clone occurs on first slot use, not during init/startup.
6. Convert state APIs and schema in `src/mergexo/state.py` to repo-scoped keys and add legacy-schema detection with reinit error messaging.
7. Ensure restart operations carry repo origin for reply routing in `src/mergexo/state.py`, `src/mergexo/orchestrator.py`, and `src/mergexo/service_runner.py`.
8. Update docs and config examples in `README.md` and `mergexo.toml.example`.
9. Expand tests across config, CLI, orchestrator, state, service runner, and git ops.

## Testing Plan

1. Config parsing:
- legacy `[repo]` config normalizes to a single `AppConfig.repos` entry.
- `[repo.<id>]` config supports omitted/explicit `name` and owner-defaulted `allowed_users`.
- documented defaults resolve correctly when optional repo keys are omitted.
- `coding_guidelines_path` is rejected when omitted.
- single-repo mode falls back to `[owner]` for `allowed_users` when no allowlist is explicitly configured.
- invalid mixed/duplicate repo definitions fail with clear errors.

2. Orchestrator behavior:
- two repos in one process both enqueue and process work.
- issue/PR number collisions across repos remain isolated.
- per-repo allowlist and operator allowlist enforcement works independently.
- with three repos and `poll_interval_seconds = 600`, each repo is polled every 1800 seconds.

3. Git behavior:
- init/startup does not clone all slot checkouts.
- first use of a slot for a repo performs lazy clone from mirror.

4. State behavior:
- repo-scoped keys are used in all state read/write paths.
- startup with legacy schema returns a clear error instructing reinit.

5. CLI behavior:
- `feedback blocked list` displays `repo_full_name`.
- `feedback blocked reset --pr` requires repo disambiguation in multi-repo mode.

## Acceptance Criteria

1. A config with two `[repo.<id>]` entries runs one process that polls both repos.
2. For multi-repo configs, `allowed_users` is enforced per repo and defaults to `[owner]` when omitted.
3. Legacy single-repo config input is normalized into one internal repo entry after parsing.
4. Omitted repo fields resolve to documented defaults (`default_branch=main`, `trigger_label=agent:design`, `bugfix_label=agent:bugfix`, `small_job_label=agent:small-job`, `design_docs_dir=docs/design`).
5. `coding_guidelines_path` is required and startup fails when it is missing.
6. In legacy single-repo mode, when no allowlist is provided, `allowed_users` defaults to `[owner]`.
7. The same issue number in two repos can run concurrently without state collisions.
8. The same PR number in two repos can be tracked independently in feedback state.
9. With `poll_interval_seconds = P` and `R` repos, each repo is polled every `P * R` seconds.
10. `init` and startup do not eagerly clone all worker slots for all repos.
11. First use of an unused `(repo, slot)` performs a lazy clone from that repo mirror.
12. `feedback blocked list` clearly identifies repo for each row.
13. `feedback blocked reset --pr` in multi-repo mode requires repo disambiguation and only mutates that repo’s rows.
14. `/mergexo restart` remains global single-flight and posts result in the originating repo thread.
15. Upgrading to this version requires state reinit; documented operator steps work end-to-end.

## Risks and Mitigations

1. Risk: requiring state reinit drops historical local run metadata.
Mitigation: explicit rollout/runbook steps and clear startup error messaging for legacy DB detection.

2. Risk: API usage scales with repo count.
Mitigation: round-robin per-tick polling so configured interval represents process-level GitHub polling cadence.

3. Risk: one noisy repo starves others.
Mitigation: fair cross-repo scheduling with global capacity checks.

4. Risk: disk growth from many repo checkout trees.
Mitigation: lazy slot clone strategy plus shared mirror references.

5. Risk: operator confusion when resetting blocked PRs across repos.
Mitigation: require repo disambiguation for targeted reset and include repo in CLI output.

## Rollout Notes

1. Ship parser/runtime changes with updated docs showing per-repo `allowed_users`.
2. Before upgrade, stop MergeXO and remove old state DB.
3. Run `mergexo init` to recreate state and mirrors under the new expectations.
4. Start service and canary with two repos.
5. Validate per-repo auth boundaries and operator command routing.
6. Validate split polling cadence and GitHub request volume against expectations.
