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
  - src/mergexo/feedback_loop.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_service_runner.py
  - tests/test_feedback_loop.py
  - tests/test_git_ops.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T19:25:36Z
---

# Multi repo support

_Issue: #33 (https://github.com/johnynek/mergexo/issues/33)_

## Summary

Design for adding multi-repository monitoring to one MergeXO process with per-repo auth and operations configuration, while keeping restart and update global.

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
  - src/mergexo/feedback_loop.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_service_runner.py
  - tests/test_feedback_loop.py
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

Current code assumes exactly one repo in `AppConfig.repo`, one `GitHubGateway`, one `GitRepoManager`, and state rows keyed only by issue and PR numbers. That breaks for multi-repo operation because issue and PR numbers collide across repos and auth policy must be per repo.

## Goals

1. Support one process monitoring N repositories from one config file.
2. Keep per-repo configuration for `operations_issue_number`, `operator_logins`, and allowed users.
3. Preserve clean migration from the current single-repo config.
4. Keep restart and update behavior global to the process.

## Non-goals

1. Cross-repo dependency scheduling.
2. Webhook redesign.
3. Per-repo worker-count limits in this issue (worker count remains global).
4. Multi-process distributed coordination.

## Proposed Architecture

### 1. Config contract and parsing

Add first-class multi-repo config in `src/mergexo/config.py`:

1. `AppConfig` changes from `repo` to `repos`.
2. `RepoConfig` adds:
- `repo_id`: local identifier from table key.
- `allowed_users`: per-repo normalized allowlist.
3. Supported TOML shapes:
- Legacy single repo: `[repo]` with existing fields. This remains supported.
- Multi repo: `[repo.<id>]` entries.
4. Name inference rule:
- For `[repo.<id>]`, if `name` is omitted, use `<id>` as repo name.
- If `name` is provided, `<id>` is only a local config identifier.
5. Auth migration:
- `allowed_users` becomes a repo field.
- Keep `[auth].allowed_users` as a deprecated fallback only for legacy single-repo configs during transition.
- Reject `[auth]` when multiple repo tables are configured to avoid ambiguity.
6. Validation:
- At least one repo must be configured.
- `repo_id` values must be unique.
- `owner/name` pairs must be unique across repos.
- Every repo must have a non-empty allowlist after fallback resolution.

### 2. Repo runtime contexts

Refactor orchestration setup to build one `RepoRuntime` per configured repo:

- `RepoRuntime` contains `RepoConfig`, `GitHubGateway`, `GitRepoManager`, and a normalized operator allowlist.
- `Phase1Orchestrator` becomes multi-repo aware and accepts a map of repo runtimes.
- Logging adds `repo_full_name` to all repo-scoped events.

### 3. Global worker scheduling with fairness

Keep `runtime.worker_count` as one global limit across all repos:

1. Maintain global running sets keyed by `(repo_full_name, issue_number)` and `(repo_full_name, pr_number)`.
2. Poll each repo for candidates, then enqueue with round-robin fairness so one busy repo does not starve others.
3. Continue using one `ThreadPoolExecutor` capped by `worker_count`.

### 4. State store repo scoping

Update `src/mergexo/state.py` schema so all repo-bound records are keyed by repo:

- `issue_runs`: primary key `(repo_full_name, issue_number)`
- `agent_sessions`: primary key `(repo_full_name, issue_number)`
- `feedback_events`: primary key `(repo_full_name, event_key)`
- `pr_feedback_state`: primary key `(repo_full_name, pr_number)`
- `operator_commands`: primary key `(repo_full_name, command_key)`
- `runtime_operations`: stays global, but add `request_repo_full_name` for restart reply routing.

All state APIs take `repo_full_name` for read and write operations. Returned dataclasses include `repo_full_name` so CLI and orchestrator can disambiguate rows.

### 5. DB migration strategy

Because primary keys change, implement an explicit SQLite migration path:

1. Detect legacy schema at startup.
2. Create v2 tables with repo-scoped keys.
3. Copy legacy rows into v2 using the configured legacy repo full name.
4. Swap tables in one transaction.
5. Keep migration idempotent.

If migration data cannot be mapped to exactly one configured repo, fail fast with a clear error.

### 6. Git layout and checkout strategy

Keep per-repo mirrors and checkout roots, but avoid eager clone explosion:

1. `GitRepoManager.ensure_layout` ensures mirror and root directories.
2. Worker checkouts are created lazily on first slot use per repo.
3. Paths remain repo-scoped under `base_dir/checkouts/<owner>/<name>/worker-xx`.

### 7. Operator commands and auth behavior

Per-repo behavior:

1. `allowed_users` gates issue intake and PR feedback for that repo only.
2. `operations_issue_number` and `operator_logins` are resolved from that repo config.
3. `/mergexo unblock` only affects blocked PRs in the same repo context.

Global behavior:

1. `/mergexo restart` remains single-flight for the whole process.
2. Restart command acknowledgement posts back to the originating repo issue thread using `request_repo_full_name`.

### 8. CLI changes

`src/mergexo/cli.py` updates:

1. `init` initializes mirrors and layout for all configured repos.
2. `run` and `service` construct multi-repo runtimes.
3. `feedback blocked list` output includes `repo_full_name`.
4. `feedback blocked reset` adds `--repo` filter and requires it with `--pr` when multiple repos are configured.

## Implementation Plan

1. Refactor config models and parser in `src/mergexo/config.py`, including legacy compatibility logic.
2. Update config consumers for `AppConfig.repos` in `src/mergexo/cli.py`, `src/mergexo/orchestrator.py`, and `src/mergexo/service_runner.py`.
3. Move allowlist ownership from global auth to repo config and update auth checks in `src/mergexo/orchestrator.py`.
4. Introduce repo-scoped runtime context objects in `src/mergexo/models.py` or `src/mergexo/orchestrator.py`.
5. Add repo-scoped state schema and API signatures in `src/mergexo/state.py` with migration.
6. Update feedback and operator key handling as needed in `src/mergexo/feedback_loop.py`.
7. Adjust git manager behavior for lazy checkout creation in `src/mergexo/git_ops.py`.
8. Update restart reply routing and global update branch sourcing in `src/mergexo/service_runner.py`.
9. Update docs and sample config in `README.md` and `mergexo.toml.example`.
10. Expand tests across config, CLI, orchestrator, state, feedback, service runner, and git ops.

## Testing Plan

1. Config parsing:
- legacy `[repo]` plus `[auth]` still loads.
- `[repo.<id>]` multi config loads and normalizes ids and names correctly.
- `name` omission uses table id.
- invalid mixed modes and duplicate repos fail.

2. Orchestrator behavior:
- two repos in one process both enqueue and process work.
- issue and PR number collisions across repos remain isolated.
- per-repo allowlist and operator allowlist enforcement works independently.
- restart request from repo A drains globally and replies in repo A thread.

3. State behavior:
- all APIs operate correctly with repo-scoped keys.
- migration preserves existing single-repo data.
- blocked reset works with repo filters.

4. CLI behavior:
- `init` reports all repos.
- feedback list and reset include repo disambiguation.
- multi-repo reset requires repo when `--pr` is used.

5. Performance and safety:
- startup does not clone `worker_count * repo_count` checkouts eagerly.
- round-robin scheduling prevents starvation in synthetic backlog tests.

## Acceptance Criteria

1. A config with two `[repo.<id>]` entries runs one process that polls both repos.
2. Existing single-repo configs continue to run without mandatory immediate rewrite.
3. In `[repo.<id>]`, omitted `name` resolves to `<id>`, and explicit `name` overrides only repo name, not id.
4. `allowed_users` is enforced per repo for issue intake and feedback comments.
5. `operations_issue_number` and `operator_logins` are enforced per repo.
6. The same issue number in two repos can run concurrently without state collisions.
7. The same PR number in two repos can be tracked independently in feedback state.
8. `feedback blocked list` clearly identifies repo for each row.
9. `feedback blocked reset --pr` in multi-repo mode requires repo disambiguation and only mutates that repo rows.
10. `/mergexo restart` remains global single-flight and posts result in the originating repo thread.
11. State migration from legacy single-repo schema to repo-scoped schema completes without data loss for tracked runs.
12. Startup clone behavior is lazy enough to avoid cloning all worker slots for all repos during init.

## Risks and Mitigations

1. Risk: schema migration bugs could corrupt existing state.
Mitigation: transactional migration, backup recommendation, and migration tests with real legacy fixtures.

2. Risk: API usage scales linearly with repo count and may hit GitHub limits.
Mitigation: per-repo poll pacing, keep polling interval configurable, and add repo-level metrics.

3. Risk: one noisy repo starves others.
Mitigation: round-robin enqueue policy with global capacity checks.

4. Risk: disk growth from many repo checkout trees.
Mitigation: lazy checkout creation and clear cleanup guidance in docs.

5. Risk: operator confusion when resetting blocked PRs across repos.
Mitigation: require repo disambiguation for targeted reset and include repo in CLI output.

## Rollout Notes

1. Release parser support first with legacy compatibility enabled.
2. Upgrade one existing single-repo install and verify no behavior change.
3. Add a second low-traffic repo using `[repo.<id>]` and run in canary mode.
4. Validate per-repo auth and operator command boundaries with test accounts.
5. Validate restart command from each repo operations issue posts in the correct thread.
6. After canary stability, migrate production configs to multi-repo shape.
7. Document deprecation timeline for global `[auth]` fallback.
8. Keep a rollback plan: revert to one repo entry and restore pre-migration state DB backup if needed.
