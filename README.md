# MergeXO

MergeXO is a local-first Python orchestrator that watches labeled issues and routes each issue into one startup flow:

- `design_doc`: open a design-doc PR first, then implementation PR work after merge
- `bugfix`: open a direct bugfix PR
- `small_job`: open a direct scoped-change PR

## Quickstart Session (Start Here)

This is the fastest way to get a working local service + TUI session.

### 1. Requirements

- Python 3.11+
- `uv`
- `git`
- `gh` authenticated for the target repository
- `codex` authenticated locally

### 2. Create `mergexo.toml`

MergeXO defaults to `mergexo.toml`. If you prefer a different filename (for example `mergexo.conf`), pass `--config <file>`.

Recommended minimal starting config:

```toml
[runtime]
base_dir = "~/.local/share/mergexo"
worker_count = 2
poll_interval_seconds = 60

[repo.mergexo]
owner = "johnynek"
# name omitted => "mergexo"
default_branch = "main"
coding_guidelines_path = "docs/python_style.md"
required_tests = "scripts/test.sh"

[codex]
enabled = true
extra_args = []
```

Why this is minimal:

- required runtime keys are only `base_dir`, `worker_count`, `poll_interval_seconds`
- required repo keys are `owner` and `coding_guidelines_path` (plus repo table id)
- everything else can use defaults

If issue authors are not the repo owner, set `allowed_users = ["..."]` under each repo table.

### 3. Sync dependencies

```bash
uv sync
```

### 4. Initialize local state

```bash
uv run mergexo init
```

### 5. Start with no command options (recommended)

```bash
uv run mergexo
```

This starts default `console` mode, which runs:

- service loop
- interactive observability TUI
- file logging under `<runtime.base_dir>/logs/YYYY-MM-DD.log`

This is the recommended first run because it gives you both orchestration and visibility immediately.

### 6. Useful first checks

- open issues with your configured trigger labels
- verify status comments appear on issues
- watch active/blocked work in the TUI

## Expected Workflows

### Flow Selection Rules

MergeXO reads these repo labels:

- `trigger_label` (default design flow)
- `bugfix_label` (direct bugfix)
- `small_job_label` (direct small job)

If multiple labels are present, precedence is deterministic:

1. `bugfix_label`
2. `small_job_label`
3. `trigger_label`

### Example User Story

Assume one repo with labels:

- `agent:design`
- `agent:bugfix`
- `agent:small-job`

1. Design flow issue:
   - Issue `#120` has `agent:design`.
   - MergeXO opens a status comment, creates `agent/design/120-...`, commits design doc to `docs/design/120-...md`, and opens a design PR with `Refs #120`.
   - Reviewers add comments and request changes on the design PR; MergeXO responds to that PR feedback and updates the PR until it is ready to merge.
   - After that design PR is merged, MergeXO automatically treats it as an implementation candidate and opens a follow-up implementation PR.

2. Bugfix flow issue:
   - Issue `#121` has `agent:bugfix`.
   - MergeXO creates `agent/bugfix/121-...`, tries to reproduce the reported bug, and then applies a fix with regression tests.
   - The user reviews both the tests and the bug fix in the PR.
   - PR body uses `Fixes #121`.
   - If `repo.test_file_regex` is set, at least one staged file must match it before PR creation.

3. Small-job flow issue:
   - Issue `#122` has `agent:small-job`.
   - MergeXO creates `agent/small/122-...` and runs scoped direct changes.
   - PR body uses `Fixes #122`.

For both direct flows, MergeXO asks the agent to follow `coding_guidelines_path` and runs `required_tests` before each push when configured.

## Workflow Details

### Observability Dashboard (`top` / `console`)

The dashboard is read-only and pulls from `state.db`.

Keybindings:

- `r`: refresh now
- `f`: cycle repo filter
- `w`: cycle window (`1h`, `24h`, `7d`, `30d`)
- `tab`: cycle focused panel
- `enter`: open GitHub URL when focused on `Issue`, `PR`, or `Branch`; otherwise open detail dialog
- `q`: quit

Metrics definitions:

- terminal sample set: finished runs with terminal status in `completed`, `failed`, `blocked`, `interrupted`
- mean runtime: `AVG(duration_seconds)`
- std-dev runtime: `sqrt(AVG(x^2) - AVG(x)^2)` (`0` when sample size < 2)
- failure rate: `failed_count / terminal_count`

Related runtime settings:

- `runtime.observability_refresh_seconds`
- `runtime.observability_default_window`
- `runtime.observability_row_limit`
- `runtime.observability_history_retention_days`

### Polling and Logging

- Phase 1 uses polling (for example every 60 seconds); webhooks can be added later.
- `console` defaults to `--verbose low`.
- `--verbose high` enables full event stream logging.

`console` lifecycle logs are written to stderr and `<runtime.base_dir>/logs/YYYY-MM-DD.log` (UTC day rotation).

### Authentication Allowlist Behavior

MergeXO normalizes allowlist entries to lowercase and matches case-insensitively at runtime.

- Multi-repo (`[repo.<id>]`): set `allowed_users` in each repo table (default is `[owner]` when omitted).
- Legacy single-repo (`[repo]`): `repo.allowed_users` is preferred; `[auth].allowed_users` is fallback.
- If no allowlist is configured in legacy mode, default is `[owner]`.

For users not in `allowed_users`, MergeXO ignores:

- new issue intake
- PR review comments in feedback loop
- PR issue comments in feedback loop

### Source Issue Comment Routing (Pre-PR and Post-PR)

Enable with:

- `runtime.enable_issue_comment_routing = true`

Behavior:

1. Before a PR exists:
   - recoverable blocked outcomes move to `awaiting_issue_followup` instead of terminal `failed`
   - MergeXO checkpoints the blocked tree to the flow branch before cleanup and posts one status comment with blocked reason, branch, commit SHA, tree link, and compare link
   - reply on the source issue to unblock/resume
   - comments are queued by comment id and consumed in order on the next retry turn
2. While a retry worker is active:
   - new source-issue comments remain queued for the following retry turn
3. Right before PR creation:
   - MergeXO checks for newly queued source-issue comments
   - if any are pending, PR creation is deferred until those comments are processed
4. After a PR exists (`awaiting_feedback` or PR-level `blocked`):
   - source-issue comments are no longer actioned
   - MergeXO posts a deterministic redirect reply that links the PR and instructs users to comment on the PR thread
5. Legacy recovery after upgrade:
   - historical `failed` runs with no PR that match known pre-PR blocked signatures are adopted into `awaiting_issue_followup`

Recommended rollout:

1. leave the flag off by default
2. enable on one repo and verify canary scenarios:
   - blocked-before-PR then issue reply then retry
   - comment during active retry
   - issue comment after PR exists (redirect only)
3. expand once stable

### GitHub Actions Feedback Monitoring

Enable with:

- `runtime.enable_pr_actions_monitoring = true`
- optional `runtime.pr_actions_log_tail_lines = 500`

Behavior:

1. MergeXO scans tracked `awaiting_feedback` PRs and checks workflow runs for current PR head SHAs.
2. If any run is still active, MergeXO logs monitoring state and waits.
3. When all runs are terminal and at least one run is non-green (anything except `success`, `neutral`, `skipped`), MergeXO enqueues deterministic `actions` feedback events keyed by run id + `updated_at`.
4. On the next feedback turn, MergeXO revalidates those events against current head/run state:
   - stale events (run now green, run no longer on current head, or run updated) are auto-resolved
   - actionable events inject CI context into agent turn, including failed action names and `last N log lines` tails
5. If no PR review/issue comments exist, CI context alone can trigger a feedback remediation turn.

Notes:

- feature is currently GitHub Actions only
- remote CI status does not replace local `required_tests` pre-push checks

### GitHub Operator Commands

Enable with:

- `runtime.enable_github_operations = true`
- `repo.operator_logins = ["<maintainer-login>", ...]`
- optional `repo.operations_issue_number = <issue-number>`

Supported commands:

- `/mergexo unblock`
- `/mergexo unblock head_sha=<sha>`
- `/mergexo unblock pr=<number> [head_sha=<sha>]`
- `/mergexo restart`
- `/mergexo restart mode=git_checkout|pypi`
- `/mergexo help`

Behavior notes:

- operator commands are processed only from blocked PR threads and the optional operations issue
- `pr=` is optional on PR threads (`/mergexo unblock` can target that thread's PR)
- `pr=` is required on operations issue comments
- every `/mergexo ...` command receives one deterministic reply comment with status and detail
- restart automation requires `mergexo service`; `mergexo run` rejects restart commands
- `git_checkout` restart mode runs `git pull --ff-only` and `uv sync` before re-exec
- `pypi` mode is available only when configured (`restart_supported_modes` + `service_python`)

Unblock user story (`head_sha` override):

1. PR `#101` is blocked because MergeXO detected a non-fast-forward head change.
2. MergeXO pauses feedback automation for that PR.
3. A maintainer resumes from canonical head:
   - `/mergexo unblock` (PR thread)
   - `/mergexo unblock head_sha=<sha>` (PR thread)
   - `/mergexo unblock pr=101 [head_sha=<sha>]` (operations issue)
4. MergeXO transitions PR state from `blocked` back to `awaiting_feedback`.
5. If `head_sha` is supplied, MergeXO updates stored `last_seen_head_sha` before resuming.

### Abandoning Work Safely

1. If the active artifact is a design PR, close that PR to stop design-loop automation.
2. If the active artifact is implementation PR work, close that PR to stop implementation feedback automation.
3. Closing only the source issue is not sufficient if the active PR remains open.

### State Schema Upgrade Note

Multi-repo support uses repo-scoped state schema. If upgrading from old single-repo schema:

1. stop MergeXO
2. remove old `state.db`
3. run `mergexo init`
4. restart MergeXO

### Generated Design Doc Contract

Design prompts require `touch_paths` in output; these are recorded in design doc frontmatter.

## Configuration Reference

### Repo Table Shapes

Use exactly one repo shape:

1. Multi-repo keyed tables (recommended): `[repo.<repo_id>]`
2. Legacy single-repo table: `[repo]` (optional `[auth]` fallback for allowlist)

Do not mix both shapes in one file.

### `[runtime]` Options

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `base_dir` | yes | none | local state/log/mirror root |
| `worker_count` | yes | none | must be `>= 1` |
| `poll_interval_seconds` | yes | none | must be `>= 5` |
| `enable_github_operations` | no | `false` | enables `/mergexo ...` command processing |
| `enable_issue_comment_routing` | no | `false` | enables pre-PR follow-up + post-PR source redirects |
| `enable_pr_actions_monitoring` | no | `false` | enables GitHub Actions feedback events |
| `pr_actions_log_tail_lines` | no | `500` | valid range `1..5000` |
| `restart_drain_timeout_seconds` | no | `900` | must be `>= 1` |
| `restart_default_mode` | no | `"git_checkout"` | must be in `restart_supported_modes` |
| `restart_supported_modes` | no | `["git_checkout"]` | list of `git_checkout` and/or `pypi` |
| `git_checkout_root` | no | unset | optional for `git_checkout` restarts; defaults to current working directory when omitted |
| `service_python` | no | unset | required for `pypi` restart mode |
| `observability_refresh_seconds` | no | `2` | must be `>= 1` |
| `observability_default_window` | no | `"24h"` | one of `1h`, `24h`, `7d`, `30d` |
| `observability_row_limit` | no | `200` | positive int or `null` |
| `observability_history_retention_days` | no | `90` | must be `>= 1` |

### `[repo.<repo_id>]` Options (or legacy `[repo]`)

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `owner` | yes | none | GitHub org/user |
| `name` | no (keyed) / yes (legacy) | keyed: `<repo_id>` | repo name |
| `default_branch` | no | `"main"` | base branch for PRs |
| `trigger_label` | no | `"agent:design"` | design flow trigger |
| `bugfix_label` | no | `"agent:bugfix"` | bugfix flow trigger |
| `small_job_label` | no | `"agent:small-job"` | small-job flow trigger |
| `coding_guidelines_path` | yes | none | repo-relative path for coding/testing guidance |
| `design_docs_dir` | no | `"docs/design"` | design doc output directory |
| `allowed_users` | no | `[owner]` | normalized lowercase allowlist |
| `local_clone_source` | no | unset | local repo/.git used to seed mirror |
| `remote_url` | no | `git@github.com:<owner>/<name>.git` | explicit remote override |
| `required_tests` | no | unset | repo-relative or absolute executable path |
| `test_file_regex` | no | unset | bugfix-only regression-test staged-file gate; string or list |
| `operations_issue_number` | no | unset | optional global ops issue |
| `operator_logins` | no | `[]` | allowed `/mergexo` command authors |

### Legacy `[auth]`

- only for legacy single-repo shape
- `auth.allowed_users` is a compatibility fallback when `repo.allowed_users` is absent

### `[codex]` Options

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | no | `true` | master enable flag |
| `model` | no | unset | adapter/model hint |
| `sandbox` | no | unset | Codex sandbox mode |
| `profile` | no | unset | Codex profile |
| `extra_args` | no | `[]` | extra CLI args |

### `[codex.repo.<repo_id>]` Overrides

- optional per-repo overrides
- same keys as `[codex]`
- omitted keys inherit from global `[codex]`

## CLI Help and Modes

`mergexo` uses subcommands, with `console` as the default command when omitted.

### Top-Level Help

```text
usage: mergexo [-h] {console,init,run,service,top,feedback} ...
```

Subcommands:

- `console`: run service + TUI + file logging together
- `init`: initialize state DB, mirror, and checkouts
- `run`: run orchestrator polling loop
- `service`: orchestrator with supervisor/restart workflow support
- `top`: observability-only TUI
- `feedback`: inspect/manage feedback-loop state

### Mode Motivations

- `console`: best for local interactive operation (recommended default); requires an interactive terminal
- `service`: best for non-interactive deployments and GitHub-driven restart automation
- `run`: simple polling mode when you do not need supervisor/restart integration
- `top`: read-only operational visibility without running service
- `feedback`: manual inspection/reset of blocked feedback state
- `init`: one-time setup and occasional reinit after schema upgrades

### Command Help (Full)

```text
usage: mergexo console [-h] [--config CONFIG] [-v [MODE]]
usage: mergexo init [-h] [--config CONFIG] [-v [MODE]]
usage: mergexo run [-h] [--config CONFIG] [--once] [-v [MODE]]
usage: mergexo service [-h] [--config CONFIG] [--once] [-v [MODE]]
usage: mergexo top [-h] [--config CONFIG] [-v [MODE]]
usage: mergexo feedback [-h] [--config CONFIG] [-v [MODE]] {blocked} ...
```

Shared options:

- `--config CONFIG`: config path (default command expects `mergexo.toml` unless overridden)
- `-v, --verbose [MODE]`: `low` or `high`

Additional options:

- `run --once`: single poll + wait for active workers
- `service --once`: single poll + wait for active workers

Feedback subcommands:

```text
usage: mergexo feedback blocked [-h] {list,reset} ...
usage: mergexo feedback blocked list [-h] [--json]
usage: mergexo feedback blocked reset [-h] (--pr PR | --all) [--yes] [--dry-run] [--head-sha HEAD_SHA] [--repo REPO]
```

`feedback blocked reset` options:

- `--pr PR` (repeatable): reset specific blocked PR numbers
- `--all`: reset all blocked PRs
- `--yes`: required with `--all` unless `--dry-run`
- `--dry-run`: preview only
- `--head-sha <sha>`: override stored `last_seen_head_sha` on reset
- `--repo <repo-id-or-owner/name>`: required with `--pr` in multi-repo configs

## Label and PR Body Rules

- `bugfix` and `small_job` PR bodies include `Fixes #<issue_number>`.
- `design_doc` PR bodies include `Refs #<issue_number>`.
- If multiple trigger labels are present, precedence is `bugfix` > `small_job` > `design_doc`.
