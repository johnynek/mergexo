# MergeXO

MergeXO uses GitHub as the UX and control plane for a team of local coding agents. You create issues, review design docs and pull requests, and manage progress from the same GitHub workflow you already use, including from the web or your phone.

The goal is agentic development with normal software rigor: decisions are captured in issues, plans are reviewed in design docs, and code changes land through PR review. MergeXO orchestrates the local agent runtime so this workflow is automated, traceable, and easy to control.

MergeXO is a local-first Python orchestrator that watches labeled issues and routes each issue into one startup flow:

- `roadmap`: open a roadmap PR with canonical DAG JSON, then keep the same roadmap updated as dependency work changes what should happen next
- `design_doc`: open a design-doc PR first, then implementation PR work after merge
- `bugfix`: open a direct bugfix PR
- `small_job`: open a direct scoped-change PR

## Quickstart Session (Start Here)

The best way to start now is to clone this repo, open `codex`, and ask it to read `README.md` and the codebase and set up MergeXO for your repos. It should explain options and ask which repos to manage, which users can send commands, and restart preferences.

Example prompt:

> "Read README.md and the codebase. Propose and set up a MergeXO config for my repos, explain key options, and ask me which repos to manage, which users can send commands, and restart preferences."

Today MergeXO supports GitHub and Codex. We are open to adding support for other code hosts and coding agents.

If you want to do it old school, here is how it works:

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

Roadmap support is off by default. If you want multi-step roadmap orchestration, enable it explicitly as described in `Optional: Enable Roadmaps` below.

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

## MergeXO Roadmaps

Roadmaps are a feature-flagged flow for multi-step work. MergeXO stores the roadmap as canonical markdown plus versioned DAG JSON, then treats that roadmap as a living plan: before issuing each ready frontier, it evaluates whether the current roadmap still matches what completed dependency work taught us.

### Quick Start

Enable roadmap support in config:

```toml
[runtime]
enable_roadmaps = true

[repo.mergexo]
roadmap_label = "agent:roadmap"
roadmap_docs_dir = "docs/roadmap"
roadmap_revision_label = "agent:roadmap-revise"
roadmap_abandon_label = "agent:roadmap-abandon"
roadmap_recommended_node_count = 7
```

Then do this:

1. Set `runtime.enable_roadmaps = true`.
2. Create the GitHub labels you want to use.
3. Apply `roadmap_label` to a source issue.
4. Review and merge the generated roadmap PR.

### Example User Stories

1. Starting a roadmap
   - You open issue `#123` for a larger feature and add `agent:roadmap`.
   - MergeXO opens a roadmap PR with a narrative plan and canonical graph JSON (`docs/roadmap/123-....graph.json`).
   - You review and merge it to activate the plan.

2. Watching the roadmap execute
   - MergeXO finds the "ready frontier" (nodes with satisfied dependencies).
   - **The Adjustment Gate:** Before opening any child issues, MergeXO gathers "dependency artifacts" from recently completed work: merged PR bodies, code diffs, review summaries, and key comments.
   - It evaluates these against the current roadmap. If the plan still fits, it returns `proceed` and opens the next batch of child issues.

3. Steering the roadmap as reality changes
   - If a completed PR reveals a technical hurdle, MergeXO returns `revise`.
   - **Auto-Revision:** It opens or reuses a **same-roadmap revision PR** (e.g., on branch `agent/roadmap/123-revision-v2`). This PR updates the canonical Markdown and JSON files in your repo.
   - **Manual Steering:** You can force a revision at any time by adding `agent:roadmap-revise` to the roadmap issue. MergeXO will then ask the agent to propose a concrete update based on current progress.
   - The roadmap pauses fan-out until you review and merge this revision PR.

### Visibility and Control

The roadmap issue is your durable command center:

- **`/roadmap status`**: Comment this to get a rich report including the active graph, blocked nodes, the latest agent rationale, and links to any pending revision PRs.
- **Node Linking**: Every child issue created by MergeXO is automatically labeled and includes a "Parent Roadmap" link back to the source issue.
- **Sizing**: `roadmap_recommended_node_count` (default: 7) encourages small, reviewable epics. If a roadmap gets too large, MergeXO will suggest splitting it into nested roadmaps.

### What To Expect During Execution

Roadmaps are now a continuous control loop, not a one-time plan that fans out unchanged.

- Before issuing each ready frontier, MergeXO evaluates the current roadmap against concrete dependency artifacts from already-completed child work.
- The adjustment result is one of `proceed`, `revise`, or `abandon`.
- `proceed` issues the ready child issues now.
- `revise` creates or reuses a same-roadmap revision PR instead of opening a replacement roadmap issue in the normal revision path.
- While a tracked revision PR is open, the roadmap stays in `awaiting_revision_merge` and no further child issues are issued from that roadmap.
- Once the tracked revision PR merges and the updated graph passes transition validation, MergeXO applies the new graph version and resumes in the same scheduler loop.
- `roadmap_recommended_node_count` is a sizing recommendation, not a hard cap. Oversized roadmaps still work, but MergeXO posts a suggestion to split them when practical.

### How A Roadmap Completes

A roadmap is complete when every roadmap node reaches a terminal outcome.

- If all roadmap nodes end in `completed` or `abandoned`, MergeXO marks the roadmap completed and closes the parent roadmap issue.
- If the roadmap is no longer viable, MergeXO can abandon it directly and close remaining work without pretending it completed successfully.
- The roadmap issue remains the durable control point for status, revision, and completion throughout the life of the roadmap.

### Safety And Restart Behavior

- Roadmap child issue creation, same-roadmap revision PR creation, and tokenized roadmap status comments go through the sqlite-backed GitHub outbox.
- If GitHub calls fail transiently or MergeXO crashes mid-flight, restart and replay can resume from durable intent rows instead of requiring manual cleanup.

## Expected Workflows

### Flow Selection Rules

MergeXO reads these repo labels:

- `ignore_label` (human takeover pause)
- `roadmap_label` (multi-step roadmap flow)
- `trigger_label` (default design flow)
- `bugfix_label` (direct bugfix)
- `small_job_label` (direct small job)

`roadmap_label` is only active when `runtime.enable_roadmaps = true`.

If multiple labels are present, precedence is deterministic:

1. `ignore_label`
2. `roadmap_label`
3. `bugfix_label`
4. `small_job_label`
5. `trigger_label`

### Example User Story

Assume one repo with labels:

- `agent:design`
- `agent:bugfix`
- `agent:small-job`
- `agent:roadmap`
- `agent:roadmap-revise`
- `agent:roadmap-abandon`

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

4. Roadmap flow issue (feature-flagged):
   - Set `runtime.enable_roadmaps = true` and apply `agent:roadmap` to issue `#123`.
   - MergeXO opens `agent/roadmap/123-...` and creates both `docs/roadmap/123-...md` and `docs/roadmap/123-....graph.json`.
   - After merge, MergeXO activates the roadmap DAG, then runs an adjustment gate before each ready frontier using the current roadmap plus dependency artifacts from completed child work.
   - If the gate says `proceed`, MergeXO opens only the currently ready child issues and keeps parent-child links in issue bodies.
   - If the gate says `revise`, or if a maintainer applies `agent:roadmap-revise`, MergeXO pauses fan-out and opens or reuses a same-roadmap revision PR against the canonical roadmap markdown and graph.
   - Comment `/roadmap status` on the roadmap issue for a live status report. Apply `agent:roadmap-abandon` to abandon the roadmap entirely.
   - Child PRs should reference only their direct child issue; MergeXO closes the parent roadmap issue once all roadmap nodes reach terminal outcomes or the roadmap is abandoned.

5. Human takeover flow (`agent:ignore`):
   - Issue `#123` starts in bugfix flow and already has an open PR `#205`.
   - Maintainer adds label `agent:ignore` on issue `#123`.
   - MergeXO pauses enqueue/feedback/redirect automation for that issue + linked PR while takeover is active.
   - Maintainer (or realtime agent pair) pushes manual commits, updates PR `#205`, and resolves review comments directly.
   - Maintainer removes `agent:ignore` after manual takeover work is done.
   - Maintainer leaves a fresh PR or source-issue comment after label removal; MergeXO resumes from that post-takeover boundary without replaying takeover-period comments.

For both direct flows, MergeXO asks the agent to follow `coding_guidelines_path` and runs `required_tests` before each push when configured.

## Workflow Details

### Observability Dashboard (`top` / `console`)

The dashboard is read-only and pulls from `state.db`.

Keybindings:

- `r`: refresh now
- `f`: cycle repo filter
- `w`: cycle window (`1h`, `24h`, `7d`, `30d`)
- `tab`: cycle focused panel
- `enter`: open the detail pane for the selected row, with full branch names and full URLs for copy/paste
- `o`: open the selected GitHub URL explicitly when focused on a URL-bearing field such as `Issue`, `PR`, `Branch`, or a URL row inside the detail pane
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

### Incremental Comment Polling

Enable with:

- `runtime.enable_incremental_comment_fetch = true`
- optional `runtime.comment_fetch_overlap_seconds = 5`
- optional `runtime.comment_fetch_safe_backfill_seconds = 86400`

Behavior:

1. MergeXO tracks per-surface GitHub comment cursors and fetches comments with `since + pagination`.
2. Cursor filtering uses `(updated_at, comment_id)` so same-second updates are not dropped.
3. Token dedupe uses local `action_tokens` state, plus bounded targeted refetch when needed.
4. Steady-state polling cost stays bounded by active monitored surfaces + new/edited comments.
5. `closed` and `merged` PR feedback states remain terminal; reopening a PR does not auto-resume monitoring.

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

### PR Takeover (Human Override)

Use this when a human wants to pause MergeXO for a specific source issue and linked PR work.

1. Add `repo.ignore_label` (default: `agent:ignore`) to the source issue.
2. MergeXO pauses automation for that source issue and linked PR:
   - no new intake/implementation/follow-up/feedback enqueue
   - no CI feedback enqueue
   - no post-PR source-issue redirects
3. Comments posted while takeover is active are intentionally not replayed later.
4. Remove the label to resume automation.
5. After removing the label, leave a fresh comment to trigger the next turn.

### GitHub Actions Feedback Monitoring

Enable with:

- `repo.pr_actions_feedback_policy = "never" | "first_fail" | "all_complete"`
- optional compatibility fallback: `runtime.enable_pr_actions_monitoring = true` (only used when repo policy is omitted)
- optional `runtime.pr_actions_log_tail_lines = 500`

Behavior:

1. MergeXO scans tracked `awaiting_feedback` PRs and checks workflow runs for current PR head SHAs.
2. Effective policy precedence is:
   - explicit `repo.pr_actions_feedback_policy`
   - otherwise `runtime.enable_pr_actions_monitoring = true` maps to `all_complete`
   - otherwise `never`
3. Policy behavior:
   - `never`: skip Actions monitoring for that repo
   - `all_complete`: if any run is active, wait; when all runs are terminal enqueue non-green failures
   - `first_fail`: enqueue non-green completed failures even if other runs are still active
4. Actions events are deterministic and deduped by run id + `updated_at`.
5. On the next feedback turn, MergeXO revalidates those events against current head/run state:
   - stale events (run now green, run no longer on current head, or run updated) are auto-resolved
   - actionable events inject CI context into agent turn, including failed action names and `last N log lines` tails
6. If no PR review/issue comments exist, CI context alone can trigger a feedback remediation turn.
7. If the feedback agent classifies an actionable Actions failure as an unrelated flaky test, MergeXO:
   - opens a repo issue (no labels) with agent rationale, relevant log excerpt, and full captured CI context
   - posts a tokenized PR comment linking that flaky-test issue
   - reruns failed jobs once for that run
8. While waiting for the rerun result, MergeXO suppresses additional Actions feedback turns for that same flaky incident.
9. If the rerun finishes green, MergeXO marks the flaky incident resolved and resumes normal monitoring.
10. If the rerun fails again (or rerun request fails), MergeXO marks the PR `blocked` and posts a tokenized blocking PR comment linking the same flaky-test issue.

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

Roadmap controls are separate from `/mergexo` operator commands:

- `/roadmap status` on a roadmap issue posts a deterministic roadmap status report
- `roadmap_revision_label` on a roadmap issue requests a same-roadmap revision and pauses further fan-out until that revision is handled
- `roadmap_abandon_label` on a roadmap issue abandons the roadmap
- these roadmap controls do not require `enable_github_operations`

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

Design prompts require `touch_paths` in output; these are recorded in design doc frontmatter. The orchestrator also renders the document frontmatter, H1 title, issue line, and summary. `design_doc_markdown` should therefore contain only the body that starts at the first substantive section.

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
| `enable_roadmaps` | no | `false` | enables the `roadmap_label` flow plus roadmap revision/abandon controls |
| `enable_issue_comment_routing` | no | `false` | enables pre-PR follow-up + post-PR source redirects |
| `enable_pr_actions_monitoring` | no | `false` | compatibility fallback: when true, repos without explicit policy behave as `all_complete` |
| `enable_incremental_comment_fetch` | no | `false` | enables incremental GitHub comment polling with persisted cursors |
| `comment_fetch_overlap_seconds` | no | `5` | overlap replay window used with `since` queries; must be `>= 0` |
| `comment_fetch_safe_backfill_seconds` | no | `86400` | cursor-reset safe backfill window; must be `>= overlap` |
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
| `ignore_label` | no | `"agent:ignore"` | human takeover label; preempts all automation while present |
| `roadmap_label` | no | `"agent:roadmap"` | roadmap flow trigger; only active when `runtime.enable_roadmaps = true` |
| `roadmap_docs_dir` | no | `"docs/roadmap"` | canonical markdown + `.graph.json` output directory for roadmap PRs |
| `roadmap_revision_label` | no | `"agent:roadmap-revise"` | manual same-roadmap revision request label |
| `roadmap_abandon_label` | no | `"agent:roadmap-abandon"` | manual roadmap abandon label |
| `roadmap_recommended_node_count` | no | `7` | sizing recommendation for roadmap graphs; must be `>= 1` |
| `coding_guidelines_path` | yes | none | repo-relative path for coding/testing guidance |
| `design_docs_dir` | no | `"docs/design"` | design doc output directory |
| `allowed_users` | no | `[owner]` | normalized lowercase allowlist |
| `local_clone_source` | no | unset | local repo/.git used to seed mirror |
| `remote_url` | no | `git@github.com:<owner>/<name>.git` | explicit remote override |
| `required_tests` | no | unset | repo-relative or absolute executable path |
| `test_file_regex` | no | unset | bugfix-only regression-test staged-file gate; string or list |
| `pr_actions_feedback_policy` | no | unset | one of `never`, `first_fail`, `all_complete`; when omitted runtime fallback applies (`true -> all_complete`, `false -> never`) |
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
- If multiple flow labels are present, precedence is `ignore` > `bugfix` > `small_job` > `design_doc`.
