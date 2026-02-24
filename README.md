# MergeXO

MergeXO is a local-first Python orchestrator that watches labeled issues and routes each issue into one startup flow:

- `design_doc`: generate a design-doc PR
- `bugfix`: generate a direct bugfix PR
- `small_job`: generate a direct scoped-change PR

This repository currently implements Phase 1 of the MVP:

- Configure one or more target repositories, worker count `N`, base state directory, poll interval, and flow labels.
- Initialize a shared mirror plus `N` checkout slots.
- Poll GitHub issues with configured trigger labels using `gh api`.
- Farm each new issue to an available worker slot.
- Run the flow-specific Codex prompt and open a linked PR.

## Requirements

- Python 3.11+
- `uv`
- `git`
- `gh` authenticated for the target repository
- `codex` authenticated locally

## Quickstart

1. Copy and edit config:

```bash
cp mergexo.toml.example mergexo.toml
```

Set allowlists per repo with `repo.allowed_users` (or use `[auth].allowed_users` only as a
legacy single-repo fallback). In multi-repo configs, each `[repo.<id>]` can set its own
allowlist; if omitted it defaults to `[owner]`.

Agent settings under `[codex]` are global defaults. To override per repo, set
`[codex.repo.<repo_id>]` and only include keys that should differ (all omitted keys inherit
from `[codex]`).

2. Sync environment:

```bash
uv sync
```

3. Initialize local state + mirror + checkout slots:

```bash
uv run mergexo init --config mergexo.toml
```

4. Run orchestrator once (single poll + wait for active workers):

```bash
uv run mergexo run --config mergexo.toml --once
```

Verbose mode (structured stderr runtime events):

```bash
uv run mergexo run --config mergexo.toml --once --verbose
uv run mergexo run --config mergexo.toml --once --verbose high
```

5. Run continuously:

```bash
uv run mergexo run --config mergexo.toml
```

For GitHub-operated restart/update workflows, run supervisor mode instead:

```bash
uv run mergexo service --config mergexo.toml
```

6. Run the observability dashboard:

```bash
uv run mergexo top --config mergexo.toml
```

The dashboard is read-only and pulls directly from `state.db`. Keybindings:

- `r`: refresh now
- `f`: cycle repo filter
- `w`: cycle window (`1h`, `24h`, `7d`, `30d`)
- `tab`: cycle focused panel
- `enter`: open GitHub URL when focused on `Issue`, `PR`, or `Branch`; otherwise show detail history
- `q`: quit

Metrics definitions:

- terminal sample set: finished runs with terminal status in `completed`, `failed`, `blocked`, `interrupted`
- mean runtime: `AVG(duration_seconds)`
- std-dev runtime: `sqrt(AVG(x^2) - AVG(x)^2)` (returns `0` when sample size < 2)
- failure rate: `failed_count / terminal_count`

Runtime settings for dashboard and retention:

- `runtime.observability_refresh_seconds`
- `runtime.observability_default_window`
- `runtime.observability_row_limit`
- `runtime.observability_history_retention_days`

## Notes on polling

Phase 1 uses slow polling (for example every 60 seconds). Webhooks can be added later for lower latency and lower API usage.

Use `--verbose` on `init`, `run`, `service`, or `feedback` for high-signal lifecycle logs (`low` mode), or `--verbose high` for full event logs including poll internals. Verbose logs are also appended under `<runtime.base_dir>/logs/YYYY-MM-DD.log` (UTC day rotation).

## State schema upgrade note

Multi-repo support uses a repo-scoped state schema. Upgrading from older builds requires reinitializing the state DB:

1. Stop MergeXO.
2. Remove the previous `state.db`.
3. Run `mergexo init`.
4. Restart MergeXO.

## Source issue comment routing (before/after PR)

Enable issue-comment routing with:

- `runtime.enable_issue_comment_routing = true`

Behavior:

1. Before a PR exists:
   - Recoverable pre-PR blocked outcomes move to `awaiting_issue_followup` instead of terminal `failed`.
   - Reply on the source issue to unblock/resume.
   - Comments are queued by comment id and consumed in order on the next retry turn.
2. While a retry worker is active:
   - New source-issue comments are not lost; they remain queued for the following retry turn.
3. Right before PR creation:
   - MergeXO checks for newly queued source-issue comments.
   - If any are pending, PR creation is deferred until those comments are processed.
4. After a PR exists (`awaiting_feedback` or PR-level `blocked`):
   - Source-issue comments are no longer actioned.
   - MergeXO posts a deterministic redirect reply that links the PR and instructs users to comment on the PR thread.
5. Legacy recovery after upgrade:
   - Historical `failed` runs with no PR that match known pre-PR blocked signatures are adopted into `awaiting_issue_followup` automatically, so a new source-issue comment can resume work.

Recommended rollout:

1. Leave the flag off by default.
2. Enable on one repo and verify canary scenarios:
   - blocked-before-PR then issue reply then retry;
   - comment during active retry;
   - issue comment after PR exists (redirect only).
3. Expand once stable.

## GitHub operator commands

Enable GitHub operations with:

- `runtime.enable_github_operations = true`
- `repo.operator_logins = ["<maintainer-login>", ...]`
- optional `repo.operations_issue_number = <issue-number>` for global commands
  (the issue does not need to stay open; MergeXO scans comments by issue number
  and reads comments from open or closed issues)

Supported comment commands:

- `/mergexo unblock` (target PR defaults from a blocked PR thread)
- `/mergexo unblock head_sha=<sha>` (same target resolution as above)
- `/mergexo unblock pr=<number> [head_sha=<sha>]`
- `/mergexo restart`
- `/mergexo restart mode=git_checkout|pypi`
- `/mergexo help`

Behavior notes:

- Operator commands are processed only from blocked PR threads and the optional operations issue.
- `pr=` is optional on PR threads: `/mergexo unblock` and `/mergexo unblock head_sha=...` target that PR.
- `pr=` is required on the operations issue: `/mergexo unblock` without `pr=<number>` is rejected.
- Every `/mergexo ...` command receives one deterministic reply comment with status and detail.
- Restart automation requires `mergexo service`; `mergexo run` rejects restart commands.
- `git_checkout` restart mode runs `git pull --ff-only` + `uv sync` before re-exec.
- `pypi` mode is available only when configured (`restart_supported_modes` + `service_python`).

Unblock user story (`head_sha` override):

1. PR `#101` is in `blocked` status because MergeXO detected a non-fast-forward head change, for example:
   - previous expected head: `abc1234`
   - current observed head after force-push/rewrite: `def5678`
2. MergeXO will not continue feedback automation while the PR remains blocked.
3. A maintainer verifies the canonical head to resume from:
   - if no override is needed, comment `/mergexo unblock` on the blocked PR thread;
   - if canonical head should be explicit, comment `/mergexo unblock head_sha=def5678` on the blocked PR thread;
   - from the operations issue, use `/mergexo unblock pr=101` or `/mergexo unblock pr=101 head_sha=def5678`.
4. MergeXO transitions the PR from `blocked` back to `awaiting_feedback`.
5. If `head_sha` is supplied, MergeXO also updates the stored `last_seen_head_sha` to that value before resuming.

## Issue labels and precedence

MergeXO reads these labels from each repo config (`[repo]` or `[repo.<id>]`):

- `trigger_label` (default behavior, design-doc flow)
- `bugfix_label` (direct bugfix flow)
- `small_job_label` (direct small-job flow)
- `coding_guidelines_path` (repo-relative file that defines coding style and required pre-PR tests for direct flows)
- `required_tests` (optional repo-relative or absolute executable path that must pass before MergeXO pushes)
- `test_file_regex` (optional bugfix-only regression-test file gate; accepts a regex string or a list of regex strings)

When an issue has more than one trigger label, precedence is deterministic:

1. `bugfix_label`
2. `small_job_label`
3. `trigger_label`

Example:

- issue labels: `agent:design` + `agent:bugfix` -> bugfix flow
- issue labels: `agent:design` + `agent:small-job` -> small-job flow
- issue labels: `agent:design` only -> design-doc flow

Direct-flow PR bodies include `Fixes #<issue_number>`. Design-doc PR bodies keep `Refs #<issue_number>`.
When `test_file_regex` is configured, bugfix flow enforces at least one staged file matching one of
those regexes before opening a PR. Matching uses OR semantics across configured regexes.
When `test_file_regex` is not configured, MergeXO skips this staged-test-file gate.
Bugfix and small-job prompts require the agent to read and follow `coding_guidelines_path`.
When `required_tests` is configured, MergeXO runs it before every push. If it fails on direct/implementation/feedback flows, MergeXO feeds stdout/stderr back to the agent for repair attempts; if the agent reports impossible, the PR is marked blocked with that explanation.

## Authentication allowlist

MergeXO normalizes allowlist entries to lowercase and matches case-insensitively at runtime.

- Multi-repo (`[repo.<id>]`): use `allowed_users` in each repo table. If omitted, it defaults to `[owner]`.
- Legacy single-repo (`[repo]`): `repo.allowed_users` is preferred; `[auth].allowed_users` is a compatibility fallback.
- If neither is configured in legacy single-repo mode, allowlist defaults to `[owner]`.

For users not in `allowed_users`, MergeXO fully ignores:

- new issue intake (no branch/commit/PR/comment side effects),
- PR review comments in feedback loop,
- PR issue comments in feedback loop.

If an already tracked PR is linked to an issue whose author is no longer allowlisted, MergeXO
marks that PR feedback state as blocked internally and stays silent on GitHub.

## Abandoning work (changing direction)

Sometimes we discover a deeper problem late, or priorities change. That is expected. Use this playbook to abandon work safely:

1. After design doc PR is opened (design phase):
   - Close the design PR to stop active review-loop automation for that design.
   - Optionally close the issue as well for project hygiene and visibility.
   - Important: closing only the issue is not enough if the design PR remains open.

2. After design doc is merged and implementation PR is opened:
   - Close the implementation PR to stop active review-loop automation for implementation.
   - Close the issue so it is clearly no longer in scope.
   - The merged design doc will remain in `main`; if you want to remove or supersede it, open a follow-up doc/change PR.

In short: closing the currently active PR is the key action to stop automation for that phase.

## Generated design doc contract

The Codex prompt requires reporting likely implementation files in `touch_paths`, which are written into the design doc frontmatter.
