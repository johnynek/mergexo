# MergeXO

MergeXO is a local-first Python orchestrator that watches labeled issues and routes each issue into one startup flow:

- `design_doc`: generate a design-doc PR
- `bugfix`: generate a direct bugfix PR
- `small_job`: generate a direct scoped-change PR

This repository currently implements Phase 1 of the MVP:

- Configure one target repository, worker count `N`, base state directory, poll interval, and flow labels.
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

Set `[auth].allowed_users` to the exact GitHub usernames that are allowed to trigger MergeXO.
Startup fails closed when `[auth]` is missing or empty.

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
```

5. Run continuously:

```bash
uv run mergexo run --config mergexo.toml
```

For GitHub-operated restart/update workflows, run supervisor mode instead:

```bash
uv run mergexo service --config mergexo.toml
```

## Notes on polling

Phase 1 uses slow polling (for example every 60 seconds). Webhooks can be added later for lower latency and lower API usage.

The PR feedback loop is guarded by `runtime.enable_feedback_loop` (default `false`) until rollout is complete.

Use `--verbose` on `init`, `run`, or `service` to print lifecycle logs for polling, worker actions, git writes, and GitHub writes.

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

MergeXO reads three labels from `[repo]`:

- `trigger_label` (default behavior, design-doc flow)
- `bugfix_label` (direct bugfix flow)
- `small_job_label` (direct small-job flow)
- `coding_guidelines_path` (repo-relative file that defines coding style and required pre-PR tests for direct flows)

When an issue has more than one trigger label, precedence is deterministic:

1. `bugfix_label`
2. `small_job_label`
3. `trigger_label`

Example:

- issue labels: `agent:design` + `agent:bugfix` -> bugfix flow
- issue labels: `agent:design` + `agent:small-job` -> small-job flow
- issue labels: `agent:design` only -> design-doc flow

Direct-flow PR bodies include `Fixes #<issue_number>`. Design-doc PR bodies keep `Refs #<issue_number>`.
Bugfix flow enforces at least one staged file under `tests/` before opening a PR.
Bugfix and small-job prompts require the agent to read and follow `coding_guidelines_path`.

## Authentication allowlist

`[auth].allowed_users` is required. MergeXO normalizes entries to lowercase at startup and
matches case-insensitively at runtime.

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
