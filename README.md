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

## Notes on polling

Phase 1 uses slow polling (for example every 60 seconds). Webhooks can be added later for lower latency and lower API usage.

The PR feedback loop is guarded by `runtime.enable_feedback_loop` (default `false`) until rollout is complete.

Use `--verbose` on `init` or `run` to print lifecycle logs for polling, worker actions, git writes, and GitHub writes.

## Issue labels and precedence

MergeXO reads three labels from `[repo]`:

- `trigger_label` (default behavior, design-doc flow)
- `bugfix_label` (direct bugfix flow)
- `small_job_label` (direct small-job flow)

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

## Generated design doc contract

The Codex prompt requires reporting likely implementation files in `touch_paths`, which are written into the design doc frontmatter.
