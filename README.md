# MergeXO

MergeXO is a local-first Python orchestrator that watches labeled issues and turns them into design-doc pull requests using Codex.

This repository currently implements Phase 1 of the MVP:

- Configure one target repository, worker count `N`, base state directory, poll interval, and issue label.
- Initialize a shared mirror plus `N` checkout slots.
- Poll GitHub issues with the configured label using `gh api`.
- Farm each new issue to an available worker slot.
- Generate a design doc via Codex and open a PR linked to the issue.

## Requirements

- Python 3.9+
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

5. Run continuously:

```bash
uv run mergexo run --config mergexo.toml
```

## Notes on polling

Phase 1 uses slow polling (for example every 60 seconds). Webhooks can be added later for lower latency and lower API usage.

## Generated design doc contract

The Codex prompt requires reporting likely implementation files in `touch_paths`, which are written into the design doc frontmatter.
