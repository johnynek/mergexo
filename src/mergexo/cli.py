from __future__ import annotations

import argparse
from pathlib import Path

from mergexo.codex_adapter import CodexAdapter
from mergexo.config import AppConfig, load_config
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.orchestrator import Phase1Orchestrator
from mergexo.state import StateStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mergexo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init", help="Initialize state DB, mirror, and worker checkouts"
    )
    init_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))

    run_parser = subparsers.add_parser(
        "run", help="Run phase-1 issue polling and design PR generation"
    )
    run_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))
    run_parser.add_argument(
        "--once", action="store_true", help="Poll once and wait for active workers"
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)

    if args.command == "init":
        _cmd_init(config)
        return
    if args.command == "run":
        _cmd_run(config, once=bool(args.once))
        return

    raise RuntimeError(f"Unknown command: {args.command}")


def _cmd_init(config: AppConfig) -> None:
    config.runtime.base_dir.mkdir(parents=True, exist_ok=True)
    state = StateStore(_state_db_path(config))
    _ = state

    git_manager = GitRepoManager(config.runtime, config.repo)
    git_manager.ensure_layout()

    print(f"Initialized MergeXO base dir: {config.runtime.base_dir}")
    print(f"Mirror: {git_manager.layout.mirror_path}")
    print(f"Checkouts: {git_manager.layout.checkouts_root}")


def _cmd_run(config: AppConfig, *, once: bool) -> None:
    config.runtime.base_dir.mkdir(parents=True, exist_ok=True)
    state = StateStore(_state_db_path(config))
    github = GitHubGateway(config.repo.owner, config.repo.name)
    git_manager = GitRepoManager(config.runtime, config.repo)
    agent = CodexAdapter(config.codex)

    orchestrator = Phase1Orchestrator(
        config,
        state=state,
        github=github,
        git_manager=git_manager,
        agent=agent,
    )
    orchestrator.run(once=once)


def _state_db_path(config: AppConfig) -> Path:
    return config.runtime.base_dir / "state.db"
