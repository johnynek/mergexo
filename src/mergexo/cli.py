from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from mergexo.codex_adapter import CodexAdapter
from mergexo.config import AppConfig, load_config
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.observability import configure_logging
from mergexo.orchestrator import Phase1Orchestrator
from mergexo.state import StateStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mergexo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init", help="Initialize state DB, mirror, and worker checkouts"
    )
    init_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))
    init_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose runtime logging to stderr",
    )

    run_parser = subparsers.add_parser(
        "run", help="Run phase-1 issue polling and design PR generation"
    )
    run_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))
    run_parser.add_argument(
        "--once", action="store_true", help="Poll once and wait for active workers"
    )
    run_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose runtime logging to stderr",
    )

    feedback_parser = subparsers.add_parser(
        "feedback",
        help="Inspect and manage feedback-loop state",
    )
    feedback_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))
    feedback_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose runtime logging to stderr",
    )
    feedback_subparsers = feedback_parser.add_subparsers(dest="feedback_command", required=True)

    blocked_parser = feedback_subparsers.add_parser(
        "blocked",
        help="List and reset blocked pull requests",
    )
    blocked_subparsers = blocked_parser.add_subparsers(dest="blocked_command", required=True)

    blocked_list_parser = blocked_subparsers.add_parser(
        "list",
        help="List blocked pull requests and reasons",
    )
    blocked_list_parser.add_argument(
        "--json",
        action="store_true",
        help="Print blocked pull requests as JSON",
    )

    blocked_reset_parser = blocked_subparsers.add_parser(
        "reset",
        help="Reset blocked pull requests back to awaiting_feedback",
    )
    reset_target = blocked_reset_parser.add_mutually_exclusive_group(required=True)
    reset_target.add_argument(
        "--pr",
        type=int,
        action="append",
        help="Specific blocked pull request number to reset (repeatable)",
    )
    reset_target.add_argument(
        "--all",
        action="store_true",
        help="Reset all blocked pull requests",
    )
    blocked_reset_parser.add_argument(
        "--yes",
        action="store_true",
        help="Required with --all unless --dry-run is used",
    )
    blocked_reset_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be reset without mutating state",
    )
    blocked_reset_parser.add_argument(
        "--head-sha",
        type=str,
        help="Override last_seen_head_sha when resetting blocked PRs",
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()
    configure_logging(bool(getattr(args, "verbose", False)))
    config = load_config(args.config)

    if args.command == "init":
        _cmd_init(config)
        return
    if args.command == "run":
        _cmd_run(config, once=bool(args.once))
        return
    if args.command == "feedback":
        _cmd_feedback(config, args)
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


def _cmd_feedback(config: AppConfig, args: argparse.Namespace) -> None:
    state = StateStore(_state_db_path(config))
    if args.feedback_command == "blocked":
        _cmd_feedback_blocked(state, args)
        return
    raise RuntimeError(f"Unknown feedback command: {args.feedback_command}")


def _cmd_feedback_blocked(state: StateStore, args: argparse.Namespace) -> None:
    if args.blocked_command == "list":
        _cmd_feedback_blocked_list(state, as_json=bool(args.json))
        return
    if args.blocked_command == "reset":
        _cmd_feedback_blocked_reset(
            state,
            pr_numbers=tuple(args.pr or ()),
            reset_all=bool(args.all),
            yes=bool(args.yes),
            dry_run=bool(args.dry_run),
            head_sha_override=str(args.head_sha) if args.head_sha is not None else None,
        )
        return
    raise RuntimeError(f"Unknown blocked command: {args.blocked_command}")


def _cmd_feedback_blocked_list(state: StateStore, *, as_json: bool) -> None:
    blocked = state.list_blocked_pull_requests()
    if as_json:
        payload = [
            {
                "pr_number": item.pr_number,
                "issue_number": item.issue_number,
                "branch": item.branch,
                "last_seen_head_sha": item.last_seen_head_sha,
                "pending_event_count": item.pending_event_count,
                "blocked_at": item.updated_at,
                "reason": item.error,
            }
            for item in blocked
        ]
        print(json.dumps(payload, indent=2))
        return

    if not blocked:
        print("No blocked pull requests.")
        return

    for item in blocked:
        print(
            f"pr_number={item.pr_number} issue_number={item.issue_number} "
            f"pending_events={item.pending_event_count} blocked_at={item.updated_at}"
        )
        print(f"branch={item.branch}")
        print(f"reason={_summarize_block_reason(item.error)}")
        print()


def _cmd_feedback_blocked_reset(
    state: StateStore,
    *,
    pr_numbers: tuple[int, ...],
    reset_all: bool,
    yes: bool,
    dry_run: bool,
    head_sha_override: str | None,
) -> None:
    if reset_all and not (yes or dry_run):
        raise RuntimeError("--all requires --yes (or run with --dry-run)")
    normalized_head_sha: str | None = None
    if head_sha_override is not None:
        candidate = head_sha_override.strip()
        if not re.fullmatch(r"[0-9a-fA-F]{7,64}", candidate):
            raise RuntimeError("--head-sha must be a hex git commit SHA (7-64 chars)")
        normalized_head_sha = candidate.lower()

    blocked = state.list_blocked_pull_requests()
    blocked_by_pr = {item.pr_number: item for item in blocked}
    if reset_all:
        selected_pr_numbers = tuple(item.pr_number for item in blocked)
    else:
        selected_pr_numbers = tuple(dict.fromkeys(pr_numbers))

    matched_pr_numbers = tuple(
        pr_number for pr_number in selected_pr_numbers if pr_number in blocked_by_pr
    )
    if not matched_pr_numbers:
        print("No blocked pull requests matched.")
        return

    if dry_run:
        summary = "Would reset blocked pull requests: " + ", ".join(
            str(pr_number) for pr_number in matched_pr_numbers
        )
        if normalized_head_sha is not None:
            summary += f" (override last_seen_head_sha={normalized_head_sha})"
        print(summary)
        return

    reset_count = state.reset_blocked_pull_requests(
        pr_numbers=None if reset_all else matched_pr_numbers,
        last_seen_head_sha_override=normalized_head_sha,
    )
    print(f"Reset {reset_count} blocked pull request(s).")


def _summarize_block_reason(error: str | None) -> str:
    if error is None:
        return "<none>"
    first_line = error.strip().splitlines()[0] if error.strip() else ""
    if not first_line:
        return "<none>"
    if len(first_line) <= 200:
        return first_line
    return f"{first_line[:197]}..."


def _state_db_path(config: AppConfig) -> Path:
    return config.runtime.base_dir / "state.db"
