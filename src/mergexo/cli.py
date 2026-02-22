from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import time

from mergexo.codex_adapter import CodexAdapter
from mergexo.config import AppConfig, RepoConfig, load_config
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.observability import configure_logging
from mergexo.orchestrator import Phase1Orchestrator
from mergexo.service_runner import run_service
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

    service_parser = subparsers.add_parser(
        "service",
        help="Run orchestrator under supervisor mode with GitHub-driven restart support",
    )
    service_parser.add_argument("--config", type=Path, default=Path("mergexo.toml"))
    service_parser.add_argument(
        "--once", action="store_true", help="Poll once and wait for active workers"
    )
    service_parser.add_argument(
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
    blocked_reset_parser.add_argument(
        "--repo",
        type=str,
        help="Optional repo filter (repo id or owner/name). Required with --pr in multi-repo mode.",
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
    if args.command == "service":
        _cmd_service(config, once=bool(args.once))
        return
    if args.command == "feedback":
        _cmd_feedback(config, args)
        return

    raise RuntimeError(f"Unknown command: {args.command}")


def _cmd_init(config: AppConfig) -> None:
    config.runtime.base_dir.mkdir(parents=True, exist_ok=True)
    state = StateStore(_state_db_path(config))
    _ = state

    repo_managers: list[tuple[RepoConfig, GitRepoManager]] = []
    for repo in config.repos:
        git_manager = GitRepoManager(config.runtime, repo)
        git_manager.ensure_layout()
        repo_managers.append((repo, git_manager))

    print(f"Initialized MergeXO base dir: {config.runtime.base_dir}")
    for repo, git_manager in repo_managers:
        print(f"Repo: {repo.full_name}")
        print(f"Mirror: {git_manager.layout.mirror_path}")
        print(f"Checkouts: {git_manager.layout.checkouts_root}")


def _cmd_run(config: AppConfig, *, once: bool) -> None:
    config.runtime.base_dir.mkdir(parents=True, exist_ok=True)
    state = StateStore(_state_db_path(config))
    agent = CodexAdapter(config.codex)
    runtimes = _build_repo_runtimes(config)
    github_by_repo_full_name = {repo.full_name: github for repo, github, _ in runtimes}
    orchestrators = [
        Phase1Orchestrator(
            config,
            state=state,
            github=github,
            git_manager=git_manager,
            repo=repo,
            github_by_repo_full_name=github_by_repo_full_name,
            agent=agent,
        )
        for repo, github, git_manager in runtimes
    ]
    if len(orchestrators) == 1:
        orchestrators[0].run(once=once)
        return
    if once:
        for orchestrator in orchestrators:
            orchestrator.run(once=True)
        return

    idx = 0
    while True:
        orchestrators[idx].run(once=True)
        idx = (idx + 1) % len(orchestrators)
        time.sleep(config.runtime.poll_interval_seconds)


def _cmd_service(config: AppConfig, *, once: bool) -> None:
    config.runtime.base_dir.mkdir(parents=True, exist_ok=True)
    state = StateStore(_state_db_path(config))
    agent = CodexAdapter(config.codex)
    runtimes = _build_repo_runtimes(config)
    run_service(
        config=config,
        state=state,
        repo_runtimes=runtimes,
        agent=agent,
        once=once,
    )


def _cmd_feedback(config: AppConfig, args: argparse.Namespace) -> None:
    state = StateStore(_state_db_path(config))
    if args.feedback_command == "blocked":
        _cmd_feedback_blocked(config, state, args)
        return
    raise RuntimeError(f"Unknown feedback command: {args.feedback_command}")


def _cmd_feedback_blocked(config: AppConfig, state: StateStore, args: argparse.Namespace) -> None:
    if args.blocked_command == "list":
        _cmd_feedback_blocked_list(state, as_json=bool(args.json))
        return
    if args.blocked_command == "reset":
        repo_filter = (
            _resolve_repo_filter(config, str(args.repo)) if args.repo is not None else None
        )
        _cmd_feedback_blocked_reset(
            state,
            configured_repos=config.repos,
            pr_numbers=tuple(args.pr or ()),
            reset_all=bool(args.all),
            yes=bool(args.yes),
            dry_run=bool(args.dry_run),
            head_sha_override=str(args.head_sha) if args.head_sha is not None else None,
            repo_full_name_filter=repo_filter,
        )
        return
    raise RuntimeError(f"Unknown blocked command: {args.blocked_command}")


def _cmd_feedback_blocked_list(state: StateStore, *, as_json: bool) -> None:
    blocked = state.list_blocked_pull_requests()
    if as_json:
        payload = [
            {
                "repo_full_name": item.repo_full_name,
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
            f"repo={item.repo_full_name} pr_number={item.pr_number} issue_number={item.issue_number} "
            f"pending_events={item.pending_event_count} blocked_at={item.updated_at}"
        )
        print(f"branch={item.branch}")
        print(f"reason={_summarize_block_reason(item.error)}")
        print()


def _cmd_feedback_blocked_reset(
    state: StateStore,
    *,
    configured_repos: tuple[RepoConfig, ...],
    pr_numbers: tuple[int, ...],
    reset_all: bool,
    yes: bool,
    dry_run: bool,
    head_sha_override: str | None,
    repo_full_name_filter: str | None,
) -> None:
    if reset_all and not (yes or dry_run):
        raise RuntimeError("--all requires --yes (or run with --dry-run)")
    if pr_numbers and len(configured_repos) > 1 and repo_full_name_filter is None:
        raise RuntimeError("--repo is required with --pr when multiple repos are configured")
    normalized_head_sha: str | None = None
    if head_sha_override is not None:
        candidate = head_sha_override.strip()
        if not re.fullmatch(r"[0-9a-fA-F]{7,64}", candidate):
            raise RuntimeError("--head-sha must be a hex git commit SHA (7-64 chars)")
        normalized_head_sha = candidate.lower()

    blocked = state.list_blocked_pull_requests(repo_full_name=repo_full_name_filter)
    blocked_by_pr: dict[int, list[str]] = {}
    for item in blocked:
        blocked_by_pr.setdefault(item.pr_number, []).append(item.repo_full_name)
    if reset_all:
        selected_pr_numbers = tuple(item.pr_number for item in blocked)
    else:
        selected_pr_numbers = tuple(dict.fromkeys(pr_numbers))

    matched_pr_numbers = tuple(
        pr_number for pr_number in selected_pr_numbers if pr_number in blocked_by_pr
    )
    if not matched_pr_numbers and not reset_all:
        print("No blocked pull requests matched.")
        return

    if dry_run:
        if reset_all:
            summary = "Would reset all blocked pull requests"
        else:
            summary = "Would reset blocked pull requests: " + ", ".join(
                str(pr_number) for pr_number in matched_pr_numbers
            )
        if repo_full_name_filter is not None:
            summary += f" (repo={repo_full_name_filter})"
        if normalized_head_sha is not None:
            summary += f" (override last_seen_head_sha={normalized_head_sha})"
        print(summary)
        return

    reset_count = state.reset_blocked_pull_requests(
        pr_numbers=None if reset_all else matched_pr_numbers,
        last_seen_head_sha_override=normalized_head_sha,
        repo_full_name=repo_full_name_filter,
    )
    print(f"Reset {reset_count} blocked pull request(s).")


def _build_repo_runtimes(
    config: AppConfig,
) -> tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...]:
    runtimes: list[tuple[RepoConfig, GitHubGateway, GitRepoManager]] = []
    for repo in config.repos:
        runtimes.append(
            (
                repo,
                GitHubGateway(repo.owner, repo.name),
                GitRepoManager(config.runtime, repo),
            )
        )
    return tuple(runtimes)


def _resolve_repo_filter(config: AppConfig, raw_repo_filter: str) -> str:
    candidate = raw_repo_filter.strip()
    if not candidate:
        raise RuntimeError("--repo must be non-empty")
    for repo in config.repos:
        if candidate == repo.repo_id or candidate == repo.full_name:
            return repo.full_name
    available = ", ".join(sorted(repo.full_name for repo in config.repos))
    raise RuntimeError(f"Unknown --repo value {candidate!r}. Expected one of: {available}")


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
