from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
import sys
import time
from typing import cast

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, RepoConfig
from mergexo.feedback_loop import (
    append_action_token,
    compute_operator_command_token,
    extract_action_tokens,
)
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.models import OperatorCommandRecord, OperatorReplyStatus, RestartMode
from mergexo.observability import log_event, logging_repo_context
from mergexo.orchestrator import (
    GlobalWorkLimiter,
    Phase1Orchestrator,
    RestartRequested,
    _operator_normalized_command,
    _operator_reply_issue_number,
    _operator_source_comment_url,
    _render_operator_command_result,
)
from mergexo.shell import run
from mergexo.state import StateStore


LOGGER = logging.getLogger("mergexo.service_runner")
_RESTART_OPERATION_NAME = "restart"
_RESTART_PENDING_STATUSES = {"pending", "running"}


@dataclass(frozen=True)
class ServiceRunner:
    config: AppConfig
    state: StateStore
    agent: AgentAdapter
    startup_argv: tuple[str, ...]
    agent_by_repo_full_name: dict[str, AgentAdapter] = field(default_factory=dict)
    github: GitHubGateway | None = None
    git_manager: GitRepoManager | None = None
    repo_runtimes: tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...] = ()

    def run(self, *, once: bool) -> None:
        runtimes = self._effective_repo_runtimes()
        repo_count = len(runtimes)
        if repo_count < 1:
            raise RuntimeError("No repositories configured for service runner")
        github_by_repo_full_name = {repo.full_name: github for repo, github, _ in runtimes}
        work_limiter = GlobalWorkLimiter(self.config.runtime.worker_count)
        orchestrators = tuple(
            Phase1Orchestrator(
                self.config,
                state=self.state,
                github=github,
                git_manager=git_manager,
                repo=repo,
                github_by_repo_full_name=github_by_repo_full_name,
                agent=self._agent_for_repo(repo),
                allow_runtime_restart=True,
                work_limiter=work_limiter,
            )
            for repo, github, git_manager in runtimes
        )

        with ThreadPoolExecutor(max_workers=self.config.runtime.worker_count) as pool:
            if once:
                for index, (repo, _, _) in enumerate(runtimes):
                    self._poll_repo_once(
                        repo=repo,
                        orchestrator=orchestrators[index],
                        pool=pool,
                        work_limiter=work_limiter,
                        allow_enqueue=True,
                    )
                if (
                    self._total_pending_futures(orchestrators) == 0
                    and not self._restart_operation_is_pending()
                ):
                    return

                restart_drain_started_at_monotonic: float | None = None
                while True:
                    for index, (repo, _, _) in enumerate(runtimes):
                        self._poll_repo_once(
                            repo=repo,
                            orchestrator=orchestrators[index],
                            pool=pool,
                            work_limiter=work_limiter,
                            allow_enqueue=False,
                        )
                    restart_drain_started_at_monotonic, should_exit = (
                        self._process_global_restart_drain(
                            orchestrators=orchestrators,
                            restart_drain_started_at_monotonic=restart_drain_started_at_monotonic,
                            exit_after_terminal=True,
                        )
                    )
                    if should_exit:
                        return
                    if self._total_pending_futures(orchestrators) == 0:
                        return
                    time.sleep(0.1)

            poll_index = 0
            restart_drain_started_at_monotonic: float | None = None
            while True:
                repo, _, _ = runtimes[poll_index]
                # When restart is pending we disable enqueue for every repo. We still call
                # poll_once so each orchestrator can reap finished futures and checkpoint
                # worker terminal state. That gives us a drain loop with no new GitHub work
                # ingestion while preserving state consistency before supervisor re-exec.
                self._poll_repo_once(
                    repo=repo,
                    orchestrator=orchestrators[poll_index],
                    pool=pool,
                    work_limiter=work_limiter,
                    allow_enqueue=not self._restart_operation_is_pending(),
                )
                restart_drain_started_at_monotonic, should_exit = (
                    self._process_global_restart_drain(
                        orchestrators=orchestrators,
                        restart_drain_started_at_monotonic=restart_drain_started_at_monotonic,
                        exit_after_terminal=False,
                    )
                )
                if should_exit:
                    return
                poll_index = (poll_index + 1) % repo_count
                time.sleep(self.config.runtime.poll_interval_seconds)

    def _poll_repo_once(
        self,
        *,
        repo: RepoConfig,
        orchestrator: Phase1Orchestrator,
        pool: ThreadPoolExecutor,
        work_limiter: GlobalWorkLimiter,
        allow_enqueue: bool,
    ) -> None:
        with logging_repo_context(repo.full_name):
            orchestrator.poll_once(pool, allow_enqueue=allow_enqueue)
            running_issue_count, running_feedback_count = orchestrator.queue_counts()
            log_event(
                LOGGER,
                "service_repo_polled",
                repo_full_name=repo.full_name,
                allow_enqueue=allow_enqueue,
                running_issue_count=running_issue_count,
                running_feedback_count=running_feedback_count,
            )
            log_event(
                LOGGER,
                "service_global_capacity",
                in_flight=work_limiter.in_flight(),
                repo_pending_future_count=orchestrator.pending_work_count(),
            )

    def _total_pending_futures(self, orchestrators: tuple[Phase1Orchestrator, ...]) -> int:
        return sum(orchestrator.pending_work_count() for orchestrator in orchestrators)

    def _restart_operation_is_pending(self) -> bool:
        operation = self.state.get_runtime_operation(_RESTART_OPERATION_NAME)
        return operation is not None and operation.status in _RESTART_PENDING_STATUSES

    def _process_global_restart_drain(
        self,
        *,
        orchestrators: tuple[Phase1Orchestrator, ...],
        restart_drain_started_at_monotonic: float | None,
        exit_after_terminal: bool,
    ) -> tuple[float | None, bool]:
        operation = self.state.get_runtime_operation(_RESTART_OPERATION_NAME)
        if operation is None or operation.status not in _RESTART_PENDING_STATUSES:
            return None, False

        with logging_repo_context(operation.request_repo_full_name):
            pending_futures = self._total_pending_futures(orchestrators)
            # Each orchestrator poll reaps finished futures and writes terminal worker state
            # into sqlite. During this drain phase, operator command scans and work enqueue are
            # already paused by restart-pending gates in the orchestrators, so we are not
            # consuming new messages. Waiting for this global count to reach zero gives us a
            # safe restart boundary with no in-flight worker state left uncheckpointed.
            now = time.monotonic()
            if restart_drain_started_at_monotonic is None:
                restart_drain_started_at_monotonic = now
                log_event(
                    LOGGER,
                    "service_restart_drain_started",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                )

            if pending_futures == 0:
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="running",
                    detail="workers drained; supervisor update running",
                )
                log_event(
                    LOGGER,
                    "service_restart_drain_completed",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                )
                restarted = self._handle_restart_requested(
                    requested=RestartRequested(
                        mode=operation.mode,
                        command_key=operation.request_command_key,
                        repo_full_name=operation.request_repo_full_name,
                    )
                )
                if restarted:
                    return None, True
                return None, exit_after_terminal

            elapsed = now - restart_drain_started_at_monotonic
            drain_timeout = self.config.runtime.restart_drain_timeout_seconds
            if elapsed >= drain_timeout:
                detail = (
                    f"Restart drain timed out after {drain_timeout} seconds with "
                    f"{pending_futures} pending future(s)."
                )
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="failed",
                    detail=detail,
                )
                command = self.state.update_operator_command_result(
                    command_key=operation.request_command_key,
                    status="failed",
                    result=detail,
                    repo_full_name=operation.request_repo_full_name,
                )
                if command is not None:
                    self._post_operator_command_result(
                        command=command,
                        reply_status="failed",
                        detail=detail,
                    )
                log_event(
                    LOGGER,
                    "operator_command_failed",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    command="restart",
                    reason="drain_timeout",
                    pending_futures=pending_futures,
                    timeout_seconds=drain_timeout,
                )
                log_event(
                    LOGGER,
                    "service_restart_drain_timeout",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                    pending_futures=pending_futures,
                    timeout_seconds=drain_timeout,
                )
                return None, exit_after_terminal

            log_event(
                LOGGER,
                "service_restart_drain_progress",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                mode=operation.mode,
                pending_futures=pending_futures,
                elapsed_seconds=round(elapsed, 3),
            )
            return restart_drain_started_at_monotonic, False

    def _handle_restart_requested(self, *, requested: RestartRequested) -> bool:
        command_key = requested.command_key
        mode = requested.mode
        request_repo_full_name = requested.repo_full_name
        with logging_repo_context(request_repo_full_name):
            log_event(
                LOGGER,
                "restart_update_started",
                command_key=command_key,
                mode=mode,
            )
            try:
                self._run_update(mode=mode)
            except Exception as exc:  # noqa: BLE001
                detail = f"Restart update failed in mode={mode}: {exc}"
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="failed",
                    detail=detail,
                )
                record = self.state.update_operator_command_result(
                    command_key=command_key,
                    status="failed",
                    result=detail,
                    repo_full_name=request_repo_full_name or None,
                )
                if record is not None:
                    self._post_operator_command_result(
                        command=record,
                        reply_status="failed",
                        detail=detail,
                    )
                log_event(
                    LOGGER,
                    "operator_command_failed",
                    command_key=command_key,
                    actor=record.author_login if record is not None else "<unknown>",
                    command="restart",
                )
                log_event(
                    LOGGER,
                    "restart_update_failed",
                    command_key=command_key,
                    mode=mode,
                )
                return False

            detail = f"Restart update completed in mode={mode}; re-executing service command."
            self.state.set_runtime_operation_status(
                op_name=_RESTART_OPERATION_NAME,
                status="completed",
                detail=detail,
            )
            record = self.state.update_operator_command_result(
                command_key=command_key,
                status="applied",
                result=detail,
                repo_full_name=request_repo_full_name or None,
            )
            if record is not None:
                self._post_operator_command_result(
                    command=record, reply_status="applied", detail=detail
                )
            log_event(
                LOGGER,
                "operator_command_applied",
                command_key=command_key,
                actor=record.author_login if record is not None else "<unknown>",
                command="restart",
                mode=mode,
            )
            log_event(
                LOGGER,
                "restart_completed",
                command_key=command_key,
                mode=mode,
            )
            self._reexec()
            return True

    def _run_update(self, *, mode: RestartMode) -> None:
        if mode not in self.config.runtime.restart_supported_modes:
            supported = ", ".join(self.config.runtime.restart_supported_modes)
            raise RuntimeError(f"Mode {mode} is not supported. Enabled modes: {supported}")

        if mode == "git_checkout":
            checkout_root = self.config.runtime.git_checkout_root or Path.cwd()
            run(
                [
                    "git",
                    "-C",
                    str(checkout_root),
                    "pull",
                    "--ff-only",
                    "origin",
                    self.config.repo.default_branch,
                ]
            )
            run(["uv", "sync"], cwd=checkout_root)
            return

        service_python = self.config.runtime.service_python
        if service_python is None:
            raise RuntimeError("runtime.service_python is required for mode=pypi")
        run(["uv", "pip", "install", "--python", service_python, "--upgrade", "mergexo"])

    def _post_operator_command_result(
        self,
        *,
        command: OperatorCommandRecord,
        reply_status: OperatorReplyStatus,
        detail: str,
    ) -> None:
        default_repo_full_name = command.repo_full_name or self.config.repo.full_name
        with logging_repo_context(default_repo_full_name):
            github = self._github_for_repo(command.repo_full_name)
            issue_number = _operator_reply_issue_number(command)
            token = compute_operator_command_token(command_key=command.command_key)
            if self._issue_has_action_token(github=github, issue_number=issue_number, token=token):
                return
            body = _render_operator_command_result(
                normalized_command=_operator_normalized_command(command),
                status=reply_status,
                detail=detail,
                source_comment_url=_operator_source_comment_url(
                    command=command,
                    repo_full_name=default_repo_full_name,
                ),
            )
            github.post_issue_comment(
                issue_number=issue_number,
                body=append_action_token(body=body, token=token),
            )

    def _issue_has_action_token(
        self, *, github: GitHubGateway, issue_number: int, token: str
    ) -> bool:
        comments = github.list_issue_comments(issue_number)
        return any(token in extract_action_tokens(comment.body) for comment in comments)

    def _effective_repo_runtimes(
        self,
    ) -> tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...]:
        if self.repo_runtimes:
            return self.repo_runtimes
        if self.github is not None and self.git_manager is not None:
            return ((self.config.repo, self.github, self.git_manager),)
        runtimes: list[tuple[RepoConfig, GitHubGateway, GitRepoManager]] = []
        for repo in self.config.repos:
            runtimes.append(
                (
                    repo,
                    GitHubGateway(repo.owner, repo.name),
                    GitRepoManager(self.config.runtime, repo),
                )
            )
        return tuple(runtimes)

    def _agent_for_repo(self, repo: RepoConfig) -> AgentAdapter:
        return self.agent_by_repo_full_name.get(repo.full_name, self.agent)

    def _github_for_repo(self, repo_full_name: str) -> GitHubGateway:
        if repo_full_name:
            for repo, github, _ in self._effective_repo_runtimes():
                if repo.full_name == repo_full_name:
                    return github
        if self.github is not None:
            return self.github
        # Fallback to first configured runtime for defensive paths.
        runtimes = self._effective_repo_runtimes()
        return runtimes[0][1]

    def _reexec(self) -> None:
        if not self.startup_argv:
            raise RuntimeError("Cannot restart service without startup argv")
        argv = list(self.startup_argv)
        os.execvpe(argv[0], argv, os.environ.copy())


def run_service(
    *,
    config: AppConfig,
    state: StateStore,
    agent: AgentAdapter,
    agent_by_repo_full_name: dict[str, AgentAdapter] | None = None,
    once: bool,
    github: GitHubGateway | None = None,
    git_manager: GitRepoManager | None = None,
    repo_runtimes: tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...] | None = None,
    startup_argv: tuple[str, ...] | None = None,
) -> None:
    argv = startup_argv or tuple(sys.argv)
    runner = ServiceRunner(
        config=config,
        state=state,
        agent=agent,
        agent_by_repo_full_name=cast(dict[str, AgentAdapter], dict(agent_by_repo_full_name or {})),
        startup_argv=argv,
        github=github,
        git_manager=git_manager,
        repo_runtimes=repo_runtimes or (),
    )
    runner.run(once=once)
