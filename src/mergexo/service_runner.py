from __future__ import annotations

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
from mergexo.observability import log_event
from mergexo.orchestrator import (
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
        github_by_repo_full_name = {repo.full_name: github for repo, github, _ in runtimes}
        repo_count = len(runtimes)
        if repo_count < 1:
            raise RuntimeError("No repositories configured for service runner")

        if repo_count == 1:
            repo, github, git_manager = runtimes[0]
            while True:
                orchestrator = Phase1Orchestrator(
                    self.config,
                    state=self.state,
                    github=github,
                    git_manager=git_manager,
                    repo=repo,
                    github_by_repo_full_name=github_by_repo_full_name,
                    agent=self._agent_for_repo(repo),
                    allow_runtime_restart=True,
                )
                try:
                    orchestrator.run(once=once)
                    return
                except RestartRequested as requested:
                    restarted = self._handle_restart_requested(requested=requested)
                    if restarted:
                        return
                    if once:
                        return

        polls_completed = 0
        poll_index = 0
        while True:
            repo, github, git_manager = runtimes[poll_index]
            orchestrator = Phase1Orchestrator(
                self.config,
                state=self.state,
                github=github,
                git_manager=git_manager,
                repo=repo,
                github_by_repo_full_name=github_by_repo_full_name,
                agent=self._agent_for_repo(repo),
                allow_runtime_restart=True,
            )
            try:
                orchestrator.run(once=True)
            except RestartRequested as requested:
                restarted = self._handle_restart_requested(requested=requested)
                if restarted:
                    return
                if once:
                    return
            polls_completed += 1
            if once and polls_completed >= repo_count:
                return
            poll_index = (poll_index + 1) % repo_count
            if not once:
                time.sleep(self.config.runtime.poll_interval_seconds)

    def _handle_restart_requested(self, *, requested: RestartRequested) -> bool:
        command_key = requested.command_key
        mode = requested.mode
        request_repo_full_name = requested.repo_full_name
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
                repo_full_name=command.repo_full_name or self.config.repo.full_name,
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
