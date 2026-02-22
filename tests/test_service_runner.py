from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import cast

import pytest

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import append_action_token, compute_operator_command_token
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.models import PullRequestIssueComment, RestartMode
from mergexo.orchestrator import RestartRequested
from mergexo.service_runner import ServiceRunner, run_service
from mergexo.state import StateStore


def _app_config(
    tmp_path: Path,
    *,
    restart_supported_modes: tuple[RestartMode, ...] = ("git_checkout",),
    restart_default_mode: RestartMode = "git_checkout",
    service_python: str | None = None,
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
            enable_feedback_loop=False,
            enable_github_operations=True,
            restart_drain_timeout_seconds=60,
            restart_default_mode=restart_default_mode,
            restart_supported_modes=restart_supported_modes,
            git_checkout_root=tmp_path,
            service_python=service_python,
        ),
        repo=RepoConfig(
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            local_clone_source=None,
            remote_url=None,
            operations_issue_number=77,
            operator_logins=("alice",),
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


@dataclass
class FakeGitHub:
    threads: dict[int, list[PullRequestIssueComment]]
    posted: list[tuple[int, str]]
    next_comment_id: int = 1000

    def __init__(self) -> None:
        self.threads = {}
        self.posted = []
        self.next_comment_id = 1000

    def list_issue_comments(self, issue_number: int) -> list[PullRequestIssueComment]:
        return list(self.threads.get(issue_number, ()))

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        self.posted.append((issue_number, body))
        self.next_comment_id += 1
        self.threads.setdefault(issue_number, []).append(
            PullRequestIssueComment(
                comment_id=self.next_comment_id,
                body=body,
                user_login="mergexo[bot]",
                html_url=f"https://example/issues/{issue_number}#issuecomment-{self.next_comment_id}",
                created_at="now",
                updated_at="now",
            )
        )


def _seed_restart_command(state: StateStore, *, command_key: str) -> None:
    state.record_operator_command(
        command_key=command_key,
        issue_number=77,
        pr_number=None,
        comment_id=15,
        author_login="alice",
        command="restart",
        args_json=json.dumps(
            {
                "normalized_command": "/mergexo restart",
                "args": {},
                "comment_url": "https://example/issues/77#issuecomment-15",
            }
        ),
        status="applied",
        result="pending",
    )
    state.request_runtime_restart(
        requested_by="alice",
        request_command_key=command_key,
        mode="git_checkout",
    )


def test_service_runner_returns_when_orchestrator_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub()
    called = {"runs": 0}

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def run(self, *, once: bool) -> None:
            _ = once
            called["runs"] += 1

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner.run(once=False)
    assert called["runs"] == 1


def test_service_runner_restart_failure_marks_failed_and_posts_reply(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    command_key = "77:15:2026-02-22T13:00:00Z"
    _seed_restart_command(state, command_key=command_key)
    github = FakeGitHub()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def run(self, *, once: bool) -> None:
            _ = once
            raise RestartRequested(mode="git_checkout", command_key=command_key)

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(
        "mergexo.service_runner.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )  # type: ignore[no-untyped-call]

    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner.run(once=True)

    op = state.get_runtime_operation("restart")
    assert op is not None
    assert op.status == "failed"
    command = state.get_operator_command(command_key)
    assert command is not None
    assert command.status == "failed"
    assert len(github.posted) == 1
    assert "status: `failed`" in github.posted[0][1]


def test_service_runner_restart_success_updates_and_reexecs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    command_key = "77:16:2026-02-22T13:01:00Z"
    _seed_restart_command(state, command_key=command_key)
    github = FakeGitHub()
    commands: list[tuple[list[str], Path | None]] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def run(self, *, once: bool) -> None:
            _ = once
            raise RestartRequested(mode="git_checkout", command_key=command_key)

    class ExecCalled(Exception):
        pass

    def fake_run(argv: list[str], *, cwd: Path | None = None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = input_text, check
        commands.append((argv, cwd))
        return ""

    def fake_execvpe(file: str, argv: list[str], env: dict[str, str]) -> None:
        _ = file, argv, env
        raise ExecCalled()

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr("mergexo.service_runner.run", fake_run)
    monkeypatch.setattr(os, "execvpe", fake_execvpe)

    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service", "--config", "mergexo.toml"),
    )
    with pytest.raises(ExecCalled):
        runner.run(once=False)

    assert commands[0][0][:5] == ["git", "-C", str(tmp_path), "pull", "--ff-only"]
    assert commands[1][0] == ["uv", "sync"]
    assert commands[1][1] == tmp_path

    op = state.get_runtime_operation("restart")
    assert op is not None
    assert op.status == "completed"
    command = state.get_operator_command(command_key)
    assert command is not None
    assert command.status == "applied"
    assert len(github.posted) == 1
    assert "status: `applied`" in github.posted[0][1]


def test_service_runner_update_modes_and_reexec_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(
        tmp_path, restart_supported_modes=("git_checkout", "pypi"), service_python="py3"
    )
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub()
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    called: list[list[str]] = []
    monkeypatch.setattr(
        "mergexo.service_runner.run",
        lambda argv, **kwargs: called.append(argv) or "",
    )

    runner._run_update(mode="pypi")
    assert called == [["uv", "pip", "install", "--python", "py3", "--upgrade", "mergexo"]]

    cfg_missing_python = _app_config(
        tmp_path,
        restart_supported_modes=("git_checkout", "pypi"),
        service_python=None,
    )
    runner_missing = ServiceRunner(
        config=cfg_missing_python,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    with pytest.raises(RuntimeError, match="service_python"):
        runner_missing._run_update(mode="pypi")

    cfg_unsupported = _app_config(tmp_path, restart_supported_modes=("git_checkout",))
    runner_unsupported = ServiceRunner(
        config=cfg_unsupported,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    with pytest.raises(RuntimeError, match="not supported"):
        runner_unsupported._run_update(mode="pypi")

    runner_empty_argv = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=(),
    )
    with pytest.raises(RuntimeError, match="startup argv"):
        runner_empty_argv._reexec()


def test_service_runner_post_operator_result_is_idempotent(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    command = state.record_operator_command(
        command_key="77:17:2026-02-22T13:02:00Z",
        issue_number=77,
        pr_number=None,
        comment_id=17,
        author_login="alice",
        command="restart",
        args_json=json.dumps(
            {
                "normalized_command": "/mergexo restart",
                "args": {},
                "comment_url": "https://example/issues/77#issuecomment-17",
            }
        ),
        status="applied",
        result="ok",
    )
    github = FakeGitHub()
    token = compute_operator_command_token(command_key=command.command_key)
    github.threads[77] = [
        PullRequestIssueComment(
            comment_id=999,
            body=append_action_token(body="already posted", token=token),
            user_login="mergexo[bot]",
            html_url="https://example/issues/77#issuecomment-999",
            created_at="now",
            updated_at="now",
        )
    ]
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner._post_operator_command_result(command=command, reply_status="applied", detail="ok")
    assert github.posted == []


def test_service_runner_run_returns_when_restart_handled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def run(self, *, once: bool) -> None:
            _ = once
            raise RestartRequested(mode="git_checkout", command_key="k")

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(ServiceRunner, "_handle_restart_requested", lambda self, requested: True)
    runner.run(once=False)


def test_handle_restart_requested_returns_true_when_reexec_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    command_key = "77:18:2026-02-22T13:03:00Z"
    _seed_restart_command(state, command_key=command_key)
    github = FakeGitHub()
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(ServiceRunner, "_run_update", lambda self, mode: None)
    monkeypatch.setattr(ServiceRunner, "_reexec", lambda self: None)
    assert (
        runner._handle_restart_requested(
            requested=RestartRequested(mode="git_checkout", command_key=command_key)
        )
        is True
    )


def test_run_service_wrapper_constructs_runner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub()
    calls: dict[str, object] = {}

    class FakeRunner:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            calls["kwargs"] = kwargs

        def run(self, *, once: bool) -> None:
            calls["once"] = once

    monkeypatch.setattr("mergexo.service_runner.ServiceRunner", FakeRunner)
    run_service(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        once=True,
        startup_argv=("mergexo", "service"),
    )
    assert calls["once"] is True
    kwargs = cast(dict[str, object], calls["kwargs"])
    assert kwargs["startup_argv"] == ("mergexo", "service")
