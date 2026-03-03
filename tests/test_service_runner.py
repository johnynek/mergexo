from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
from threading import Event
import time
from typing import cast
from urllib import error as urlerror
from urllib import request

import pytest

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import append_action_token, compute_operator_command_token
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.models import Issue, PullRequestIssueComment, RestartMode
from mergexo.orchestrator import Phase1Orchestrator, RestartRequested
from mergexo.service_runner import (
    _ContinuousDeployCandidate,
    _ServiceHealthTracker,
    ServiceRunner,
    ServiceSignal,
    run_service,
)
from mergexo.state import StateStore


def _app_config(
    tmp_path: Path,
    *,
    restart_supported_modes: tuple[RestartMode, ...] = ("git_checkout",),
    restart_default_mode: RestartMode = "git_checkout",
    service_python: str | None = None,
    restart_drain_timeout_seconds: int = 60,
    continuous_deploy_enabled: bool = False,
    continuous_deploy_check_interval_seconds: int = 300,
    continuous_deploy_branch: str = "main",
    continuous_deploy_healthcheck_host: str = "127.0.0.1",
    continuous_deploy_healthcheck_port: int = 0,
    continuous_deploy_max_boot_failures: int = 2,
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
            enable_github_operations=True,
            restart_drain_timeout_seconds=restart_drain_timeout_seconds,
            restart_default_mode=restart_default_mode,
            restart_supported_modes=restart_supported_modes,
            git_checkout_root=tmp_path,
            service_python=service_python,
            continuous_deploy_enabled=continuous_deploy_enabled,
            continuous_deploy_check_interval_seconds=continuous_deploy_check_interval_seconds,
            continuous_deploy_branch=continuous_deploy_branch,
            continuous_deploy_healthcheck_host=continuous_deploy_healthcheck_host,
            continuous_deploy_healthcheck_port=continuous_deploy_healthcheck_port,
            continuous_deploy_max_boot_failures=continuous_deploy_max_boot_failures,
        ),
        repos=(
            RepoConfig(
                repo_id="mergexo",
                owner="johnynek",
                name="mergexo",
                default_branch="main",
                trigger_label="agent:design",
                bugfix_label="agent:bugfix",
                small_job_label="agent:small-job",
                coding_guidelines_path="docs/python_style.md",
                design_docs_dir="docs/design",
                allowed_users=frozenset({"alice"}),
                local_clone_source=None,
                remote_url=None,
                operations_issue_number=77,
                operator_logins=("alice",),
            ),
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def _multi_repo_config(tmp_path: Path) -> AppConfig:
    base = _app_config(tmp_path)
    second = RepoConfig(
        repo_id="bosatsu",
        owner="johnynek",
        name="bosatsu",
        default_branch="main",
        trigger_label="agent:design",
        bugfix_label="agent:bugfix",
        small_job_label="agent:small-job",
        coding_guidelines_path="docs/python_style.md",
        design_docs_dir="docs/design",
        allowed_users=frozenset({"alice"}),
        local_clone_source=None,
        remote_url=None,
        operations_issue_number=78,
        operator_logins=("alice",),
    )
    return AppConfig(runtime=base.runtime, repos=(base.repo, second), codex=base.codex)


@dataclass
class FakeGitHub:
    threads: dict[int, list[PullRequestIssueComment]]
    posted: list[tuple[int, str]]
    created_issues: list[Issue]
    next_comment_id: int = 1000
    next_issue_number: int = 2000

    def __init__(self) -> None:
        self.threads = {}
        self.posted = []
        self.created_issues = []
        self.next_comment_id = 1000
        self.next_issue_number = 2000

    def list_issue_comments(
        self, issue_number: int, *, since: str | None = None
    ) -> list[PullRequestIssueComment]:
        _ = since
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

    def create_issue(
        self,
        *,
        title: str,
        body: str,
        labels: tuple[str, ...] | None = None,
    ) -> Issue:
        issue = Issue(
            number=self.next_issue_number,
            title=title,
            body=body,
            html_url=f"https://example/issues/{self.next_issue_number}",
            labels=labels or (),
            author_login="mergexo[bot]",
        )
        self.next_issue_number += 1
        self.created_issues.append(issue)
        return issue


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
        repo_full_name="johnynek/mergexo",
    )
    state.request_runtime_restart(
        requested_by="alice",
        request_command_key=command_key,
        mode="git_checkout",
        request_repo_full_name="johnynek/mergexo",
    )


def _free_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def test_service_runner_returns_when_orchestrator_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub()
    called = {"polls": 0}

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            called["polls"] += 1

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner.run(once=True)
    assert called["polls"] == 1


def test_service_runner_rejects_empty_repo_runtimes(tmp_path: Path) -> None:
    cfg = AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
        ),
        repos=(),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )
    runner = ServiceRunner(
        config=cfg,
        state=StateStore(tmp_path / "state.db"),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    with pytest.raises(RuntimeError, match="No repositories configured"):
        runner.run(once=True)


def test_service_runner_multi_repo_once_polls_each_repo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    runs: list[str] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args
            repo = cast(RepoConfig, kwargs["repo"])
            self._repo_full_name = repo.full_name

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool
            if allow_enqueue:
                runs.append(self._repo_full_name)

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )
    runner.run(once=True)
    assert runs == [cfg.repos[0].full_name, cfg.repos[1].full_name]


def test_service_runner_multi_repo_uses_repo_specific_agents(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    calls: list[tuple[str, object]] = []
    agent_a = cast(AgentAdapter, object())
    agent_b = cast(AgentAdapter, object())

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args
            repo = cast(RepoConfig, kwargs["repo"])
            agent = kwargs["agent"]
            calls.append((repo.full_name, agent))

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        agent_by_repo_full_name={
            cfg.repos[0].full_name: agent_a,
            cfg.repos[1].full_name: agent_b,
        },
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )

    runner.run(once=True)
    assert calls == [
        (cfg.repos[0].full_name, agent_a),
        (cfg.repos[1].full_name, agent_b),
    ]


def test_service_runner_multi_repo_non_once_sleeps_between_polls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    runs: list[str] = []
    sleep_calls: list[int] = []

    class StopLoop(Exception):
        pass

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args
            repo = cast(RepoConfig, kwargs["repo"])
            self._repo_full_name = repo.full_name

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            runs.append(self._repo_full_name)

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        raise StopLoop()

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr("mergexo.service_runner.time.sleep", fake_sleep)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )
    with pytest.raises(StopLoop):
        runner.run(once=False)
    assert runs == [cfg.repos[0].full_name]
    assert sleep_calls == [cfg.runtime.poll_interval_seconds]


def test_service_runner_stop_event_interrupts_continuous_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    stop_event = Event()
    polls: list[bool] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool
            polls.append(allow_enqueue)
            stop_event.set()

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(
        "mergexo.service_runner.time.sleep",
        lambda seconds: (_ for _ in ()).throw(AssertionError(f"unexpected sleep: {seconds}")),
    )
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        stop_event=stop_event,
    )
    runner.run(once=False)
    assert polls == [True]


def test_service_runner_signal_sink_receives_poll_reap_and_restart_hints(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    _seed_restart_command(state, command_key="77:24:2026-02-24T10:00:00Z")
    signals: list[ServiceSignal] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            self.pending = 0

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(ServiceRunner, "_handle_restart_requested", lambda self, requested: False)

    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        signal_sink=signals.append,
    )
    runner.run(once=True)

    kinds = [signal.kind for signal in signals]
    assert "poll_completed" in kinds
    assert "work_reaped" in kinds
    restart_details = [
        signal.detail for signal in signals if signal.kind == "restart_drain_state_changed"
    ]
    assert restart_details == ["started", "completed"]


def test_service_runner_signal_sink_failure_is_swallowed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[str] = []
    runner = ServiceRunner(
        config=_app_config(tmp_path),
        state=StateStore(tmp_path / "state.db"),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        signal_sink=lambda signal: (_ for _ in ()).throw(RuntimeError(signal.kind)),
    )
    monkeypatch.setattr(
        "mergexo.service_runner.LOGGER.exception", lambda message: captured.append(message)
    )
    runner._emit_signal(ServiceSignal(kind="poll_completed"))
    assert captured == ["service_signal_sink_failed"]


def test_service_runner_once_returns_when_stop_event_set_before_poll(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    stop_event = Event()
    stop_event.set()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            raise AssertionError("poll_once should not run when stop_event is set")

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        stop_event=stop_event,
    )
    runner.run(once=True)


def test_service_runner_once_returns_when_stop_event_set_before_drain_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    stop_event = Event()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1
            self.calls = 0

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            self.calls += 1
            if self.calls == 1:
                stop_event.set()

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        stop_event=stop_event,
    )
    runner.run(once=True)


def test_service_runner_once_returns_when_stop_requested_mid_drain_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    should_stop = iter((False, False, True))
    monkeypatch.setattr(ServiceRunner, "_should_stop", lambda self: next(should_stop))
    runner.run(once=True)


def test_service_runner_once_returns_when_stop_wait_is_triggered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(ServiceRunner, "_wait_for_stop", lambda self, timeout_seconds: True)
    runner.run(once=True)


def test_service_runner_continuous_returns_when_stop_event_set_before_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    stop_event = Event()
    stop_event.set()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue
            raise AssertionError("poll_once should not run when stop_event is set")

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        stop_event=stop_event,
    )
    runner.run(once=False)


def test_service_runner_multi_repo_continuous_reuses_orchestrators_and_shared_pool(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    init_calls: list[tuple[str, int]] = []
    poll_calls: list[tuple[str, int, bool]] = []

    class StopLoop(Exception):
        pass

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args
            repo = cast(RepoConfig, kwargs["repo"])
            work_limiter = kwargs["work_limiter"]
            self._repo_full_name = repo.full_name
            init_calls.append((self._repo_full_name, id(work_limiter)))

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            poll_calls.append((self._repo_full_name, id(pool), allow_enqueue))

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    sleep_calls: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 3:
            raise StopLoop()

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr("mergexo.service_runner.time.sleep", fake_sleep)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )

    with pytest.raises(StopLoop):
        runner.run(once=False)

    assert len(init_calls) == 2
    assert len({limiter_id for _, limiter_id in init_calls}) == 1
    assert [call[0] for call in poll_calls[:3]] == [
        cfg.repos[0].full_name,
        cfg.repos[1].full_name,
        cfg.repos[0].full_name,
    ]
    assert len({pool_id for _, pool_id, _ in poll_calls}) == 1


def test_service_runner_once_drains_pending_work_with_enqueue_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    allow_enqueue_calls: list[bool] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool
            allow_enqueue_calls.append(allow_enqueue)
            if not allow_enqueue:
                self.pending = 0

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner.run(once=True)

    assert allow_enqueue_calls == [True, False]


def test_service_runner_shuts_down_after_github_auth_drain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    allow_enqueue_calls: list[bool] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1
            self.auth_pending = False

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool
            allow_enqueue_calls.append(allow_enqueue)
            if allow_enqueue:
                self.auth_pending = True
                return
            self.pending = 0

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

        def github_auth_shutdown_pending(self) -> bool:
            return self.auth_pending

        def github_auth_shutdown_reason(self) -> str:
            return "gh is not authenticated"

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr("mergexo.service_runner.time.sleep", lambda seconds: None)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    with pytest.raises(RuntimeError, match="gh is not authenticated"):
        runner.run(once=True)

    assert allow_enqueue_calls == [True, False]


def test_service_runner_auth_drain_continuous_wait_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    wait_calls: list[float] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 1
            self.auth_pending = True

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

        def github_auth_shutdown_pending(self) -> bool:
            return self.auth_pending

        def github_auth_shutdown_reason(self) -> str:
            return "gh is not authenticated"

    wait_outcomes = iter((False, True))

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(ServiceRunner, "_should_stop", lambda self: False)

    def fake_wait(self, timeout_seconds: float) -> bool:  # type: ignore[no-untyped-def]
        wait_calls.append(timeout_seconds)
        return next(wait_outcomes)

    monkeypatch.setattr(ServiceRunner, "_wait_for_stop", fake_wait)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner.run(once=False)

    assert wait_calls[:2] == [0.1, 0.1]


def test_service_runner_auth_drain_continuous_raises_when_drained(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs
            self.pending = 0
            self.auth_pending = True

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

        def github_auth_shutdown_pending(self) -> bool:
            return self.auth_pending

        def github_auth_shutdown_reason(self) -> str:
            return "gh is not authenticated"

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(ServiceRunner, "_should_stop", lambda self: False)
    monkeypatch.setattr(ServiceRunner, "_wait_for_stop", lambda self, timeout_seconds: False)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    with pytest.raises(RuntimeError, match="gh is not authenticated"):
        runner.run(once=False)


def test_service_runner_auth_shutdown_helper_fallbacks(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    class BadPending:
        def github_auth_shutdown_pending(self) -> bool:
            raise RuntimeError("boom")

    class BadReason:
        def github_auth_shutdown_reason(self) -> str:
            raise RuntimeError("boom")

    assert runner._orchestrator_github_auth_shutdown_pending(BadPending()) is False
    assert (
        runner._orchestrator_github_auth_shutdown_reason(BadReason())
        == "GitHub CLI is not authenticated. Run `gh auth login` and restart MergeXO."
    )


def test_service_runner_multi_repo_restart_returns_when_handled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    _seed_restart_command(state, command_key="77:19:2026-02-22T13:04:00Z")

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )
    monkeypatch.setattr(ServiceRunner, "_handle_restart_requested", lambda self, requested: True)
    runner.run(once=False)


def test_service_runner_multi_repo_restart_returns_on_once_when_not_restarted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    _seed_restart_command(state, command_key="77:20:2026-02-22T13:05:00Z")

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )
    monkeypatch.setattr(ServiceRunner, "_handle_restart_requested", lambda self, requested: False)
    runner.run(once=True)


def test_service_runner_restart_drain_waits_for_other_repo_pending_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    _seed_restart_command(state, command_key="77:22:2026-02-22T13:07:00Z")
    poll_calls: list[tuple[str, bool]] = []
    sleep_calls: list[int] = []

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args
            repo = cast(RepoConfig, kwargs["repo"])
            self._repo_full_name = repo.full_name
            self.pending = 1 if repo.repo_id == "bosatsu" else 0

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool
            poll_calls.append((self._repo_full_name, allow_enqueue))
            if self._repo_full_name.endswith("/bosatsu"):
                self.pending = 0

        def queue_counts(self) -> tuple[int, int]:
            return self.pending, 0

        def pending_work_count(self) -> int:
            return self.pending

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    restart_calls: list[str] = []

    def fake_handle_restart(self, *, requested: RestartRequested) -> bool:  # type: ignore[no-untyped-def]
        restart_calls.append(requested.command_key)
        return True

    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr("mergexo.service_runner.time.sleep", fake_sleep)
    monkeypatch.setattr(ServiceRunner, "_handle_restart_requested", fake_handle_restart)

    runner = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (
                cfg.repos[0],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
            (
                cfg.repos[1],
                cast(GitHubGateway, FakeGitHub()),
                cast(GitRepoManager, object()),
            ),
        ),
    )

    runner.run(once=False)
    assert restart_calls == ["77:22:2026-02-22T13:07:00Z"]
    assert [repo for repo, _ in poll_calls] == [cfg.repos[0].full_name, cfg.repos[1].full_name]
    assert [allow_enqueue for _, allow_enqueue in poll_calls] == [False, False]
    assert sleep_calls == [cfg.runtime.poll_interval_seconds]


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

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

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


def test_service_runner_restart_drain_timeout_marks_failed_and_posts_reply(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, restart_drain_timeout_seconds=1)
    state = StateStore(tmp_path / "state.db")
    command_key = "77:23:2026-02-22T13:08:00Z"
    _seed_restart_command(state, command_key=command_key)
    github = FakeGitHub()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 1, 0

        def pending_work_count(self) -> int:
            return 1

    monotonic_values = iter([0.0, 2.0, 2.0, 2.0])
    monkeypatch.setattr("mergexo.service_runner.Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(
        "mergexo.service_runner.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("mergexo.service_runner.time.sleep", lambda seconds: None)

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
    assert "timed out" in command.result
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

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

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

    pull_commands = [
        argv
        for argv, _ in commands
        if argv[:5] == ["git", "-C", str(tmp_path), "pull", "--ff-only"]
    ]
    assert pull_commands
    sync_commands = [(argv, cwd) for argv, cwd in commands if argv == ["uv", "sync"]]
    assert sync_commands
    assert sync_commands[0][1] == tmp_path

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
    runner._run_update(mode="git_checkout", git_branch="release")
    assert called[0] == ["uv", "pip", "install", "--python", "py3", "--upgrade", "mergexo"]
    assert called[1][:5] == ["git", "-C", str(tmp_path), "pull", "--ff-only"]
    assert called[1][-2:] == ["origin", "release"]
    assert called[2] == ["uv", "sync"]

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


def test_service_runner_continuous_deploy_schedules_restart_when_idle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(
        tmp_path,
        continuous_deploy_enabled=True,
        continuous_deploy_check_interval_seconds=10,
    )
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    class IdleOrchestrator:
        def pending_work_count(self) -> int:
            return 0

    monkeypatch.setattr(
        ServiceRunner,
        "_detect_continuous_deploy_candidate",
        lambda self, blocked_target_sha: _ContinuousDeployCandidate(
            from_sha="aaa111", to_sha="bbb222"
        ),
    )

    next_deadline = runner._maybe_schedule_continuous_deploy(
        enabled=True,
        orchestrators=cast(
            tuple[Phase1Orchestrator, ...],
            (cast(Phase1Orchestrator, IdleOrchestrator()),),
        ),
        auth_shutdown_detail=None,
        next_check_deadline_monotonic=0.0,
    )
    operation = state.get_runtime_operation("restart")
    assert operation is not None
    assert operation.requested_by == "continuous_deploy"
    assert operation.request_command_key.startswith("continuous:")
    deploy_state = state.get_continuous_deploy_state()
    assert deploy_state.status == "awaiting_health"
    assert deploy_state.previous_sha == "aaa111"
    assert deploy_state.target_sha == "bbb222"
    assert next_deadline > 0.0


def test_service_runner_continuous_deploy_does_not_schedule_when_pending_work_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    class BusyOrchestrator:
        def pending_work_count(self) -> int:
            return 1

    monkeypatch.setattr(
        ServiceRunner,
        "_detect_continuous_deploy_candidate",
        lambda self, blocked_target_sha: (_ for _ in ()).throw(
            AssertionError("candidate detection should not run while work is pending")
        ),
    )
    assert (
        runner._maybe_schedule_continuous_deploy(
            enabled=True,
            orchestrators=cast(
                tuple[Phase1Orchestrator, ...],
                (cast(Phase1Orchestrator, BusyOrchestrator()),),
            ),
            auth_shutdown_detail=None,
            next_check_deadline_monotonic=0.0,
        )
        == 0.0
    )
    assert state.get_runtime_operation("restart") is None


def test_service_runner_detect_continuous_deploy_candidate_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    def fake_run_factory(local_sha: str, remote_sha: str, merge_base_sha: str):  # type: ignore[no-untyped-def]
        def fake_run(argv: list[str], *, cwd=None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
            _ = cwd, input_text, check
            if "fetch" in argv:
                return ""
            if argv[-2:] == ["rev-parse", "HEAD"]:
                return f"{local_sha}\n"
            if len(argv) >= 2 and argv[-2] == "rev-parse" and argv[-1].startswith("origin/"):
                return f"{remote_sha}\n"
            if "merge-base" in argv:
                return f"{merge_base_sha}\n"
            raise AssertionError(f"unexpected command: {argv}")

        return fake_run

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_factory("a", "b", "a"))
    assert runner._detect_continuous_deploy_candidate(blocked_target_sha="b") is None

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_factory("a", "c", "d"))
    assert runner._detect_continuous_deploy_candidate(blocked_target_sha=None) is None

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_factory("a", "d", "a"))
    candidate = runner._detect_continuous_deploy_candidate(blocked_target_sha=None)
    assert candidate == _ContinuousDeployCandidate(from_sha="a", to_sha="d")


def test_service_runner_detect_continuous_deploy_candidate_validates_revisions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    runner = ServiceRunner(
        config=cfg,
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    def fake_run_empty_head(argv: list[str], *, cwd=None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = cwd, input_text, check
        if "fetch" in argv:
            return ""
        if argv[-2:] == ["rev-parse", "HEAD"]:
            return "\n"
        if len(argv) >= 2 and argv[-2] == "rev-parse" and argv[-1].startswith("origin/"):
            return "abc\n"
        return ""

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_empty_head)
    with pytest.raises(RuntimeError, match="HEAD returned empty revision"):
        runner._detect_continuous_deploy_candidate(blocked_target_sha=None)

    def fake_run_empty_remote(argv: list[str], *, cwd=None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = cwd, input_text, check
        if "fetch" in argv:
            return ""
        if argv[-2:] == ["rev-parse", "HEAD"]:
            return "abc\n"
        if len(argv) >= 2 and argv[-2] == "rev-parse" and argv[-1].startswith("origin/"):
            return "\n"
        return ""

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_empty_remote)
    with pytest.raises(RuntimeError, match="returned empty revision"):
        runner._detect_continuous_deploy_candidate(blocked_target_sha=None)

    def fake_run_same_revisions(argv: list[str], *, cwd=None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = cwd, input_text, check
        if "fetch" in argv:
            return ""
        if argv[-2:] == ["rev-parse", "HEAD"]:
            return "abc\n"
        if len(argv) >= 2 and argv[-2] == "rev-parse" and argv[-1].startswith("origin/"):
            return "abc\n"
        return ""

    monkeypatch.setattr("mergexo.service_runner.run", fake_run_same_revisions)
    assert runner._detect_continuous_deploy_candidate(blocked_target_sha=None) is None


def test_service_runner_maybe_schedule_continuous_deploy_guard_paths_and_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(
        tmp_path,
        continuous_deploy_enabled=True,
        continuous_deploy_check_interval_seconds=10,
    )
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )

    class IdleOrchestrator:
        def pending_work_count(self) -> int:
            return 0

    idle_orchestrators = cast(
        tuple[Phase1Orchestrator, ...],
        (cast(Phase1Orchestrator, IdleOrchestrator()),),
    )

    future_deadline = time.monotonic() + 60.0
    assert (
        runner._maybe_schedule_continuous_deploy(
            enabled=True,
            orchestrators=idle_orchestrators,
            auth_shutdown_detail=None,
            next_check_deadline_monotonic=future_deadline,
        )
        == future_deadline
    )
    assert (
        runner._maybe_schedule_continuous_deploy(
            enabled=True,
            orchestrators=idle_orchestrators,
            auth_shutdown_detail="auth pending",
            next_check_deadline_monotonic=0.0,
        )
        == 0.0
    )

    _seed_restart_command(state, command_key="77:cd:2026-03-03T00:00:00Z")
    assert (
        runner._maybe_schedule_continuous_deploy(
            enabled=True,
            orchestrators=idle_orchestrators,
            auth_shutdown_detail=None,
            next_check_deadline_monotonic=0.0,
        )
        == 0.0
    )
    state.set_runtime_operation_status(op_name="restart", status="completed", detail="done")

    state.start_continuous_deploy_attempt(previous_sha="a", target_sha="b")
    awaiting_deadline = runner._maybe_schedule_continuous_deploy(
        enabled=True,
        orchestrators=idle_orchestrators,
        auth_shutdown_detail=None,
        next_check_deadline_monotonic=0.0,
    )
    assert awaiting_deadline > 0.0
    state.mark_continuous_deploy_healthy(active_sha="b")

    monkeypatch.setattr(
        ServiceRunner,
        "_detect_continuous_deploy_candidate",
        lambda self, blocked_target_sha: None,
    )
    none_deadline = runner._maybe_schedule_continuous_deploy(
        enabled=True,
        orchestrators=idle_orchestrators,
        auth_shutdown_detail=None,
        next_check_deadline_monotonic=0.0,
    )
    assert none_deadline > 0.0

    monkeypatch.setattr(
        state,
        "get_continuous_deploy_state",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    errored_deadline = runner._maybe_schedule_continuous_deploy(
        enabled=True,
        orchestrators=idle_orchestrators,
        auth_shutdown_detail=None,
        next_check_deadline_monotonic=0.0,
    )
    assert errored_deadline > 0.0


def test_service_runner_current_checkout_sha_handles_success_and_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = ServiceRunner(
        config=_app_config(tmp_path),
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr("mergexo.service_runner.run", lambda argv, **kwargs: "abc123\n")
    assert runner._current_checkout_sha() == "abc123"
    monkeypatch.setattr(
        "mergexo.service_runner.run",
        lambda argv, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert runner._current_checkout_sha() is None


def test_service_runner_request_continuous_deploy_restart_respects_single_flight(
    tmp_path: Path,
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    _seed_restart_command(state, command_key="77:pending:2026-03-03T00:00:00Z")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner._request_continuous_deploy_restart(
        candidate=_ContinuousDeployCandidate(from_sha="a", to_sha="b")
    )
    deploy_state = state.get_continuous_deploy_state()
    assert deploy_state.status == "idle"


def test_service_runner_reconcile_continuous_deploy_startup_noop_states(
    tmp_path: Path,
) -> None:
    disabled_runner = ServiceRunner(
        config=_app_config(tmp_path, continuous_deploy_enabled=False),
        state=StateStore(tmp_path / "state-disabled.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    assert (
        disabled_runner._reconcile_continuous_deploy_startup(health_tracker=_ServiceHealthTracker())
        is False
    )

    enabled_runner = ServiceRunner(
        config=_app_config(tmp_path, continuous_deploy_enabled=True),
        state=StateStore(tmp_path / "state-enabled.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    assert (
        enabled_runner._reconcile_continuous_deploy_startup(health_tracker=_ServiceHealthTracker())
        is False
    )


def test_service_runner_run_returns_when_startup_reconcile_requests_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = ServiceRunner(
        config=_app_config(tmp_path),
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(
        ServiceRunner, "_reconcile_continuous_deploy_startup", lambda self, health_tracker: True
    )
    runner.run(once=True)


def test_service_runner_record_boot_error_handles_empty_detail_and_internal_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="a", target_sha="b")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner._record_continuous_deploy_boot_error(detail="   ")
    assert state.get_continuous_deploy_state().last_error is None

    monkeypatch.setattr(
        state,
        "get_continuous_deploy_state",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    runner._record_continuous_deploy_boot_error(detail="Traceback: boom")


def test_service_runner_mark_continuous_deploy_healthy_handles_missing_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = ServiceRunner(
        config=_app_config(tmp_path),
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(
        runner.state,
        "get_continuous_deploy_state",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert runner._mark_continuous_deploy_healthy_if_ready() is None


def test_service_runner_marks_continuous_deploy_healthy_after_poll_cycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr(ServiceRunner, "_current_checkout_sha", lambda self: "bbb222")
    health_tracker = _ServiceHealthTracker()
    runner._process_completed_poll_cycle(
        health_tracker=health_tracker,
        auth_shutdown_detail=None,
    )
    deploy_state = state.get_continuous_deploy_state()
    assert deploy_state.status == "healthy"
    status_code, payload = health_tracker.response_payload()
    assert status_code == 200
    assert payload["active_sha"] == "bbb222"

    blocked_tracker = _ServiceHealthTracker()
    runner._process_completed_poll_cycle(
        health_tracker=blocked_tracker,
        auth_shutdown_detail="gh auth pending",
    )
    blocked_status_code, blocked_payload = blocked_tracker.response_payload()
    assert blocked_status_code == 503
    assert blocked_payload["reason"] == "starting"


def test_service_runner_reconcile_continuous_deploy_startup_rolls_back_after_repeated_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(
        tmp_path,
        continuous_deploy_enabled=True,
        continuous_deploy_max_boot_failures=1,
    )
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    rollback_calls: list[int] = []

    def fake_rollback(
        self,
        *,
        attempt,
        error_detail: str,
        health_tracker,
    ) -> bool:  # type: ignore[no-untyped-def]
        _ = error_detail, health_tracker
        rollback_calls.append(attempt.boot_attempt_count)
        return True

    monkeypatch.setattr(ServiceRunner, "_perform_continuous_deploy_rollback", fake_rollback)
    health_tracker = _ServiceHealthTracker()
    assert runner._reconcile_continuous_deploy_startup(health_tracker=health_tracker) is False
    assert rollback_calls == []
    assert runner._reconcile_continuous_deploy_startup(health_tracker=health_tracker) is True
    assert rollback_calls == [2]


def test_service_runner_continuous_deploy_rollback_reports_issue_and_quarantines_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")
    state.set_continuous_deploy_last_error(detail="Traceback: startup failed")
    github = FakeGitHub()
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    commands: list[tuple[list[str], Path | None]] = []

    def fake_run(argv: list[str], *, cwd: Path | None = None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = input_text, check
        commands.append((argv, cwd))
        return ""

    monkeypatch.setattr("mergexo.service_runner.run", fake_run)
    monkeypatch.setattr(ServiceRunner, "_reexec", lambda self: None)
    attempt = state.get_continuous_deploy_state()
    assert (
        runner._perform_continuous_deploy_rollback(
            attempt=attempt,
            error_detail="health checks failed",
            health_tracker=_ServiceHealthTracker(),
        )
        is True
    )

    deploy_state = state.get_continuous_deploy_state()
    assert deploy_state.status == "rolled_back"
    assert deploy_state.blocked_target_sha == "bbb222"
    assert deploy_state.last_error is not None
    assert "health checks failed" in deploy_state.last_error
    assert commands[0][0][:4] == ["git", "-C", str(tmp_path), "fetch"]
    assert commands[1][0][:5] == ["git", "-C", str(tmp_path), "checkout", "-B"]
    assert commands[2][0] == ["uv", "sync"]
    assert commands[2][1] == tmp_path
    assert len(github.created_issues) == 1
    assert github.created_issues[0].labels == ()
    assert "target_sha: bbb222" in github.created_issues[0].body
    assert "Traceback: startup failed" in github.created_issues[0].body


def test_service_runner_continuous_deploy_rollback_continues_when_issue_creation_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")

    class FailingIssueGitHub(FakeGitHub):
        def create_issue(
            self, *, title: str, body: str, labels: tuple[str, ...] | None = None
        ) -> Issue:
            _ = title, body, labels
            raise RuntimeError("missing issue scope")

    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FailingIssueGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    monkeypatch.setattr("mergexo.service_runner.run", lambda argv, **kwargs: "")
    monkeypatch.setattr(ServiceRunner, "_reexec", lambda self: None)
    attempt = state.get_continuous_deploy_state()
    assert (
        runner._perform_continuous_deploy_rollback(
            attempt=attempt,
            error_detail="health checks failed",
            health_tracker=_ServiceHealthTracker(),
        )
        is True
    )
    deploy_state = state.get_continuous_deploy_state()
    assert deploy_state.status == "rolled_back"
    assert deploy_state.last_error is not None
    assert "rollback_issue_error" in deploy_state.last_error
    assert "missing issue scope" in deploy_state.last_error


def test_service_runner_continuous_deploy_rollback_failures_mark_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    with pytest.raises(RuntimeError, match="missing previous_sha"):
        runner._perform_continuous_deploy_rollback(
            attempt=state.get_continuous_deploy_state(),
            error_detail="health checks failed",
            health_tracker=_ServiceHealthTracker(),
        )
    assert state.get_continuous_deploy_state().status == "failed"

    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")

    def fake_run(argv: list[str], *, cwd: Path | None = None, input_text=None, check=True) -> str:  # type: ignore[no-untyped-def]
        _ = cwd, input_text, check
        if "checkout" in argv:
            raise RuntimeError("checkout failed")
        return ""

    monkeypatch.setattr("mergexo.service_runner.run", fake_run)
    with pytest.raises(RuntimeError, match="rollback failed while restoring"):
        runner._perform_continuous_deploy_rollback(
            attempt=state.get_continuous_deploy_state(),
            error_detail="health checks failed",
            health_tracker=_ServiceHealthTracker(),
        )
    assert state.get_continuous_deploy_state().status == "failed"


def test_service_runner_continuous_deploy_boot_error_recording(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_enabled=True)
    state = StateStore(tmp_path / "state.db")
    state.start_continuous_deploy_attempt(previous_sha="aaa111", target_sha="bbb222")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    runner._record_continuous_deploy_boot_error(detail="Traceback: boom")
    assert "Traceback: boom" in (state.get_continuous_deploy_state().last_error or "")

    state.mark_continuous_deploy_healthy(active_sha="bbb222")
    runner._record_continuous_deploy_boot_error(detail="Traceback: ignored")
    assert state.get_continuous_deploy_state().last_error is None


def test_service_runner_health_endpoint_reports_expected_states(tmp_path: Path) -> None:
    port = _free_local_port()
    cfg = _app_config(
        tmp_path,
        continuous_deploy_healthcheck_port=port,
    )
    state = StateStore(tmp_path / "state.db")
    runner = ServiceRunner(
        config=cfg,
        state=state,
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    tracker = _ServiceHealthTracker()
    server = runner._start_health_server(health_tracker=tracker)
    assert server is not None
    try:
        with pytest.raises(urlerror.HTTPError) as missing_error:
            request.urlopen(f"http://127.0.0.1:{port}/missing")
        assert missing_error.value.code == 404

        with pytest.raises(urlerror.HTTPError) as starting_error:
            request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert starting_error.value.code == 503
        assert '"reason": "starting"' in starting_error.value.read().decode("utf-8")

        tracker.set_poll_cycle_completed()
        tracker.set_active_sha("abc123")
        healthy = request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert healthy.status == 200
        assert '"status": "healthy"' in healthy.read().decode("utf-8")

        tracker.set_auth_shutdown_pending(True)
        with pytest.raises(urlerror.HTTPError) as auth_error:
            request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert auth_error.value.code == 503
        assert '"reason": "auth_shutdown_pending"' in auth_error.value.read().decode("utf-8")

        tracker.set_rollback_in_progress(True)
        with pytest.raises(urlerror.HTTPError) as rollback_error:
            request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert rollback_error.value.code == 503
        assert '"reason": "rollback_in_progress"' in rollback_error.value.read().decode("utf-8")

        tracker.set_fatal_error("boom")
        with pytest.raises(urlerror.HTTPError) as fatal_error:
            request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert fatal_error.value.code == 503
        assert '"reason": "fatal_error"' in fatal_error.value.read().decode("utf-8")
    finally:
        runner._stop_health_server(server)


def test_service_runner_health_endpoint_can_be_disabled(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path, continuous_deploy_healthcheck_port=0)
    runner = ServiceRunner(
        config=cfg,
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    assert runner._start_health_server(health_tracker=_ServiceHealthTracker()) is None
    runner._stop_health_server(None)


def test_service_runner_handle_restart_uses_continuous_branch_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(
        tmp_path,
        continuous_deploy_enabled=True,
        continuous_deploy_branch="release",
    )
    runner = ServiceRunner(
        config=cfg,
        state=StateStore(tmp_path / "state.db"),
        github=cast(GitHubGateway, FakeGitHub()),
        git_manager=cast(GitRepoManager, object()),
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    branches: list[str | None] = []

    def fake_run_update(self, *, mode: RestartMode, git_branch: str | None = None) -> None:
        _ = mode
        branches.append(git_branch)

    monkeypatch.setattr(ServiceRunner, "_run_update", fake_run_update)
    monkeypatch.setattr(ServiceRunner, "_reexec", lambda self: None)
    assert (
        runner._handle_restart_requested(
            requested=RestartRequested(
                mode="git_checkout", command_key="continuous:deadbeef:2026-03-03T00:00:00Z"
            )
        )
        is True
    )
    assert branches == ["release"]


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
    _seed_restart_command(state, command_key="77:21:2026-02-22T13:06:00Z")
    github = FakeGitHub()

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            _ = args, kwargs

        def poll_once(self, pool, *, allow_enqueue: bool) -> None:  # type: ignore[no-untyped-def]
            _ = pool, allow_enqueue

        def queue_counts(self) -> tuple[int, int]:
            return 0, 0

        def pending_work_count(self) -> int:
            return 0

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
    monkeypatch.setattr(ServiceRunner, "_run_update", lambda self, mode, git_branch=None: None)
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
    agent_overrides = {cfg.repo.full_name: cast(AgentAdapter, object())}

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
        agent_by_repo_full_name=agent_overrides,
        once=True,
        startup_argv=("mergexo", "service"),
    )
    assert calls["once"] is True
    kwargs = cast(dict[str, object], calls["kwargs"])
    assert kwargs["startup_argv"] == ("mergexo", "service")
    built_overrides = cast(dict[str, AgentAdapter], kwargs["agent_by_repo_full_name"])
    assert built_overrides == agent_overrides
    assert built_overrides is not agent_overrides


def test_effective_repo_runtimes_and_github_fallback_paths(tmp_path: Path) -> None:
    cfg = _multi_repo_config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    first_github = cast(GitHubGateway, FakeGitHub())
    second_github = cast(GitHubGateway, FakeGitHub())

    runner_with_supplied = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
        repo_runtimes=(
            (cfg.repos[0], first_github, cast(GitRepoManager, object())),
            (cfg.repos[1], second_github, cast(GitRepoManager, object())),
        ),
    )
    supplied = runner_with_supplied._effective_repo_runtimes()
    assert supplied[0][0].full_name == cfg.repos[0].full_name
    assert runner_with_supplied._github_for_repo("missing/repo") is first_github

    runner_with_factory = ServiceRunner(
        config=cfg,
        state=state,
        agent=cast(AgentAdapter, object()),
        startup_argv=("mergexo", "service"),
    )
    built = runner_with_factory._effective_repo_runtimes()
    assert len(built) == 2
    assert built[0][0].full_name == cfg.repos[0].full_name
