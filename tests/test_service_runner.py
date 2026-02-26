from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from threading import Event
from typing import cast

import pytest

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import append_action_token, compute_operator_command_token
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.models import PullRequestIssueComment, RestartMode
from mergexo.orchestrator import RestartRequested
from mergexo.service_runner import ServiceRunner, ServiceSignal, run_service
from mergexo.state import StateStore


def _app_config(
    tmp_path: Path,
    *,
    restart_supported_modes: tuple[RestartMode, ...] = ("git_checkout",),
    restart_default_mode: RestartMode = "git_checkout",
    service_python: str | None = None,
    restart_drain_timeout_seconds: int = 60,
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
    next_comment_id: int = 1000

    def __init__(self) -> None:
        self.threads = {}
        self.posted = []
        self.next_comment_id = 1000

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
