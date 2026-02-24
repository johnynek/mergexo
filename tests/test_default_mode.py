from __future__ import annotations

from pathlib import Path
import sys
from threading import Event
from types import SimpleNamespace
from typing import cast

import pytest

from mergexo import default_mode
from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.state import StateStore


def _app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
            observability_refresh_seconds=3,
            observability_default_window="7d",
            observability_row_limit=50,
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
                allowed_users=frozenset({"issue-author"}),
                local_clone_source=None,
                remote_url=None,
            ),
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def test_run_default_mode_rejects_non_interactive_terminal(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    with pytest.raises(RuntimeError, match="interactive terminal"):
        default_mode.run_default_mode(
            config=cfg,
            state=cast(StateStore, object()),
            agent=cast(AgentAdapter, object()),
            agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
            repo_runtimes=(
                (cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),
            ),
        )


def test_run_default_mode_starts_service_and_stops_on_tui_shutdown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    service_started = Event()
    service_stopped = Event()
    called: dict[str, object] = {}

    def fake_run_service(**kwargs) -> None:  # type: ignore[no-untyped-def]
        stop_event = kwargs["stop_event"]
        assert isinstance(stop_event, Event)
        kwargs["signal_sink"](default_mode.ServiceSignal(kind="poll_completed"))
        service_started.set()
        stop_event.wait(timeout=1.0)
        service_stopped.set()

    def fake_run_observability_tui(**kwargs) -> None:  # type: ignore[no-untyped-def]
        called["kwargs"] = kwargs
        kwargs["on_shutdown"]()

    monkeypatch.setattr(default_mode, "_is_interactive_terminal", lambda: True)
    monkeypatch.setattr(default_mode, "run_service", fake_run_service)
    monkeypatch.setattr(default_mode, "run_observability_tui", fake_run_observability_tui)

    default_mode.run_default_mode(
        config=cfg,
        state=cast(StateStore, object()),
        agent=cast(AgentAdapter, object()),
        agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
        repo_runtimes=((cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),),
        startup_argv=("mergexo",),
    )

    assert service_started.is_set()
    assert service_stopped.is_set()
    kwargs = cast(dict[str, object], called["kwargs"])
    assert kwargs["db_path"] == cfg.runtime.base_dir / "state.db"
    assert kwargs["refresh_seconds"] == cfg.runtime.observability_refresh_seconds
    assert kwargs["default_window"] == cfg.runtime.observability_default_window
    assert kwargs["row_limit"] == cfg.runtime.observability_row_limit


def test_run_default_mode_propagates_service_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)

    def fake_run_service(**kwargs) -> None:  # type: ignore[no-untyped-def]
        _ = kwargs
        raise RuntimeError("boom")

    monkeypatch.setattr(default_mode, "_is_interactive_terminal", lambda: True)
    monkeypatch.setattr(default_mode, "run_service", fake_run_service)
    monkeypatch.setattr(default_mode, "run_observability_tui", lambda **kwargs: None)

    with pytest.raises(RuntimeError, match="boom"):
        default_mode.run_default_mode(
            config=cfg,
            state=cast(StateStore, object()),
            agent=cast(AgentAdapter, object()),
            agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
            repo_runtimes=(
                (cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),
            ),
        )


def test_run_default_mode_wraps_non_exception_service_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)

    class HardStop(BaseException):
        pass

    def fake_run_service(**kwargs) -> None:  # type: ignore[no-untyped-def]
        _ = kwargs
        raise HardStop("hard-stop")

    monkeypatch.setattr(default_mode, "_is_interactive_terminal", lambda: True)
    monkeypatch.setattr(default_mode, "run_service", fake_run_service)
    monkeypatch.setattr(default_mode, "run_observability_tui", lambda **kwargs: None)

    with pytest.raises(RuntimeError, match="non-Exception"):
        default_mode.run_default_mode(
            config=cfg,
            state=cast(StateStore, object()),
            agent=cast(AgentAdapter, object()),
            agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
            repo_runtimes=(
                (cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),
            ),
        )


def test_run_default_mode_propagates_tui_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)

    def fake_run_service(**kwargs) -> None:  # type: ignore[no-untyped-def]
        kwargs["stop_event"].wait(timeout=1.0)

    def fake_tui(**kwargs) -> None:  # type: ignore[no-untyped-def]
        _ = kwargs
        raise RuntimeError("ui-failed")

    monkeypatch.setattr(default_mode, "_is_interactive_terminal", lambda: True)
    monkeypatch.setattr(default_mode, "run_service", fake_run_service)
    monkeypatch.setattr(default_mode, "run_observability_tui", fake_tui)

    with pytest.raises(RuntimeError, match="ui-failed"):
        default_mode.run_default_mode(
            config=cfg,
            state=cast(StateStore, object()),
            agent=cast(AgentAdapter, object()),
            agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
            repo_runtimes=(
                (cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),
            ),
        )


def test_run_default_mode_raises_when_service_thread_does_not_stop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)

    class FakeThread:
        def __init__(self, *, target, name: str, daemon: bool) -> None:  # type: ignore[no-untyped-def]
            _ = target, name, daemon

        def start(self) -> None:
            return None

        def join(self, timeout: float | None = None) -> None:
            _ = timeout
            return None

        def is_alive(self) -> bool:
            return True

    monkeypatch.setattr(default_mode, "_is_interactive_terminal", lambda: True)
    monkeypatch.setattr(default_mode, "Thread", FakeThread)
    monkeypatch.setattr(default_mode, "run_observability_tui", lambda **kwargs: None)
    monkeypatch.setattr(default_mode, "run_service", lambda **kwargs: None)

    with pytest.raises(RuntimeError, match="did not stop"):
        default_mode.run_default_mode(
            config=cfg,
            state=cast(StateStore, object()),
            agent=cast(AgentAdapter, object()),
            agent_by_repo_full_name={cfg.repo.full_name: cast(AgentAdapter, object())},
            repo_runtimes=(
                (cfg.repo, cast(GitHubGateway, object()), cast(GitRepoManager, object())),
            ),
        )


def test_service_join_timeout_and_is_interactive_terminal_helper(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    assert default_mode._service_join_timeout_seconds(cfg) == float(
        cfg.runtime.poll_interval_seconds + 5
    )

    original_stdin = sys.stdin
    original_stdout = sys.stdout
    try:
        sys.stdin = cast(object, SimpleNamespace(isatty=lambda: True))
        sys.stdout = cast(object, SimpleNamespace(isatty=lambda: True))
        assert default_mode._is_interactive_terminal() is True
        sys.stdout = cast(object, SimpleNamespace(isatty=lambda: False))
        assert default_mode._is_interactive_terminal() is False
    finally:
        sys.stdin = original_stdin
        sys.stdout = original_stdout
