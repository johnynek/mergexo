from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from mergexo import cli
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig


def _app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
            enable_feedback_loop=False,
        ),
        repo=RepoConfig(
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            design_docs_dir="docs/design",
            local_clone_source=None,
            remote_url=None,
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def test_build_parser_supports_commands() -> None:
    parser = cli.build_parser()

    parsed_init = parser.parse_args(["init", "--verbose"])
    parsed_run = parser.parse_args(["run", "--once", "--verbose"])

    assert parsed_init.command == "init"
    assert parsed_init.verbose is True
    assert parsed_run.command == "run"
    assert parsed_run.once is True
    assert parsed_run.verbose is True


def test_main_dispatches_init(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
            return SimpleNamespace(command="init", config=Path("cfg.toml"), verbose=True)

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli, "configure_logging", lambda verbose: called.setdefault("verbose", verbose)
    )
    monkeypatch.setattr(cli, "_cmd_init", lambda c: called.setdefault("init", c))

    cli.main()
    assert called["verbose"] is True
    assert called["init"] == cfg


def test_main_dispatches_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
            return SimpleNamespace(command="run", config=Path("cfg.toml"), once=True, verbose=False)

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli, "configure_logging", lambda verbose: called.setdefault("verbose", verbose)
    )
    monkeypatch.setattr(cli, "_cmd_run", lambda c, once: called.setdefault("run", (c, once)))

    cli.main()
    assert called["verbose"] is False
    assert called["run"] == (cfg, True)


def test_main_unknown_command_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
            return SimpleNamespace(command="unknown", config=Path("cfg.toml"), verbose=False)

    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)

    with pytest.raises(RuntimeError, match="Unknown command"):
        cli.main()


def test_main_defaults_verbose_to_false_when_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
            return SimpleNamespace(command="init", config=Path("cfg.toml"))

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli, "configure_logging", lambda verbose: called.setdefault("verbose", verbose)
    )
    monkeypatch.setattr(cli, "_cmd_init", lambda c: called.setdefault("init", c))

    cli.main()
    assert called["verbose"] is False


def test_cmd_init_creates_layout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeState:
        def __init__(self, path: Path) -> None:
            self.path = path

    class FakeGit:
        def __init__(self, runtime: RuntimeConfig, repo: RepoConfig) -> None:
            _ = runtime, repo
            self.layout = SimpleNamespace(
                mirror_path=tmp_path / "mirror.git",
                checkouts_root=tmp_path / "checkouts",
            )
            self.ensure_layout_called = False

        def ensure_layout(self) -> None:
            self.ensure_layout_called = True

    monkeypatch.setattr(cli, "StateStore", FakeState)
    monkeypatch.setattr(cli, "GitRepoManager", FakeGit)

    cli._cmd_init(cfg)
    assert cfg.runtime.base_dir.exists()


def test_cmd_run_constructs_orchestrator(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}

    class FakeOrchestrator:
        def __init__(
            self,
            config: AppConfig,
            *,
            state: object,
            github: object,
            git_manager: object,
            agent: object,
        ) -> None:
            called["ctor"] = (config, state, github, git_manager, agent)

        def run(self, *, once: bool) -> None:
            called["run"] = once

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(cli, "Phase1Orchestrator", FakeOrchestrator)

    cli._cmd_run(cfg, once=False)

    assert called["run"] is False
    ctor = called["ctor"]
    assert isinstance(ctor, tuple)
    assert ctor[0] == cfg


def test_state_db_path(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    assert cli._state_db_path(cfg) == cfg.runtime.base_dir / "state.db"
