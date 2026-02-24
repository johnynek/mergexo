from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from mergexo import cli
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.state import BlockedPullRequestState


def _app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=1,
            poll_interval_seconds=60,
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
                allowed_users=frozenset({"issue-author", "reviewer"}),
                local_clone_source=None,
                remote_url=None,
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
        allowed_users=frozenset({"issue-author"}),
        local_clone_source=None,
        remote_url=None,
    )
    return AppConfig(runtime=base.runtime, repos=(base.repo, second), codex=base.codex)


def test_build_parser_supports_commands() -> None:
    parser = cli.build_parser()

    parsed_console = parser.parse_args(["console", "--verbose"])
    parsed_init = parser.parse_args(["init", "--verbose"])
    parsed_run = parser.parse_args(["run", "--once", "--verbose"])
    parsed_run_high = parser.parse_args(["run", "--once", "--verbose", "high"])
    parsed_service = parser.parse_args(["service", "--once", "--verbose"])
    parsed_top = parser.parse_args(["top", "--verbose"])
    parsed_feedback_list = parser.parse_args(["feedback", "blocked", "list", "--json"])
    parsed_feedback_reset = parser.parse_args(
        [
            "feedback",
            "blocked",
            "reset",
            "--pr",
            "12",
            "--pr",
            "14",
            "--dry-run",
            "--head-sha",
            "abc1234",
        ]
    )

    assert parsed_console.command == "console"
    assert parsed_console.verbose == "low"
    assert parsed_init.command == "init"
    assert parsed_init.verbose == "low"
    assert parsed_run.command == "run"
    assert parsed_run.once is True
    assert parsed_run.verbose == "low"
    assert parsed_run_high.verbose == "high"
    assert parsed_service.command == "service"
    assert parsed_service.once is True
    assert parsed_service.verbose == "low"
    assert parsed_top.command == "top"
    assert parsed_top.verbose == "low"
    assert parsed_feedback_list.command == "feedback"
    assert parsed_feedback_list.feedback_command == "blocked"
    assert parsed_feedback_list.blocked_command == "list"
    assert parsed_feedback_list.json is True
    assert parsed_feedback_reset.blocked_command == "reset"
    assert parsed_feedback_reset.pr == [12, 14]
    assert parsed_feedback_reset.dry_run is True
    assert parsed_feedback_reset.head_sha == "abc1234"
    assert parsed_feedback_reset.repo is None


def test_main_dispatches_init(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(command="init", config=Path("cfg.toml"), verbose=True)

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_init", lambda c: called.setdefault("init", c))

    cli.main()
    assert called["verbose"] is True
    assert called["state_dir"] == cfg.runtime.base_dir
    assert called["init"] == cfg


def test_main_dispatches_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(command="run", config=Path("cfg.toml"), once=True, verbose=False)

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_run", lambda c, once: called.setdefault("run", (c, once)))

    cli.main()
    assert called["verbose"] is False
    assert called["state_dir"] == cfg.runtime.base_dir
    assert called["run"] == (cfg, True)


def test_main_dispatches_feedback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(
                command="feedback",
                config=Path("cfg.toml"),
                feedback_command="blocked",
                blocked_command="list",
                json=False,
                verbose=False,
            )

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_feedback", lambda c, a: called.setdefault("feedback", (c, a)))

    cli.main()

    assert called["verbose"] is False
    assert called["state_dir"] == cfg.runtime.base_dir
    feedback_call = called["feedback"]
    assert isinstance(feedback_call, tuple)
    assert feedback_call[0] == cfg
    assert feedback_call[1].blocked_command == "list"


def test_main_dispatches_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(
                command="service",
                config=Path("cfg.toml"),
                once=True,
                verbose=True,
            )

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(
        cli, "_cmd_service", lambda c, once: called.setdefault("service", (c, once))
    )

    cli.main()
    assert called["verbose"] is True
    assert called["state_dir"] == cfg.runtime.base_dir
    assert called["service"] == (cfg, True)


def test_main_dispatches_top(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(
                command="top",
                config=Path("cfg.toml"),
                verbose=False,
            )

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_top", lambda c: called.setdefault("top", c))

    cli.main()
    assert called["verbose"] is False
    assert called["state_dir"] == cfg.runtime.base_dir
    assert called["top"] == cfg


def test_main_unknown_command_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
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
        def parse_args(self, argv: tuple[str, ...]) -> SimpleNamespace:
            _ = argv
            return SimpleNamespace(command="init", config=Path("cfg.toml"))

    called: dict[str, object] = {}
    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())
    monkeypatch.setattr(cli, "load_config", lambda p: cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_init", lambda c: called.setdefault("init", c))

    cli.main()
    assert called["verbose"] is None
    assert called["state_dir"] == cfg.runtime.base_dir


def test_main_defaults_to_console_and_low_verbosity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}
    monkeypatch.setattr(sys, "argv", ["mergexo"])
    monkeypatch.setattr(cli, "load_config", lambda p: called.setdefault("config_path", p) and cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_console", lambda c: called.setdefault("console", c))

    cli.main()

    assert called["config_path"] == Path("mergexo.toml")
    assert called["verbose"] == "low"
    assert called["state_dir"] == cfg.runtime.base_dir
    assert called["console"] == cfg


def test_main_defaults_to_console_when_option_precedes_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}
    custom_config = tmp_path / "custom.toml"
    monkeypatch.setattr(sys, "argv", ["mergexo", "--config", str(custom_config)])
    monkeypatch.setattr(cli, "load_config", lambda p: called.setdefault("config_path", p) and cfg)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda verbose, state_dir=None: called.update({"verbose": verbose, "state_dir": state_dir}),
    )
    monkeypatch.setattr(cli, "_cmd_console", lambda c: called.setdefault("console", c))

    cli.main()

    assert called["config_path"] == custom_config
    assert called["verbose"] == "low"
    assert called["console"] == cfg


def test_argv_with_default_command_helper() -> None:
    assert cli._argv_with_default_command(()) == ("console",)
    assert cli._argv_with_default_command(("--config", "mergexo.toml")) == (
        "console",
        "--config",
        "mergexo.toml",
    )
    assert cli._argv_with_default_command(("service", "--once")) == ("service", "--once")
    assert cli._argv_with_default_command(("--help",)) == ("--help",)


def test_effective_verbose_mode_defaults_console_to_low() -> None:
    assert cli._effective_verbose_mode(SimpleNamespace(command="console", verbose=None)) == "low"
    assert cli._effective_verbose_mode(SimpleNamespace(command="service", verbose=None)) is None
    assert cli._effective_verbose_mode(SimpleNamespace(command="console", verbose="high")) == "high"


def test_cmd_console_runs_default_mode_with_runtime_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    cfg = AppConfig(
        runtime=RuntimeConfig(
            base_dir=cfg.runtime.base_dir,
            worker_count=cfg.runtime.worker_count,
            poll_interval_seconds=cfg.runtime.poll_interval_seconds,
            observability_history_retention_days=42,
        ),
        repos=cfg.repos,
        codex=cfg.codex,
    )
    called: dict[str, object] = {}

    class FakeState:
        def __init__(self, path: Path) -> None:
            called["state_path"] = path

        def reconcile_unfinished_agent_runs(self) -> int:
            called["reconciled"] = True
            return 1

        def prune_observability_history(self, *, retention_days: int) -> tuple[int, int]:
            called["retention_days"] = retention_days
            return 2, 3

    monkeypatch.setattr(cli, "StateStore", FakeState)
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(
        cli,
        "run_default_mode",
        lambda **kwargs: called.setdefault("default_mode", kwargs),
    )

    cli._cmd_console(cfg)

    assert called["reconciled"] is True
    assert called["retention_days"] == 42
    default_mode_call = called["default_mode"]
    assert isinstance(default_mode_call, dict)
    assert default_mode_call["config"] == cfg
    assert default_mode_call["agent"] == "codex:True"
    assert default_mode_call["agent_by_repo_full_name"] == {"johnynek/mergexo": "codex:True"}
    assert len(default_mode_call["repo_runtimes"]) == 1


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
            repo: RepoConfig,
            github_by_repo_full_name: dict[str, object],
            agent: object,
        ) -> None:
            called["ctor"] = (
                config,
                state,
                github,
                git_manager,
                repo,
                github_by_repo_full_name,
                agent,
            )

        def run(self, *, once: bool) -> None:
            called["run"] = once

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(cli, "Phase1Orchestrator", FakeOrchestrator)

    cli._cmd_run(cfg, once=True)

    assert called["run"] is True
    ctor = called["ctor"]
    assert isinstance(ctor, tuple)
    assert ctor[0] == cfg


def test_cmd_run_multi_repo_once_polls_each_repo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    run_calls: list[tuple[str, bool]] = []

    class FakeOrchestrator:
        def __init__(
            self,
            config: AppConfig,
            *,
            state: object,
            github: object,
            git_manager: object,
            repo: RepoConfig,
            github_by_repo_full_name: dict[str, object],
            agent: object,
        ) -> None:
            _ = config, state, github, git_manager, github_by_repo_full_name, agent
            self._repo = repo

        def run(self, *, once: bool) -> None:
            run_calls.append((self._repo.full_name, once))

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(cli, "Phase1Orchestrator", FakeOrchestrator)

    cli._cmd_run(cfg, once=True)
    assert run_calls == [
        ("johnynek/mergexo", True),
        ("johnynek/bosatsu", True),
    ]


def test_cmd_run_multi_repo_round_robin_sleep(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    run_calls: list[str] = []
    sleep_calls: list[int] = []

    class StopLoop(RuntimeError):
        pass

    class FakeOrchestrator:
        def __init__(
            self,
            config: AppConfig,
            *,
            state: object,
            github: object,
            git_manager: object,
            repo: RepoConfig,
            github_by_repo_full_name: dict[str, object],
            agent: object,
        ) -> None:
            _ = config, state, github, git_manager, github_by_repo_full_name, agent
            self._repo = repo

        def run(self, *, once: bool) -> None:
            _ = once
            run_calls.append(self._repo.full_name)

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            raise StopLoop("done")

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(cli, "Phase1Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)

    with pytest.raises(StopLoop, match="done"):
        cli._cmd_run(cfg, once=False)
    assert run_calls == ["johnynek/mergexo", "johnynek/bosatsu"]
    assert sleep_calls == [cfg.runtime.poll_interval_seconds, cfg.runtime.poll_interval_seconds]


def test_cmd_service_constructs_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(cli, "GitHubGateway", lambda owner, name: f"gh:{owner}/{name}")
    monkeypatch.setattr(cli, "GitRepoManager", lambda runtime, repo: f"git:{repo.full_name}")
    monkeypatch.setattr(cli, "CodexAdapter", lambda codex: f"codex:{codex.enabled}")
    monkeypatch.setattr(
        cli,
        "run_service",
        lambda **kwargs: called.setdefault("service", kwargs),
    )

    cli._cmd_service(cfg, once=False)

    service_call = called["service"]
    assert isinstance(service_call, dict)
    assert service_call["config"] == cfg
    assert service_call["once"] is False
    assert len(service_call["repo_runtimes"]) == 1
    assert service_call["agent_by_repo_full_name"] == {"johnynek/mergexo": "codex:True"}


def test_cmd_top_runs_tui_with_runtime_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _app_config(tmp_path)
    cfg = AppConfig(
        runtime=RuntimeConfig(
            base_dir=cfg.runtime.base_dir,
            worker_count=cfg.runtime.worker_count,
            poll_interval_seconds=cfg.runtime.poll_interval_seconds,
            observability_refresh_seconds=5,
            observability_default_window="7d",
            observability_row_limit=111,
            observability_history_retention_days=45,
        ),
        repos=cfg.repos,
        codex=cfg.codex,
    )
    called: dict[str, object] = {}

    class FakeState:
        def __init__(self, path: Path) -> None:
            called["state_path"] = path

        def reconcile_unfinished_agent_runs(self) -> int:
            called["reconciled"] = True
            return 1

        def prune_observability_history(self, *, retention_days: int) -> tuple[int, int]:
            called["retention_days"] = retention_days
            return 2, 3

    monkeypatch.setattr(cli, "StateStore", FakeState)
    monkeypatch.setattr(
        cli,
        "run_observability_tui",
        lambda **kwargs: called.setdefault("top", kwargs),
    )

    cli._cmd_top(cfg)

    assert called["reconciled"] is True
    assert called["retention_days"] == 45
    top_call = called["top"]
    assert isinstance(top_call, dict)
    assert top_call["refresh_seconds"] == 5
    assert top_call["default_window"] == "7d"
    assert top_call["row_limit"] == 111


def test_build_codex_agents_by_repo_uses_repo_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _multi_repo_config(tmp_path)
    cfg = AppConfig(
        runtime=cfg.runtime,
        repos=cfg.repos,
        codex=cfg.codex,
        codex_overrides=(
            (
                "bosatsu",
                CodexConfig(
                    enabled=False,
                    model="gpt-b",
                    sandbox="danger-full-access",
                    profile=None,
                    extra_args=("--b",),
                ),
            ),
        ),
    )
    calls: list[CodexConfig] = []

    def fake_codex_adapter(codex: CodexConfig) -> str:
        calls.append(codex)
        return f"codex:{codex.enabled}:{codex.model}"

    monkeypatch.setattr(cli, "CodexAdapter", fake_codex_adapter)

    built = cli._build_codex_agents_by_repo(cfg)
    assert set(built) == {"johnynek/mergexo", "johnynek/bosatsu"}
    assert built["johnynek/mergexo"] == "codex:True:None"
    assert built["johnynek/bosatsu"] == "codex:False:gpt-b"
    assert calls == [cfg.codex, cfg.codex_for_repo("bosatsu")]


def test_cmd_feedback_dispatches_blocked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}
    args = SimpleNamespace(feedback_command="blocked", blocked_command="list", json=False)

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked",
        lambda config, state, call_args: called.setdefault("blocked", (config, state, call_args)),
    )

    cli._cmd_feedback(cfg, args)

    blocked_call = called["blocked"]
    assert isinstance(blocked_call, tuple)
    assert blocked_call[0] == cfg
    assert str(blocked_call[1]).startswith("state:")
    assert blocked_call[2] is args


def test_cmd_feedback_unknown_command_raises(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    args = SimpleNamespace(feedback_command="oops")

    with pytest.raises(RuntimeError, match="Unknown feedback command"):
        cli._cmd_feedback(cfg, args)


def test_cmd_feedback_blocked_dispatches_reset_and_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: dict[str, object] = {}

    class FakeState:
        pass

    args = SimpleNamespace(
        blocked_command="reset",
        pr=[12],
        all=False,
        yes=False,
        dry_run=True,
        head_sha="abc1234",
        repo=None,
    )
    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked_reset",
        lambda state, **kwargs: called.setdefault("reset", (state, kwargs)),
    )

    cli._cmd_feedback_blocked(_app_config(Path("/tmp")), FakeState(), args)
    reset_call = called["reset"]
    assert isinstance(reset_call, tuple)
    assert reset_call[1]["pr_numbers"] == (12,)
    assert reset_call[1]["head_sha_override"] == "abc1234"
    assert reset_call[1]["repo_full_name_filter"] is None

    with pytest.raises(RuntimeError, match="Unknown blocked command"):
        cli._cmd_feedback_blocked(
            _app_config(Path("/tmp")),
            FakeState(),
            SimpleNamespace(blocked_command="oops"),
        )


def test_cmd_feedback_blocked_dispatches_list(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    class FakeState:
        pass

    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked_list",
        lambda state, *, as_json: called.setdefault("list", (state, as_json)),
    )

    cli._cmd_feedback_blocked(
        _app_config(Path("/tmp")),
        FakeState(),
        SimpleNamespace(blocked_command="list", json=True),
    )
    list_call = called["list"]
    assert isinstance(list_call, tuple)
    assert list_call[1] is True


def test_cmd_feedback_blocked_list_text_and_json(capsys: pytest.CaptureFixture[str]) -> None:
    blocked = (
        BlockedPullRequestState(
            pr_number=12,
            issue_number=11,
            branch="agent/design/11-x",
            last_seen_head_sha="abc123",
            error="Command failed\nstderr details",
            updated_at="2026-02-22T06:00:57.859Z",
            pending_event_count=1,
            repo_full_name="johnynek/mergexo",
        ),
    )

    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return blocked

    cli._cmd_feedback_blocked_list(FakeState(), as_json=False)
    text_out = capsys.readouterr().out
    assert "repo=johnynek/mergexo pr_number=12 issue_number=11 pending_events=1" in text_out
    assert "branch=agent/design/11-x" in text_out
    assert "reason=Command failed" in text_out

    cli._cmd_feedback_blocked_list(FakeState(), as_json=True)
    json_out = capsys.readouterr().out
    payload = json.loads(json_out)
    assert payload[0]["repo_full_name"] == "johnynek/mergexo"
    assert payload[0]["pr_number"] == 12
    assert payload[0]["reason"].startswith("Command failed")


def test_cmd_feedback_blocked_list_empty(capsys: pytest.CaptureFixture[str]) -> None:
    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return ()

    cli._cmd_feedback_blocked_list(FakeState(), as_json=False)
    assert capsys.readouterr().out.strip() == "No blocked pull requests."


def test_cmd_feedback_blocked_reset_variants(capsys: pytest.CaptureFixture[str]) -> None:
    blocked = (
        BlockedPullRequestState(
            pr_number=12,
            issue_number=11,
            branch="agent/design/11-x",
            last_seen_head_sha=None,
            error="boom",
            updated_at="2026-02-22T06:00:57.859Z",
            pending_event_count=1,
            repo_full_name="johnynek/mergexo",
        ),
        BlockedPullRequestState(
            pr_number=14,
            issue_number=13,
            branch="agent/design/13-y",
            last_seen_head_sha=None,
            error="boom2",
            updated_at="2026-02-22T06:01:57.859Z",
            pending_event_count=2,
            repo_full_name="johnynek/mergexo",
        ),
    )
    reset_calls: list[tuple[tuple[int, ...] | None, str | None, str | None]] = []

    class FakeState:
        def list_blocked_pull_requests(
            self, *, repo_full_name: str | None = None
        ) -> tuple[BlockedPullRequestState, ...]:
            _ = repo_full_name
            return blocked

        def reset_blocked_pull_requests(
            self,
            *,
            pr_numbers: tuple[int, ...] | None = None,
            last_seen_head_sha_override: str | None = None,
            repo_full_name: str | None = None,
        ) -> int:
            reset_calls.append((pr_numbers, last_seen_head_sha_override, repo_full_name))
            if pr_numbers is None:
                return len(blocked)
            return len(pr_numbers)

    with pytest.raises(RuntimeError, match="--all requires --yes"):
        cli._cmd_feedback_blocked_reset(
            FakeState(),
            configured_repos=_app_config(Path("/tmp")).repos,
            pr_numbers=(),
            reset_all=True,
            yes=False,
            dry_run=False,
            head_sha_override=None,
            repo_full_name_filter=None,
        )

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        configured_repos=_app_config(Path("/tmp")).repos,
        pr_numbers=(12, 99),
        reset_all=False,
        yes=False,
        dry_run=True,
        head_sha_override="abc1234",
        repo_full_name_filter="johnynek/mergexo",
    )
    assert (
        "Would reset blocked pull requests: 12 (repo=johnynek/mergexo)" in capsys.readouterr().out
    )
    assert reset_calls == []

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        configured_repos=_app_config(Path("/tmp")).repos,
        pr_numbers=(12, 99),
        reset_all=False,
        yes=False,
        dry_run=False,
        head_sha_override=None,
        repo_full_name_filter="johnynek/mergexo",
    )
    assert "Reset 1 blocked pull request(s)." in capsys.readouterr().out
    assert reset_calls == [((12,), None, "johnynek/mergexo")]

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        configured_repos=_app_config(Path("/tmp")).repos,
        pr_numbers=(),
        reset_all=True,
        yes=True,
        dry_run=False,
        head_sha_override="ABCDEF1",
        repo_full_name_filter=None,
    )
    assert "Reset 2 blocked pull request(s)." in capsys.readouterr().out
    assert reset_calls[-1] == (None, "abcdef1", None)

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        configured_repos=_app_config(Path("/tmp")).repos,
        pr_numbers=(99,),
        reset_all=False,
        yes=False,
        dry_run=False,
        head_sha_override=None,
        repo_full_name_filter=None,
    )
    assert "No blocked pull requests matched." in capsys.readouterr().out


def test_cmd_feedback_blocked_reset_dry_run_all(capsys: pytest.CaptureFixture[str]) -> None:
    blocked = (
        BlockedPullRequestState(
            pr_number=12,
            issue_number=11,
            branch="agent/design/11-x",
            last_seen_head_sha=None,
            error="boom",
            updated_at="2026-02-22T06:00:57.859Z",
            pending_event_count=1,
            repo_full_name="johnynek/mergexo",
        ),
    )

    class FakeState:
        def list_blocked_pull_requests(
            self, *, repo_full_name: str | None = None
        ) -> tuple[BlockedPullRequestState, ...]:
            _ = repo_full_name
            return blocked

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        configured_repos=_app_config(Path("/tmp")).repos,
        pr_numbers=(),
        reset_all=True,
        yes=False,
        dry_run=True,
        head_sha_override=None,
        repo_full_name_filter=None,
    )
    assert "Would reset all blocked pull requests" in capsys.readouterr().out


def test_cmd_feedback_blocked_reset_rejects_invalid_head_sha() -> None:
    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return ()

    with pytest.raises(RuntimeError, match="--head-sha must be a hex git commit SHA"):
        cli._cmd_feedback_blocked_reset(
            FakeState(),
            configured_repos=_app_config(Path("/tmp")).repos,
            pr_numbers=(12,),
            reset_all=False,
            yes=False,
            dry_run=True,
            head_sha_override="not-a-sha",
            repo_full_name_filter=None,
        )


def test_cmd_feedback_blocked_reset_requires_repo_with_pr_for_multi_repo() -> None:
    cfg = AppConfig(
        runtime=RuntimeConfig(
            base_dir=Path("/tmp/state"),
            worker_count=1,
            poll_interval_seconds=60,
        ),
        repos=(
            RepoConfig(
                repo_id="a",
                owner="owner",
                name="repo-a",
                default_branch="main",
                trigger_label="agent:design",
                bugfix_label="agent:bugfix",
                small_job_label="agent:small-job",
                coding_guidelines_path="docs/python_style.md",
                design_docs_dir="docs/design",
                allowed_users=frozenset({"owner"}),
                local_clone_source=None,
                remote_url=None,
            ),
            RepoConfig(
                repo_id="b",
                owner="owner",
                name="repo-b",
                default_branch="main",
                trigger_label="agent:design",
                bugfix_label="agent:bugfix",
                small_job_label="agent:small-job",
                coding_guidelines_path="docs/python_style.md",
                design_docs_dir="docs/design",
                allowed_users=frozenset({"owner"}),
                local_clone_source=None,
                remote_url=None,
            ),
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )

    class FakeState:
        def list_blocked_pull_requests(
            self, *, repo_full_name: str | None = None
        ) -> tuple[BlockedPullRequestState, ...]:
            _ = repo_full_name
            return ()

    with pytest.raises(RuntimeError, match="--repo is required"):
        cli._cmd_feedback_blocked_reset(
            FakeState(),
            configured_repos=cfg.repos,
            pr_numbers=(12,),
            reset_all=False,
            yes=False,
            dry_run=True,
            head_sha_override=None,
            repo_full_name_filter=None,
        )


def test_summarize_block_reason_truncation() -> None:
    assert cli._summarize_block_reason(None) == "<none>"
    assert cli._summarize_block_reason("  \n  ") == "<none>"
    assert cli._summarize_block_reason("first line\nsecond") == "first line"
    assert cli._summarize_block_reason("x" * 220).endswith("...")


def test_state_db_path(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    assert cli._state_db_path(cfg) == cfg.runtime.base_dir / "state.db"


def test_resolve_repo_filter_valid_and_error_paths(tmp_path: Path) -> None:
    cfg = _multi_repo_config(tmp_path)
    assert cli._resolve_repo_filter(cfg, "mergexo") == "johnynek/mergexo"
    assert cli._resolve_repo_filter(cfg, "johnynek/bosatsu") == "johnynek/bosatsu"

    with pytest.raises(RuntimeError, match="--repo must be non-empty"):
        cli._resolve_repo_filter(cfg, "   ")

    with pytest.raises(RuntimeError, match="Unknown --repo value"):
        cli._resolve_repo_filter(cfg, "missing")
