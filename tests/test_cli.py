from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

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
            enable_feedback_loop=False,
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
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def test_build_parser_supports_commands() -> None:
    parser = cli.build_parser()

    parsed_init = parser.parse_args(["init", "--verbose"])
    parsed_run = parser.parse_args(["run", "--once", "--verbose"])
    parsed_service = parser.parse_args(["service", "--once", "--verbose"])
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

    assert parsed_init.command == "init"
    assert parsed_init.verbose is True
    assert parsed_run.command == "run"
    assert parsed_run.once is True
    assert parsed_run.verbose is True
    assert parsed_service.command == "service"
    assert parsed_service.once is True
    assert parsed_service.verbose is True
    assert parsed_feedback_list.command == "feedback"
    assert parsed_feedback_list.feedback_command == "blocked"
    assert parsed_feedback_list.blocked_command == "list"
    assert parsed_feedback_list.json is True
    assert parsed_feedback_reset.blocked_command == "reset"
    assert parsed_feedback_reset.pr == [12, 14]
    assert parsed_feedback_reset.dry_run is True
    assert parsed_feedback_reset.head_sha == "abc1234"


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


def test_main_dispatches_feedback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
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
        cli, "configure_logging", lambda verbose: called.setdefault("verbose", verbose)
    )
    monkeypatch.setattr(cli, "_cmd_feedback", lambda c, a: called.setdefault("feedback", (c, a)))

    cli.main()

    assert called["verbose"] is False
    feedback_call = called["feedback"]
    assert isinstance(feedback_call, tuple)
    assert feedback_call[0] == cfg
    assert feedback_call[1].blocked_command == "list"


def test_main_dispatches_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)

    class FakeParser:
        def parse_args(self) -> SimpleNamespace:
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
        cli, "configure_logging", lambda verbose: called.setdefault("verbose", verbose)
    )
    monkeypatch.setattr(
        cli, "_cmd_service", lambda c, once: called.setdefault("service", (c, once))
    )

    cli.main()
    assert called["verbose"] is True
    assert called["service"] == (cfg, True)


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


def test_cmd_feedback_dispatches_blocked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    called: dict[str, object] = {}
    args = SimpleNamespace(feedback_command="blocked", blocked_command="list", json=False)

    monkeypatch.setattr(cli, "StateStore", lambda p: f"state:{p}")
    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked",
        lambda state, call_args: called.setdefault("blocked", (state, call_args)),
    )

    cli._cmd_feedback(cfg, args)

    blocked_call = called["blocked"]
    assert isinstance(blocked_call, tuple)
    assert str(blocked_call[0]).startswith("state:")
    assert blocked_call[1] is args


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
    )
    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked_reset",
        lambda state, **kwargs: called.setdefault("reset", (state, kwargs)),
    )

    cli._cmd_feedback_blocked(FakeState(), args)
    reset_call = called["reset"]
    assert isinstance(reset_call, tuple)
    assert reset_call[1]["pr_numbers"] == (12,)
    assert reset_call[1]["head_sha_override"] == "abc1234"

    with pytest.raises(RuntimeError, match="Unknown blocked command"):
        cli._cmd_feedback_blocked(FakeState(), SimpleNamespace(blocked_command="oops"))


def test_cmd_feedback_blocked_dispatches_list(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    class FakeState:
        pass

    monkeypatch.setattr(
        cli,
        "_cmd_feedback_blocked_list",
        lambda state, *, as_json: called.setdefault("list", (state, as_json)),
    )

    cli._cmd_feedback_blocked(FakeState(), SimpleNamespace(blocked_command="list", json=True))
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
        ),
    )

    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return blocked

    cli._cmd_feedback_blocked_list(FakeState(), as_json=False)
    text_out = capsys.readouterr().out
    assert "pr_number=12 issue_number=11 pending_events=1" in text_out
    assert "branch=agent/design/11-x" in text_out
    assert "reason=Command failed" in text_out

    cli._cmd_feedback_blocked_list(FakeState(), as_json=True)
    json_out = capsys.readouterr().out
    payload = json.loads(json_out)
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
        ),
        BlockedPullRequestState(
            pr_number=14,
            issue_number=13,
            branch="agent/design/13-y",
            last_seen_head_sha=None,
            error="boom2",
            updated_at="2026-02-22T06:01:57.859Z",
            pending_event_count=2,
        ),
    )
    reset_calls: list[tuple[tuple[int, ...] | None, str | None]] = []

    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return blocked

        def reset_blocked_pull_requests(
            self,
            *,
            pr_numbers: tuple[int, ...] | None = None,
            last_seen_head_sha_override: str | None = None,
        ) -> int:
            reset_calls.append((pr_numbers, last_seen_head_sha_override))
            if pr_numbers is None:
                return len(blocked)
            return len(pr_numbers)

    with pytest.raises(RuntimeError, match="--all requires --yes"):
        cli._cmd_feedback_blocked_reset(
            FakeState(),
            pr_numbers=(),
            reset_all=True,
            yes=False,
            dry_run=False,
            head_sha_override=None,
        )

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        pr_numbers=(12, 99),
        reset_all=False,
        yes=False,
        dry_run=True,
        head_sha_override="abc1234",
    )
    assert (
        "Would reset blocked pull requests: 12 (override last_seen_head_sha=abc1234)"
        in capsys.readouterr().out
    )
    assert reset_calls == []

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        pr_numbers=(12, 99),
        reset_all=False,
        yes=False,
        dry_run=False,
        head_sha_override=None,
    )
    assert "Reset 1 blocked pull request(s)." in capsys.readouterr().out
    assert reset_calls == [((12,), None)]

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        pr_numbers=(),
        reset_all=True,
        yes=True,
        dry_run=False,
        head_sha_override="ABCDEF1",
    )
    assert "Reset 2 blocked pull request(s)." in capsys.readouterr().out
    assert reset_calls[-1] == (None, "abcdef1")

    cli._cmd_feedback_blocked_reset(
        FakeState(),
        pr_numbers=(99,),
        reset_all=False,
        yes=False,
        dry_run=False,
        head_sha_override=None,
    )
    assert "No blocked pull requests matched." in capsys.readouterr().out


def test_cmd_feedback_blocked_reset_rejects_invalid_head_sha() -> None:
    class FakeState:
        def list_blocked_pull_requests(self) -> tuple[BlockedPullRequestState, ...]:
            return ()

    with pytest.raises(RuntimeError, match="--head-sha must be a hex git commit SHA"):
        cli._cmd_feedback_blocked_reset(
            FakeState(),
            pr_numbers=(12,),
            reset_all=False,
            yes=False,
            dry_run=True,
            head_sha_override="not-a-sha",
        )


def test_summarize_block_reason_truncation() -> None:
    assert cli._summarize_block_reason(None) == "<none>"
    assert cli._summarize_block_reason("  \n  ") == "<none>"
    assert cli._summarize_block_reason("first line\nsecond") == "first line"
    assert cli._summarize_block_reason("x" * 220).endswith("...")


def test_state_db_path(tmp_path: Path) -> None:
    cfg = _app_config(tmp_path)
    assert cli._state_db_path(cfg) == cfg.runtime.base_dir / "state.db"
