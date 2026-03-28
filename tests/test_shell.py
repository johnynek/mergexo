from __future__ import annotations

import io
from pathlib import Path
import runpy
import signal
import sys

import pytest

import mergexo.shell as shell
from mergexo.shell import (
    CommandError,
    CommandTimeoutError,
    DurableLaunch,
    ResolvedLaunch,
    RunningCommand,
    _durable_launch_wrapper_main,
    _preview,
    is_process_alive,
    main,
    resolve_durable_launch,
    run,
    terminate_process_tree,
)


def test_preview_handles_empty_and_truncates() -> None:
    assert _preview("") == "<empty>"
    assert _preview("abcdef", limit=3) == "abc..."


def test_run_reports_start_and_progress_callbacks(tmp_path: Path) -> None:
    started: list[RunningCommand] = []
    output_chunks: list[tuple[str, str]] = []

    stdout = run(
        [
            sys.executable,
            "-c",
            "import sys; print('hello'); print('err', file=sys.stderr)",
        ],
        cwd=tmp_path,
        on_start=started.append,
        on_output=lambda stream, chunk: output_chunks.append((stream, chunk)),
    )

    assert stdout == "hello\n"
    assert len(started) == 1
    assert started[0].cwd == tmp_path
    assert started[0].pid > 0
    assert any(stream == "stdout" and chunk == "hello\n" for stream, chunk in output_chunks)
    assert any(stream == "stderr" and chunk == "err\n" for stream, chunk in output_chunks)


def test_run_supports_durable_launch_wrapper(tmp_path: Path) -> None:
    durable_launch = DurableLaunch(
        token="launch-token",
        metadata_path=tmp_path / "launch.json",
    )
    started: list[RunningCommand] = []

    stdout = run(
        [sys.executable, "-c", "print('wrapped')"],
        cwd=tmp_path,
        on_start=started.append,
        durable_launch=durable_launch,
    )

    assert stdout == "wrapped\n"
    assert len(started) == 1
    assert started[0].argv == (sys.executable, "-c", "print('wrapped')")
    assert started[0].pid > 0
    assert durable_launch.metadata_path.exists() is False


def test_run_raises_timeout_and_preserves_partial_output(tmp_path: Path) -> None:
    with pytest.raises(CommandTimeoutError, match="timeout_kind: wall_clock") as exc_info:
        run(
            [
                sys.executable,
                "-c",
                ("import sys, time; print('started'); sys.stdout.flush(); time.sleep(5)"),
            ],
            cwd=tmp_path,
            timeout_seconds=0.2,
            idle_timeout_seconds=10.0,
        )

    exc = exc_info.value
    assert exc.timeout_kind == "wall_clock"
    assert exc.timeout_seconds == 0.2
    assert "started\n" in exc.stdout
    assert exc.pid > 0


def test_run_raises_idle_timeout_after_input_progress(tmp_path: Path) -> None:
    with pytest.raises(CommandTimeoutError, match="timeout_kind: idle") as exc_info:
        run(
            [
                sys.executable,
                "-c",
                (
                    "import sys, time; "
                    "payload = sys.stdin.read(); "
                    "print(payload); sys.stdout.flush(); "
                    "time.sleep(5)"
                ),
            ],
            cwd=tmp_path,
            input_text="payload",
            timeout_seconds=10.0,
            idle_timeout_seconds=0.2,
        )

    exc = exc_info.value
    assert exc.timeout_kind == "idle"
    assert "payload\n" in exc.stdout


def test_run_raises_command_error_when_command_fails_and_pgid_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(shell.os, "getpgid", lambda _pid: (_ for _ in ()).throw(OSError("gone")))

    with pytest.raises(CommandError, match="exit: 2"):
        run(
            [
                sys.executable,
                "-c",
                "import sys; print('oops'); print('bad', file=sys.stderr); sys.exit(2)",
            ],
            cwd=tmp_path,
        )


def test_run_raises_when_subprocess_pipes_are_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeProc:
        pid = 123
        stdin = None
        stdout = None
        stderr = None

    monkeypatch.setattr(shell.subprocess, "Popen", lambda *args, **kwargs: FakeProc())
    monkeypatch.setattr(shell.os, "getpgid", lambda _pid: 123)

    with pytest.raises(RuntimeError, match="subprocess pipes were not created"):
        run(["echo", "hello"])


def test_run_ignores_broken_pipe_when_child_closes_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenPipeWriter:
        def write(self, _text: str) -> None:
            raise BrokenPipeError

        def flush(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeProc:
        pid = 123
        stdin = BrokenPipeWriter()
        stdout = io.StringIO("")
        stderr = io.StringIO("")

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(shell.subprocess, "Popen", lambda *args, **kwargs: FakeProc())
    monkeypatch.setattr(shell.os, "getpgid", lambda _pid: 123)

    assert run(["echo", "hello"], input_text="payload") == ""


def test_resolve_durable_launch_reads_metadata_file(tmp_path: Path) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    durable_launch.metadata_path.write_text(
        '{"pgid": 456, "pid": 123, "token": "launch-token"}\n',
        encoding="utf-8",
    )

    assert resolve_durable_launch(durable_launch) == ResolvedLaunch(pid=123, pgid=456)


def test_resolve_durable_launch_handles_invalid_metadata_payloads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")

    assert resolve_durable_launch(durable_launch) is None

    monkeypatch.setattr(
        Path, "read_text", lambda self, encoding="utf-8": (_ for _ in ()).throw(OSError())
    )
    assert resolve_durable_launch(durable_launch) is None

    monkeypatch.setattr(Path, "read_text", lambda self, encoding="utf-8": "not-json")
    assert resolve_durable_launch(durable_launch) is None

    monkeypatch.setattr(Path, "read_text", lambda self, encoding="utf-8": "[]")
    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_scans_process_list_when_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=0,
            stdout=" 123 /usr/bin/python -m mergexo.shell --durable-launch-wrapper --launch-token launch-token\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(shell.os, "getpgid", lambda _pid: 456)

    assert resolve_durable_launch(durable_launch) == ResolvedLaunch(pid=123, pgid=456)


def test_resolve_durable_launch_skips_bad_process_scan_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=0,
            stdout=(
                " not-a-pid /usr/bin/python --launch-token launch-token\n"
                " 0 /usr/bin/python --launch-token launch-token\n"
            ),
            stderr="",
        ),
    )

    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_requires_exact_launch_token_argument(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=0,
            stdout=(
                " 123 /usr/bin/python --launch-token mergexo-codex-real-token\n"
                " 456 /usr/bin/python -m something launch-token\n"
            ),
            stderr="",
        ),
    )

    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_skips_unparseable_process_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=0,
            stdout=' 123 /usr/bin/python --launch-token "launch-token\n',
            stderr="",
        ),
    )

    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_scans_process_list_without_pgid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=0,
            stdout=" 123 /usr/bin/python --launch-token launch-token\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(shell.os, "getpgid", lambda _pid: (_ for _ in ()).throw(OSError("gone")))

    assert resolve_durable_launch(durable_launch) == ResolvedLaunch(pid=123, pgid=None)


def test_resolve_durable_launch_returns_none_when_process_scan_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: shell.subprocess.CompletedProcess(
            args=["ps"],
            returncode=1,
            stdout="",
            stderr="boom",
        ),
    )

    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_returns_none_when_process_scan_raises_os_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(
        shell.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("ps missing")),
    )

    assert resolve_durable_launch(durable_launch) is None


def test_resolve_durable_launch_returns_none_on_windows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    durable_launch = DurableLaunch(token="launch-token", metadata_path=tmp_path / "launch.json")
    monkeypatch.setattr(shell.os, "name", "nt")

    assert resolve_durable_launch(durable_launch) is None


def test_remove_durable_launch_metadata_ignores_missing_file(tmp_path: Path) -> None:
    shell._remove_durable_launch_metadata(tmp_path / "missing.json")


def test_resolved_launch_from_payload_rejects_invalid_values() -> None:
    assert shell._resolved_launch_from_payload({}, launch_token="launch-token") is None
    assert (
        shell._resolved_launch_from_payload(
            {"token": "other", "pid": 123},
            launch_token="launch-token",
        )
        is None
    )
    assert (
        shell._resolved_launch_from_payload(
            {"token": "launch-token", "pid": 0},
            launch_token="launch-token",
        )
        is None
    )


def test_durable_launch_wrapper_main_records_parent_and_child_pids(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metadata_path = tmp_path / "launch.json"

    class FakeChild:
        pid = 222

        def wait(self) -> int:
            return 7

    monkeypatch.setattr(shell.os, "getpid", lambda: 111)
    monkeypatch.setattr(shell.os, "getpgrp", lambda: 333)
    monkeypatch.setattr(shell.subprocess, "Popen", lambda cmd: FakeChild())

    exit_code = _durable_launch_wrapper_main(
        [
            "--launch-token",
            "launch-token",
            "--metadata-path",
            str(metadata_path),
            "--",
            "codex",
            "exec",
        ]
    )

    assert exit_code == 7
    payload = shell.json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["token"] == "launch-token"
    assert payload["pid"] == 111
    assert payload["pgid"] == 333
    assert payload["child_pid"] == 222


def test_durable_launch_wrapper_main_handles_missing_command(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="requires a command"):
        _durable_launch_wrapper_main(
            [
                "--launch-token",
                "launch-token",
                "--metadata-path",
                str(tmp_path / "launch.json"),
            ]
        )


def test_durable_launch_wrapper_main_handles_missing_pgid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metadata_path = tmp_path / "launch.json"

    class FakeChild:
        pid = 222

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(shell.os, "getpgrp", lambda: (_ for _ in ()).throw(OSError("gone")))
    monkeypatch.setattr(shell.subprocess, "Popen", lambda cmd: FakeChild())

    assert (
        _durable_launch_wrapper_main(
            [
                "--launch-token",
                "launch-token",
                "--metadata-path",
                str(metadata_path),
                "--",
                "codex",
                "exec",
            ]
        )
        == 0
    )
    payload = shell.json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["pgid"] is None


def test_main_dispatches_durable_launch_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[list[str]] = []

    def fake_wrapper_main(argv: list[str]) -> int:
        called.append(argv)
        return 9

    monkeypatch.setattr(shell, "_durable_launch_wrapper_main", fake_wrapper_main)

    assert (
        main(
            [
                "--durable-launch-wrapper",
                "--launch-token",
                "launch-token",
                "--metadata-path",
                "/tmp/launch.json",
                "--",
                "codex",
                "exec",
            ]
        )
        == 9
    )
    assert called == [
        [
            "--launch-token",
            "launch-token",
            "--metadata-path",
            "/tmp/launch.json",
            "--",
            "codex",
            "exec",
        ]
    ]


def test_main_rejects_direct_invocation() -> None:
    with pytest.raises(RuntimeError, match="internal module"):
        main([])


def test_module_main_executes_wrapper_entrypoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metadata_path = tmp_path / "launch.json"

    class FakeChild:
        pid = 222

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mergexo.shell",
            "--durable-launch-wrapper",
            "--launch-token",
            "launch-token",
            "--metadata-path",
            str(metadata_path),
            "--",
            "codex",
            "exec",
        ],
    )
    monkeypatch.setattr(shell.subprocess, "Popen", lambda cmd: FakeChild())
    monkeypatch.delitem(sys.modules, "mergexo.shell", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("mergexo.shell", run_name="__main__")

    assert exc_info.value.code == 0


def test_is_process_alive_returns_false_on_os_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell.os, "kill", lambda _pid, _sig: (_ for _ in ()).throw(OSError("nope")))
    assert is_process_alive(123) is False


def test_is_process_alive_returns_true_when_signal_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shell.os, "kill", lambda _pid, _sig: None)
    assert is_process_alive(123) is True


def test_is_process_group_alive_handles_invalid_group() -> None:
    assert shell._is_process_group_alive(0) is False


def test_is_process_group_alive_returns_false_on_os_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        shell.os, "killpg", lambda _pgid, _sig: (_ for _ in ()).throw(OSError("gone"))
    )

    assert shell._is_process_group_alive(123) is False


def test_terminate_process_tree_returns_when_process_is_already_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killpg_calls: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(shell, "_is_process_group_alive", lambda _pgid: False)
    monkeypatch.setattr(shell.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)))

    terminate_process_tree(pid=5, pgid=7)

    assert killpg_calls == []


def test_terminate_process_tree_returns_after_sigterm_when_process_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alive_states = iter([True, False])
    killpg_calls: list[tuple[int, signal.Signals]] = []
    monotonic_values = iter([0.0, 0.0])

    monkeypatch.setattr(shell, "_is_process_group_alive", lambda _pgid: next(alive_states))
    monkeypatch.setattr(shell.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)))
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, pgid=7, graceful_shutdown_seconds=1.0)

    assert killpg_calls == [(7, signal.SIGTERM)]


def test_terminate_process_tree_ignores_killpg_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monotonic_values = iter([0.0, 2.0])

    monkeypatch.setattr(shell, "_is_process_group_alive", lambda _pgid: True)
    monkeypatch.setattr(
        shell.os,
        "killpg",
        lambda _pgid, _sig: (_ for _ in ()).throw(OSError("killpg failed")),
    )
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, pgid=7, graceful_shutdown_seconds=1.0)


def test_terminate_process_tree_kills_process_group_when_leader_is_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killpg_calls: list[tuple[int, signal.Signals]] = []
    monotonic_values = iter([0.0, 2.0])

    monkeypatch.setattr(shell, "_is_process_group_alive", lambda _pgid: True)
    monkeypatch.setattr(shell.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)))
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, pgid=7, graceful_shutdown_seconds=1.0)

    assert killpg_calls == [(7, signal.SIGTERM), (7, signal.SIGKILL)]


def test_terminate_process_tree_falls_back_to_kill_without_killpg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kill_calls: list[tuple[int, signal.Signals]] = []
    monotonic_values = iter([0.0, 2.0])

    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: True)
    monkeypatch.delattr(shell.os, "killpg", raising=False)

    def fake_kill(pid: int, sig: signal.Signals) -> None:
        kill_calls.append((pid, sig))
        raise OSError("kill failed")

    monkeypatch.setattr(shell.os, "kill", fake_kill)
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, graceful_shutdown_seconds=1.0)

    assert kill_calls == [(5, signal.SIGTERM), (5, signal.SIGKILL)]


def test_terminate_process_tree_returns_without_kill_when_pid_is_already_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kill_calls: list[tuple[int, signal.Signals]] = []

    monkeypatch.delattr(shell.os, "killpg", raising=False)
    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: False)
    monkeypatch.setattr(shell.os, "kill", lambda pid, sig: kill_calls.append((pid, sig)))

    terminate_process_tree(pid=5, graceful_shutdown_seconds=1.0)

    assert kill_calls == []


def test_terminate_process_tree_returns_after_sigterm_without_killpg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alive_states = iter([True, False])
    kill_calls: list[tuple[int, signal.Signals]] = []
    monotonic_values = iter([0.0, 0.0])

    monkeypatch.delattr(shell.os, "killpg", raising=False)
    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: next(alive_states))
    monkeypatch.setattr(shell.os, "kill", lambda pid, sig: kill_calls.append((pid, sig)))
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, graceful_shutdown_seconds=1.0)

    assert kill_calls == [(5, signal.SIGTERM)]
