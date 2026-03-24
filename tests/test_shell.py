from __future__ import annotations

import io
from pathlib import Path
import signal
import sys

import pytest

import mergexo.shell as shell
from mergexo.shell import (
    CommandError,
    CommandTimeoutError,
    RunningCommand,
    _preview,
    is_process_alive,
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


def test_is_process_alive_returns_false_on_os_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell.os, "kill", lambda _pid, _sig: (_ for _ in ()).throw(OSError("nope")))
    assert is_process_alive(123) is False


def test_terminate_process_tree_returns_when_process_is_already_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killpg_calls: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: False)
    monkeypatch.setattr(shell.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)))

    terminate_process_tree(pid=5, pgid=7)

    assert killpg_calls == []


def test_terminate_process_tree_returns_after_sigterm_when_process_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alive_states = iter([True, False])
    killpg_calls: list[tuple[int, signal.Signals]] = []
    monotonic_values = iter([0.0, 0.0])

    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: next(alive_states))
    monkeypatch.setattr(shell.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)))
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, pgid=7, graceful_shutdown_seconds=1.0)

    assert killpg_calls == [(7, signal.SIGTERM)]


def test_terminate_process_tree_ignores_killpg_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monotonic_values = iter([0.0, 2.0])

    monkeypatch.setattr(shell, "is_process_alive", lambda _pid: True)
    monkeypatch.setattr(
        shell.os,
        "killpg",
        lambda _pgid, _sig: (_ for _ in ()).throw(OSError("killpg failed")),
    )
    monkeypatch.setattr(shell.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(shell.time, "sleep", lambda _seconds: None)

    terminate_process_tree(pid=5, pgid=7, graceful_shutdown_seconds=1.0)


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
