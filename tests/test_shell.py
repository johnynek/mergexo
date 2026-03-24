from __future__ import annotations

import sys

import pytest

from mergexo.shell import CommandTimeoutError, RunningCommand, run


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
                (
                    "import sys, time; "
                    "print('started'); sys.stdout.flush(); "
                    "time.sleep(5)"
                ),
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
