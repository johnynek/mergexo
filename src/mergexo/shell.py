from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
import logging
import os
from queue import Empty, Queue
import signal
import subprocess
import threading
import time
from typing import TextIO


class CommandError(RuntimeError):
    pass


class CommandTimeoutError(CommandError):
    def __init__(
        self,
        *,
        argv: list[str],
        timeout_kind: str,
        timeout_seconds: float,
        pid: int,
        pgid: int | None,
        stdout: str,
        stderr: str,
    ) -> None:
        self.argv = tuple(argv)
        self.timeout_kind = timeout_kind
        self.timeout_seconds = timeout_seconds
        self.pid = pid
        self.pgid = pgid
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            "Command timed out\n"
            f"cmd: {' '.join(argv)}\n"
            f"timeout_kind: {timeout_kind}\n"
            f"timeout_seconds: {timeout_seconds}\n"
            f"pid: {pid}\n"
            f"pgid: {pgid if pgid is not None else '<none>'}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )


@dataclass(frozen=True)
class RunningCommand:
    argv: tuple[str, ...]
    cwd: Path | None
    pid: int
    pgid: int | None
    timeout_seconds: float | None
    idle_timeout_seconds: float | None


LOGGER = logging.getLogger("mergexo.shell")


def _preview(text: str, *, limit: int = 200) -> str:
    compact = text.replace("\n", "\\n").strip()
    if not compact:
        return "<empty>"
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit]}..."


def run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    check: bool = True,
    timeout_seconds: float | None = None,
    idle_timeout_seconds: float | None = None,
    graceful_shutdown_seconds: float = 5.0,
    on_start: Callable[[RunningCommand], None] | None = None,
    on_output: Callable[[str, str], None] | None = None,
) -> str:
    start_new_session = os.name != "nt"
    proc = subprocess.Popen(
        argv,
        cwd=str(cwd) if cwd else None,
        stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=start_new_session,
    )
    pgid: int | None
    try:
        pgid = os.getpgid(proc.pid)
    except OSError:
        pgid = None

    if on_start is not None:
        on_start(
            RunningCommand(
                argv=tuple(argv),
                cwd=cwd,
                pid=proc.pid,
                pgid=pgid,
                timeout_seconds=timeout_seconds,
                idle_timeout_seconds=idle_timeout_seconds,
            )
        )

    if proc.stdout is None or proc.stderr is None:
        raise RuntimeError("subprocess pipes were not created")

    queue: Queue[tuple[str, str | None]] = Queue()
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    closed_streams = 0
    started_at = time.monotonic()
    last_progress_at = started_at
    timeout_kind: str | None = None
    timeout_value: float | None = None

    def _reader(stream_name: str, stream: TextIO) -> None:
        try:
            for chunk in iter(stream.readline, ""):
                queue.put((stream_name, chunk))
        finally:
            stream.close()
            queue.put((stream_name, None))

    stdout_thread = threading.Thread(
        target=_reader,
        args=("stdout", proc.stdout),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_reader,
        args=("stderr", proc.stderr),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    stdin_thread: threading.Thread | None = None
    if input_text is not None and proc.stdin is not None:
        def _writer() -> None:
            try:
                proc.stdin.write(input_text)
                proc.stdin.flush()
            except BrokenPipeError:
                return
            finally:
                proc.stdin.close()

        stdin_thread = threading.Thread(target=_writer, daemon=True)
        stdin_thread.start()

    while closed_streams < 2:
        try:
            stream_name, chunk = queue.get(timeout=0.1)
        except Empty:
            stream_name = ""
            chunk = None
        if stream_name:
            if chunk is None:
                closed_streams += 1
            else:
                last_progress_at = time.monotonic()
                if stream_name == "stdout":
                    stdout_chunks.append(chunk)
                else:
                    stderr_chunks.append(chunk)
                if on_output is not None:
                    on_output(stream_name, chunk)

        if timeout_kind is None:
            now = time.monotonic()
            if timeout_seconds is not None and now - started_at >= timeout_seconds:
                timeout_kind = "wall_clock"
                timeout_value = timeout_seconds
                terminate_process_tree(
                    pid=proc.pid,
                    pgid=pgid,
                    graceful_shutdown_seconds=graceful_shutdown_seconds,
                )
            elif idle_timeout_seconds is not None and now - last_progress_at >= idle_timeout_seconds:
                timeout_kind = "idle"
                timeout_value = idle_timeout_seconds
                terminate_process_tree(
                    pid=proc.pid,
                    pgid=pgid,
                    graceful_shutdown_seconds=graceful_shutdown_seconds,
                )

    if stdin_thread is not None:
        stdin_thread.join(timeout=0.5)
    stdout_thread.join(timeout=0.5)
    stderr_thread.join(timeout=0.5)
    returncode = proc.wait()
    stdout = "".join(stdout_chunks)
    stderr = "".join(stderr_chunks)

    if timeout_kind is not None and timeout_value is not None:
        LOGGER.error(
            "event=command_timeout command=%s timeout_kind=%s timeout_seconds=%s pid=%s pgid=%s stderr=%s stdout=%s",
            " ".join(argv),
            timeout_kind,
            timeout_value,
            proc.pid,
            pgid if pgid is not None else "<none>",
            _preview(stderr),
            _preview(stdout),
        )
        raise CommandTimeoutError(
            argv=argv,
            timeout_kind=timeout_kind,
            timeout_seconds=timeout_value,
            pid=proc.pid,
            pgid=pgid,
            stdout=stdout,
            stderr=stderr,
        )

    if check and returncode != 0:
        LOGGER.error(
            "event=command_failed command=%s exit_code=%s stderr=%s stdout=%s",
            " ".join(argv),
            returncode,
            _preview(stderr),
            _preview(stdout),
        )
        raise CommandError(
            "Command failed\n"
            f"cmd: {' '.join(argv)}\n"
            f"exit: {returncode}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
    return stdout


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def terminate_process_tree(
    *,
    pid: int,
    pgid: int | None = None,
    graceful_shutdown_seconds: float = 5.0,
) -> None:
    if not is_process_alive(pid):
        return

    target_pgid = pgid if pgid is not None else pid
    if hasattr(os, "killpg"):
        try:
            os.killpg(target_pgid, signal.SIGTERM)
        except OSError:
            pass
    else:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    deadline = time.monotonic() + graceful_shutdown_seconds
    while time.monotonic() < deadline:
        if not is_process_alive(pid):
            return
        time.sleep(0.05)

    if hasattr(os, "killpg"):
        try:
            os.killpg(target_pgid, signal.SIGKILL)
        except OSError:
            return
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            return
