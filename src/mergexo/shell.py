from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import logging
import os
from queue import Empty, Queue
import shlex
import signal
import subprocess
import sys
import threading
import time
from typing import Sequence, TextIO


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


@dataclass(frozen=True)
class DurableLaunch:
    token: str
    metadata_path: Path


@dataclass(frozen=True)
class ResolvedLaunch:
    pid: int
    pgid: int | None


LOGGER = logging.getLogger("mergexo.shell")


def _preview(text: str, *, limit: int = 200) -> str:
    compact = text.replace("\n", "\\n").strip()
    if not compact:
        return "<empty>"
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit]}..."


def _utc_now_iso8601() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _durable_launch_wrapper_argv(argv: list[str], durable_launch: DurableLaunch) -> list[str]:
    return [
        sys.executable,
        "-m",
        "mergexo.shell",
        "--durable-launch-wrapper",
        "--launch-token",
        durable_launch.token,
        "--metadata-path",
        str(durable_launch.metadata_path),
        "--",
        *argv,
    ]


def _write_durable_launch_metadata(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _read_durable_launch_metadata(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except OSError:
        return None
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _remove_durable_launch_metadata(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _resolved_launch_from_payload(
    payload: dict[str, object], *, launch_token: str
) -> ResolvedLaunch | None:
    token = payload.get("token")
    pid = payload.get("pid")
    pgid = payload.get("pgid")
    if token != launch_token or not isinstance(pid, int) or pid < 1:
        return None
    return ResolvedLaunch(
        pid=pid,
        pgid=pgid if isinstance(pgid, int) and pgid > 0 else None,
    )


def _find_launch_by_token(launch_token: str) -> ResolvedLaunch | None:
    if os.name == "nt":
        return None
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=", "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        pid_text, _, command = line.strip().partition(" ")
        try:
            argv = shlex.split(command)
        except ValueError:
            continue
        if "--launch-token" not in argv:
            continue
        launch_flag_index = argv.index("--launch-token")
        if launch_flag_index + 1 >= len(argv) or argv[launch_flag_index + 1] != launch_token:
            continue
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid < 1:
            continue
        try:
            pgid = os.getpgid(pid)
        except OSError:
            pgid = None
        return ResolvedLaunch(pid=pid, pgid=pgid)
    return None


def resolve_durable_launch(durable_launch: DurableLaunch) -> ResolvedLaunch | None:
    payload = _read_durable_launch_metadata(durable_launch.metadata_path)
    if payload is not None:
        resolved = _resolved_launch_from_payload(payload, launch_token=durable_launch.token)
        if resolved is not None:
            return resolved
    return _find_launch_by_token(durable_launch.token)


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
    durable_launch: DurableLaunch | None = None,
) -> str:
    start_new_session = os.name != "nt"
    spawn_argv = _durable_launch_wrapper_argv(argv, durable_launch) if durable_launch else argv
    proc = subprocess.Popen(
        spawn_argv,
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

    try:
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
            stdin = proc.stdin

            def _writer() -> None:
                try:
                    stdin.write(input_text)
                    stdin.flush()
                except BrokenPipeError:
                    return
                finally:
                    stdin.close()

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
                elif (
                    idle_timeout_seconds is not None
                    and now - last_progress_at >= idle_timeout_seconds
                ):
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
    finally:
        if durable_launch is not None:
            _remove_durable_launch_metadata(durable_launch.metadata_path)


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _is_process_group_alive(pgid: int) -> bool:
    if pgid < 1 or not hasattr(os, "killpg"):
        return False
    try:
        os.killpg(pgid, 0)
    except OSError:
        return False
    return True


def terminate_process_tree(
    *,
    pid: int,
    pgid: int | None = None,
    graceful_shutdown_seconds: float = 5.0,
) -> None:
    target_pgid = pgid if pgid is not None else pid
    use_process_group = hasattr(os, "killpg") and pgid is not None
    if use_process_group:
        if not _is_process_group_alive(target_pgid):
            return
        try:
            os.killpg(target_pgid, signal.SIGTERM)
        except OSError:
            pass
    else:
        if not is_process_alive(pid):
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    deadline = time.monotonic() + graceful_shutdown_seconds
    while time.monotonic() < deadline:
        if use_process_group:
            if not _is_process_group_alive(target_pgid):
                return
        elif not is_process_alive(pid):
            return
        time.sleep(0.05)

    if use_process_group:
        try:
            os.killpg(target_pgid, signal.SIGKILL)
        except OSError:
            return
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            return


def _durable_launch_wrapper_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mergexo-shell-wrapper")
    parser.add_argument("--launch-token", required=True)
    parser.add_argument("--metadata-path", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    command = list(parsed.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise RuntimeError("durable launch wrapper requires a command")

    try:
        pgid = os.getpgrp()
    except OSError:
        pgid = None
    metadata_path = Path(parsed.metadata_path)
    payload: dict[str, object] = {
        "token": parsed.launch_token,
        "pid": os.getpid(),
        "pgid": pgid,
        "started_at": _utc_now_iso8601(),
    }
    _write_durable_launch_metadata(metadata_path, payload)
    child = subprocess.Popen(command)
    payload["child_pid"] = child.pid
    _write_durable_launch_metadata(metadata_path, payload)
    return child.wait()


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    if args and args[0] == "--durable-launch-wrapper":
        return _durable_launch_wrapper_main(args[1:])
    raise RuntimeError("mergexo.shell is an internal module")


if __name__ == "__main__":
    raise SystemExit(main())
