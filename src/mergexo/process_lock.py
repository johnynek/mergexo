from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import json
import os
from pathlib import Path
import secrets
from typing import Iterator


_WRITER_LOCK_FILENAME = "writer.lock"


class ProcessLockError(RuntimeError):
    """Raised when an exclusive writer lock cannot be acquired."""


@dataclass(frozen=True)
class _LockOwner:
    pid: int | None
    command: str | None
    started_at: str | None
    token: str | None


@contextmanager
def writer_process_lock(*, base_dir: Path, command: str) -> Iterator[None]:
    lock = _WriterProcessLock(lock_path=base_dir / _WRITER_LOCK_FILENAME, command=command)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class _WriterProcessLock:
    def __init__(self, *, lock_path: Path, command: str) -> None:
        self._lock_path = lock_path
        self._command = command
        self._inode: int | None = None
        self._token: str | None = None

    def acquire(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._token = None
        for _ in range(2):
            try:
                fd = os.open(
                    self._lock_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o644,
                )
            except FileExistsError:
                if self._clear_stale_lock_if_dead_owner():
                    continue
                raise ProcessLockError(self._active_lock_error_message()) from None

            try:
                stat_result = os.fstat(fd)
                self._inode = stat_result.st_ino
                lock_token = secrets.token_hex(16)
                payload = {
                    "pid": os.getpid(),
                    "command": self._command,
                    "started_at": _utc_now_iso8601(),
                    "token": lock_token,
                }
                os.write(fd, (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
                os.fsync(fd)
            except Exception:
                try:
                    os.close(fd)
                finally:
                    try:
                        os.unlink(self._lock_path)
                    except FileNotFoundError:
                        pass
                self._inode = None
                self._token = None
                raise
            else:
                os.close(fd)
                self._token = lock_token
                return

        raise ProcessLockError(self._active_lock_error_message())

    def release(self) -> None:
        if self._inode is None or self._token is None:
            self._token = None
            return
        try:
            current = self._lock_path.stat()
        except FileNotFoundError:
            self._inode = None
            self._token = None
            return
        if current.st_ino != self._inode:
            self._inode = None
            self._token = None
            return
        owner = _read_lock_owner(self._lock_path)
        if owner.token != self._token:
            self._inode = None
            self._token = None
            return
        try:
            os.unlink(self._lock_path)
        except FileNotFoundError:
            pass
        finally:
            self._inode = None
            self._token = None

    def _clear_stale_lock_if_dead_owner(self) -> bool:
        owner = _read_lock_owner(self._lock_path)
        if owner.pid is None or owner.pid == os.getpid():
            return False
        if _pid_is_running(owner.pid):
            return False
        try:
            os.unlink(self._lock_path)
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return True

    def _active_lock_error_message(self) -> str:
        owner = _read_lock_owner(self._lock_path)
        owner_parts: list[str] = []
        if owner.pid is not None:
            owner_parts.append(f"pid={owner.pid}")
        if owner.command:
            owner_parts.append(f"command={owner.command}")
        owner_detail = f" ({', '.join(owner_parts)})" if owner_parts else ""
        return (
            "Another MergeXO writer process appears active"
            f"{owner_detail}. Lock file: {self._lock_path}. "
            "If this lock is stale, stop running writer processes and remove the lock file, then "
            "retry."
        )


def _read_lock_owner(lock_path: Path) -> _LockOwner:
    try:
        payload_text = lock_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return _LockOwner(pid=None, command=None, started_at=None, token=None)
    except OSError:
        return _LockOwner(pid=None, command=None, started_at=None, token=None)
    if not payload_text:
        return _LockOwner(pid=None, command=None, started_at=None, token=None)
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return _LockOwner(pid=None, command=None, started_at=None, token=None)
    if not isinstance(payload, dict):
        return _LockOwner(pid=None, command=None, started_at=None, token=None)
    raw_pid = payload.get("pid")
    raw_command = payload.get("command")
    raw_started_at = payload.get("started_at")
    raw_token = payload.get("token")
    owner_pid = raw_pid if isinstance(raw_pid, int) else None
    owner_command = raw_command if isinstance(raw_command, str) else None
    owner_started_at = raw_started_at if isinstance(raw_started_at, str) else None
    owner_token = raw_token if isinstance(raw_token, str) else None
    return _LockOwner(
        pid=owner_pid,
        command=owner_command,
        started_at=owner_started_at,
        token=owner_token,
    )


def _pid_is_running(pid: int) -> bool:
    if pid < 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        return True
    return True


def _utc_now_iso8601() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
