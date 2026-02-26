from __future__ import annotations

import errno
import json
import os
from pathlib import Path

import pytest

from mergexo import process_lock as process_lock_module
from mergexo.process_lock import ProcessLockError, writer_process_lock


def _lock_path(base_dir: Path) -> Path:
    return base_dir / "writer.lock"


def test_writer_process_lock_creates_and_removes_lock_file(tmp_path: Path) -> None:
    with writer_process_lock(base_dir=tmp_path, command="run"):
        lock_path = _lock_path(tmp_path)
        assert lock_path.exists()
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        assert isinstance(payload["pid"], int)
        assert payload["command"] == "run"
        assert isinstance(payload["started_at"], str)
    assert not _lock_path(tmp_path).exists()


def test_writer_process_lock_rejects_when_active_lock_exists(tmp_path: Path) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "command": "service",
                "started_at": "2026-01-01T00:00:00.000000Z",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ProcessLockError, match="Another MergeXO writer process appears active"):
        with writer_process_lock(base_dir=tmp_path, command="run"):
            pass


def test_writer_process_lock_reclaims_stale_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "pid": 424242,
                "command": "run",
                "started_at": "2026-01-01T00:00:00.000000Z",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(process_lock_module, "_pid_is_running", lambda pid: False)

    with writer_process_lock(base_dir=tmp_path, command="service"):
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        assert payload["command"] == "service"

    assert not lock_path.exists()


def test_writer_process_lock_handles_write_failure_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")
    monkeypatch.setattr(
        process_lock_module.os, "write", lambda fd, data: (_ for _ in ()).throw(OSError("boom"))
    )

    with pytest.raises(OSError, match="boom"):
        lock.acquire()
    assert not lock_path.exists()


def test_writer_process_lock_handles_write_failure_when_lock_file_disappears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")
    monkeypatch.setattr(
        process_lock_module.os, "write", lambda fd, data: (_ for _ in ()).throw(OSError("boom"))
    )
    monkeypatch.setattr(
        process_lock_module.os,
        "unlink",
        lambda path: (_ for _ in ()).throw(FileNotFoundError()),
    )

    with pytest.raises(OSError, match="boom"):
        lock.acquire()

    monkeypatch.undo()
    if lock_path.exists():
        os.unlink(lock_path)


def test_writer_process_lock_raises_after_stale_clear_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = process_lock_module._WriterProcessLock(lock_path=_lock_path(tmp_path), command="run")

    monkeypatch.setattr(
        process_lock_module.os,
        "open",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileExistsError()),
    )
    monkeypatch.setattr(
        process_lock_module._WriterProcessLock,
        "_clear_stale_lock_if_dead_owner",
        lambda self: True,
    )
    monkeypatch.setattr(
        process_lock_module._WriterProcessLock,
        "_active_lock_error_message",
        lambda self: "lock still held",
    )

    with pytest.raises(ProcessLockError, match="lock still held"):
        lock.acquire()


def test_writer_process_lock_release_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_path = _lock_path(tmp_path)
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")

    lock.release()

    lock.acquire()
    os.unlink(lock_path)
    lock.release()

    lock.acquire()
    os.unlink(lock_path)
    lock_path.write_text("{}", encoding="utf-8")
    lock.release()

    os.unlink(lock_path)
    lock.acquire()
    same_inode = lock._inode

    class _StatResult:
        st_ino = same_inode

    original_stat = Path.stat

    def fake_stat(path_obj: Path) -> object:
        if path_obj == lock._lock_path:
            return _StatResult()
        return original_stat(path_obj)

    monkeypatch.setattr(Path, "stat", fake_stat)
    monkeypatch.setattr(
        process_lock_module.os,
        "unlink",
        lambda path: (_ for _ in ()).throw(FileNotFoundError()),
    )
    lock.release()


def test_writer_process_lock_release_skips_replacement_lock_when_inode_reused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")

    lock.acquire()
    original_inode = lock._inode
    assert original_inode is not None
    os.unlink(lock_path)
    lock_path.write_text("{}", encoding="utf-8")

    class _StatResult:
        st_ino = original_inode

    original_stat = Path.stat

    def fake_stat(path_obj: Path) -> object:
        if path_obj == lock._lock_path:
            return _StatResult()
        return original_stat(path_obj)

    with monkeypatch.context() as m:
        m.setattr(Path, "stat", fake_stat)
        lock.release()

    assert lock_path.exists()


def test_writer_process_lock_release_skips_when_inode_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")

    lock.acquire()
    original_inode = lock._inode
    assert original_inode is not None

    class _StatResult:
        st_ino = original_inode + 1

    original_stat = Path.stat

    def fake_stat(path_obj: Path) -> object:
        if path_obj == lock._lock_path:
            return _StatResult()
        return original_stat(path_obj)

    with monkeypatch.context() as m:
        m.setattr(Path, "stat", fake_stat)
        lock.release()

    assert lock_path.exists()


def test_clear_stale_lock_handles_unlink_races(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text('{"pid": 777}', encoding="utf-8")
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")
    monkeypatch.setattr(process_lock_module, "_pid_is_running", lambda pid: False)

    monkeypatch.setattr(
        process_lock_module.os,
        "unlink",
        lambda path: (_ for _ in ()).throw(FileNotFoundError()),
    )
    assert lock._clear_stale_lock_if_dead_owner() is True

    lock_path.write_text('{"pid": 777}', encoding="utf-8")
    monkeypatch.setattr(
        process_lock_module.os,
        "unlink",
        lambda path: (_ for _ in ()).throw(OSError("deny")),
    )
    assert lock._clear_stale_lock_if_dead_owner() is False


def test_clear_stale_lock_returns_false_when_owner_pid_is_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = _lock_path(tmp_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text('{"pid": 777}', encoding="utf-8")
    lock = process_lock_module._WriterProcessLock(lock_path=lock_path, command="run")
    monkeypatch.setattr(process_lock_module, "_pid_is_running", lambda pid: True)
    assert lock._clear_stale_lock_if_dead_owner() is False
    assert lock_path.exists()


def test_read_lock_owner_edge_cases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = process_lock_module._read_lock_owner(_lock_path(tmp_path))
    assert missing.pid is None

    path_with_read_error = _lock_path(tmp_path)
    path_with_read_error.parent.mkdir(parents=True, exist_ok=True)
    path_with_read_error.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, encoding="utf-8": (_ for _ in ()).throw(OSError("nope")),
    )
    read_error = process_lock_module._read_lock_owner(path_with_read_error)
    assert read_error.pid is None

    monkeypatch.undo()
    path_with_read_error.write_text("", encoding="utf-8")
    assert process_lock_module._read_lock_owner(path_with_read_error).pid is None

    path_with_read_error.write_text("{not-json", encoding="utf-8")
    assert process_lock_module._read_lock_owner(path_with_read_error).pid is None

    path_with_read_error.write_text("[]", encoding="utf-8")
    assert process_lock_module._read_lock_owner(path_with_read_error).pid is None


@pytest.mark.parametrize(
    ("pid", "kill_side_effect", "expected"),
    [
        (-1, None, False),
        (12, ProcessLookupError(), False),
        (12, PermissionError(), True),
        (12, OSError(errno.EEXIST, "other"), True),
        (12, None, True),
    ],
)
def test_pid_is_running_branches(
    pid: int,
    kill_side_effect: BaseException | None,
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_kill(target_pid: int, sig: int) -> None:
        _ = target_pid, sig
        if kill_side_effect is None:
            return
        raise kill_side_effect

    monkeypatch.setattr(process_lock_module.os, "kill", fake_kill)
    assert process_lock_module._pid_is_running(pid) is expected


def test_pid_is_running_handles_oserror_errno_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    class GenericKillError(OSError):
        pass

    def kill_esrch(target_pid: int, sig: int) -> None:
        _ = target_pid, sig
        exc = GenericKillError("gone")
        exc.errno = errno.ESRCH
        raise exc

    def kill_eperm(target_pid: int, sig: int) -> None:
        _ = target_pid, sig
        exc = GenericKillError("denied")
        exc.errno = errno.EPERM
        raise exc

    monkeypatch.setattr(process_lock_module.os, "kill", kill_esrch)
    assert process_lock_module._pid_is_running(12) is False

    monkeypatch.setattr(process_lock_module.os, "kill", kill_eperm)
    assert process_lock_module._pid_is_running(12) is True
