from __future__ import annotations

from pathlib import Path

import pytest

from mergexo.config import RepoConfig, RuntimeConfig
from mergexo.git_ops import GitRepoManager
from mergexo.observability import configure_logging


def _config(tmp_path: Path, *, worker_count: int = 2) -> tuple[RuntimeConfig, RepoConfig]:
    runtime = RuntimeConfig(
        base_dir=tmp_path / "state",
        worker_count=worker_count,
        poll_interval_seconds=60,
        enable_feedback_loop=False,
    )
    repo = RepoConfig(
        owner="johnynek",
        name="mergexo",
        default_branch="main",
        trigger_label="agent:design",
        design_docs_dir="docs/design",
        local_clone_source=None,
        remote_url=None,
    )
    return runtime, repo


def test_ensure_layout_initializes_mirror_and_slots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=2)
    manager = GitRepoManager(runtime, repo)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        calls.append(cmd)
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    manager.ensure_layout()

    assert manager.layout.mirror_path.parent.exists()
    assert manager.layout.checkouts_root.exists()
    assert any(cmd[:3] == ["git", "clone", "--mirror"] for cmd in calls)
    clone_calls = [cmd for cmd in calls if cmd[:2] == ["git", "clone"] and "--mirror" not in cmd]
    assert len(clone_calls) == 2


def test_ensure_checkout_existing_slot_sets_remote(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)
    slot_path = manager.slot_path(0)
    slot_path.mkdir(parents=True)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        calls.append(cmd)
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    out = manager.ensure_checkout(0)

    assert out == slot_path
    assert calls == [
        ["git", "-C", str(slot_path), "remote", "set-url", "origin", repo.effective_remote_url]
    ]


def test_prepare_branch_push_and_cleanup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        calls.append(cmd)
        if cmd[-3:] == ["diff", "--cached", "--name-only"]:
            return "file.py\n"
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    checkout = tmp_path / "checkout"
    manager.prepare_checkout(checkout)
    manager.create_or_reset_branch(checkout, "feature")
    manager.commit_all(checkout, "msg")
    manager.push_branch(checkout, "feature")
    manager.cleanup_slot(checkout)

    assert ["git", "-C", str(checkout), "checkout", "-B", "feature"] in calls
    assert ["git", "-C", str(checkout), "push", "-u", "origin", "feature"] in calls


def test_git_ops_emits_action_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        if cmd[-3:] == ["diff", "--cached", "--name-only"]:
            return "file.py\n"
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return "goodsha\n"
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)
    configure_logging(verbose=True)

    checkout = tmp_path / "checkout"
    manager.ensure_checkout(0)
    manager.prepare_checkout(checkout)
    manager.create_or_reset_branch(checkout, "feature")
    manager.commit_all(checkout, "msg")
    manager.push_branch(checkout, "feature")
    manager.restore_feedback_branch(checkout, "feature", "goodsha")

    text = capsys.readouterr().err
    assert "event=git_prepare_checkout" in text
    assert "event=git_branch_reset" in text
    assert "event=git_commit" in text
    assert "event=git_push" in text
    assert "event=git_restore_feedback_branch" in text


def test_restore_feedback_branch_retries_once_on_head_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)

    checkout = tmp_path / "checkout"
    seen_rev_parse = 0

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        nonlocal seen_rev_parse
        _ = kwargs
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            seen_rev_parse += 1
            return "badsha\n" if seen_rev_parse == 1 else "goodsha\n"
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    assert manager.restore_feedback_branch(checkout, "feature", "goodsha") is True
    assert seen_rev_parse == 2


def test_restore_feedback_branch_returns_true_on_first_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)
    checkout = tmp_path / "checkout"

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return "goodsha\n"
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)
    assert manager.restore_feedback_branch(checkout, "feature", "goodsha") is True


def test_commit_all_raises_when_no_diff(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        if cmd[-3:] == ["diff", "--cached", "--name-only"]:
            return ""
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    with pytest.raises(RuntimeError, match="No staged changes"):
        manager.commit_all(tmp_path, "msg")


def test_ensure_mirror_skips_clone_if_existing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)
    manager.layout.mirror_path.mkdir(parents=True)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        calls.append(cmd)
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    manager._ensure_mirror()

    assert not any(cmd[:3] == ["git", "clone", "--mirror"] for cmd in calls)
    assert any(cmd[2:4] == ["remote", "set-url"] for cmd in calls)
    assert any(cmd[2:4] == ["fetch", "origin"] for cmd in calls)


def test_fetch_origin_and_merge_default_branch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime, repo = _config(tmp_path, worker_count=1)
    manager = GitRepoManager(runtime, repo)
    checkout = tmp_path / "checkout"

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> str:
        _ = kwargs
        calls.append(cmd)
        return ""

    monkeypatch.setattr("mergexo.git_ops.run", fake_run)

    manager.fetch_origin(checkout)
    manager.merge_origin_default_branch(checkout)

    assert calls == [
        ["git", "-C", str(checkout), "fetch", "origin", "--prune", "--tags"],
        ["git", "-C", str(checkout), "merge", "--no-edit", "origin/main"],
    ]
