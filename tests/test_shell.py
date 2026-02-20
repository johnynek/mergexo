from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from mergexo.shell import CommandError, run


def test_run_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called: dict[str, object] = {}

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        called["args"] = args
        called["kwargs"] = kwargs
        return subprocess.CompletedProcess(args=["echo"], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    out = run(["echo", "hello"], cwd=tmp_path, input_text="hi")

    assert out == "ok"
    kwargs = called["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["cwd"] == str(tmp_path)
    assert kwargs["input"] == "hi"
    assert kwargs["check"] is False


def test_run_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        _ = args, kwargs
        return subprocess.CompletedProcess(args=["bad"], returncode=2, stdout="out", stderr="err")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(CommandError, match="Command failed"):
        run(["bad"])
