from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from mergexo.observability import configure_logging
from mergexo.shell import CommandError, _preview, run


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


def test_run_failure_raises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        _ = args, kwargs
        return subprocess.CompletedProcess(args=["bad"], returncode=2, stdout="out", stderr="err")

    monkeypatch.setattr(subprocess, "run", fake_run)
    configure_logging(verbose=True)

    with pytest.raises(CommandError, match="Command failed"):
        run(["bad"])
    stderr = capsys.readouterr().err
    assert "event=command_failed command=bad exit_code=2" in stderr
    assert "stderr=err" in stderr
    assert "stdout=out" in stderr


def test_preview_handles_empty_and_truncation() -> None:
    assert _preview("") == "<empty>"
    assert _preview("x" * 10, limit=4) == "xxxx..."
