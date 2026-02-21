from __future__ import annotations

import runpy

import pytest


def test_main_module_invokes_cli_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"count": 0}

    def fake_main() -> None:
        called["count"] += 1

    monkeypatch.setattr("mergexo.cli.main", fake_main)

    runpy.run_module("mergexo.__main__", run_name="__main__")

    assert called["count"] == 1
