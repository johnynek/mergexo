from __future__ import annotations

from pathlib import Path
import json

import pytest

from mergexo.codex_adapter import (
    CodexAdapter,
    _parse_json_payload,
    _require_str,
    _require_str_list,
)
from mergexo.config import CodexConfig


def _enabled_config() -> CodexConfig:
    return CodexConfig(
        enabled=True,
        model="gpt-5-codex",
        sandbox="workspace-write",
        profile="default",
        extra_args=("--full-auto",),
    )


def test_generate_design_doc_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, cwd: Path | None = None, input_text: str | None = None, check: bool = True) -> str:
        assert cwd == tmp_path
        assert check is True
        assert input_text == "prompt"
        calls.append(cmd)

        idx = cmd.index("--output-last-message")
        output_path = Path(cmd[idx + 1])
        output_path.write_text(
            json.dumps(
                {
                    "title": "Design",
                    "summary": "Summary",
                    "touch_paths": ["src/a.py", "src/b.py"],
                    "design_doc_markdown": "## Plan\n\nDo work",
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    generated = adapter.generate_design_doc(prompt="prompt", cwd=tmp_path)

    assert generated.title == "Design"
    assert generated.summary == "Summary"
    assert generated.touch_paths == ("src/a.py", "src/b.py")
    cmd = calls[0]
    assert "--model" in cmd
    assert "--sandbox" in cmd
    assert "--profile" in cmd
    assert "--full-auto" in cmd


def test_generate_design_doc_rejects_disabled() -> None:
    adapter = CodexAdapter(
        CodexConfig(enabled=False, model=None, sandbox=None, profile=None, extra_args=())
    )
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.generate_design_doc(prompt="p", cwd=Path("."))


def test_parse_json_payload_variants_and_errors() -> None:
    fenced = "```json\n{\"title\":\"x\"}\n```"
    assert _parse_json_payload(fenced) == {"title": "x"}

    with pytest.raises(RuntimeError, match="JSON object"):
        _parse_json_payload("[]")


def test_require_str_and_list_errors() -> None:
    assert _require_str({"k": "v"}, "k") == "v"
    with pytest.raises(RuntimeError, match="missing non-empty string"):
        _require_str({"k": ""}, "k")

    assert _require_str_list({"k": ["a", "b"]}, "k") == ["a", "b"]
    with pytest.raises(RuntimeError, match="missing list"):
        _require_str_list({"k": "bad"}, "k")
    with pytest.raises(RuntimeError, match="invalid"):
        _require_str_list({"k": ["ok", ""]}, "k")
    with pytest.raises(RuntimeError, match="must be non-empty"):
        _require_str_list({"k": []}, "k")
