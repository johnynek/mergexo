from __future__ import annotations

from pathlib import Path
import json
import tempfile

from mergexo.config import CodexConfig
from mergexo.models import GeneratedDesign
from mergexo.shell import run


_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "summary", "touch_paths", "design_doc_markdown"],
    "properties": {
        "title": {"type": "string", "minLength": 1},
        "summary": {"type": "string", "minLength": 1},
        "touch_paths": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "design_doc_markdown": {"type": "string", "minLength": 1},
    },
}


class CodexAdapter:
    def __init__(self, config: CodexConfig) -> None:
        self._config = config

    def generate_design_doc(self, *, prompt: str, cwd: Path) -> GeneratedDesign:
        if not self._config.enabled:
            raise RuntimeError("Codex is disabled in config")

        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(json.dumps(_OUTPUT_SCHEMA), encoding="utf-8")

            cmd = [
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(output_path),
                "-",
            ]
            if self._config.model:
                cmd.extend(["--model", self._config.model])
            if self._config.sandbox:
                cmd.extend(["--sandbox", self._config.sandbox])
            if self._config.profile:
                cmd.extend(["--profile", self._config.profile])
            if self._config.extra_args:
                cmd.extend(self._config.extra_args)

            run(cmd, cwd=cwd, input_text=prompt)

            raw = output_path.read_text(encoding="utf-8").strip()
            payload = _parse_json_payload(raw)

        title = _require_str(payload, "title")
        summary = _require_str(payload, "summary")
        design_doc_markdown = _require_str(payload, "design_doc_markdown")
        touch_paths = _require_str_list(payload, "touch_paths")

        return GeneratedDesign(
            title=title,
            summary=summary,
            design_doc_markdown=design_doc_markdown,
            touch_paths=tuple(touch_paths),
        )


def _parse_json_payload(raw: str) -> dict[str, object]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise RuntimeError("Codex response must be a JSON object")
    return payload


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Codex response missing non-empty string field: {key}")
    return value


def _require_str_list(payload: dict[str, object], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"Codex response missing list field: {key}")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"Codex response has invalid {key} entry")
        out.append(item)
    if not out:
        raise RuntimeError(f"Codex response field {key} must be non-empty")
    return out
