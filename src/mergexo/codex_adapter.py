from __future__ import annotations

from pathlib import Path
import json
import logging
import tempfile
from typing import cast

from mergexo.agent_adapter import (
    AgentAdapter,
    AgentSession,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    ReviewReply,
)
from mergexo.config import CodexConfig
from mergexo.models import GeneratedDesign, Issue
from mergexo.observability import log_event
from mergexo.prompts import build_design_prompt, build_feedback_prompt
from mergexo.shell import run


_DESIGN_OUTPUT_SCHEMA: dict[str, object] = {
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


LOGGER = logging.getLogger("mergexo.codex_adapter")


class CodexAdapter(AgentAdapter):
    def __init__(self, config: CodexConfig) -> None:
        self._config = config

    def start_design_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        design_doc_path: str,
        default_branch: str,
        cwd: Path,
    ) -> DesignStartResult:
        log_event(
            LOGGER,
            "design_turn_started",
            issue_number=issue.number,
            design_doc_path=design_doc_path,
        )
        prompt = build_design_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            design_doc_path=design_doc_path,
            default_branch=default_branch,
        )
        design, thread_id = self._run_design_turn(prompt=prompt, cwd=cwd)
        log_event(
            LOGGER,
            "design_turn_completed",
            issue_number=issue.number,
            thread_id=thread_id or "<none>",
            touch_path_count=len(design.touch_paths),
        )
        return DesignStartResult(
            design=design, session=AgentSession(adapter="codex", thread_id=thread_id)
        )

    def respond_to_feedback(
        self,
        *,
        session: AgentSession,
        turn: FeedbackTurn,
        cwd: Path,
    ) -> FeedbackResult:
        if session.thread_id is None:
            raise RuntimeError("Cannot resume Codex feedback turn without a thread_id")

        log_event(
            LOGGER,
            "feedback_agent_call_started",
            issue_number=turn.issue.number,
            pr_number=turn.pull_request.number,
            thread_id=session.thread_id,
            turn_key=turn.turn_key,
        )
        prompt = build_feedback_prompt(turn=turn)
        cmd = [
            "codex",
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
            session.thread_id,
            "-",
        ]
        self._append_common_options(cmd)

        raw_events = run(cmd, cwd=cwd, input_text=prompt)
        message = _extract_final_agent_message(raw_events)
        payload = _parse_json_payload(message)

        replies = _parse_review_replies(payload.get("review_replies"))
        general_comment = _optional_output_text(payload.get("general_comment"))
        commit_message = _optional_output_text(payload.get("commit_message"))

        resumed_thread_id = _extract_thread_id(raw_events) or session.thread_id

        log_event(
            LOGGER,
            "feedback_agent_call_completed",
            issue_number=turn.issue.number,
            pr_number=turn.pull_request.number,
            thread_id=resumed_thread_id,
            review_reply_count=len(replies),
            has_general_comment=general_comment is not None,
            has_commit_message=commit_message is not None,
        )
        return FeedbackResult(
            session=AgentSession(adapter="codex", thread_id=resumed_thread_id),
            review_replies=tuple(replies),
            general_comment=general_comment,
            commit_message=commit_message,
        )

    def _run_design_turn(self, *, prompt: str, cwd: Path) -> tuple[GeneratedDesign, str | None]:
        if not self._config.enabled:
            raise RuntimeError("Codex is disabled in config")

        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(json.dumps(_DESIGN_OUTPUT_SCHEMA), encoding="utf-8")

            cmd = [
                "codex",
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(output_path),
                "-",
            ]
            self._append_common_options(cmd)

            raw_events = run(cmd, cwd=cwd, input_text=prompt)

            raw = output_path.read_text(encoding="utf-8").strip()
            payload = _parse_json_payload(raw)

        title = _require_str(payload, "title")
        summary = _require_str(payload, "summary")
        design_doc_markdown = _require_str(payload, "design_doc_markdown")
        touch_paths = _require_str_list(payload, "touch_paths")

        return (
            GeneratedDesign(
                title=title,
                summary=summary,
                design_doc_markdown=design_doc_markdown,
                touch_paths=tuple(touch_paths),
            ),
            _extract_thread_id(raw_events),
        )

    def _append_common_options(self, cmd: list[str]) -> None:
        if self._config.model:
            cmd.extend(["--model", self._config.model])
        if self._config.sandbox:
            cmd.extend(["--sandbox", self._config.sandbox])
        if self._config.profile:
            cmd.extend(["--profile", self._config.profile])
        if self._config.extra_args:
            cmd.extend(self._config.extra_args)


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


def _extract_thread_id(raw_events: str) -> str | None:
    for line in raw_events.splitlines():
        text = line.strip()
        if not text:
            continue
        payload = _parse_event_line(text)
        if payload is None:
            continue
        if payload.get("type") != "thread.started":
            continue
        thread_id = payload.get("thread_id")
        if isinstance(thread_id, str) and thread_id:
            return thread_id
    return None


def _extract_final_agent_message(raw_events: str) -> str:
    last_message: str | None = None
    for line in raw_events.splitlines():
        text = line.strip()
        if not text:
            continue
        payload = _parse_event_line(text)
        if payload is None:
            continue
        if payload.get("type") != "item.completed":
            continue
        item_obj = _as_object_dict(payload.get("item"))
        if item_obj is None:
            continue
        item_type = item_obj.get("type")
        message_text = item_obj.get("text")
        if item_type == "agent_message" and isinstance(message_text, str):
            last_message = message_text
    if not last_message:
        raise RuntimeError("Codex resume did not emit a final agent message")
    return last_message


def _parse_event_line(line: str) -> dict[str, object] | None:
    if not line.startswith("{"):
        return None
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
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


def _optional_output_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError("Codex response field must be a string or null")
    normalized = value.strip()
    return normalized or None


def _parse_review_replies(value: object) -> list[ReviewReply]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError("Codex response field review_replies must be a list")

    replies: list[ReviewReply] = []
    for item in value:
        item_obj = _as_object_dict(item)
        if item_obj is None:
            raise RuntimeError("Each review_replies entry must be an object")
        comment_id = item_obj.get("review_comment_id")
        body = item_obj.get("body")
        if not isinstance(comment_id, int):
            raise RuntimeError("review_comment_id must be an integer")
        if not isinstance(body, str) or not body.strip():
            raise RuntimeError("review_replies body must be a non-empty string")
        replies.append(ReviewReply(review_comment_id=comment_id, body=body))
    return replies


def _as_object_dict(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    if not all(isinstance(key, str) for key in value.keys()):
        return None
    return cast(dict[str, object], value)
