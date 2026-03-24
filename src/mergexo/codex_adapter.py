from __future__ import annotations

from pathlib import Path
import json
import logging
import tempfile
import time
from typing import cast

from mergexo.agent_adapter import (
    AgentAdapter,
    AgentSession,
    DirectStartResult,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
    RoadmapAdjustmentAction,
    RoadmapDependencyArtifact,
    RoadmapAdjustmentResult,
    RoadmapStartResult,
    ReviewReply,
)
from mergexo.config import CodexConfig
from mergexo.models import (
    FlakyTestReport,
    GeneratedDesign,
    GeneratedRoadmap,
    Issue,
    RoadmapRevisionEscalation,
)
from mergexo.observability import log_event
from mergexo.prompts import (
    build_bugfix_prompt,
    build_design_prompt,
    build_feedback_prompt,
    build_implementation_prompt,
    build_roadmap_adjustment_prompt,
    build_roadmap_prompt,
    build_small_job_prompt,
)
from mergexo.roadmap_parser import parse_roadmap_graph_object
from mergexo.shell import CommandError, run


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

_DIRECT_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["pr_title", "pr_summary", "commit_message", "blocked_reason"],
    "properties": {
        "pr_title": {"type": "string", "minLength": 1},
        "pr_summary": {"type": "string", "minLength": 1},
        "commit_message": {"type": ["string", "null"]},
        "blocked_reason": {"type": ["string", "null"]},
        "escalation": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["kind", "summary", "details"],
                    "properties": {
                        "kind": {"type": "string", "enum": ["roadmap_revision"]},
                        "summary": {"type": "string", "minLength": 1},
                        "details": {"type": "string", "minLength": 1},
                    },
                },
            ],
        },
    },
}

_ROADMAP_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "summary", "roadmap_markdown", "graph_json"],
    "properties": {
        "title": {"type": "string", "minLength": 1},
        "summary": {"type": "string", "minLength": 1},
        "roadmap_markdown": {"type": "string", "minLength": 1},
        "graph_json": {"type": "object"},
    },
}

_ROADMAP_ADJUSTMENT_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "action",
        "summary",
        "details",
        "updated_roadmap_markdown",
        "updated_graph_json",
    ],
    "properties": {
        "action": {
            "type": "string",
            "enum": ["proceed", "revise", "abandon"],
        },
        "summary": {"type": "string", "minLength": 1},
        "details": {"type": "string", "minLength": 1},
        "updated_roadmap_markdown": {"type": ["string", "null"]},
        "updated_graph_json": {"type": ["object", "null"]},
    },
}

_FEEDBACK_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "review_replies": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["review_comment_id", "body"],
                "properties": {
                    "review_comment_id": {"type": "integer"},
                    "body": {"type": "string", "minLength": 1},
                },
            },
        },
        "general_comment": {"type": ["string", "null"]},
        "commit_message": {"type": ["string", "null"]},
        "git_ops": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["op"],
                "properties": {
                    "op": {
                        "type": "string",
                        "enum": ["fetch_origin", "merge_origin_default_branch"],
                    }
                },
            },
        },
        "flaky_test_report": {
            "type": ["object", "null"],
            "additionalProperties": False,
            "required": ["run_id", "title", "summary", "relevant_log_excerpt"],
            "properties": {
                "run_id": {"type": "integer", "minimum": 1},
                "title": {"type": "string", "minLength": 1},
                "summary": {"type": "string", "minLength": 1},
                "relevant_log_excerpt": {"type": "string", "minLength": 1},
            },
        },
        "escalation": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["kind", "summary", "details"],
                    "properties": {
                        "kind": {"type": "string", "enum": ["roadmap_revision"]},
                        "summary": {"type": "string", "minLength": 1},
                        "details": {"type": "string", "minLength": 1},
                    },
                },
            ],
        },
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
        started_at = time.monotonic()
        log_event(
            LOGGER,
            "codex_invocation_started",
            mode="writing_doc",
            repo_full_name=repo_full_name,
            issue_number=issue.number,
        )
        log_event(
            LOGGER,
            "design_turn_started",
            issue_number=issue.number,
            design_doc_path=design_doc_path,
        )
        try:
            prompt = build_design_prompt(
                issue=issue,
                repo_full_name=repo_full_name,
                design_doc_path=design_doc_path,
                default_branch=default_branch,
            )
            design, thread_id = self._run_design_turn(prompt=prompt, cwd=cwd)
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "codex_invocation_finished",
                mode="writing_doc",
                repo_full_name=repo_full_name,
                issue_number=issue.number,
                status="fault",
                duration_seconds=_elapsed_seconds(started_at),
                error_type=type(exc).__name__,
            )
            raise
        log_event(
            LOGGER,
            "design_turn_completed",
            issue_number=issue.number,
            thread_id=thread_id or "<none>",
            touch_path_count=len(design.touch_paths),
        )
        log_event(
            LOGGER,
            "codex_invocation_finished",
            mode="writing_doc",
            repo_full_name=repo_full_name,
            issue_number=issue.number,
            status="success",
            duration_seconds=_elapsed_seconds(started_at),
        )
        return DesignStartResult(
            design=design, session=AgentSession(adapter="codex", thread_id=thread_id)
        )

    def start_bugfix_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        prompt = build_bugfix_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            default_branch=default_branch,
            coding_guidelines_path=coding_guidelines_path,
        )
        return self._start_direct_turn(
            issue=issue,
            repo_full_name=repo_full_name,
            flow_name="bugfix",
            prompt=prompt,
            cwd=cwd,
        )

    def start_roadmap_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        roadmap_docs_dir: str,
        recommended_node_count: int,
        cwd: Path,
    ) -> RoadmapStartResult:
        started_at = time.monotonic()
        log_event(
            LOGGER,
            "codex_invocation_started",
            mode="roadmap",
            repo_full_name=repo_full_name,
            issue_number=issue.number,
        )
        try:
            prompt = build_roadmap_prompt(
                issue=issue,
                repo_full_name=repo_full_name,
                default_branch=default_branch,
                roadmap_docs_dir=roadmap_docs_dir,
                recommended_node_count=recommended_node_count,
                coding_guidelines_path=None,
            )
            roadmap, thread_id = self._run_roadmap_turn(
                prompt=prompt,
                expected_issue_number=issue.number,
                cwd=cwd,
            )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "codex_invocation_finished",
                mode="roadmap",
                repo_full_name=repo_full_name,
                issue_number=issue.number,
                status="fault",
                duration_seconds=_elapsed_seconds(started_at),
                error_type=type(exc).__name__,
            )
            raise
        log_event(
            LOGGER,
            "codex_invocation_finished",
            mode="roadmap",
            repo_full_name=repo_full_name,
            issue_number=issue.number,
            status="success",
            duration_seconds=_elapsed_seconds(started_at),
        )
        return RoadmapStartResult(
            roadmap=roadmap,
            session=AgentSession(adapter="codex", thread_id=thread_id),
        )

    def start_small_job_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        prompt = build_small_job_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            default_branch=default_branch,
            coding_guidelines_path=coding_guidelines_path,
        )
        return self._start_direct_turn(
            issue=issue,
            repo_full_name=repo_full_name,
            flow_name="small_job",
            prompt=prompt,
            cwd=cwd,
        )

    def evaluate_roadmap_adjustment(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        roadmap_doc_path: str,
        graph_path: str,
        graph_version: int,
        ready_node_ids: tuple[str, ...],
        dependency_artifacts: tuple[RoadmapDependencyArtifact, ...],
        roadmap_status_report: str,
        roadmap_markdown: str,
        canonical_graph_json: str,
        cwd: Path,
    ) -> RoadmapAdjustmentResult:
        prompt = build_roadmap_adjustment_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            default_branch=default_branch,
            coding_guidelines_path=coding_guidelines_path,
            roadmap_doc_path=roadmap_doc_path,
            graph_path=graph_path,
            graph_version=graph_version,
            ready_node_ids=ready_node_ids,
            dependency_artifacts=dependency_artifacts,
            roadmap_status_report=roadmap_status_report,
            roadmap_markdown=roadmap_markdown,
            canonical_graph_json=canonical_graph_json,
        )
        return self._run_roadmap_adjustment_turn(
            prompt=prompt,
            cwd=cwd,
            expected_issue_number=issue.number,
            current_graph_version=graph_version,
        )

    def start_implementation_from_design(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        design_doc_path: str,
        design_doc_markdown: str,
        design_pr_number: int | None,
        design_pr_url: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        prompt = build_implementation_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            default_branch=default_branch,
            coding_guidelines_path=coding_guidelines_path,
            design_doc_path=design_doc_path,
            design_doc_markdown=design_doc_markdown,
            design_pr_number=design_pr_number,
            design_pr_url=design_pr_url,
        )
        return self._start_direct_turn(
            issue=issue,
            repo_full_name=repo_full_name,
            flow_name="implementation",
            prompt=prompt,
            cwd=cwd,
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

        started_at = time.monotonic()
        log_event(
            LOGGER,
            "codex_invocation_started",
            mode="respond_to_review",
            issue_number=turn.issue.number,
            pr_number=turn.pull_request.number,
        )
        log_event(
            LOGGER,
            "feedback_agent_call_started",
            issue_number=turn.issue.number,
            pr_number=turn.pull_request.number,
            thread_id=session.thread_id,
            turn_key=turn.turn_key,
        )
        try:
            prompt = build_feedback_prompt(turn=turn)
            try:
                payload, resumed_thread_id = self._run_feedback_turn_resume(
                    session=session,
                    prompt=prompt,
                    cwd=cwd,
                )
            except CommandError as exc:
                if not _is_context_window_exhaustion_error(exc):
                    raise
                log_event(
                    LOGGER,
                    "feedback_agent_call_restart_fresh_thread",
                    issue_number=turn.issue.number,
                    pr_number=turn.pull_request.number,
                    thread_id=session.thread_id,
                )
                payload, resumed_thread_id = self._run_feedback_turn_fresh(
                    prompt=prompt,
                    cwd=cwd,
                )
                resumed_thread_id = resumed_thread_id or session.thread_id

            replies = _parse_review_replies(payload.get("review_replies"))
            general_comment = _optional_output_text(payload.get("general_comment"))
            commit_message = _optional_output_text(payload.get("commit_message"))
            git_ops = _parse_git_ops(payload.get("git_ops"))
            flaky_test_report = _parse_flaky_test_report(payload.get("flaky_test_report"))
            escalation = _parse_roadmap_revision_escalation(payload.get("escalation"))
            if flaky_test_report is not None and commit_message is not None:
                raise RuntimeError(
                    "Codex response cannot set commit_message when flaky_test_report is present"
                )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "codex_invocation_finished",
                mode="respond_to_review",
                issue_number=turn.issue.number,
                pr_number=turn.pull_request.number,
                status="fault",
                duration_seconds=_elapsed_seconds(started_at),
                error_type=type(exc).__name__,
            )
            raise

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
        log_event(
            LOGGER,
            "codex_invocation_finished",
            mode="respond_to_review",
            issue_number=turn.issue.number,
            pr_number=turn.pull_request.number,
            status="success",
            duration_seconds=_elapsed_seconds(started_at),
        )
        return FeedbackResult(
            session=AgentSession(adapter="codex", thread_id=resumed_thread_id),
            review_replies=tuple(replies),
            general_comment=general_comment,
            commit_message=commit_message,
            git_ops=tuple(git_ops),
            flaky_test_report=flaky_test_report,
            escalation=escalation,
        )

    def _run_feedback_turn_resume(
        self,
        *,
        session: AgentSession,
        prompt: str,
        cwd: Path,
    ) -> tuple[dict[str, object], str | None]:
        if session.thread_id is None:
            raise RuntimeError("Cannot resume Codex feedback turn without a thread_id")

        cmd = [
            "codex",
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
        ]
        self._append_resume_options(cmd)
        cmd.extend([session.thread_id, "-"])

        raw_events = run(cmd, cwd=cwd, input_text=prompt)
        message = _extract_final_agent_message(raw_events)
        payload = _parse_json_payload(message)
        return payload, _extract_thread_id(raw_events) or session.thread_id

    def _run_feedback_turn_fresh(
        self,
        *,
        prompt: str,
        cwd: Path,
    ) -> tuple[dict[str, object], str | None]:
        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(json.dumps(_FEEDBACK_OUTPUT_SCHEMA), encoding="utf-8")

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

        return payload, _extract_thread_id(raw_events)

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
            ]
            self._append_common_options(cmd)
            cmd.append("-")

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

    def _run_roadmap_turn(
        self, *, prompt: str, expected_issue_number: int, cwd: Path
    ) -> tuple[GeneratedRoadmap, str | None]:
        if not self._config.enabled:
            raise RuntimeError("Codex is disabled in config")

        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(json.dumps(_ROADMAP_OUTPUT_SCHEMA), encoding="utf-8")

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
        roadmap_markdown = _require_str(payload, "roadmap_markdown")
        graph_obj = payload.get("graph_json")
        if graph_obj is None:
            raise RuntimeError("Codex response missing required graph_json object")
        parsed_graph = parse_roadmap_graph_object(
            graph_obj, expected_issue_number=expected_issue_number
        )
        return (
            GeneratedRoadmap(
                title=title,
                summary=summary,
                roadmap_markdown=roadmap_markdown,
                roadmap_issue_number=parsed_graph.graph.roadmap_issue_number,
                version=parsed_graph.graph.version,
                graph_nodes=parsed_graph.graph.nodes,
                canonical_graph_json=parsed_graph.canonical_json,
            ),
            _extract_thread_id(raw_events),
        )

    def _start_direct_turn(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        flow_name: str,
        prompt: str,
        cwd: Path,
    ) -> DirectStartResult:
        started_at = time.monotonic()
        invocation_mode = _direct_invocation_mode(flow_name)
        log_event(
            LOGGER,
            "codex_invocation_started",
            mode=invocation_mode,
            repo_full_name=repo_full_name,
            issue_number=issue.number,
        )
        log_event(
            LOGGER,
            "direct_turn_started",
            issue_number=issue.number,
            flow=flow_name,
        )
        try:
            result, thread_id = self._run_direct_turn(prompt=prompt, cwd=cwd)
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "codex_invocation_finished",
                mode=invocation_mode,
                repo_full_name=repo_full_name,
                issue_number=issue.number,
                status="fault",
                duration_seconds=_elapsed_seconds(started_at),
                error_type=type(exc).__name__,
            )
            raise
        log_event(
            LOGGER,
            "direct_turn_completed",
            issue_number=issue.number,
            flow=flow_name,
            thread_id=thread_id or "<none>",
            blocked=result.blocked_reason is not None,
        )
        log_event(
            LOGGER,
            "codex_invocation_finished",
            mode=invocation_mode,
            repo_full_name=repo_full_name,
            issue_number=issue.number,
            status="success",
            duration_seconds=_elapsed_seconds(started_at),
        )
        return DirectStartResult(
            pr_title=result.pr_title,
            pr_summary=result.pr_summary,
            commit_message=result.commit_message,
            blocked_reason=result.blocked_reason,
            session=AgentSession(adapter="codex", thread_id=thread_id),
        )

    def _run_direct_turn(self, *, prompt: str, cwd: Path) -> tuple[DirectStartResult, str | None]:
        if not self._config.enabled:
            raise RuntimeError("Codex is disabled in config")

        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(json.dumps(_DIRECT_OUTPUT_SCHEMA), encoding="utf-8")

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

        pr_title = _require_str(payload, "pr_title")
        pr_summary = _require_str(payload, "pr_summary")
        commit_message = _optional_output_text(payload.get("commit_message"))
        blocked_reason = _optional_output_text(payload.get("blocked_reason"))
        escalation = _parse_roadmap_revision_escalation(payload.get("escalation"))

        return (
            DirectStartResult(
                pr_title=pr_title,
                pr_summary=pr_summary,
                commit_message=commit_message,
                blocked_reason=blocked_reason,
                session=None,
                escalation=escalation,
            ),
            _extract_thread_id(raw_events),
        )

    def _run_roadmap_adjustment_turn(
        self,
        *,
        prompt: str,
        cwd: Path,
        expected_issue_number: int,
        current_graph_version: int,
    ) -> RoadmapAdjustmentResult:
        if not self._config.enabled:
            raise RuntimeError("Codex is disabled in config")

        with tempfile.TemporaryDirectory(prefix="mergexo_codex_") as tmp:
            tmp_path = Path(tmp)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.txt"
            schema_path.write_text(
                json.dumps(_ROADMAP_ADJUSTMENT_OUTPUT_SCHEMA),
                encoding="utf-8",
            )

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
            run(cmd, cwd=cwd, input_text=prompt)

            raw = output_path.read_text(encoding="utf-8").strip()
            payload = _parse_json_payload(raw)

        action = _require_str(payload, "action")
        if action not in {"proceed", "revise", "abandon"}:
            raise RuntimeError("action must be one of: proceed, revise, abandon")
        updated_roadmap_markdown = _optional_output_text(payload.get("updated_roadmap_markdown"))
        updated_graph_json_payload = payload.get("updated_graph_json")
        updated_canonical_graph_json: str | None = None
        if action == "revise":
            if updated_roadmap_markdown is None:
                raise RuntimeError("revise action requires non-null updated_roadmap_markdown")
            updated_graph_json = _as_object_dict(updated_graph_json_payload)
            if updated_graph_json is None:
                raise RuntimeError("revise action requires non-null updated_graph_json object")
            parsed_graph = parse_roadmap_graph_object(
                updated_graph_json, expected_issue_number=expected_issue_number
            )
            expected_version = current_graph_version + 1
            if parsed_graph.graph.version != expected_version:
                raise RuntimeError(
                    "revise action must bump roadmap graph version by exactly 1: "
                    f"expected {expected_version}, got {parsed_graph.graph.version}"
                )
            updated_canonical_graph_json = parsed_graph.canonical_json
        else:
            if updated_roadmap_markdown is not None or updated_graph_json_payload is not None:
                raise RuntimeError(
                    "proceed and abandon actions must set updated roadmap payload fields to null"
                )
        return RoadmapAdjustmentResult(
            action=cast(RoadmapAdjustmentAction, action),
            summary=_require_str(payload, "summary"),
            details=_require_str(payload, "details"),
            updated_roadmap_markdown=updated_roadmap_markdown,
            updated_canonical_graph_json=updated_canonical_graph_json,
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

    def _append_resume_options(self, cmd: list[str]) -> None:
        # `codex exec resume` does not accept `--sandbox` or `--profile`.
        # Keep model/extra args, while stripping known unsupported options.
        if self._config.model:
            cmd.extend(["--model", self._config.model])
        if self._config.extra_args:
            cmd.extend(_filter_resume_extra_args(self._config.extra_args))


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


def _parse_git_ops(value: object) -> list[GitOpRequest]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError("Codex response field git_ops must be a list")

    requests: list[GitOpRequest] = []
    for item in value:
        item_obj = _as_object_dict(item)
        if item_obj is None:
            raise RuntimeError("Each git_ops entry must be an object")
        op = item_obj.get("op")
        if op == "fetch_origin":
            requests.append(GitOpRequest(op="fetch_origin"))
            continue
        if op == "merge_origin_default_branch":
            requests.append(GitOpRequest(op="merge_origin_default_branch"))
            continue
        raise RuntimeError("git_ops op must be one of: fetch_origin, merge_origin_default_branch")
    return requests


def _parse_flaky_test_report(value: object) -> FlakyTestReport | None:
    if value is None:
        return None
    payload = _as_object_dict(value)
    if payload is None:
        raise RuntimeError("flaky_test_report must be an object or null")

    run_id = payload.get("run_id")
    title = payload.get("title")
    summary = payload.get("summary")
    relevant_log_excerpt = payload.get("relevant_log_excerpt")

    if not isinstance(run_id, int):
        raise RuntimeError("flaky_test_report.run_id must be an integer")
    if run_id < 1:
        raise RuntimeError("flaky_test_report.run_id must be >= 1")
    if not isinstance(title, str) or not title.strip():
        raise RuntimeError("flaky_test_report.title must be a non-empty string")
    if not isinstance(summary, str) or not summary.strip():
        raise RuntimeError("flaky_test_report.summary must be a non-empty string")
    if not isinstance(relevant_log_excerpt, str) or not relevant_log_excerpt.strip():
        raise RuntimeError("flaky_test_report.relevant_log_excerpt must be a non-empty string")

    return FlakyTestReport(
        run_id=run_id,
        title=title.strip(),
        summary=summary.strip(),
        relevant_log_excerpt=relevant_log_excerpt.strip(),
    )


def _parse_roadmap_revision_escalation(value: object) -> RoadmapRevisionEscalation | None:
    if value is None:
        return None
    payload = _as_object_dict(value)
    if payload is None:
        raise RuntimeError("escalation must be an object or null")
    kind = payload.get("kind")
    summary = payload.get("summary")
    details = payload.get("details")
    if kind != "roadmap_revision":
        raise RuntimeError("escalation.kind must be roadmap_revision")
    if not isinstance(summary, str) or not summary.strip():
        raise RuntimeError("escalation.summary must be a non-empty string")
    if not isinstance(details, str) or not details.strip():
        raise RuntimeError("escalation.details must be a non-empty string")
    return RoadmapRevisionEscalation(
        kind="roadmap_revision",
        summary=summary.strip(),
        details=details.strip(),
    )


def _as_object_dict(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    if not all(isinstance(key, str) for key in value.keys()):
        return None
    return cast(dict[str, object], value)


def _filter_resume_extra_args(extra_args: tuple[str, ...]) -> list[str]:
    filtered: list[str] = []
    idx = 0
    while idx < len(extra_args):
        arg = extra_args[idx]
        if arg in {"--sandbox", "--profile", "-s", "-p"}:
            idx += 2
            continue
        if (
            arg.startswith("--sandbox=")
            or arg.startswith("--profile=")
            or arg.startswith("-s=")
            or arg.startswith("-p=")
        ):
            idx += 1
            continue
        filtered.append(arg)
        idx += 1
    return filtered


def _is_context_window_exhaustion_error(exc: Exception) -> bool:
    if not isinstance(exc, CommandError):
        return False
    normalized = str(exc).lower()
    return "context window" in normalized and (
        "start a new thread" in normalized or "ran out of room" in normalized
    )


def _direct_invocation_mode(flow_name: str) -> str:
    if flow_name == "small_job":
        return "small-job"
    return flow_name


def _elapsed_seconds(started_at: float) -> float:
    elapsed = time.monotonic() - started_at
    return round(elapsed, 3)
