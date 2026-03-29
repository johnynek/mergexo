from __future__ import annotations

import base64
import json
import logging
import re
from typing import cast

from mergexo.agent_adapter import (
    FeedbackTurn,
    PrePrReviewFlow,
    PrePrReviewResult,
    RoadmapChildDependencyArtifact,
    RoadmapChildDependencyArtifactPath,
    RoadmapChildDependencyHandoff,
    RoadmapDependencyArtifactPathRole,
    RoadmapDependencyArtifact,
    RoadmapFeedbackTurn,
)
from mergexo.models import (
    ROADMAP_NODE_KINDS,
    Issue,
    RoadmapDependencyRequirement,
    RoadmapNodeKind,
)


LOGGER = logging.getLogger("mergexo.prompts")
_ROADMAP_DEPENDENCY_HANDOFF_SCHEMA_VERSION = 1
_ROADMAP_DEPENDENCY_HANDOFF_PAYLOAD_MAX_CHARS = 16000
_ROADMAP_DEPENDENCY_HANDOFF_MARKER_PATTERN = re.compile(
    r"<!--\s*mergexo-roadmap-dependency-handoff:v(?P<version>\d+):"
    r"(?P<payload>[A-Za-z0-9_-]+)\s*-->"
)


def _coding_guidelines_task_lines(*, coding_guidelines_path: str | None) -> str:
    if coding_guidelines_path:
        return f"- Review and follow the coding/testing guidelines in: {coding_guidelines_path}"
    return (
        "- Coding/testing guidelines file is not present in this repo version.\n"
        "- Follow a style consistent with the existing codebase and target 100% test coverage."
    )


def _implementation_checks_line(*, coding_guidelines_path: str | None) -> str:
    if coding_guidelines_path:
        return f"  - Re-run formatting and CI-required checks from {coding_guidelines_path}."
    return "  - Re-run formatting and tests expected by this repo and target 100% test coverage."


def _roadmap_node_kinds_text() -> str:
    return ", ".join(f"`{kind}`" for kind in ROADMAP_NODE_KINDS)


def _truncate_prompt_text(value: str, *, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 15] + "... [truncated]"


def _roadmap_dependency_artifacts_json(
    artifacts: tuple[RoadmapDependencyArtifact, ...],
) -> str:
    payload = [
        {
            "dependency_node_id": artifact.dependency_node_id,
            "dependency_kind": artifact.dependency_kind,
            "dependency_title": artifact.dependency_title,
            "frontier_references": [
                {
                    "ready_node_id": reference.ready_node_id,
                    "requires": reference.requires,
                }
                for reference in artifact.frontier_references
            ],
            "child_issue_number": artifact.child_issue_number,
            "child_issue_url": artifact.child_issue_url,
            "child_issue_title": artifact.child_issue_title,
            "child_issue_body": (
                _truncate_prompt_text(artifact.child_issue_body, max_chars=1200)
                if artifact.child_issue_body is not None
                else None
            ),
            "issue_run_status": artifact.issue_run_status,
            "issue_run_branch": artifact.issue_run_branch,
            "issue_run_error": (
                _truncate_prompt_text(artifact.issue_run_error, max_chars=600)
                if artifact.issue_run_error is not None
                else None
            ),
            "resolution_markers": list(artifact.resolution_markers),
            "pr_number": artifact.pr_number,
            "pr_url": artifact.pr_url,
            "pr_title": artifact.pr_title,
            "pr_body": (
                _truncate_prompt_text(artifact.pr_body, max_chars=1600)
                if artifact.pr_body is not None
                else None
            ),
            "pr_state": artifact.pr_state,
            "pr_merged": artifact.pr_merged,
            "changed_files": list(artifact.changed_files),
            "review_summaries": [
                {
                    "user_login": comment.user_login,
                    "body": _truncate_prompt_text(comment.body, max_chars=600),
                    "html_url": comment.html_url,
                }
                for comment in artifact.review_summaries
            ],
            "issue_comments": [
                {
                    "user_login": comment.user_login,
                    "body": _truncate_prompt_text(comment.body, max_chars=600),
                    "html_url": comment.html_url,
                }
                for comment in artifact.issue_comments
            ],
        }
        for artifact in artifacts
    ]
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _normalize_roadmap_child_dependency_handoff(
    handoff: RoadmapChildDependencyHandoff,
    *,
    include_body_excerpts: bool,
    changed_files_limit: int,
    notes_limit: int,
    total_artifact_paths_limit: int,
    supporting_artifact_paths_limit: int | None,
) -> RoadmapChildDependencyHandoff:
    normalized_dependencies: list[RoadmapChildDependencyArtifact] = []
    for dependency in sorted(handoff.dependencies, key=lambda item: item.node_id):
        primary_paths = sorted(
            (path for path in dependency.artifact_paths if path.role == "primary"),
            key=lambda path: path.path,
        )
        supporting_paths = sorted(
            (path for path in dependency.artifact_paths if path.role != "primary"),
            key=lambda path: path.path,
        )
        if supporting_artifact_paths_limit is not None:
            supporting_paths = supporting_paths[:supporting_artifact_paths_limit]
        max_supporting_paths = max(0, total_artifact_paths_limit - len(primary_paths))
        supporting_paths = supporting_paths[:max_supporting_paths]
        normalized_dependencies.append(
            RoadmapChildDependencyArtifact(
                node_id=dependency.node_id,
                kind=dependency.kind,
                title=dependency.title,
                requires=dependency.requires,
                satisfied_by=dependency.satisfied_by,
                child_issue_number=dependency.child_issue_number,
                child_issue_url=dependency.child_issue_url,
                child_issue_title=dependency.child_issue_title,
                pr_number=dependency.pr_number,
                pr_url=dependency.pr_url,
                pr_title=dependency.pr_title,
                pr_merged=dependency.pr_merged,
                branch=dependency.branch,
                head_sha=dependency.head_sha,
                merge_commit_sha=dependency.merge_commit_sha,
                artifact_paths=tuple(primary_paths + supporting_paths),
                changed_files=tuple(sorted(dependency.changed_files)[:changed_files_limit]),
                review_notes=tuple(
                    _truncate_prompt_text(note, max_chars=400)
                    for note in dependency.review_notes[:notes_limit]
                ),
                issue_notes=tuple(
                    _truncate_prompt_text(note, max_chars=400)
                    for note in dependency.issue_notes[:notes_limit]
                ),
                child_issue_body_excerpt=(
                    _truncate_prompt_text(dependency.child_issue_body_excerpt, max_chars=800)
                    if include_body_excerpts and dependency.child_issue_body_excerpt is not None
                    else None
                ),
                pr_body_excerpt=(
                    _truncate_prompt_text(dependency.pr_body_excerpt, max_chars=800)
                    if include_body_excerpts and dependency.pr_body_excerpt is not None
                    else None
                ),
            )
        )
    return RoadmapChildDependencyHandoff(
        schema_version=handoff.schema_version,
        roadmap_issue_number=handoff.roadmap_issue_number,
        node_id=handoff.node_id,
        node_kind=handoff.node_kind,
        dependencies=tuple(normalized_dependencies),
    )


def _roadmap_child_dependency_handoff_payload(
    handoff: RoadmapChildDependencyHandoff,
) -> dict[str, object]:
    return {
        "schema_version": handoff.schema_version,
        "roadmap_issue_number": handoff.roadmap_issue_number,
        "node_id": handoff.node_id,
        "node_kind": handoff.node_kind,
        "dependencies": [
            {
                "node_id": dependency.node_id,
                "kind": dependency.kind,
                "title": dependency.title,
                "requires": dependency.requires,
                "satisfied_by": dependency.satisfied_by,
                "child_issue_number": dependency.child_issue_number,
                "child_issue_url": dependency.child_issue_url,
                "child_issue_title": dependency.child_issue_title,
                "pr_number": dependency.pr_number,
                "pr_url": dependency.pr_url,
                "pr_title": dependency.pr_title,
                "pr_merged": dependency.pr_merged,
                "branch": dependency.branch,
                "head_sha": dependency.head_sha,
                "merge_commit_sha": dependency.merge_commit_sha,
                "artifact_paths": [
                    {
                        "path": artifact_path.path,
                        "role": artifact_path.role,
                        "default_branch_url": artifact_path.default_branch_url,
                        "blob_url": artifact_path.blob_url,
                    }
                    for artifact_path in dependency.artifact_paths
                ],
                "changed_files": list(dependency.changed_files),
                "review_notes": list(dependency.review_notes),
                "issue_notes": list(dependency.issue_notes),
                "child_issue_body_excerpt": dependency.child_issue_body_excerpt,
                "pr_body_excerpt": dependency.pr_body_excerpt,
            }
            for dependency in handoff.dependencies
        ],
    }


def _serialize_roadmap_child_dependency_handoff(
    handoff: RoadmapChildDependencyHandoff,
) -> str:
    if handoff.schema_version != _ROADMAP_DEPENDENCY_HANDOFF_SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported roadmap dependency handoff schema version: {handoff.schema_version}"
        )
    phases = (
        _normalize_roadmap_child_dependency_handoff(
            handoff,
            include_body_excerpts=True,
            changed_files_limit=25,
            notes_limit=3,
            total_artifact_paths_limit=10,
            supporting_artifact_paths_limit=None,
        ),
        _normalize_roadmap_child_dependency_handoff(
            handoff,
            include_body_excerpts=False,
            changed_files_limit=25,
            notes_limit=3,
            total_artifact_paths_limit=10,
            supporting_artifact_paths_limit=None,
        ),
        _normalize_roadmap_child_dependency_handoff(
            handoff,
            include_body_excerpts=False,
            changed_files_limit=10,
            notes_limit=3,
            total_artifact_paths_limit=10,
            supporting_artifact_paths_limit=None,
        ),
        _normalize_roadmap_child_dependency_handoff(
            handoff,
            include_body_excerpts=False,
            changed_files_limit=10,
            notes_limit=1,
            total_artifact_paths_limit=10,
            supporting_artifact_paths_limit=None,
        ),
        _normalize_roadmap_child_dependency_handoff(
            handoff,
            include_body_excerpts=False,
            changed_files_limit=10,
            notes_limit=1,
            total_artifact_paths_limit=10,
            supporting_artifact_paths_limit=3,
        ),
    )
    for normalized in phases:
        raw_payload = json.dumps(
            _roadmap_child_dependency_handoff_payload(normalized),
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(raw_payload) <= _ROADMAP_DEPENDENCY_HANDOFF_PAYLOAD_MAX_CHARS:
            return raw_payload
    raise RuntimeError("roadmap dependency handoff payload exceeds size limit")


def render_roadmap_child_dependency_handoff_marker(
    handoff: RoadmapChildDependencyHandoff,
) -> str:
    raw_payload = _serialize_roadmap_child_dependency_handoff(handoff)
    encoded = base64.urlsafe_b64encode(raw_payload.encode("utf-8")).decode("ascii").rstrip("=")
    return f"<!-- mergexo-roadmap-dependency-handoff:v{handoff.schema_version}:{encoded} -->"


def _roadmap_child_dependency_handoff_from_payload(
    payload: object,
) -> RoadmapChildDependencyHandoff:
    if not isinstance(payload, dict):
        raise ValueError("roadmap dependency handoff payload must be an object")
    payload_obj = cast(dict[str, object], payload)
    schema_version = payload_obj.get("schema_version")
    roadmap_issue_number = payload_obj.get("roadmap_issue_number")
    node_id = payload_obj.get("node_id")
    node_kind = payload_obj.get("node_kind")
    dependencies_payload = payload_obj.get("dependencies")
    if schema_version != _ROADMAP_DEPENDENCY_HANDOFF_SCHEMA_VERSION:
        raise ValueError("unsupported roadmap dependency handoff schema version")
    if not isinstance(roadmap_issue_number, int):
        raise ValueError("roadmap_issue_number must be an integer")
    if not isinstance(node_id, str):
        raise ValueError("node_id must be a string")
    if not isinstance(node_kind, str):
        raise ValueError("node_kind must be a string")
    if not isinstance(dependencies_payload, list):
        raise ValueError("dependencies must be a list")

    dependencies: list[RoadmapChildDependencyArtifact] = []
    for dependency_payload in dependencies_payload:
        if not isinstance(dependency_payload, dict):
            raise ValueError("dependency entry must be an object")
        dependency_payload_obj = cast(dict[str, object], dependency_payload)
        artifact_paths_payload = dependency_payload_obj.get("artifact_paths")
        if not isinstance(artifact_paths_payload, list):
            raise ValueError("artifact_paths must be a list")
        artifact_paths: list[RoadmapChildDependencyArtifactPath] = []
        for artifact_path_payload in artifact_paths_payload:
            if not isinstance(artifact_path_payload, dict):
                raise ValueError("artifact path entry must be an object")
            artifact_path_payload_obj = cast(dict[str, object], artifact_path_payload)
            path = artifact_path_payload_obj.get("path")
            role = artifact_path_payload_obj.get("role")
            default_branch_url = artifact_path_payload_obj.get("default_branch_url")
            blob_url = artifact_path_payload_obj.get("blob_url")
            if not isinstance(path, str):
                raise ValueError("artifact path must be a string")
            if not isinstance(role, str):
                raise ValueError("artifact role must be a string")
            if not isinstance(default_branch_url, str):
                raise ValueError("default_branch_url must be a string")
            if blob_url is not None and not isinstance(blob_url, str):
                raise ValueError("blob_url must be a string or null")
            if role not in {"primary", "supporting"}:
                raise ValueError("artifact role must be primary or supporting")
            artifact_paths.append(
                RoadmapChildDependencyArtifactPath(
                    path=path,
                    role=cast(RoadmapDependencyArtifactPathRole, role),
                    default_branch_url=default_branch_url,
                    blob_url=blob_url,
                )
            )

        changed_files = dependency_payload_obj.get("changed_files")
        review_notes = dependency_payload_obj.get("review_notes")
        issue_notes = dependency_payload_obj.get("issue_notes")
        if not isinstance(changed_files, list) or not all(
            isinstance(item, str) for item in changed_files
        ):
            raise ValueError("changed_files must be a string list")
        if not isinstance(review_notes, list) or not all(
            isinstance(item, str) for item in review_notes
        ):
            raise ValueError("review_notes must be a string list")
        if not isinstance(issue_notes, list) or not all(
            isinstance(item, str) for item in issue_notes
        ):
            raise ValueError("issue_notes must be a string list")
        changed_files_list = cast(list[str], changed_files)
        review_notes_list = cast(list[str], review_notes)
        issue_notes_list = cast(list[str], issue_notes)

        def optional_str(key: str) -> str | None:
            value = dependency_payload_obj.get(key)
            if value is None or isinstance(value, str):
                return value
            raise ValueError(f"{key} must be a string or null")

        def optional_int(key: str) -> int | None:
            value = dependency_payload_obj.get(key)
            if value is None or isinstance(value, int):
                return value
            raise ValueError(f"{key} must be an integer or null")

        def optional_bool(key: str) -> bool | None:
            value = dependency_payload_obj.get(key)
            if value is None or isinstance(value, bool):
                return value
            raise ValueError(f"{key} must be a boolean or null")

        node_id_value = dependency_payload_obj.get("node_id")
        kind = dependency_payload_obj.get("kind")
        title = dependency_payload_obj.get("title")
        requires = dependency_payload_obj.get("requires")
        satisfied_by = dependency_payload_obj.get("satisfied_by")
        if not isinstance(node_id_value, str):
            raise ValueError("dependency node_id must be a string")
        if not isinstance(kind, str):
            raise ValueError("dependency kind must be a string")
        if not isinstance(title, str):
            raise ValueError("dependency title must be a string")
        if not isinstance(requires, str):
            raise ValueError("dependency requires must be a string")
        if not isinstance(satisfied_by, str):
            raise ValueError("dependency satisfied_by must be a string")
        if kind not in {"reference_doc", "design_doc", "small_job", "roadmap"}:
            raise ValueError("dependency kind is unsupported")
        if requires not in {"planned", "implemented"}:
            raise ValueError("dependency requires is unsupported")
        if satisfied_by not in {"planned", "implemented"}:
            raise ValueError("dependency satisfied_by is unsupported")
        dependencies.append(
            RoadmapChildDependencyArtifact(
                node_id=node_id_value,
                kind=cast(RoadmapNodeKind, kind),
                title=title,
                requires=cast(RoadmapDependencyRequirement, requires),
                satisfied_by=cast(RoadmapDependencyRequirement, satisfied_by),
                child_issue_number=optional_int("child_issue_number"),
                child_issue_url=optional_str("child_issue_url"),
                child_issue_title=optional_str("child_issue_title"),
                pr_number=optional_int("pr_number"),
                pr_url=optional_str("pr_url"),
                pr_title=optional_str("pr_title"),
                pr_merged=optional_bool("pr_merged"),
                branch=optional_str("branch"),
                head_sha=optional_str("head_sha"),
                merge_commit_sha=optional_str("merge_commit_sha"),
                artifact_paths=tuple(artifact_paths),
                changed_files=tuple(changed_files_list),
                review_notes=tuple(review_notes_list),
                issue_notes=tuple(issue_notes_list),
                child_issue_body_excerpt=optional_str("child_issue_body_excerpt"),
                pr_body_excerpt=optional_str("pr_body_excerpt"),
            )
        )

    return RoadmapChildDependencyHandoff(
        schema_version=cast(int, schema_version),
        roadmap_issue_number=roadmap_issue_number,
        node_id=node_id,
        node_kind=cast(RoadmapNodeKind, node_kind),
        dependencies=tuple(dependencies),
    )


def parse_roadmap_child_dependency_handoff_marker(
    issue_body: str,
) -> RoadmapChildDependencyHandoff | None:
    match = _ROADMAP_DEPENDENCY_HANDOFF_MARKER_PATTERN.search(issue_body)
    if match is None:
        return None
    if int(match.group("version")) != _ROADMAP_DEPENDENCY_HANDOFF_SCHEMA_VERSION:
        LOGGER.warning("unsupported_roadmap_dependency_handoff_marker_version")
        return None
    payload = match.group("payload")
    padded_payload = payload + ("=" * (-len(payload) % 4))
    try:
        raw_payload = base64.urlsafe_b64decode(padded_payload).decode("utf-8")
        parsed_payload = json.loads(raw_payload)
        return _roadmap_child_dependency_handoff_from_payload(parsed_payload)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        LOGGER.warning("failed_to_parse_roadmap_dependency_handoff_marker")
        return None


def _remove_roadmap_child_dependency_handoff_marker(issue_body: str) -> str:
    without_marker = _ROADMAP_DEPENDENCY_HANDOFF_MARKER_PATTERN.sub("", issue_body, count=1)
    without_marker = re.sub(r"\n{3,}", "\n\n", without_marker)
    return without_marker.strip()


def render_roadmap_child_dependency_handoff_summary(
    handoff: RoadmapChildDependencyHandoff,
) -> str:
    normalized = _normalize_roadmap_child_dependency_handoff(
        handoff,
        include_body_excerpts=False,
        changed_files_limit=5,
        notes_limit=2,
        total_artifact_paths_limit=10,
        supporting_artifact_paths_limit=3,
    )
    if not normalized.dependencies:
        return "Dependency handoff:\n- none"
    lines = [
        "Dependency handoff:",
        "- direct dependencies only; MergeXO resolved them ahead of time.",
    ]
    for dependency in normalized.dependencies:
        lines.append(
            f"- {dependency.node_id} [{dependency.kind}] "
            f"requires={dependency.requires} satisfied_by={dependency.satisfied_by}"
        )
        if dependency.child_issue_number is not None:
            lines.append(
                "  child issue: "
                f"#{dependency.child_issue_number}"
                + (
                    f" ({dependency.child_issue_url})"
                    if dependency.child_issue_url is not None
                    else ""
                )
            )
        if dependency.pr_number is not None:
            lines.append(
                "  PR: "
                f"#{dependency.pr_number}"
                + (f" ({dependency.pr_url})" if dependency.pr_url is not None else "")
            )
        primary_paths = [path.path for path in dependency.artifact_paths if path.role == "primary"]
        supporting_paths = [
            path.path for path in dependency.artifact_paths if path.role == "supporting"
        ]
        if primary_paths:
            lines.append(f"  primary artifact: {primary_paths[0]}")
        if supporting_paths:
            lines.append(f"  supporting artifacts: {', '.join(supporting_paths)}")
        if dependency.changed_files:
            lines.append(f"  changed files: {', '.join(dependency.changed_files)}")
        if dependency.review_notes:
            lines.append(f"  review notes: {' | '.join(dependency.review_notes)}")
        if dependency.issue_notes:
            lines.append(f"  issue notes: {' | '.join(dependency.issue_notes)}")
    return "\n".join(lines)


def _render_roadmap_child_dependency_handoff_prompt_section(
    handoff: RoadmapChildDependencyHandoff,
) -> str:
    normalized = _normalize_roadmap_child_dependency_handoff(
        handoff,
        include_body_excerpts=True,
        changed_files_limit=25,
        notes_limit=3,
        total_artifact_paths_limit=10,
        supporting_artifact_paths_limit=3,
    )
    lines = [
        "Dependency Handoff:",
        "- Direct dependencies only. MergeXO already resolved them; do not walk the roadmap graph or git history to discover more inputs.",
        f"- Roadmap issue: #{normalized.roadmap_issue_number}",
        f"- Current node: {normalized.node_id} [{normalized.node_kind}]",
    ]
    if not normalized.dependencies:
        lines.append("- Dependency artifacts: none")
        return "\n".join(lines)
    for dependency in normalized.dependencies:
        lines.append(
            f"- {dependency.node_id} [{dependency.kind}] "
            f"requires={dependency.requires} satisfied_by={dependency.satisfied_by}"
        )
        if dependency.child_issue_number is not None:
            lines.append(
                "  child issue: "
                f"#{dependency.child_issue_number}"
                + (
                    f" ({dependency.child_issue_url})"
                    if dependency.child_issue_url is not None
                    else ""
                )
            )
        if dependency.pr_number is not None:
            lines.append(
                "  satisfying PR: "
                f"#{dependency.pr_number}"
                + (f" ({dependency.pr_url})" if dependency.pr_url is not None else "")
            )
        if dependency.pr_title is not None:
            lines.append(f"  PR title: {dependency.pr_title}")
        if dependency.branch is not None or dependency.head_sha is not None:
            lines.append(
                "  git provenance: "
                f"branch={dependency.branch or '-'} "
                f"head_sha={dependency.head_sha or '-'} "
                f"merge_commit_sha={dependency.merge_commit_sha or '-'}"
            )
        for artifact_path in dependency.artifact_paths:
            label = "primary artifact" if artifact_path.role == "primary" else "supporting artifact"
            lines.append(
                f"  {label}: {artifact_path.path} "
                f"(default branch: {artifact_path.default_branch_url}; "
                f"pinned blob: {artifact_path.blob_url or 'unavailable'})"
            )
        if dependency.changed_files:
            lines.append(f"  changed files: {', '.join(dependency.changed_files)}")
        if dependency.review_notes:
            lines.append(f"  review notes: {' | '.join(dependency.review_notes)}")
        if dependency.issue_notes:
            lines.append(f"  issue notes: {' | '.join(dependency.issue_notes)}")
        if dependency.child_issue_body_excerpt is not None:
            lines.append(f"  child issue excerpt: {dependency.child_issue_body_excerpt}")
        if dependency.pr_body_excerpt is not None:
            lines.append(f"  PR body excerpt: {dependency.pr_body_excerpt}")
    return "\n".join(lines)


def _worker_issue_context_sections(issue_body: str) -> tuple[str, str]:
    handoff = parse_roadmap_child_dependency_handoff_marker(issue_body)
    if handoff is None:
        return "", issue_body
    return (
        _render_roadmap_child_dependency_handoff_prompt_section(handoff) + "\n\n",
        _remove_roadmap_child_dependency_handoff_marker(issue_body),
    )


def _direct_output_contract_lines() -> str:
    return """
Required output fields:
- Always include `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`.
- `pr_title`: non-empty string.
- `pr_summary`: non-empty string.
- `commit_message`: non-empty string or null. Use a string only when the repository changes are ready for MergeXO to commit and push.
- `blocked_reason`: non-empty string or null. Use a string only when you cannot proceed safely.
- `escalation`: null or an object with exactly `kind`, `summary`, and `details`.
- Use `escalation.kind = "roadmap_revision"` only when the current roadmap assumptions are fundamentally wrong; otherwise set `escalation` to null.
- Do not omit nullable keys. Use null when a field does not apply.
""".strip()


def _pre_pr_review_output_contract_lines() -> str:
    return """
Required output fields:
- Always include `outcome`, `summary`, `findings`, and `escalation_reason`.
- `outcome`: one of `approved`, `changes_requested`, `escalate`.
- `summary`: non-empty string.
- `findings`: array. Use `[]` when the candidate is approved.
- Each item in `findings` must be an object with exactly `finding_id`, `path`, `line`, `title`, and `details`.
- `finding_id`: non-empty string, unique within the response.
- `path`: non-empty repo-relative file path string.
- `line`: integer >= 1 or null.
- `title`: non-empty short actionable headline.
- `details`: non-empty explanation of the defect and expected repair.
- `escalation_reason`: non-empty string or null.
- `approved` requires `findings = []` and `escalation_reason = null`.
- `changes_requested` requires at least one finding and `escalation_reason = null`.
- `escalate` requires a non-null `escalation_reason`; `findings` may be empty or non-empty.
- Do not omit keys.
""".strip()


def _feedback_output_contract_lines(*, allow_escalation: bool) -> str:
    top_level_keys = (
        "`git_ops`, `review_replies`, `general_comment`, `commit_message`, "
        "`flaky_test_report`, and `escalation`"
        if allow_escalation
        else "`git_ops`, `review_replies`, `general_comment`, `commit_message`, and `flaky_test_report`"
    )
    escalation_lines = (
        "- `escalation`: null or an object with exactly `kind`, `summary`, and `details`.\n"
        '- Use `escalation.kind = "roadmap_revision"` only for foundational roadmap flaws; otherwise set `escalation` to null.\n'
        if allow_escalation
        else "- Do not include an `escalation` key for this task.\n"
    )
    return (
        "Required output fields:\n"
        f"- Always include {top_level_keys}.\n"
        "- `git_ops`: array. Use `[]` when no Git operation request is needed.\n"
        "- `review_replies`: array. Use `[]` when no line-level reply is needed.\n"
        "- `general_comment`: non-empty string or null.\n"
        "- `commit_message`: non-empty string or null.\n"
        "- `flaky_test_report`: null or an object with exactly `run_id`, `title`, `summary`, and `relevant_log_excerpt`.\n"
        f"{escalation_lines}"
        "- Do not omit nullable fields. Use null when a field does not apply.\n"
    ).strip()


def _roadmap_authoring_contract_text(*, concise: bool) -> str:
    core_lines = (
        "Roadmap dependency handoff contract:",
        "- Treat each dependency as both a sequencing constraint and a direct worker-input declaration.",
        "- A roadmap edge exists when the downstream worker must receive that upstream artifact, or rely directly on that upstream satisfied state, in its child issue body and worker prompt.",
        "- Downstream workers receive direct dependency artifacts and satisfied dependency states only, not the transitive closure of the roadmap graph.",
        "- The same direct dependency handoff appears in both the child issue body and the worker prompt.",
        "- For doc-like dependencies, MergeXO hands off exact artifact paths on the default branch plus stable GitHub and git provenance when available.",
        "- For state-oriented dependencies such as `small_job.planned` or `roadmap.implemented`, MergeXO tells the worker the exact satisfied state it may rely on directly together with the available provenance for that state.",
        "- Workers should not be expected to walk the roadmap graph, infer artifact locations from node ids, search the repository heuristically, or reconstruct intent from git history.",
        "- If a downstream worker needs another upstream artifact or satisfied upstream state, add an explicit direct dependency edge to that node.",
        "- Choose `planned` or `implemented` based on the earliest milestone at which MergeXO can truthfully hand the downstream worker the concrete direct artifact or satisfied state it needs.",
    )
    direct_edge_lines = (
        "Direct-edge reminders:",
        "- Ask what concrete upstream artifact or satisfied state the downstream worker must have on its first turn.",
        "- Add a direct edge to every upstream node whose artifact or satisfied state must appear in that worker handoff.",
        "- If a worker needs two upstream inputs, add two direct edges instead of relying on one dependency to transitively carry another node's artifact or satisfied state.",
        "- Do not rely on transitive dependencies, git-history reconstruction, or heuristic repo search to supply missing inputs.",
        "- Do not choose `planned` merely to preserve ordering if the needed artifact or satisfied state is not available yet.",
    )
    per_kind_lines = (
        "Per-kind handoff guidance:",
        "- `reference_doc`: downstream workers receive the merged doc artifact with its exact path and provenance. Usually use `planned` when the merged document itself is the needed input.",
        "- `design_doc`: `planned` hands off the merged design artifact path and design PR provenance; `implemented` waits for the shipped implementation outcome from that same child issue.",
        "- `small_job`: `planned` means the child issue and its issue text exist; merged code, changed files, and implementation behavior are not available until `implemented`.",
        "- `roadmap`: `planned` hands off the child roadmap markdown and graph artifact paths plus activation provenance; `implemented` means the child roadmap has completed.",
    )
    example_lines = (
        "Motivating example:",
        "- Treat `review_design` as a `reference_doc`.",
        "- `review_config` and `review_contract` should each depend directly on `review_design` with `requires = planned` so their workers receive the exact reviewed design artifact and constraints.",
        "- If a later node needs both the reviewed design artifact and merged output from `review_config`, add direct edges to both nodes instead of expecting `review_design` to arrive transitively through `review_config`.",
    )
    sections: tuple[tuple[str, ...], ...]
    if concise:
        sections = (core_lines, direct_edge_lines, example_lines)
    else:
        sections = (core_lines, direct_edge_lines, per_kind_lines, example_lines)
    return "\n".join("\n".join(section) for section in sections)


def _pre_pr_review_guidance_section(*, review_guidance: str | None) -> str:
    normalized = review_guidance.strip() if review_guidance is not None else ""
    if not normalized:
        return "Repo review guidance:\n- None provided by the caller."
    return f"Repo review guidance:\n{normalized}"


def _implementation_pre_pr_context_section(
    *,
    design_doc_path: str | None,
    design_doc_markdown: str | None,
    design_pr_number: int | None,
    design_pr_url: str | None,
) -> str:
    design_path = design_doc_path or "unavailable in local state"
    design_pr_line = (
        f"- Source design PR: #{design_pr_number} ({design_pr_url})"
        if design_pr_number is not None and design_pr_url
        else "- Source design PR: unavailable in local state"
    )
    design_markdown = design_doc_markdown if design_doc_markdown is not None else "Unavailable."
    return (
        "Implementation context:\n"
        f"- Design doc path on base branch: {design_path}\n"
        f"{design_pr_line}\n\n"
        f"Merged design doc markdown ({design_path}):\n"
        f"{design_markdown}"
    )


def _review_findings_json(review_result: PrePrReviewResult) -> str:
    return json.dumps(
        [
            {
                "finding_id": finding.finding_id,
                "path": finding.path,
                "line": finding.line,
                "title": finding.title,
                "details": finding.details,
            }
            for finding in review_result.findings
        ],
        indent=2,
    )


def build_design_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    design_doc_path: str,
    default_branch: str,
) -> str:
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    return f"""
You are the design-doc agent for repository {repo_full_name}.

Task:
- Create structured content for a design doc that addresses issue #{issue.number}.
- The resulting PR will add the file at: {design_doc_path}
- Base branch is: {default_branch}

Output requirements:
- Focus on architecture and implementation plan for the issue.
- Include clear acceptance criteria.
- Include risks and rollout notes.
- You MUST report likely implementation file paths in touch_paths.
- Keep touch_paths concrete and repository-relative.
- `title` and `summary` are rendered separately by the orchestrator.
- `design_doc_markdown` must contain only the body that comes after the auto-generated header.
- Do NOT include YAML frontmatter, the H1 title, the issue line, or a `## Summary` section in `design_doc_markdown`.
- Start `design_doc_markdown` at the first substantive section, for example `## Context`, `## Problem`, `## Goals`, or equivalent.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- Always include `title`, `summary`, `touch_paths`, and `design_doc_markdown`.
- `title`: non-empty string for the design PR title.
- `summary`: non-empty string for the design PR summary.
- `touch_paths`: non-empty array of likely implementation file paths.
- `design_doc_markdown`: non-empty string containing only the markdown body described above.
- Do not omit keys.

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}
""".strip()


def build_bugfix_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    return f"""
You are the bugfix agent for repository {repo_full_name}.

Task:
- Resolve issue #{issue.number} directly with code changes.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Reproduce the issue behavior from the report details.
- Add or update regression tests that fail before the fix and pass after the fix.

Output requirements:
- Implement the fix and any required supporting updates.
- Return PR metadata with a clear summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}
""".strip()


def build_small_job_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    return f"""
You are the small-job agent for repository {repo_full_name}.

Task:
- Implement issue #{issue.number} directly with focused code changes.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Keep scope tight to the requested job.

Output requirements:
- Implement only what is needed for the requested change.
- Return PR metadata with a concise summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}
""".strip()


def build_implementation_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
    design_doc_path: str,
    design_doc_markdown: str,
    design_pr_number: int | None,
    design_pr_url: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    implementation_checks_line = _implementation_checks_line(
        coding_guidelines_path=coding_guidelines_path
    )
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    design_pr_line = (
        f"- Source design PR: #{design_pr_number} ({design_pr_url})"
        if design_pr_number is not None and design_pr_url
        else "- Source design PR: unavailable in local state"
    )
    return f"""
You are the implementation agent for repository {repo_full_name}.

Task:
- Implement issue #{issue.number} by following the merged design doc.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Design doc path on base branch: {design_doc_path}
{design_pr_line}

Output requirements:
- Implement the accepted design in repository code and tests.
- Keep changes aligned to the design doc scope unless explicitly blocked by missing requirements.
- Return PR metadata with a clear summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details
- Before finalizing your output:
  - Re-read the full diff against {default_branch}.
  - Re-read the design doc and any PR comments provided in context.
  - Confirm all requested work is addressed.
  - Minimize duplication and remove confusing or unnecessary code.
  - Add concise comments around subtle logic to help future readers.
{implementation_checks_line}
  - Only finalize when those checks pass so MergeXO can commit and push confidently.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}

Merged design doc markdown ({design_doc_path}):
{design_doc_markdown}
""".strip()


def build_pre_pr_review_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    flow_name: PrePrReviewFlow,
    coding_guidelines_path: str | None,
    review_guidance: str | None,
    branch: str,
    head_sha: str,
    changed_files: tuple[str, ...],
    diff_text: str,
    design_doc_path: str | None,
    design_doc_markdown: str | None,
    design_pr_number: int | None,
    design_pr_url: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    changed_files_json = json.dumps(list(changed_files), indent=2)
    rendered_diff = _truncate_prompt_text(diff_text.strip() or "<empty diff>", max_chars=20000)
    implementation_context = (
        "\n\n"
        + _implementation_pre_pr_context_section(
            design_doc_path=design_doc_path,
            design_doc_markdown=design_doc_markdown,
            design_pr_number=design_pr_number,
            design_pr_url=design_pr_url,
        )
        if flow_name == "implementation"
        else ""
    )
    return f"""
You are the pre-PR reviewer for repository {repo_full_name}.

Task:
- Review the current local `{flow_name}` candidate for issue #{issue.number} before MergeXO opens a PR.
- Base branch is: {default_branch}
- Active flow: {flow_name}
- Candidate branch: {branch}
- Candidate head: {head_sha}
{coding_guidelines_lines}
- Review only approval-blocking problems in correctness, safety, missing tests, or contract violations.
- Ignore non-blocking style nits or optional cleanup.

Output requirements:
- Return a machine-consumable reviewer result for `approved`, `changes_requested`, or `escalate`.
- Only include findings that must be fixed before approval.
- Use `escalate` when the candidate needs human judgment or broader product/architecture direction.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_pre_pr_review_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}

Changed files:
{changed_files_json}

Current local diff against {default_branch} (truncated if needed):
```diff
{rendered_diff}
```

{_pre_pr_review_guidance_section(review_guidance=review_guidance)}{implementation_context}
""".strip()


def build_pre_pr_author_repair_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    flow_name: PrePrReviewFlow,
    coding_guidelines_path: str | None,
    review_guidance: str | None,
    branch: str,
    head_sha: str,
    review_result: PrePrReviewResult,
    design_doc_path: str | None,
    design_doc_markdown: str | None,
    design_pr_number: int | None,
    design_pr_url: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    implementation_context = (
        "\n\n"
        + _implementation_pre_pr_context_section(
            design_doc_path=design_doc_path,
            design_doc_markdown=design_doc_markdown,
            design_pr_number=design_pr_number,
            design_pr_url=design_pr_url,
        )
        if flow_name == "implementation"
        else ""
    )
    escalation_reason = review_result.escalation_reason or "null"
    return f"""
You are the pre-PR author-repair agent for repository {repo_full_name}.

Task:
- Resume the active `{flow_name}` author work for issue #{issue.number} and repair the reviewer findings before MergeXO opens a PR.
- Base branch is: {default_branch}
- Active flow: {flow_name}
- Current branch: {branch}
- Current head: {head_sha}
{coding_guidelines_lines}
- Treat the reviewer summary and findings below as the approval-blocking worklist.
- Keep the direct-turn output contract unchanged from the original author turn.

Output requirements:
- Implement the repairs needed to address the reviewer findings.
- Return the same direct-turn JSON contract used by the original author flow.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}

Reviewer outcome:
{review_result.outcome}

Reviewer summary:
{review_result.summary}

Reviewer escalation reason:
{escalation_reason}

Reviewer findings JSON:
{_review_findings_json(review_result)}

{_pre_pr_review_guidance_section(review_guidance=review_guidance)}{implementation_context}
""".strip()


def build_roadmap_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    roadmap_docs_dir: str,
    recommended_node_count: int,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    roadmap_contract_text = _roadmap_authoring_contract_text(concise=False)
    dependency_handoff_section, issue_body = _worker_issue_context_sections(issue.body)
    markdown_path = f"{roadmap_docs_dir}/{issue.number}-<slug>.md"
    graph_path = f"{roadmap_docs_dir}/{issue.number}-<slug>.graph.json"
    return f"""
You are the roadmap agent for repository {repo_full_name}.

Task:
- Author the initial roadmap PR for issue #{issue.number}.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Author the roadmap graph JSON.
- MergeXO will generate the review markdown from that graph before opening the PR.

Output requirements:
- This turn is for roadmap authoring, not repository code implementation.
- Produce a practical execution plan that decomposes the issue into child work MergeXO can issue incrementally.
- Each roadmap node should represent one concrete child issue with reviewable scope, a clear completion outcome, and text that can plausibly become that child issue's title/body.
- Keep the graph acyclic with internal `node_id` references only.
{roadmap_contract_text}
- Allowed node kinds: {_roadmap_node_kinds_text()}.
- Prefer:
  - `reference_doc` for reviewed durable docs that downstream nodes should consult without starting an implementation lane from that same child issue.
  - `design_doc` only when the roadmap intentionally wants the existing design-to-implementation lane from the same child issue.
  - `small_job` for bounded implementation work that should directly produce a PR.
  - `roadmap` only when a subtree is large enough to deserve its own nested roadmap.
- Dependency `requires` must be `planned` or `implemented`.
- Include an explicit `requires` on every dependency object.
- Include an explicit `depends_on` array on every node, using `[]` when there are no dependencies.
- Choose short, unique, stable `node_id` values that will still make sense if the roadmap is revised later.
- Recommended node count is around {recommended_node_count}; larger DAGs are allowed but should include decomposition notes.
- The generated markdown is a deterministic review view derived from `graph_json`.
- Use `python roadmap_json_to_md.py {graph_path}` locally if you want to preview that generated markdown before you return.
- Do not return roadmap markdown in this turn.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- Always include `title`, `summary`, and `graph_json`.
- `title`: non-empty string for the roadmap PR title.
- `summary`: non-empty string for the roadmap PR summary.
- `graph_json` as a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Set this to {issue.number}.
  - `version`: integer. For the initial roadmap, set this to `1`.
  - `nodes`: non-empty array of node objects.
- Each item in `graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string. Use a short stable internal identifier.
  - `kind`: one of {_roadmap_node_kinds_text()}.
  - `title`: non-empty string for the child issue title.
  - `body_markdown`: non-empty string for the child issue body.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- Do not add extra keys outside this schema.

Target file paths:
- generated roadmap markdown: {markdown_path}
- roadmap graph: {graph_path}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

{dependency_handoff_section}Issue body:
{issue_body}
""".strip()


def build_roadmap_adjustment_prompt(
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
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    roadmap_contract_text = _roadmap_authoring_contract_text(concise=False)
    ready_frontier_json = json.dumps(list(ready_node_ids), separators=(",", ":"))
    dependency_artifacts_json = _roadmap_dependency_artifacts_json(dependency_artifacts)
    return f"""
You are the roadmap-adjustment agent for repository {repo_full_name}.

Task:
- Evaluate whether roadmap issue #{issue.number} should proceed with its ready frontier or pause for a same-roadmap revision.
- Base branch is: {default_branch}
{coding_guidelines_lines}

Decision rules:
- Return `action = "proceed"` when the current roadmap still looks sound for the ready frontier.
- Return `action = "revise"` when the roadmap should change before issuing more child work.
- Return `action = "abandon"` only when continuing the roadmap is no longer viable.
- Prefer `proceed` unless the current roadmap is materially wrong.

{roadmap_contract_text}

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- `action`: one of `proceed`, `revise`, `abandon`
- `summary`: short rationale
- `details`: full rationale, referencing the ready frontier and the current roadmap state
- `updated_graph_json`: object or null
- Always include all four top-level keys. Do not omit nullable keys; use null when a field does not apply.

Payload rules:
- When `action = "revise"`, set `updated_graph_json`.
- When `action` is `proceed` or `abandon`, set `updated_graph_json` to null.
- `updated_graph_json` must be a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Keep this set to {issue.number}.
  - `version`: integer. Bump the graph version from {graph_version} to {graph_version + 1}.
  - `nodes`: non-empty array of node objects.
- Each item in `updated_graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string.
  - `kind`: one of {_roadmap_node_kinds_text()}.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- The revised graph must stay acyclic and use only internal `node_id` references.
- Allowed node kinds remain {_roadmap_node_kinds_text()}.
- Include an explicit `depends_on` array on every revised node, using `[]` when there are no dependencies.
- Include an explicit `requires` on every revised dependency object.
- The revised graph must keep `roadmap_issue_number = {issue.number}` and bump the graph `version` from {graph_version} to {graph_version + 1}.
- Treat retained `node_id`s as stable work identities.
- If you keep an existing `node_id`, preserve its `title`, `body_markdown`, and `depends_on` exactly.
- Preserve `kind` too, except you may change an unstarted node between `design_doc` and `reference_doc` when correcting artifact-only doc semantics.
- If pending work needs to change materially, retire the old pending node and add replacement work under a new `node_id`.
- Do not remove any node whose work has already started. Treat nodes with child issues, completed work, blocked work, abandoned work, or other in-flight progress as started.
- Do not churn `node_id`s for unchanged work. If the work is unchanged, keep the same `node_id`.
- MergeXO will derive `{roadmap_doc_path}` from the revised graph.
- Use `python roadmap_json_to_md.py {graph_path}` locally if you want to preview the generated markdown for the revised graph.

Current roadmap metadata:
- roadmap markdown path: {roadmap_doc_path}
- roadmap graph path: {graph_path}
- roadmap graph version: {graph_version}
- ready frontier node_ids: {ready_frontier_json}
- dependency artifacts JSON: {dependency_artifacts_json}

Roadmap issue title:
{issue.title}

Roadmap issue URL:
{issue.html_url}

Roadmap issue body:
{issue.body}

Current roadmap markdown ({roadmap_doc_path}):
{roadmap_markdown}

Current canonical roadmap graph JSON ({graph_path}):
{canonical_graph_json}

Current roadmap status report:
{roadmap_status_report}
""".strip()


def build_requested_roadmap_revision_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
    roadmap_doc_path: str,
    graph_path: str,
    graph_version: int,
    request_reason: str,
    roadmap_status_report: str,
    roadmap_markdown: str,
    canonical_graph_json: str,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    roadmap_contract_text = _roadmap_authoring_contract_text(concise=False)
    return f"""
You are the roadmap-revision agent for repository {repo_full_name}.

Task:
- A same-roadmap revision has been explicitly requested for roadmap issue #{issue.number}.
- Base branch is: {default_branch}
{coding_guidelines_lines}

Decision rules:
- Return `action = "revise"` when you can author a revised roadmap safely.
- Return `action = "abandon"` only when the roadmap should be abandoned instead of revised.
- Do not return `action = "proceed"` for this task.

{roadmap_contract_text}

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- `action`: one of `revise`, `abandon`
- `summary`: short rationale
- `details`: full rationale for the requested revision
- `updated_graph_json`: object or null
- Always include all four top-level keys. Do not omit nullable keys; use null when a field does not apply.

Payload rules:
- When `action = "revise"`, set `updated_graph_json`.
- When `action = "abandon"`, set `updated_graph_json` to null.
- `updated_graph_json` must be a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Keep this set to {issue.number}.
  - `version`: integer. Bump the graph version from {graph_version} to {graph_version + 1}.
  - `nodes`: non-empty array of node objects.
- Each item in `updated_graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string.
  - `kind`: one of {_roadmap_node_kinds_text()}.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- The revised graph must stay acyclic and use only internal `node_id` references.
- Allowed node kinds remain {_roadmap_node_kinds_text()}.
- Include an explicit `depends_on` array on every revised node, using `[]` when there are no dependencies.
- Include an explicit `requires` on every revised dependency object.
- The revised graph must keep `roadmap_issue_number = {issue.number}` and bump the graph `version` from {graph_version} to {graph_version + 1}.
- Treat retained `node_id`s as stable work identities.
- If you keep an existing `node_id`, preserve its `title`, `body_markdown`, and `depends_on` exactly.
- Preserve `kind` too, except you may change an unstarted node between `design_doc` and `reference_doc` when correcting artifact-only doc semantics.
- If pending work needs to change materially, retire the old pending node and add replacement work under a new `node_id`.
- Do not remove any node whose work has already started. Treat nodes with child issues, completed work, blocked work, abandoned work, or other in-flight progress as started.
- Do not churn `node_id`s for unchanged work. If the work is unchanged, keep the same `node_id`.
- MergeXO will derive `{roadmap_doc_path}` from the revised graph.
- Use `python roadmap_json_to_md.py {graph_path}` locally if you want to preview the generated markdown for the revised graph.

Revision request reason:
{request_reason}

Current roadmap metadata:
- roadmap markdown path: {roadmap_doc_path}
- roadmap graph path: {graph_path}
- roadmap graph version: {graph_version}

Roadmap issue title:
{issue.title}

Roadmap issue URL:
{issue.html_url}

Roadmap issue body:
{issue.body}

Current roadmap markdown ({roadmap_doc_path}):
{roadmap_markdown}

Current canonical roadmap graph JSON ({graph_path}):
{canonical_graph_json}

Current roadmap status report:
{roadmap_status_report}
""".strip()


def build_feedback_prompt(*, turn: FeedbackTurn) -> str:
    review_comments = [
        {
            "review_comment_id": comment.comment_id,
            "path": comment.path,
            "line": comment.line,
            "side": comment.side,
            "in_reply_to_id": comment.in_reply_to_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.review_comments
    ]
    issue_comments = [
        {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.issue_comments
    ]
    changed_files = (
        "\n".join(f"- {path}" for path in turn.changed_files) or "- (no changed files reported)"
    )

    return f"""
You are the PR-feedback agent for repository issue #{turn.issue.number}.

Pull request:
- number: {turn.pull_request.number}
- title: {turn.pull_request.title}
- head_sha: {turn.pull_request.head_sha}
- base_sha: {turn.pull_request.base_sha}
- turn_key: {turn.turn_key}

Changed files:
{changed_files}

Review comments (line-level):
{json.dumps(review_comments, indent=2)}

Issue comments on the PR:
{json.dumps(issue_comments, indent=2)}

Return JSON only with this object shape:
{{
  "git_ops": [
    {{"op": "fetch_origin"}},
    {{"op": "merge_origin_default_branch"}}
  ],
  "review_replies": [
    {{"review_comment_id": 123, "body": "reply body"}}
  ],
  "general_comment": "optional summary comment for the PR",
  "commit_message": "optional commit message if code changes are needed",
  "escalation": {{
    "kind": "roadmap_revision",
    "summary": "short escalation summary",
    "details": "full escalation details with impacted assumptions"
  }},
  "flaky_test_report": {{
    "run_id": 123456789,
    "title": "meaningful flaky test issue title",
    "summary": "why this failure appears unrelated to current PR and how to reproduce",
    "relevant_log_excerpt": "direct CI log excerpt supporting flaky classification"
  }}
}}

{_feedback_output_contract_lines(allow_escalation=True)}

Rules:
- Primary objective: resolve review feedback by editing repository files, then provide commit_message.
- If CI failure context from GitHub Actions is present, reproduce those failures locally before finalizing.
- Repair code/tests until local required pre-push checks pass before returning commit_message.
- A non-null commit_message is the ready-to-push signal.
- If blocked, set commit_message to null and explain the blocker concretely in general_comment.
- For comments on design docs (for example `docs/design/*.md`), prefer updating the doc directly over explanatory discussion-only replies.
- If a comment can be addressed by a concrete file change, make that change in this turn.
- Reply to specific review comments using their exact review_comment_id.
- Do not invent IDs.
- Use review_replies to summarize what changed and where.
- review_replies may only target IDs listed under "Review comments (line-level)".
- Never put PR issue-thread comment IDs into review_replies.
- Synthetic MergeXO reminders may appear in issue_comments with negative IDs; never use those in review_replies.
- Use general_comment for PR issue-thread responses when a thread-level reply is needed.
- Set flaky_test_report only when the failure is an unrelated flaky CI test and you are confident.
- flaky_test_report must include run_id/title/summary/relevant_log_excerpt with concrete reproduction details.
- Keep the total JSON response compact and under 32000 characters.
- If you have more detail than that, prioritize actionable facts and summarize the rest.
- If flaky_test_report is non-null, commit_message MUST be null.
- If uncertain whether it is flaky, leave flaky_test_report as null and continue normal remediation.
- If you discover a foundational roadmap flaw, set escalation with kind=roadmap_revision.
- Allowed git_ops are exactly: `fetch_origin`, `merge_origin_default_branch`.
- If you need MergeXO to run one of those git operations (for example because of sandbox git metadata limits), request it via git_ops and set commit_message to null for that response.
- When git_ops are requested, do not post proposal-only review replies yet; wait for the follow-up turn with operation results and then implement/finalize.
- Never rewrite git history. Do not run: `git rebase`, `git commit --amend`, `git reset`, `git push --force`, `git push --force-with-lease`.
- Keep history append-only: only add new commits on top of the current PR head.
- If you think rewrite is necessary, do not request commit_message; explain the constraint in general_comment so a human can decide.
- If you provide commit_message, you MUST have edited repository files for this turn.
- Only skip file edits when blocked by genuine ambiguity or missing requirements; in that case, set commit_message to null and ask a precise clarifying question in the review reply.
- Do not claim you pushed or updated files unless you actually edited them in this turn.
- Do not post "proposed fix" text when you can implement the change directly.
- Use null or empty values only when no action is needed or genuinely blocked.
""".strip()


def build_roadmap_feedback_prompt(*, turn: RoadmapFeedbackTurn) -> str:
    review_comments = [
        {
            "review_comment_id": comment.comment_id,
            "path": comment.path,
            "line": comment.line,
            "side": comment.side,
            "in_reply_to_id": comment.in_reply_to_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.review_comments
    ]
    issue_comments = [
        {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.issue_comments
    ]
    changed_files = (
        "\n".join(f"- {path}" for path in turn.changed_files) or "- (no changed files reported)"
    )
    markdown_status = (
        "missing in the current checkout" if turn.roadmap_markdown_missing else "present"
    )
    if turn.graph_missing:
        graph_status = "missing in the current checkout"
    elif turn.graph_validation_error is None:
        graph_status = f"valid JSON graph for issue #{turn.issue.number}" + (
            f" at version {turn.graph_version}" if turn.graph_version is not None else ""
        )
    else:
        graph_status = f"invalid: {turn.graph_validation_error}"
    graph_version_line = (
        f"- Current graph version in this PR: `{turn.graph_version}`. Preserve that version unless a review comment explicitly requires correcting it."
        if turn.graph_version is not None
        else "- Current graph version in this PR could not be validated. Fix the graph and preserve the intended PR version if it is evident from review context."
    )
    roadmap_contract_text = _roadmap_authoring_contract_text(concise=True)

    return f"""
You are the roadmap-feedback agent for roadmap issue #{turn.issue.number}.

Pull request:
- number: {turn.pull_request.number}
- title: {turn.pull_request.title}
- head_sha: {turn.pull_request.head_sha}
- base_sha: {turn.pull_request.base_sha}
- turn_key: {turn.turn_key}

Roadmap artifacts under review:
- roadmap markdown path: {turn.roadmap_doc_path}
- roadmap graph path: {turn.graph_path}
- roadmap markdown status: {markdown_status}
- roadmap graph status: {graph_status}

Changed files:
{changed_files}

Review comments (line-level):
{json.dumps(review_comments, indent=2)}

Issue comments on the PR:
{json.dumps(issue_comments, indent=2)}

Current roadmap markdown ({turn.roadmap_doc_path}):
{turn.roadmap_markdown}

Current roadmap graph text ({turn.graph_path}):
{turn.graph_json_text}

Return JSON only with this object shape:
{{
  "git_ops": [
    {{"op": "fetch_origin"}},
    {{"op": "merge_origin_default_branch"}}
  ],
  "review_replies": [
    {{"review_comment_id": 123, "body": "reply body"}}
  ],
  "general_comment": "optional summary comment for the PR",
  "commit_message": "optional commit message if file changes are ready",
  "flaky_test_report": {{
    "run_id": 123456789,
    "title": "meaningful flaky test issue title",
    "summary": "why this failure appears unrelated to current PR and how to reproduce",
    "relevant_log_excerpt": "direct CI log excerpt supporting flaky classification"
  }}
}}

{_feedback_output_contract_lines(allow_escalation=False)}

Roadmap review rules:
- Primary objective: resolve roadmap review feedback by editing `{turn.graph_path}` in this PR, then provide commit_message.
- `{turn.roadmap_doc_path}` is generated from `{turn.graph_path}`.
- Do not hand-edit `{turn.roadmap_doc_path}`.
- If you want to preview the generated markdown locally, run `python roadmap_json_to_md.py {turn.graph_path}` or `python roadmap_json_to_md.py {turn.graph_path} --output {turn.roadmap_doc_path}`.
- MergeXO will regenerate `{turn.roadmap_doc_path}` before validation and push.
- Do not return `escalation` for this task. This PR itself is the place to fix roadmap review feedback.
- Prefer concrete file edits over explanatory-only replies when a roadmap comment can be addressed directly.
- Before finalizing, re-read the roadmap graph and the generated markdown view even if the comment mentions only one artifact.
{roadmap_contract_text}

Roadmap markdown expectations:
- The markdown file is a generated review view for `{turn.graph_path}`.
- Humans should edit the graph file and regenerate the markdown, not hand-maintain two sources of truth.

Roadmap graph contract:
- The graph file must be valid JSON so MergeXO can regenerate the markdown review view.
- `roadmap_issue_number` must be the integer issue number `{turn.issue.number}`.
{graph_version_line}
- `nodes` must be a non-empty array of node objects.
- Each node object must contain exactly:
  - `node_id`: non-empty string.
  - `kind`: one of {_roadmap_node_kinds_text()}.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array, using `[]` when there are no dependencies.
- Each dependency object in `depends_on` must contain exactly:
  - `node_id`: string referencing another node in the same graph.
  - `requires`: one of `planned`, `implemented`.
- Keep the graph acyclic and use only internal `node_id` references.
- Do not add extra keys outside this schema.

Review workflow rules:
- Reply to specific review comments using their exact review_comment_id.
- Do not invent IDs.
- Use review_replies to summarize what changed and where.
- review_replies may only target IDs listed under "Review comments (line-level)".
- Never put PR issue-thread comment IDs into review_replies.
- Synthetic MergeXO reminders may appear in issue_comments with negative IDs; never use those in review_replies.
- Use general_comment for PR issue-thread responses when a thread-level reply is needed.
- If CI failure context from GitHub Actions is present, reproduce those failures locally before finalizing.
- Repair the roadmap graph and any required tests until local required pre-push checks pass before returning commit_message.
- A non-null commit_message is the ready-to-push signal.
- If blocked, set commit_message to null and explain the blocker concretely in general_comment.
- Set flaky_test_report only when the failure is an unrelated flaky CI test and you are confident.
- flaky_test_report must include run_id/title/summary/relevant_log_excerpt with concrete reproduction details.
- If flaky_test_report is non-null, commit_message MUST be null.
- Allowed git_ops are exactly: `fetch_origin`, `merge_origin_default_branch`.
- If you need MergeXO to run one of those git operations, request it via git_ops and set commit_message to null for that response.
- When git_ops are requested, do not post proposal-only review replies yet; wait for the follow-up turn with operation results and then implement/finalize.
- Never rewrite git history. Do not run: `git rebase`, `git commit --amend`, `git reset`, `git push --force`, `git push --force-with-lease`.
- Keep history append-only: only add new commits on top of the current PR head.
- If you provide commit_message, you MUST have edited repository files for this turn.
- Do not claim you updated the roadmap graph unless you actually edited it in this turn.
- Use null or empty values only when no action is needed or genuinely blocked.
""".strip()
