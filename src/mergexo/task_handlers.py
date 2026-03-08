from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import json
import os
import re
from typing import cast

from mergexo.config import RepoConfig
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.models import (
    Issue,
    ReleaseSnapshot,
    RepositoryTagSnapshot,
    TriggeredTaskExecutionResult,
    TriggeredTaskKind,
    TriggeredTaskRequest,
    WorkflowRunSnapshot,
)
from mergexo.prompts import build_triggered_task_review_prompt


_DEFAULT_RELEASE_REQUEST_REGEX = r"(?i)^\s*release\s*:?\s*(?P<tag>\S+)\s*$"
_SAFE_RELEASE_TAG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_UNSAFE_RELEASE_TAG_FRAGMENTS = ("..", "~", "^", ":", "?", "*", "[", "\\", "@{")


@dataclass(frozen=True)
class TriggeredTaskSnapshot:
    schema_version: int
    task_kind: TriggeredTaskKind
    payload: dict[str, object]

    def to_json(self) -> str:
        return json.dumps(self.payload, indent=2, sort_keys=True)


class TriggeredTaskHandler(ABC):
    @property
    @abstractmethod
    def task_kind(self) -> TriggeredTaskKind:
        """Triggered task kind handled by this implementation."""

    @abstractmethod
    def parse_request(self, issue: Issue) -> TriggeredTaskRequest | None:
        """Return a parsed task request for this issue, or None when unsupported."""

    @abstractmethod
    def collect_snapshot(
        self,
        *,
        request: TriggeredTaskRequest,
        github: GitHubGateway,
        default_branch: str,
    ) -> TriggeredTaskSnapshot:
        """Collect deterministic, reviewable state for this task request."""

    @abstractmethod
    def build_review_prompt(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        request: TriggeredTaskRequest,
        snapshot: TriggeredTaskSnapshot,
    ) -> str:
        """Build the task review prompt passed to the agent adapter."""

    @abstractmethod
    def execute(
        self,
        *,
        request: TriggeredTaskRequest,
        reviewed_snapshot: TriggeredTaskSnapshot,
        github: GitHubGateway,
        git: GitRepoManager,
        checkout_path: Path,
        default_branch: str,
    ) -> TriggeredTaskExecutionResult:
        """Execute an approved task with deterministic invariant checks."""


class ReleaseTaskHandler(TriggeredTaskHandler):
    def __init__(
        self,
        *,
        repo: RepoConfig,
        tag_list_limit: int = 20,
        workflow_run_limit: int = 20,
        output_tail_lines: int = 40,
    ) -> None:
        self._repo = repo
        self._tag_list_limit = max(1, tag_list_limit)
        self._workflow_run_limit = max(1, workflow_run_limit)
        self._output_tail_lines = max(1, output_tail_lines)
        self._request_pattern = re.compile(
            repo.release_request_regex or _DEFAULT_RELEASE_REQUEST_REGEX
        )

    @property
    def task_kind(self) -> TriggeredTaskKind:
        return "release"

    def parse_request(self, issue: Issue) -> TriggeredTaskRequest | None:
        candidate_lines: list[str] = [issue.title]
        for raw_line in issue.body.splitlines():
            stripped = raw_line.strip()
            if stripped:
                candidate_lines.append(stripped)
                break
        for source_text in candidate_lines:
            match = self._request_pattern.match(source_text.strip())
            if match is None:
                continue
            tag_text = _extract_tag_from_match(match)
            if tag_text is None:
                continue
            normalized_tag = _normalize_release_tag(tag_text)
            if normalized_tag is None:
                continue
            return TriggeredTaskRequest(
                task_kind="release",
                resource_key=normalized_tag,
                requested_tag=normalized_tag,
                source_text=source_text,
            )
        return None

    def collect_snapshot(
        self,
        *,
        request: TriggeredTaskRequest,
        github: GitHubGateway,
        default_branch: str,
    ) -> TriggeredTaskSnapshot:
        head_sha = github.get_default_branch_head_sha(default_branch)
        requested_tag_sha = github.get_tag_sha(request.requested_tag)
        recent_tags = github.list_repository_tags(limit=self._tag_list_limit)
        latest_release = github.get_latest_release()
        workflow_runs = github.list_workflow_runs_for_branch_head(
            default_branch=default_branch,
            head_sha=head_sha,
            limit=self._workflow_run_limit,
        )
        payload: dict[str, object] = {
            "requested_tag": request.requested_tag,
            "default_branch": default_branch,
            "default_branch_head_sha": head_sha,
            "requested_tag_exists": requested_tag_sha is not None,
            "requested_tag_sha": requested_tag_sha,
            "recent_tags": [_tag_to_json(tag) for tag in recent_tags],
            "latest_release": _release_to_json(latest_release),
            "workflow_runs_for_head": [_workflow_run_to_json(run) for run in workflow_runs],
        }
        return TriggeredTaskSnapshot(
            schema_version=1,
            task_kind="release",
            payload=payload,
        )

    def build_review_prompt(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        request: TriggeredTaskRequest,
        snapshot: TriggeredTaskSnapshot,
    ) -> str:
        request_json = json.dumps(
            {
                "task_kind": request.task_kind,
                "resource_key": request.resource_key,
                "requested_tag": request.requested_tag,
                "source_text": request.source_text,
            },
            indent=2,
            sort_keys=True,
        )
        snapshot_json = json.dumps(
            {
                "schema_version": snapshot.schema_version,
                "task_kind": snapshot.task_kind,
                "payload": snapshot.payload,
            },
            indent=2,
            sort_keys=True,
        )
        return build_triggered_task_review_prompt(
            issue=issue,
            repo_full_name=repo_full_name,
            default_branch=default_branch,
            task_kind=self.task_kind,
            request_json=request_json,
            snapshot_json=snapshot_json,
        )

    def execute(
        self,
        *,
        request: TriggeredTaskRequest,
        reviewed_snapshot: TriggeredTaskSnapshot,
        github: GitHubGateway,
        git: GitRepoManager,
        checkout_path: Path,
        default_branch: str,
    ) -> TriggeredTaskExecutionResult:
        expected_head_sha = _expected_snapshot_head_sha(reviewed_snapshot)
        current_remote_head = github.get_default_branch_head_sha(default_branch)
        if current_remote_head != expected_head_sha:
            return TriggeredTaskExecutionResult(
                success=False,
                detail=(
                    "default branch head drifted after review: "
                    f"expected {expected_head_sha}, observed {current_remote_head}"
                ),
                stdout_tail=None,
                stderr_tail=None,
            )
        existing_sha = github.get_tag_sha(request.requested_tag)
        if existing_sha is not None:
            return TriggeredTaskExecutionResult(
                success=False,
                detail=f"requested tag already exists remotely at {existing_sha}",
                stdout_tail=None,
                stderr_tail=None,
                observed_tag_sha=existing_sha,
            )

        script_path = _resolve_release_script_path(
            checkout_path=checkout_path,
            configured_path=self._repo.release_task_script,
        )
        if script_path is None:
            return TriggeredTaskExecutionResult(
                success=False,
                detail="repo.release_task_script is not configured",
                stdout_tail=None,
                stderr_tail=None,
            )
        if not script_path.exists() or not script_path.is_file():
            return TriggeredTaskExecutionResult(
                success=False,
                detail=f"release task script does not exist: {script_path}",
                stdout_tail=None,
                stderr_tail=None,
            )
        if not os.access(script_path, os.X_OK):
            return TriggeredTaskExecutionResult(
                success=False,
                detail=f"release task script is not executable: {script_path}",
                stdout_tail=None,
                stderr_tail=None,
            )

        git.prepare_checkout(checkout_path)
        checkout_head_sha = git.current_head_sha(checkout_path)
        if checkout_head_sha != expected_head_sha:
            return TriggeredTaskExecutionResult(
                success=False,
                detail=(
                    "checkout head does not match reviewed snapshot head after prepare: "
                    f"expected {expected_head_sha}, observed {checkout_head_sha}"
                ),
                stdout_tail=None,
                stderr_tail=None,
            )

        script_result = git.run_repo_script(
            checkout_path=checkout_path,
            script_path=script_path,
            args=(request.requested_tag,),
        )
        stdout_tail = _tail_lines(script_result.stdout, max_lines=self._output_tail_lines)
        stderr_tail = _tail_lines(script_result.stderr, max_lines=self._output_tail_lines)
        if script_result.returncode != 0:
            return TriggeredTaskExecutionResult(
                success=False,
                detail=(f"release task script failed with exit code {script_result.returncode}"),
                stdout_tail=stdout_tail,
                stderr_tail=stderr_tail,
            )

        observed_tag_sha = github.get_tag_sha(request.requested_tag)
        if observed_tag_sha is None:
            return TriggeredTaskExecutionResult(
                success=False,
                detail="release task script succeeded but requested tag is still missing remotely",
                stdout_tail=stdout_tail,
                stderr_tail=stderr_tail,
            )
        return TriggeredTaskExecutionResult(
            success=True,
            detail=f"release tag {request.requested_tag} created at {observed_tag_sha}",
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            observed_tag_sha=observed_tag_sha,
        )


def _extract_tag_from_match(match: re.Match[str]) -> str | None:
    if "tag" in match.groupdict():
        tag = match.group("tag")
        return tag.strip() if isinstance(tag, str) else None
    if match.lastindex is None or match.lastindex < 1:
        return None
    tag = match.group(1)
    if not isinstance(tag, str):
        return None
    return tag.strip()


def _normalize_release_tag(raw_tag: str) -> str | None:
    candidate = raw_tag.strip()
    if candidate.startswith("refs/tags/"):
        candidate = candidate[len("refs/tags/") :]
    if not candidate:
        return None
    if any(fragment in candidate for fragment in _UNSAFE_RELEASE_TAG_FRAGMENTS):
        return None
    if not _SAFE_RELEASE_TAG_PATTERN.fullmatch(candidate):
        return None
    return candidate


def _resolve_release_script_path(
    *, checkout_path: Path, configured_path: str | None
) -> Path | None:
    if configured_path is None:
        return None
    script_path = Path(configured_path).expanduser()
    if script_path.is_absolute():
        return script_path
    return (checkout_path / script_path).resolve()


def _expected_snapshot_head_sha(snapshot: TriggeredTaskSnapshot) -> str:
    value = snapshot.payload.get("default_branch_head_sha")
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("Triggered task snapshot is missing default_branch_head_sha")
    return value


def _tag_to_json(tag: RepositoryTagSnapshot) -> dict[str, object]:
    return {"name": tag.name, "commit_sha": tag.commit_sha}


def _release_to_json(release: ReleaseSnapshot | None) -> dict[str, object] | None:
    if release is None:
        return None
    return {
        "tag_name": release.tag_name,
        "name": release.name,
        "html_url": release.html_url,
        "published_at": release.published_at,
    }


def _workflow_run_to_json(run: WorkflowRunSnapshot) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "name": run.name,
        "status": run.status,
        "conclusion": run.conclusion,
        "html_url": run.html_url,
        "head_sha": run.head_sha,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }


def _tail_lines(raw_text: str, *, max_lines: int) -> str | None:
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return None
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])
