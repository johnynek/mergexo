from __future__ import annotations

from pathlib import Path

import pytest

from mergexo.config import RepoConfig
from mergexo.git_ops import ScriptRunResult
from mergexo.models import (
    Issue,
    ReleaseSnapshot,
    RepositoryTagSnapshot,
    WorkflowRunSnapshot,
)
from mergexo.task_handlers import ReleaseTaskHandler, TriggeredTaskSnapshot


class StubGitHub:
    def __init__(self) -> None:
        self.default_branch_head_sha = "headsha"
        self.tag_sha_by_name: dict[str, str] = {}
        self.tags: tuple[RepositoryTagSnapshot, ...] = ()
        self.latest_release: ReleaseSnapshot | None = None
        self.workflow_runs: tuple[WorkflowRunSnapshot, ...] = ()

    def get_default_branch_head_sha(self, default_branch: str) -> str:
        _ = default_branch
        return self.default_branch_head_sha

    def get_tag_sha(self, tag_name: str) -> str | None:
        return self.tag_sha_by_name.get(tag_name)

    def list_repository_tags(self, limit: int) -> tuple[RepositoryTagSnapshot, ...]:
        _ = limit
        return self.tags

    def get_latest_release(self) -> ReleaseSnapshot | None:
        return self.latest_release

    def list_workflow_runs_for_branch_head(
        self,
        *,
        default_branch: str,
        head_sha: str,
        limit: int = 20,
    ) -> tuple[WorkflowRunSnapshot, ...]:
        _ = default_branch, head_sha, limit
        return self.workflow_runs


class StubGit:
    def __init__(self) -> None:
        self.prepared: list[Path] = []
        self.current_head_sha_value = "headsha"
        self.script_result = ScriptRunResult(returncode=0, stdout="ok", stderr="")
        self.script_calls: list[tuple[Path, Path, tuple[str, ...]]] = []
        self.on_script_run: callable | None = None

    def prepare_checkout(self, checkout_path: Path) -> None:
        self.prepared.append(checkout_path)

    def current_head_sha(self, checkout_path: Path) -> str:
        _ = checkout_path
        return self.current_head_sha_value

    def run_repo_script(
        self,
        *,
        checkout_path: Path,
        script_path: Path,
        args: tuple[str, ...],
    ) -> ScriptRunResult:
        self.script_calls.append((checkout_path, script_path, args))
        if self.on_script_run is not None:
            self.on_script_run()
        return self.script_result


def _repo(*, release_task_script: str | None = "scripts/release.sh") -> RepoConfig:
    return RepoConfig(
        repo_id="mergexo",
        owner="johnynek",
        name="mergexo",
        default_branch="main",
        trigger_label="agent:design",
        bugfix_label="agent:bugfix",
        small_job_label="agent:small-job",
        task_label="agent:task",
        coding_guidelines_path="docs/python_style.md",
        design_docs_dir="docs/design",
        allowed_users=frozenset({"johnynek"}),
        local_clone_source=None,
        remote_url=None,
        release_task_script=release_task_script,
    )


def _snapshot(*, head_sha: str = "headsha") -> TriggeredTaskSnapshot:
    return TriggeredTaskSnapshot(
        schema_version=1,
        task_kind="release",
        payload={"default_branch_head_sha": head_sha},
    )


def test_release_task_parse_request_title_and_body() -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    from_title = handler.parse_request(
        Issue(
            number=1,
            title="release v1.2.3",
            body="ignored",
            html_url="https://example/issues/1",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert from_title is not None
    assert from_title.requested_tag == "v1.2.3"

    from_body = handler.parse_request(
        Issue(
            number=2,
            title="please do release",
            body="\n\nrelease: v2.0.0\nmore details",
            html_url="https://example/issues/2",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert from_body is not None
    assert from_body.requested_tag == "v2.0.0"


def test_release_task_parse_request_rejects_invalid_tags() -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    bad = handler.parse_request(
        Issue(
            number=3,
            title="release ../etc/passwd",
            body="",
            html_url="https://example/issues/3",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert bad is None


def test_release_task_collect_snapshot_contains_expected_fields() -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    request = handler.parse_request(
        Issue(
            number=4,
            title="release v1.2.3",
            body="",
            html_url="https://example/issues/4",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert request is not None
    github = StubGitHub()
    github.default_branch_head_sha = "abc123"
    github.tag_sha_by_name["v1.2.3"] = "abc123"
    github.tags = (
        RepositoryTagSnapshot(name="v1.2.2", commit_sha="oldsha"),
        RepositoryTagSnapshot(name="v1.2.1", commit_sha="oldersha"),
    )
    github.latest_release = ReleaseSnapshot(
        tag_name="v1.2.2",
        name="v1.2.2",
        html_url="https://example/releases/v1.2.2",
        published_at="2026-03-01T00:00:00Z",
    )
    github.workflow_runs = (
        WorkflowRunSnapshot(
            run_id=12,
            name="ci",
            status="completed",
            conclusion="success",
            html_url="https://example/runs/12",
            head_sha="abc123",
            created_at="t1",
            updated_at="t2",
        ),
    )

    snapshot = handler.collect_snapshot(
        request=request,
        github=github,
        default_branch="main",
    )
    assert snapshot.payload["requested_tag"] == "v1.2.3"
    assert snapshot.payload["default_branch_head_sha"] == "abc123"
    assert snapshot.payload["requested_tag_exists"] is True
    assert snapshot.payload["requested_tag_sha"] == "abc123"
    assert snapshot.payload["recent_tags"] == [
        {"name": "v1.2.2", "commit_sha": "oldsha"},
        {"name": "v1.2.1", "commit_sha": "oldersha"},
    ]


def test_release_task_execute_detects_head_drift(tmp_path: Path) -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    request = handler.parse_request(
        Issue(
            number=5,
            title="release v1.2.3",
            body="",
            html_url="https://example/issues/5",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert request is not None
    github = StubGitHub()
    github.default_branch_head_sha = "newsha"
    git = StubGit()
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(head_sha="oldsha"),
        github=github,  # type: ignore[arg-type]
        git=git,  # type: ignore[arg-type]
        checkout_path=tmp_path,
        default_branch="main",
    )
    assert result.success is False
    assert "drifted" in result.detail


def test_release_task_execute_success(tmp_path: Path) -> None:
    checkout_path = tmp_path / "checkout"
    checkout_path.mkdir()
    script_path = checkout_path / "scripts" / "release.sh"
    script_path.parent.mkdir(parents=True)
    script_path.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    script_path.chmod(0o755)

    handler = ReleaseTaskHandler(repo=_repo())
    request = handler.parse_request(
        Issue(
            number=6,
            title="release v1.2.3",
            body="",
            html_url="https://example/issues/6",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert request is not None
    github = StubGitHub()
    git = StubGit()

    def mark_tag_created() -> None:
        github.tag_sha_by_name["v1.2.3"] = "abc123"

    git.on_script_run = mark_tag_created
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(head_sha="headsha"),
        github=github,  # type: ignore[arg-type]
        git=git,  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert result.success is True
    assert result.observed_tag_sha == "abc123"
    assert git.script_calls[0][2] == ("v1.2.3",)


def test_release_task_execute_handles_script_failure(tmp_path: Path) -> None:
    checkout_path = tmp_path / "checkout"
    checkout_path.mkdir()
    script_path = checkout_path / "scripts" / "release.sh"
    script_path.parent.mkdir(parents=True)
    script_path.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
    script_path.chmod(0o755)
    handler = ReleaseTaskHandler(repo=_repo())
    request = handler.parse_request(
        Issue(
            number=7,
            title="release v1.2.3",
            body="",
            html_url="https://example/issues/7",
            labels=("agent:task",),
            author_login="johnynek",
        )
    )
    assert request is not None
    github = StubGitHub()
    git = StubGit()
    git.script_result = ScriptRunResult(
        returncode=3,
        stdout="line1\nline2\n",
        stderr="boom\ntrace\n",
    )
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(head_sha="headsha"),
        github=github,  # type: ignore[arg-type]
        git=git,  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert result.success is False
    assert "exit code 3" in result.detail
    assert result.stdout_tail == "line1\nline2"
    assert result.stderr_tail == "boom\ntrace"


def test_release_task_snapshot_json_and_kind() -> None:
    snapshot = _snapshot()
    handler = ReleaseTaskHandler(repo=_repo())
    assert handler.task_kind == "release"
    assert snapshot.to_json() == '{\n  "default_branch_head_sha": "headsha"\n}'


def test_release_task_parse_request_supports_group_one_and_ref_prefix() -> None:
    handler = ReleaseTaskHandler(repo=_repo(release_task_script="scripts/release.sh"))
    custom_handler = ReleaseTaskHandler(
        repo=RepoConfig(
            repo_id="mergexo",
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            allowed_users=frozenset({"johnynek"}),
            local_clone_source=None,
            remote_url=None,
            release_task_script="scripts/release.sh",
            release_request_regex=r"(?i)^\s*release\s+(\S+)\s*$",
        )
    )
    issue = Issue(
        number=8,
        title="release refs/tags/v3.0.0",
        body="",
        html_url="https://example/issues/8",
        labels=("agent:task",),
        author_login="johnynek",
    )
    parsed = custom_handler.parse_request(issue)
    assert parsed is not None
    assert parsed.requested_tag == "v3.0.0"
    no_capture_handler = ReleaseTaskHandler(
        repo=RepoConfig(
            repo_id="mergexo",
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            allowed_users=frozenset({"johnynek"}),
            local_clone_source=None,
            remote_url=None,
            release_task_script="scripts/release.sh",
            release_request_regex=r"(?i)^release$",
        )
    )
    assert (
        no_capture_handler.parse_request(
            Issue(
                number=17,
                title="release",
                body="",
                html_url="https://example/issues/17",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )
    assert (
        handler.parse_request(
            Issue(
                number=18,
                title="release",
                body="",
                html_url="https://example/issues/18",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )


def test_release_task_build_review_prompt_contains_serialized_request_and_snapshot() -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    issue = Issue(
        number=10,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/10",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = handler.parse_request(issue)
    assert request is not None
    prompt = handler.build_review_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        request=request,
        snapshot=_snapshot(head_sha="abc123"),
    )
    assert "Triggered task request JSON" in prompt
    assert '"requested_tag": "v1.2.3"' in prompt
    assert '"default_branch_head_sha": "abc123"' in prompt


def test_release_task_collect_snapshot_uses_none_for_absent_release() -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    issue = Issue(
        number=11,
        title="release v9.9.9",
        body="",
        html_url="https://example/issues/11",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = handler.parse_request(issue)
    assert request is not None
    snapshot = handler.collect_snapshot(request=request, github=StubGitHub(), default_branch="main")
    assert snapshot.payload["latest_release"] is None


def test_release_task_execute_rejects_existing_remote_tag(tmp_path: Path) -> None:
    handler = ReleaseTaskHandler(repo=_repo())
    issue = Issue(
        number=12,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/12",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = handler.parse_request(issue)
    assert request is not None
    github = StubGitHub()
    github.tag_sha_by_name["v1.2.3"] = "deadbeef"
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=github,  # type: ignore[arg-type]
        git=StubGit(),  # type: ignore[arg-type]
        checkout_path=tmp_path,
        default_branch="main",
    )
    assert result.success is False
    assert result.observed_tag_sha == "deadbeef"


def test_release_task_execute_rejects_missing_script_config(tmp_path: Path) -> None:
    handler = ReleaseTaskHandler(repo=_repo(release_task_script=None))
    issue = Issue(
        number=13,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/13",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = handler.parse_request(issue)
    assert request is not None
    with pytest.raises(RuntimeError, match="missing default_branch_head_sha"):
        handler.execute(
            request=request,
            reviewed_snapshot=TriggeredTaskSnapshot(
                schema_version=1, task_kind="release", payload={}
            ),
            github=StubGitHub(),  # type: ignore[arg-type]
            git=StubGit(),  # type: ignore[arg-type]
            checkout_path=tmp_path,
            default_branch="main",
        )

    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=StubGitHub(),  # type: ignore[arg-type]
        git=StubGit(),  # type: ignore[arg-type]
        checkout_path=tmp_path,
        default_branch="main",
    )
    assert result.success is False
    assert "not configured" in result.detail


def test_release_task_execute_rejects_missing_or_non_executable_script(tmp_path: Path) -> None:
    checkout_path = tmp_path / "checkout"
    checkout_path.mkdir()
    issue = Issue(
        number=14,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/14",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = ReleaseTaskHandler(repo=_repo()).parse_request(issue)
    assert request is not None

    missing = ReleaseTaskHandler(repo=_repo(release_task_script="scripts/missing.sh")).execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=StubGitHub(),  # type: ignore[arg-type]
        git=StubGit(),  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert missing.success is False
    assert "does not exist" in missing.detail

    script = checkout_path / "scripts" / "release.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    script.chmod(0o644)
    not_exec = ReleaseTaskHandler(repo=_repo(release_task_script="scripts/release.sh")).execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=StubGitHub(),  # type: ignore[arg-type]
        git=StubGit(),  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert not_exec.success is False
    assert "not executable" in not_exec.detail


def test_release_task_execute_rejects_checkout_head_mismatch(tmp_path: Path) -> None:
    checkout_path = tmp_path / "checkout"
    checkout_path.mkdir()
    script = checkout_path / "scripts" / "release.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)
    issue = Issue(
        number=15,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/15",
        labels=("agent:task",),
        author_login="johnynek",
    )
    handler = ReleaseTaskHandler(repo=_repo())
    request = handler.parse_request(issue)
    assert request is not None
    git = StubGit()
    git.current_head_sha_value = "other"
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=StubGitHub(),  # type: ignore[arg-type]
        git=git,  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert result.success is False
    assert "checkout head does not match" in result.detail


def test_release_task_execute_handles_missing_remote_tag_after_script_and_absolute_path(
    tmp_path: Path,
) -> None:
    checkout_path = tmp_path / "checkout"
    checkout_path.mkdir()
    absolute_script = tmp_path / "release.sh"
    absolute_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    absolute_script.chmod(0o755)
    issue = Issue(
        number=16,
        title="release v1.2.3",
        body="",
        html_url="https://example/issues/16",
        labels=("agent:task",),
        author_login="johnynek",
    )
    request = ReleaseTaskHandler(repo=_repo()).parse_request(issue)
    assert request is not None
    git = StubGit()
    git.script_result = ScriptRunResult(returncode=0, stdout="a\nb\n", stderr="x\ny\n")
    handler = ReleaseTaskHandler(
        repo=_repo(release_task_script=str(absolute_script)),
        output_tail_lines=1,
    )
    result = handler.execute(
        request=request,
        reviewed_snapshot=_snapshot(),
        github=StubGitHub(),  # type: ignore[arg-type]
        git=git,  # type: ignore[arg-type]
        checkout_path=checkout_path,
        default_branch="main",
    )
    assert result.success is False
    assert "still missing remotely" in result.detail
    assert result.stdout_tail == "b"
    assert result.stderr_tail == "y"


def test_release_task_parse_request_handles_optional_group_none_and_pattern_reject() -> None:
    optional_group_handler = ReleaseTaskHandler(
        repo=RepoConfig(
            repo_id="mergexo",
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            allowed_users=frozenset({"johnynek"}),
            local_clone_source=None,
            remote_url=None,
            release_task_script="scripts/release.sh",
            release_request_regex=r"(?i)^release(?:\s+(\S+))?$",
        )
    )
    assert (
        optional_group_handler.parse_request(
            Issue(
                number=19,
                title="release",
                body="",
                html_url="https://example/issues/19",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )

    permissive_handler = ReleaseTaskHandler(
        repo=RepoConfig(
            repo_id="mergexo",
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            allowed_users=frozenset({"johnynek"}),
            local_clone_source=None,
            remote_url=None,
            release_task_script="scripts/release.sh",
            release_request_regex=r"(?i)^release\s*(.*)$",
        )
    )
    assert (
        permissive_handler.parse_request(
            Issue(
                number=20,
                title="release refs/tags/",
                body="",
                html_url="https://example/issues/20",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )
    assert (
        permissive_handler.parse_request(
            Issue(
                number=21,
                title="release -bad",
                body="",
                html_url="https://example/issues/21",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )

    group_one_none_handler = ReleaseTaskHandler(
        repo=RepoConfig(
            repo_id="mergexo",
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            allowed_users=frozenset({"johnynek"}),
            local_clone_source=None,
            remote_url=None,
            release_task_script="scripts/release.sh",
            release_request_regex=r"^release (?:(XXX)|(\S+))$",
        )
    )
    assert (
        group_one_none_handler.parse_request(
            Issue(
                number=22,
                title="release v1.2.3",
                body="",
                html_url="https://example/issues/22",
                labels=("agent:task",),
                author_login="johnynek",
            )
        )
        is None
    )
