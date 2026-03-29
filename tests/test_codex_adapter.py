from __future__ import annotations

from pathlib import Path
import json

import pytest

from mergexo.agent_adapter import (
    AgentSession,
    DirectStartResult,
    FeedbackTurn,
    PrePrReviewResult,
    RoadmapDependencyArtifact,
    RoadmapDependencyReference,
    RoadmapFeedbackTurn,
    ReviewFinding,
)
from mergexo.codex_adapter import (
    CodexAdapter,
    CodexInvocationHooks,
    _DESIGN_OUTPUT_SCHEMA,
    _DIRECT_OUTPUT_SCHEMA,
    _FEEDBACK_OUTPUT_SCHEMA,
    _PRE_PR_REVIEW_OUTPUT_SCHEMA,
    _ROADMAP_ADJUSTMENT_OUTPUT_SCHEMA,
    _ROADMAP_OUTPUT_SCHEMA,
    _as_object_dict,
    _extract_final_agent_message,
    _extract_thread_id,
    _filter_resume_extra_args,
    _is_context_window_exhaustion_error,
    _parse_direct_result_payload,
    _parse_flaky_test_report,
    _parse_git_ops,
    _optional_output_text,
    _parse_event_line,
    _parse_json_payload,
    _parse_pre_pr_review_result_payload,
    _parse_roadmap_revision_escalation,
    _parse_review_findings,
    _parse_review_replies,
    _require_str,
    _require_str_list,
    _validate_strict_output_schema,
)
from mergexo.config import CodexConfig
from mergexo.models import (
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
)
from mergexo.observability import configure_logging
from mergexo.shell import CommandError, RunningCommand


def _enabled_config() -> CodexConfig:
    return CodexConfig(
        enabled=True,
        model="gpt-5-codex",
        sandbox="workspace-write",
        profile="default",
        extra_args=("--full-auto",),
    )


def _feedback_turn() -> FeedbackTurn:
    return FeedbackTurn(
        turn_key="turn-abc",
        issue=Issue(number=1, title="Issue", body="Body", html_url="u", labels=("x",)),
        pull_request=PullRequestSnapshot(
            number=8,
            title="PR",
            body="desc",
            head_sha="headsha",
            base_sha="basesha",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(
            PullRequestReviewComment(
                comment_id=101,
                body="Please rename",
                path="src/a.py",
                line=10,
                side="RIGHT",
                in_reply_to_id=None,
                user_login="reviewer",
                html_url="http://review",
                created_at="now",
                updated_at="now",
            ),
        ),
        issue_comments=(
            PullRequestIssueComment(
                comment_id=201,
                body="general",
                user_login="reviewer",
                html_url="http://issue",
                created_at="now",
                updated_at="now",
            ),
        ),
        changed_files=("src/a.py",),
    )


def _roadmap_feedback_turn() -> RoadmapFeedbackTurn:
    return RoadmapFeedbackTurn(
        turn_key="turn-roadmap",
        issue=Issue(
            number=1,
            title="Roadmap Issue",
            body="Body",
            html_url="u",
            labels=("agent:roadmap",),
        ),
        pull_request=PullRequestSnapshot(
            number=8,
            title="Roadmap PR",
            body="desc",
            head_sha="headsha",
            base_sha="basesha",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(),
        issue_comments=(),
        changed_files=("docs/roadmap/1-issue.md", "docs/roadmap/1-issue.graph.json"),
        roadmap_doc_path="docs/roadmap/1-issue.md",
        graph_path="docs/roadmap/1-issue.graph.json",
        roadmap_markdown="# Roadmap",
        graph_json_text='{"roadmap_issue_number":1,"version":1,"nodes":[]}',
        graph_version=1,
        roadmap_markdown_missing=False,
        graph_missing=False,
        graph_validation_error=None,
    )


def _pre_pr_review_result() -> PrePrReviewResult:
    return PrePrReviewResult(
        outcome="changes_requested",
        summary="Reviewer found one blocking issue.",
        findings=(
            ReviewFinding(
                finding_id="R1",
                path="src/mergexo/codex_adapter.py",
                line=101,
                title="Validate reviewer output",
                details="Reject invalid outcome combinations.",
            ),
        ),
        escalation_reason=None,
    )


def test_output_schemas_satisfy_strict_backend_contract() -> None:
    schemas = {
        "design": _DESIGN_OUTPUT_SCHEMA,
        "direct": _DIRECT_OUTPUT_SCHEMA,
        "pre_pr_review": _PRE_PR_REVIEW_OUTPUT_SCHEMA,
        "roadmap": _ROADMAP_OUTPUT_SCHEMA,
        "roadmap_adjustment": _ROADMAP_ADJUSTMENT_OUTPUT_SCHEMA,
        "feedback": _FEEDBACK_OUTPUT_SCHEMA,
    }

    for name, schema in schemas.items():
        _validate_strict_output_schema(schema_name=name, schema=schema)

    assert _DIRECT_OUTPUT_SCHEMA["required"] == [
        "pr_title",
        "pr_summary",
        "commit_message",
        "blocked_reason",
        "escalation",
    ]
    assert _PRE_PR_REVIEW_OUTPUT_SCHEMA["required"] == [
        "outcome",
        "summary",
        "findings",
        "escalation_reason",
    ]
    assert _ROADMAP_OUTPUT_SCHEMA["required"] == ["title", "summary", "graph_json"]
    assert _ROADMAP_ADJUSTMENT_OUTPUT_SCHEMA["required"] == [
        "action",
        "summary",
        "details",
        "updated_graph_json",
    ]
    assert _FEEDBACK_OUTPUT_SCHEMA["required"] == [
        "review_replies",
        "general_comment",
        "commit_message",
        "git_ops",
        "flaky_test_report",
        "escalation",
    ]


def test_validate_strict_output_schema_rejects_missing_required_property() -> None:
    with pytest.raises(RuntimeError, match="missing required keys: escalation"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["title"],
                "properties": {
                    "title": {"type": "string"},
                    "escalation": {"type": "null"},
                },
            },
        )


def test_validate_strict_output_schema_rejects_non_object_schema() -> None:
    with pytest.raises(RuntimeError, match="must be a JSON object"):
        _validate_strict_output_schema(schema_name="broken", schema="not-a-schema")


def test_validate_strict_output_schema_rejects_open_object_schema() -> None:
    with pytest.raises(RuntimeError, match="must set additionalProperties to false"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "type": "object",
                "required": ["title"],
                "properties": {
                    "title": {"type": "string"},
                },
            },
        )


def test_validate_strict_output_schema_rejects_missing_properties_and_required_list() -> None:
    with pytest.raises(RuntimeError, match="must define properties"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["title"],
            },
        )

    with pytest.raises(RuntimeError, match="must define required as a string list"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["title", 1],
                "properties": {
                    "title": {"type": "string"},
                },
            },
        )


def test_validate_strict_output_schema_reports_unknown_required_keys() -> None:
    with pytest.raises(RuntimeError, match="unknown required keys: ghost"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "ghost"],
                "properties": {
                    "title": {"type": "string"},
                },
            },
        )


def test_validate_strict_output_schema_handles_items_lists_and_rejects_invalid_combiners() -> None:
    _validate_strict_output_schema(
        schema_name="tuple_array",
        schema={
            "type": "array",
            "items": [
                {"type": "string", "minLength": 1},
                {"type": "null"},
            ],
        },
    )

    with pytest.raises(RuntimeError, match=r"\$\.anyOf must be a list"):
        _validate_strict_output_schema(
            schema_name="broken",
            schema={
                "anyOf": {
                    "type": "null",
                },
            },
        )


def test_start_design_from_issue_happy_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        assert cwd == tmp_path
        assert check is True
        assert input_text is not None
        assert "issue #1" in input_text.lower()
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
        return '{"type":"thread.started","thread_id":"thread-123"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    configure_logging(verbose=True)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_design_from_issue(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/1-issue.md",
        default_branch="main",
        cwd=tmp_path,
    )
    generated = result.design

    assert generated.title == "Design"
    assert generated.summary == "Summary"
    assert generated.touch_paths == ("src/a.py", "src/b.py")
    assert result.session is not None
    assert result.session.thread_id == "thread-123"
    cmd = calls[0]
    assert "--json" in cmd
    assert "--model" in cmd
    assert "--sandbox" in cmd
    assert "--profile" in cmd
    assert "--full-auto" in cmd
    stderr = capsys.readouterr().err
    assert (
        "event=design_turn_started design_doc_path=docs/design/1-issue.md issue_number=1" in stderr
    )
    assert (
        "event=design_turn_completed issue_number=1 thread_id=thread-123 touch_path_count=2"
        in stderr
    )
    assert "Issue body:" not in stderr


def test_start_design_from_issue_returns_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Design",
                    "summary": "Summary",
                    "touch_paths": ["src/a.py"],
                    "design_doc_markdown": "Doc",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-abc"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_design_from_issue(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/1-issue.md",
        default_branch="main",
        cwd=tmp_path,
    )

    assert result.design.title == "Design"
    assert result.session is not None
    assert result.session.thread_id == "thread-abc"


def test_start_design_from_issue_reports_process_start_and_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    started: list[RunningCommand] = []
    progress_ticks: list[str] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        timeout_seconds: float | None = None,
        idle_timeout_seconds: float | None = None,
        on_start=None,
        on_output=None,
        **_kwargs: object,
    ) -> str:
        _ = input_text, check
        assert cwd == tmp_path
        assert timeout_seconds == 3600.0
        assert idle_timeout_seconds == 900
        if on_start is not None:
            on_start(
                RunningCommand(
                    argv=tuple(cmd),
                    cwd=cwd,
                    pid=123,
                    pgid=456,
                    timeout_seconds=timeout_seconds,
                    idle_timeout_seconds=idle_timeout_seconds,
                )
            )
        if on_output is not None:
            on_output("stdout", '{"type":"thread.started","thread_id":"thread-xyz"}\n')
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Design",
                    "summary": "Summary",
                    "touch_paths": ["src/a.py"],
                    "design_doc_markdown": "Doc",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-xyz"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())
    with adapter.observe_invocations(
        CodexInvocationHooks(
            on_start=started.append,
            on_progress=lambda: progress_ticks.append("tick"),
        )
    ):
        result = adapter.start_design_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            design_doc_path="docs/design/1-issue.md",
            default_branch="main",
            cwd=tmp_path,
        )

    assert result.session is not None
    assert result.session.thread_id == "thread-xyz"
    assert started == [
        RunningCommand(
            argv=(
                "codex",
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--output-schema",
                started[0].argv[5],
                "--output-last-message",
                started[0].argv[7],
                "--model",
                "gpt-5-codex",
                "--sandbox",
                "workspace-write",
                "--profile",
                "default",
                "--full-auto",
                "-",
            ),
            cwd=tmp_path,
            pid=123,
            pgid=456,
            timeout_seconds=3600.0,
            idle_timeout_seconds=900,
        )
    ]
    assert progress_ticks == ["tick"]


def test_observe_invocations_restores_outer_hooks_after_nested_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outer_started: list[int] = []
    outer_progress: list[str] = []
    inner_started: list[int] = []
    inner_progress: list[str] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        on_start=None,
        on_output=None,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        if on_start is not None:
            on_start(
                RunningCommand(
                    argv=tuple(cmd),
                    cwd=tmp_path,
                    pid=321,
                    pgid=654,
                    timeout_seconds=3600.0,
                    idle_timeout_seconds=900.0,
                )
            )
        if on_output is not None:
            on_output("stdout", "progress\n")
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Design",
                    "summary": "Summary",
                    "touch_paths": ["src/a.py"],
                    "design_doc_markdown": "Doc",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-abc"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())

    with adapter.observe_invocations(
        CodexInvocationHooks(
            on_start=lambda command: outer_started.append(command.pid),
            on_progress=lambda: outer_progress.append("outer"),
        )
    ):
        with adapter.observe_invocations(
            CodexInvocationHooks(
                on_start=lambda command: inner_started.append(command.pid),
                on_progress=lambda: inner_progress.append("inner"),
            )
        ):
            adapter.start_design_from_issue(
                issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
                repo_full_name="johnynek/mergexo",
                design_doc_path="docs/design/1-issue.md",
                default_branch="main",
                cwd=tmp_path,
            )

        adapter.start_design_from_issue(
            issue=Issue(number=2, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            design_doc_path="docs/design/2-issue.md",
            default_branch="main",
            cwd=tmp_path,
        )

    assert inner_started == [321]
    assert inner_progress == ["inner"]
    assert outer_started == [321]
    assert outer_progress == ["outer"]


def test_observe_invocations_allows_missing_callback_functions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        on_start=None,
        on_output=None,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        assert on_start is not None
        assert on_output is not None
        on_start(
            RunningCommand(
                argv=tuple(cmd),
                cwd=tmp_path,
                pid=222,
                pgid=333,
                timeout_seconds=3600.0,
                idle_timeout_seconds=900.0,
            )
        )
        on_output("stdout", "progress\n")
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Design",
                    "summary": "Summary",
                    "touch_paths": ["src/a.py"],
                    "design_doc_markdown": "Doc",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-optional"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())

    with adapter.observe_invocations(CodexInvocationHooks()):
        result = adapter.start_design_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            design_doc_path="docs/design/1-issue.md",
            default_branch="main",
            cwd=tmp_path,
        )

    assert result.session is not None
    assert result.session.thread_id == "thread-optional"


def test_start_roadmap_from_issue_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "roadmap agent" in input_text
        schema_idx = cmd.index("--output-schema")
        schema = json.loads(Path(cmd[schema_idx + 1]).read_text(encoding="utf-8"))
        assert schema["required"] == ["title", "summary", "graph_json"]
        assert "roadmap_markdown" not in schema["properties"]
        graph_schema = schema["properties"]["graph_json"]
        assert graph_schema["additionalProperties"] is False
        assert graph_schema["required"] == ["roadmap_issue_number", "version", "nodes"]
        node_schema = graph_schema["properties"]["nodes"]["items"]
        assert node_schema["additionalProperties"] is False
        assert node_schema["properties"]["kind"]["enum"] == [
            "reference_doc",
            "design_doc",
            "small_job",
            "roadmap",
        ]
        assert node_schema["required"] == [
            "node_id",
            "kind",
            "title",
            "body_markdown",
            "depends_on",
        ]
        dependency_schema = node_schema["properties"]["depends_on"]["items"]
        assert dependency_schema["additionalProperties"] is False
        assert dependency_schema["required"] == ["node_id", "requires"]
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Roadmap",
                    "summary": "Summary",
                    "graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 1,
                        "nodes": [
                            {
                                "node_id": "n1",
                                "kind": "small_job",
                                "title": "Ship",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-roadmap"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_roadmap_from_issue(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        roadmap_docs_dir="docs/roadmap",
        recommended_node_count=7,
        cwd=tmp_path,
    )

    assert result.roadmap.title == "Roadmap"
    assert result.roadmap.roadmap_issue_number == 1
    assert result.roadmap.version == 1
    assert len(result.roadmap.graph_nodes) == 1
    assert "Generated from roadmap graph JSON." in result.roadmap.roadmap_markdown
    assert result.session is not None
    assert result.session.thread_id == "thread-roadmap"


def test_start_roadmap_from_issue_reports_process_start_and_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    started: list[RunningCommand] = []
    progress_ticks: list[str] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        timeout_seconds: float | None = None,
        idle_timeout_seconds: float | None = None,
        on_start=None,
        on_output=None,
        **_kwargs: object,
    ) -> str:
        _ = input_text, check
        assert cwd == tmp_path
        assert timeout_seconds == 5400.0
        assert idle_timeout_seconds == 900
        if on_start is not None:
            on_start(
                RunningCommand(
                    argv=tuple(cmd),
                    cwd=cwd,
                    pid=124,
                    pgid=457,
                    timeout_seconds=timeout_seconds,
                    idle_timeout_seconds=idle_timeout_seconds,
                )
            )
        if on_output is not None:
            on_output("stdout", '{"type":"thread.started","thread_id":"thread-roadmap-hooks"}\n')
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Roadmap",
                    "summary": "Summary",
                    "graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 1,
                        "nodes": [
                            {
                                "node_id": "n1",
                                "kind": "small_job",
                                "title": "Ship",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-roadmap-hooks"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())
    with adapter.observe_invocations(
        CodexInvocationHooks(
            on_start=started.append,
            on_progress=lambda: progress_ticks.append("tick"),
        )
    ):
        result = adapter.start_roadmap_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            roadmap_docs_dir="docs/roadmap",
            recommended_node_count=7,
            cwd=tmp_path,
        )

    assert result.session is not None
    assert result.session.thread_id == "thread-roadmap-hooks"
    assert started == [
        RunningCommand(
            argv=(
                "codex",
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--output-schema",
                started[0].argv[5],
                "--output-last-message",
                started[0].argv[7],
                "--model",
                "gpt-5-codex",
                "--sandbox",
                "workspace-write",
                "--profile",
                "default",
                "--full-auto",
                "-",
            ),
            cwd=tmp_path,
            pid=124,
            pgid=457,
            timeout_seconds=5400.0,
            idle_timeout_seconds=900,
        )
    ]
    assert progress_ticks == ["tick"]


def test_start_roadmap_from_issue_requires_graph_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Roadmap",
                    "summary": "Summary",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-roadmap"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="missing required graph_json"):
        adapter.start_roadmap_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            roadmap_docs_dir="docs/roadmap",
            recommended_node_count=7,
            cwd=tmp_path,
        )


def test_evaluate_roadmap_adjustment_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "roadmap-adjustment agent" in input_text
        assert '["n2","n3"]' in input_text
        schema_idx = cmd.index("--output-schema")
        schema = json.loads(Path(cmd[schema_idx + 1]).read_text(encoding="utf-8"))
        assert schema["required"] == ["action", "summary", "details", "updated_graph_json"]
        assert "updated_roadmap_markdown" not in schema["properties"]
        updated_graph_schema = next(
            item
            for item in schema["properties"]["updated_graph_json"]["anyOf"]
            if item.get("type") == "object"
        )
        assert updated_graph_schema["additionalProperties"] is False
        assert updated_graph_schema["required"] == ["roadmap_issue_number", "version", "nodes"]
        node_schema = updated_graph_schema["properties"]["nodes"]["items"]
        assert node_schema["additionalProperties"] is False
        assert node_schema["properties"]["kind"]["enum"] == [
            "reference_doc",
            "design_doc",
            "small_job",
            "roadmap",
        ]
        assert node_schema["required"] == [
            "node_id",
            "kind",
            "title",
            "body_markdown",
            "depends_on",
        ]
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "revise",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
                    "updated_graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 3,
                        "nodes": [
                            {
                                "node_id": "n2",
                                "kind": "small_job",
                                "title": "Ship next",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.evaluate_roadmap_adjustment(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        roadmap_doc_path="docs/roadmap/1-issue.md",
        graph_path="docs/roadmap/1-issue.graph.json",
        graph_version=2,
        ready_node_ids=("n2", "n3"),
        dependency_artifacts=(
            RoadmapDependencyArtifact(
                dependency_node_id="n1",
                dependency_kind="small_job",
                dependency_title="Dependency",
                frontier_references=(
                    RoadmapDependencyReference(ready_node_id="n2", requires="implemented"),
                ),
                child_issue_number=10,
                child_issue_url="https://example/issues/10",
                child_issue_title="Dependency issue",
                child_issue_body="Issue body",
                issue_run_status="merged",
                issue_run_branch="agent/impl/10-dependency",
                issue_run_error=None,
                resolution_markers=("issue_run_status=merged",),
                pr_number=11,
                pr_url="https://example/pr/11",
                pr_title="Dependency PR",
                pr_body="PR body",
                pr_state="closed",
                pr_merged=True,
                changed_files=("src/a.py",),
                review_summaries=(),
                issue_comments=(),
            ),
        ),
        roadmap_status_report="status report",
        roadmap_markdown="# Roadmap",
        canonical_graph_json='{"roadmap_issue_number":1}',
        cwd=tmp_path,
    )

    assert result.action == "revise"
    assert result.summary == "Need revision"
    assert "revised roadmap" in result.details
    assert result.updated_roadmap_markdown is not None
    assert "Generated from roadmap graph JSON." in result.updated_roadmap_markdown
    assert result.updated_canonical_graph_json is not None
    assert '"version":3' in result.updated_canonical_graph_json


def test_evaluate_roadmap_adjustment_rejects_invalid_action(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "maybe",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
                    "updated_graph_json": None,
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="action must be one of"):
        adapter.evaluate_roadmap_adjustment(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            ready_node_ids=("n2",),
            dependency_artifacts=(),
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_evaluate_roadmap_adjustment_rejects_revise_without_graph_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "revise",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
                    "updated_graph_json": None,
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="requires non-null updated_graph_json object"):
        adapter.evaluate_roadmap_adjustment(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            ready_node_ids=("n2",),
            dependency_artifacts=(),
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_evaluate_roadmap_adjustment_rejects_revise_without_version_bump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "revise",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
                    "updated_graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 2,
                        "nodes": [
                            {
                                "node_id": "n2",
                                "kind": "small_job",
                                "title": "Ship next",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="must bump roadmap graph version by exactly 1"):
        adapter.evaluate_roadmap_adjustment(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            ready_node_ids=("n2",),
            dependency_artifacts=(),
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_evaluate_roadmap_adjustment_rejects_proceed_with_revision_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "proceed",
                    "summary": "Proceed",
                    "details": "No change needed.",
                    "updated_graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 3,
                        "nodes": [
                            {
                                "node_id": "n2",
                                "kind": "small_job",
                                "title": "Ship next",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="must set updated_graph_json to null"):
        adapter.evaluate_roadmap_adjustment(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            ready_node_ids=("n2",),
            dependency_artifacts=(),
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_evaluate_roadmap_adjustment_requires_enabled_config(tmp_path: Path) -> None:
    adapter = CodexAdapter(
        CodexConfig(
            enabled=False,
            model=None,
            sandbox=None,
            profile=None,
            extra_args=(),
        )
    )
    with pytest.raises(RuntimeError, match="Codex is disabled in config"):
        adapter.evaluate_roadmap_adjustment(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            ready_node_ids=("n2",),
            dependency_artifacts=(),
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_author_requested_roadmap_revision_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "roadmap-revision agent" in input_text
        assert "operator requested roadmap revision" in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "revise",
                    "summary": "Need revision",
                    "details": "Author the requested roadmap revision.",
                    "updated_graph_json": {
                        "roadmap_issue_number": 1,
                        "version": 3,
                        "nodes": [
                            {
                                "node_id": "n2",
                                "kind": "small_job",
                                "title": "Ship next",
                                "body_markdown": "Do it",
                                "depends_on": [],
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.author_requested_roadmap_revision(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        roadmap_doc_path="docs/roadmap/1-issue.md",
        graph_path="docs/roadmap/1-issue.graph.json",
        graph_version=2,
        request_reason="operator requested roadmap revision",
        roadmap_status_report="status report",
        roadmap_markdown="# Roadmap",
        canonical_graph_json='{"roadmap_issue_number":1}',
        cwd=tmp_path,
    )

    assert result.action == "revise"
    assert result.updated_roadmap_markdown is not None
    assert "Generated from roadmap graph JSON." in result.updated_roadmap_markdown
    assert result.updated_canonical_graph_json is not None
    assert '"version":3' in result.updated_canonical_graph_json


def test_author_requested_roadmap_revision_rejects_proceed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "proceed",
                    "summary": "Proceed",
                    "details": "No revision needed.",
                    "updated_graph_json": None,
                }
            ),
            encoding="utf-8",
        )
        return ""

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="must not return proceed"):
        adapter.author_requested_roadmap_revision(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            roadmap_doc_path="docs/roadmap/1-issue.md",
            graph_path="docs/roadmap/1-issue.graph.json",
            graph_version=2,
            request_reason="operator requested roadmap revision",
            roadmap_status_report="status report",
            roadmap_markdown="# Roadmap",
            canonical_graph_json='{"roadmap_issue_number":1}',
            cwd=tmp_path,
        )


def test_start_bugfix_from_issue_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "regression tests that fail before the fix and pass after the fix" in input_text
        assert "docs/python_style.md" in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "pr_title": "Fix flaky scheduler",
                    "pr_summary": "Adds a guard and regression test.",
                    "commit_message": "fix: stabilize scheduler retries",
                    "blocked_reason": None,
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-bugfix"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_bugfix_from_issue(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        cwd=tmp_path,
    )

    assert result.pr_title == "Fix flaky scheduler"
    assert result.commit_message == "fix: stabilize scheduler retries"
    assert result.blocked_reason is None
    assert result.session is not None
    assert result.session.thread_id == "thread-bugfix"


def test_start_small_job_from_issue_can_return_blocked_reason(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "small-job agent" in input_text
        assert "docs/python_style.md" in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "pr_title": "N/A",
                    "pr_summary": "N/A",
                    "commit_message": None,
                    "blocked_reason": "Missing required repository context.",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-small"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_small_job_from_issue(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        cwd=tmp_path,
    )

    assert result.pr_title == "N/A"
    assert result.commit_message is None
    assert result.blocked_reason == "Missing required repository context."
    assert result.session is not None
    assert result.session.thread_id == "thread-small"


def test_start_implementation_from_design_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "implementation agent" in input_text
        assert "docs/design/1-issue.md" in input_text
        assert "Source design PR: #77 (https://example/pr/77)" in input_text
        assert "Re-read the full diff against main." in input_text
        assert "Re-run formatting and CI-required checks from docs/python_style.md." in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "pr_title": "Implement scheduler design",
                    "pr_summary": "Implements the merged design doc plan.",
                    "commit_message": "feat: implement scheduler design",
                    "blocked_reason": None,
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-impl"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.start_implementation_from_design(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        design_doc_path="docs/design/1-issue.md",
        design_doc_markdown="# Design",
        design_pr_number=77,
        design_pr_url="https://example/pr/77",
        cwd=tmp_path,
    )

    assert result.pr_title == "Implement scheduler design"
    assert result.commit_message == "feat: implement scheduler design"
    assert result.blocked_reason is None
    assert result.session is not None
    assert result.session.thread_id == "thread-impl"


def test_review_pre_pr_candidate_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "pre-PR reviewer" in input_text
        assert "docs/python_style.md" in input_text
        assert "Prioritize correctness over style." in input_text
        assert "Source design PR: #77 (https://example/pr/77)" in input_text
        schema_idx = cmd.index("--output-schema")
        schema = json.loads(Path(cmd[schema_idx + 1]).read_text(encoding="utf-8"))
        assert schema["required"] == ["outcome", "summary", "findings", "escalation_reason"]
        assert schema["properties"]["outcome"]["enum"] == [
            "approved",
            "changes_requested",
            "escalate",
        ]
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "outcome": "changes_requested",
                    "summary": "One repair is required.",
                    "findings": [
                        {
                            "finding_id": "R1",
                            "path": "src/mergexo/codex_adapter.py",
                            "line": 101,
                            "title": "Validate reviewer output",
                            "details": "Reject invalid payload combinations.",
                        }
                    ],
                    "escalation_reason": None,
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-review"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.review_pre_pr_candidate(
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        flow_name="implementation",
        coding_guidelines_path="docs/python_style.md",
        review_guidance="Prioritize correctness over style.",
        branch="agent/impl/1-review",
        head_sha="abc123",
        changed_files=("src/mergexo/codex_adapter.py",),
        diff_text="diff --git a/src/mergexo/codex_adapter.py b/src/mergexo/codex_adapter.py",
        design_doc_path="docs/design/1-issue.md",
        design_doc_markdown="## Design\n\nReview the contract.",
        design_pr_number=77,
        design_pr_url="https://example/pr/77",
        cwd=tmp_path,
    )

    assert result.outcome == "changes_requested"
    assert result.summary == "One repair is required."
    assert result.findings[0].finding_id == "R1"
    assert result.findings[0].line == 101
    assert result.escalation_reason is None


def test_repair_from_pre_pr_review_resumes_saved_author_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert cmd[:5] == ["codex", "exec", "resume", "--json", "--skip-git-repo-check"]
        assert "pre-PR author-repair agent" in input_text
        assert "Reviewer found one blocking issue." in input_text
        assert '"finding_id": "R1"' in input_text
        assert "Source design PR: #77 (https://example/pr/77)" in input_text
        message_payload = json.dumps(
            {
                "pr_title": "Repair reviewer findings",
                "pr_summary": "Addresses the reviewer contract issues.",
                "commit_message": "fix: repair reviewer findings",
                "blocked_reason": None,
                "escalation": None,
            }
        )
        return (
            '{"type":"thread.started","thread_id":"thread-repaired"}\n'
            + json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": message_payload},
                }
            )
            + "\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.repair_from_pre_pr_review(
        session=AgentSession(adapter="codex", thread_id="thread-author"),
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        flow_name="implementation",
        coding_guidelines_path="docs/python_style.md",
        review_guidance="Keep the repair focused.",
        branch="agent/impl/1-review",
        head_sha="abc123",
        review_result=_pre_pr_review_result(),
        design_doc_path="docs/design/1-issue.md",
        design_doc_markdown="## Design\n\nReview the contract.",
        design_pr_number=77,
        design_pr_url="https://example/pr/77",
        cwd=tmp_path,
    )

    assert result.pr_title == "Repair reviewer findings"
    assert result.commit_message == "fix: repair reviewer findings"
    assert result.blocked_reason is None
    assert result.escalation is None
    assert result.session is not None
    assert result.session.thread_id == "thread-repaired"


def test_repair_from_pre_pr_review_falls_back_to_fresh_thread_on_context_window_exhaustion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        calls.append(cmd)
        if cmd[:3] == ["codex", "exec", "resume"]:
            raise CommandError(
                "Command failed\n"
                f"cmd: {' '.join(cmd)}\n"
                "exit: 1\n"
                'stdout:\n{"type":"error","message":"Codex ran out of room in the model\'s context window. Start a new thread."}\n'
                "stderr:\n"
            )

        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "pr_title": "Repair reviewer findings",
                    "pr_summary": "Addresses the reviewer contract issues.",
                    "commit_message": "fix: continue reviewer repair in new thread",
                    "blocked_reason": None,
                    "escalation": None,
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-fresh-repair"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.repair_from_pre_pr_review(
        session=AgentSession(adapter="codex", thread_id="thread-author"),
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        flow_name="small_job",
        coding_guidelines_path="docs/python_style.md",
        review_guidance=None,
        branch="agent/small/1-review",
        head_sha="abc123",
        review_result=_pre_pr_review_result(),
        design_doc_path=None,
        design_doc_markdown=None,
        design_pr_number=None,
        design_pr_url=None,
        cwd=tmp_path,
    )

    assert len(calls) == 2
    assert calls[0][:3] == ["codex", "exec", "resume"]
    assert calls[1][:2] == ["codex", "exec"]
    assert result.session is not None
    assert result.session.thread_id == "thread-fresh-repair"
    assert result.commit_message == "fix: continue reviewer repair in new thread"


def test_repair_from_pre_pr_review_uses_fresh_turn_when_author_session_lacks_thread_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    adapter = CodexAdapter(_enabled_config())
    monkeypatch.setattr(
        adapter,
        "_run_direct_turn",
        lambda **_: (
            DirectStartResult(
                pr_title="Repair reviewer findings",
                pr_summary="Summary",
                commit_message="fix: repair from fresh author thread",
                blocked_reason=None,
                session=None,
                escalation=None,
            ),
            "thread-fresh-no-session",
        ),
    )

    result = adapter.repair_from_pre_pr_review(
        session=AgentSession(adapter="codex", thread_id=None),
        issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        flow_name="small_job",
        coding_guidelines_path="docs/python_style.md",
        review_guidance=None,
        branch="agent/small/1-review",
        head_sha="abc123",
        review_result=_pre_pr_review_result(),
        design_doc_path=None,
        design_doc_markdown=None,
        design_pr_number=None,
        design_pr_url=None,
        cwd=tmp_path,
    )

    assert result.session is not None
    assert result.session.thread_id == "thread-fresh-no-session"
    assert result.commit_message == "fix: repair from fresh author thread"


def test_review_pre_pr_candidate_logs_fault_when_review_turn_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    adapter = CodexAdapter(_enabled_config())
    monkeypatch.setattr(
        adapter,
        "_run_pre_pr_review_turn",
        lambda **_: (_ for _ in ()).throw(RuntimeError("review failed")),
    )
    configure_logging(verbose=True)

    with pytest.raises(RuntimeError, match="review failed"):
        adapter.review_pre_pr_candidate(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            flow_name="small_job",
            coding_guidelines_path="docs/python_style.md",
            review_guidance=None,
            branch="agent/small/1-review",
            head_sha="abc123",
            changed_files=("src/mergexo/codex_adapter.py",),
            diff_text="diff --git a/src/mergexo/codex_adapter.py b/src/mergexo/codex_adapter.py",
            design_doc_path=None,
            design_doc_markdown=None,
            design_pr_number=None,
            design_pr_url=None,
            cwd=tmp_path,
        )

    stderr = capsys.readouterr().err
    assert "mode=pre_pr_review" in stderr
    assert "status=fault" in stderr


def test_repair_from_pre_pr_review_re_raises_non_context_error_and_logs_fault(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cmd, cwd, input_text, check
        raise CommandError(
            "Command failed\n"
            "cmd: codex exec resume --json --skip-git-repo-check thread-author -\n"
            "exit: 1\n"
            'stdout:\n{"type":"error","message":"transient CLI failure"}\n'
            "stderr:\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    configure_logging(verbose=True)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(CommandError, match="transient CLI failure"):
        adapter.repair_from_pre_pr_review(
            session=AgentSession(adapter="codex", thread_id="thread-author"),
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            flow_name="small_job",
            coding_guidelines_path="docs/python_style.md",
            review_guidance=None,
            branch="agent/small/1-review",
            head_sha="abc123",
            review_result=_pre_pr_review_result(),
            design_doc_path=None,
            design_doc_markdown=None,
            design_pr_number=None,
            design_pr_url=None,
            cwd=tmp_path,
        )

    stderr = capsys.readouterr().err
    assert "mode=pre_pr_author_repair" in stderr
    assert "status=fault" in stderr


def test_respond_to_feedback_happy_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[:5] == ["codex", "exec", "resume", "--json", "--skip-git-repo-check"]
        thread_idx = cmd.index("thread-abc")
        assert cmd[thread_idx + 1] == "-"
        assert "--sandbox" not in cmd
        assert "--profile" not in cmd
        assert "--full-auto" in cmd
        assert cmd.index("--model") < thread_idx
        message_payload = json.dumps(
            {
                "review_replies": [{"review_comment_id": 101, "body": "Done"}],
                "general_comment": "Updated",
                "commit_message": "fix: update",
            }
        )
        return (
            '{"type":"thread.started","thread_id":"thread-resumed"}\n'
            + json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": message_payload},
                }
            )
            + "\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    configure_logging(verbose=True)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.respond_to_feedback(
        session=AgentSession(adapter="codex", thread_id="thread-abc"),
        turn=_feedback_turn(),
        cwd=tmp_path,
    )

    assert result.session.thread_id == "thread-resumed"
    assert result.review_replies[0].review_comment_id == 101
    assert result.general_comment == "Updated"
    assert result.commit_message == "fix: update"
    assert result.git_ops == ()
    assert result.flaky_test_report is None
    stderr = capsys.readouterr().err
    assert "event=feedback_agent_call_started issue_number=1 pr_number=8" in stderr
    assert (
        "event=feedback_agent_call_completed has_commit_message=true has_general_comment=true issue_number=1 pr_number=8 review_reply_count=1 thread_id=thread-resumed"
        in stderr
    )


def test_respond_to_roadmap_feedback_happy_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[:5] == ["codex", "exec", "resume", "--json", "--skip-git-repo-check"]
        assert input_text is not None
        assert "roadmap-feedback agent" in input_text
        assert "docs/roadmap/1-issue.graph.json" in input_text
        message_payload = json.dumps(
            {
                "review_replies": [{"review_comment_id": 101, "body": "Updated roadmap"}],
                "general_comment": "Updated roadmap artifacts",
                "commit_message": "docs: refine roadmap after review",
            }
        )
        return (
            '{"type":"thread.started","thread_id":"thread-roadmap"}\n'
            + json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": message_payload},
                }
            )
            + "\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    configure_logging(verbose=True)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.respond_to_roadmap_feedback(
        session=AgentSession(adapter="codex", thread_id="thread-abc"),
        turn=_roadmap_feedback_turn(),
        cwd=tmp_path,
    )

    assert result.session.thread_id == "thread-roadmap"
    assert result.review_replies[0].body == "Updated roadmap"
    assert result.general_comment == "Updated roadmap artifacts"
    assert result.commit_message == "docs: refine roadmap after review"
    assert result.escalation is None
    stderr = capsys.readouterr().err
    assert "mode=roadmap_feedback" in stderr


def test_respond_to_roadmap_feedback_rejects_escalation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = CodexAdapter(_enabled_config())
    monkeypatch.setattr(
        adapter,
        "_run_feedback_turn_resume",
        lambda **_: (
            {
                "review_replies": [],
                "general_comment": "Need a bigger replan.",
                "commit_message": None,
                "git_ops": [],
                "escalation": {
                    "kind": "roadmap_revision",
                    "summary": "Replan",
                    "details": "This roadmap needs a larger revision.",
                },
            },
            "thread-roadmap",
        ),
    )

    with pytest.raises(RuntimeError, match="roadmap feedback must not return escalation"):
        adapter.respond_to_roadmap_feedback(
            session=AgentSession(adapter="codex", thread_id="thread-123"),
            turn=_roadmap_feedback_turn(),
            cwd=tmp_path,
        )


def test_respond_to_feedback_falls_back_to_fresh_thread_on_context_window_exhaustion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cwd, input_text, check
        calls.append(cmd)
        if cmd[:3] == ["codex", "exec", "resume"]:
            raise CommandError(
                "Command failed\n"
                f"cmd: {' '.join(cmd)}\n"
                "exit: 1\n"
                'stdout:\n{"type":"error","message":"Codex ran out of room in the model\'s context window. Start a new thread."}\n'
                "stderr:\n"
            )

        assert cmd[:4] == ["codex", "exec", "--json", "--skip-git-repo-check"]
        assert "resume" not in cmd
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "review_replies": [{"review_comment_id": 101, "body": "Retried in new thread"}],
                    "general_comment": "Updated after thread reset",
                    "commit_message": "fix: continue after context-window exhaustion",
                }
            ),
            encoding="utf-8",
        )
        return '{"type":"thread.started","thread_id":"thread-fresh"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)

    adapter = CodexAdapter(_enabled_config())
    result = adapter.respond_to_feedback(
        session=AgentSession(adapter="codex", thread_id="thread-abc"),
        turn=_feedback_turn(),
        cwd=tmp_path,
    )

    assert len(calls) == 2
    assert calls[0][:3] == ["codex", "exec", "resume"]
    assert calls[1][:2] == ["codex", "exec"]
    assert calls[1][2] != "resume"
    assert result.session.thread_id == "thread-fresh"
    assert result.review_replies[0].body == "Retried in new thread"
    assert result.general_comment == "Updated after thread reset"
    assert result.commit_message == "fix: continue after context-window exhaustion"


def test_respond_to_feedback_re_raises_non_context_window_command_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cmd, cwd, input_text, check
        raise CommandError(
            "Command failed\n"
            "cmd: codex exec resume --json --skip-git-repo-check thread-abc -\n"
            "exit: 1\n"
            'stdout:\n{"type":"error","message":"transient CLI failure"}\n'
            "stderr:\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())

    with pytest.raises(CommandError, match="transient CLI failure"):
        adapter.respond_to_feedback(
            session=AgentSession(adapter="codex", thread_id="thread-abc"),
            turn=_feedback_turn(),
            cwd=tmp_path,
        )


def test_run_feedback_turn_resume_requires_thread_id(tmp_path: Path) -> None:
    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="thread_id"):
        adapter._run_feedback_turn_resume(
            session=AgentSession(adapter="codex", thread_id=None),
            prompt="prompt",
            cwd=tmp_path,
        )


def test_run_direct_turn_resume_requires_thread_id(tmp_path: Path) -> None:
    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="thread_id"):
        adapter._run_direct_turn_resume(
            session=AgentSession(adapter="codex", thread_id=None),
            prompt="prompt",
            cwd=tmp_path,
            invocation_mode="pre_pr_author_repair",
        )


def test_respond_to_feedback_requires_thread_id(tmp_path: Path) -> None:
    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="thread_id"):
        adapter.respond_to_feedback(
            session=AgentSession(adapter="codex", thread_id=None),
            turn=_feedback_turn(),
            cwd=tmp_path,
        )


def test_respond_to_feedback_logs_fault_when_final_message_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cmd, cwd, input_text, check
        return '{"type":"thread.started","thread_id":"thread-resumed"}\n'

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    configure_logging(verbose=True)

    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="did not emit"):
        adapter.respond_to_feedback(
            session=AgentSession(adapter="codex", thread_id="thread-abc"),
            turn=_feedback_turn(),
            cwd=tmp_path,
        )

    stderr = capsys.readouterr().err
    assert "event=codex_invocation_finished" in stderr
    assert "issue_number=1" in stderr
    assert "pr_number=8" in stderr
    assert "mode=respond_to_review" in stderr
    assert "status=fault" in stderr


def test_start_design_from_issue_rejects_disabled(tmp_path: Path) -> None:
    adapter = CodexAdapter(
        CodexConfig(enabled=False, model=None, sandbox=None, profile=None, extra_args=())
    )
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.start_design_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            design_doc_path="docs/design/1-issue.md",
            default_branch="main",
            cwd=tmp_path,
        )


def test_review_pre_pr_candidate_rejects_disabled(tmp_path: Path) -> None:
    adapter = CodexAdapter(
        CodexConfig(enabled=False, model=None, sandbox=None, profile=None, extra_args=())
    )
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.review_pre_pr_candidate(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            flow_name="small_job",
            coding_guidelines_path="docs/python_style.md",
            review_guidance=None,
            branch="agent/small/1-review",
            head_sha="abc123",
            changed_files=("src/mergexo/codex_adapter.py",),
            diff_text="diff --git a/src/mergexo/codex_adapter.py b/src/mergexo/codex_adapter.py",
            design_doc_path=None,
            design_doc_markdown=None,
            design_pr_number=None,
            design_pr_url=None,
            cwd=tmp_path,
        )


def test_start_bugfix_from_issue_rejects_disabled(tmp_path: Path) -> None:
    adapter = CodexAdapter(
        CodexConfig(enabled=False, model=None, sandbox=None, profile=None, extra_args=())
    )
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.start_bugfix_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            coding_guidelines_path="docs/python_style.md",
            cwd=tmp_path,
        )


def test_start_roadmap_from_issue_rejects_disabled(tmp_path: Path) -> None:
    adapter = CodexAdapter(
        CodexConfig(enabled=False, model=None, sandbox=None, profile=None, extra_args=())
    )
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.start_roadmap_from_issue(
            issue=Issue(number=1, title="Issue", body="Body", html_url="url", labels=("x",)),
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            roadmap_docs_dir="docs/roadmap",
            recommended_node_count=7,
            cwd=tmp_path,
        )


def test_parse_json_payload_variants_and_errors() -> None:
    fenced = '```json\n{"title":"x"}\n```'
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


def test_event_helpers_and_review_reply_parsing() -> None:
    assert (
        _extract_thread_id(
            '\n{"type":"turn.completed"}\n{"type":"thread.started","thread_id":"t1"}'
        )
        == "t1"
    )
    assert _extract_thread_id("not json") is None

    events = (
        "\n"
        "not json\n"
        '{"type":"item.completed","item":"skip"}\n'
        '{"type":"item.completed","item":{"type":"agent_message","text":"one"}}\n'
        '{"type":"item.completed","item":{"type":"agent_message","text":"two"}}\n'
    )
    assert _extract_final_agent_message(events) == "two"
    with pytest.raises(RuntimeError, match="did not emit"):
        _extract_final_agent_message('{"type":"turn.completed"}')

    assert _parse_event_line("not json") is None
    assert _parse_event_line("{broken") is None
    assert _parse_event_line('{"type":"x"}') == {"type": "x"}

    assert _optional_output_text(None) is None
    assert _optional_output_text("  hi ") == "hi"
    assert _optional_output_text("   ") is None
    with pytest.raises(RuntimeError, match="string or null"):
        _optional_output_text(3)

    replies = _parse_review_replies([{"review_comment_id": 1, "body": "ok"}])
    assert replies[0].review_comment_id == 1
    assert _parse_review_replies(None) == []
    with pytest.raises(RuntimeError, match="must be a list"):
        _parse_review_replies("bad")
    with pytest.raises(RuntimeError, match="must be an object"):
        _parse_review_replies(["bad"])
    with pytest.raises(RuntimeError, match="must be an object"):
        _parse_review_replies([{1: "bad"}])
    with pytest.raises(RuntimeError, match="must be an integer"):
        _parse_review_replies([{"review_comment_id": "1", "body": "x"}])
    with pytest.raises(RuntimeError, match="non-empty string"):
        _parse_review_replies([{"review_comment_id": 1, "body": ""}])
    assert _as_object_dict({1: "bad"}) is None


def test_parse_event_line_rejects_non_dict_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_loads(_: str) -> object:
        return []

    monkeypatch.setattr("mergexo.codex_adapter.json.loads", fake_loads)
    assert _parse_event_line("{}") is None


def test_filter_resume_extra_args_strips_unsupported_options() -> None:
    extra = (
        "--sandbox",
        "workspace-write",
        "--profile",
        "default",
        "--full-auto",
        "--sandbox=danger-full-access",
        "--profile=alt",
        "-s",
        "read-only",
        "-p",
        "dev",
        "--ephemeral",
    )
    assert _filter_resume_extra_args(extra) == ["--full-auto", "--ephemeral"]


def test_is_context_window_exhaustion_error_detection() -> None:
    assert _is_context_window_exhaustion_error(RuntimeError("not command")) is False
    assert (
        _is_context_window_exhaustion_error(
            CommandError(
                "Command failed\n"
                "cmd: codex exec resume --json --skip-git-repo-check thread-abc -\n"
                "exit: 1\n"
                'stdout:\n{"type":"error","message":"Codex ran out of room in the model\'s context window. Start a new thread."}\n'
                "stderr:\n"
            )
        )
        is True
    )


def test_parse_git_ops_validation() -> None:
    assert _parse_git_ops(None) == []
    assert [req.op for req in _parse_git_ops([{"op": "fetch_origin"}])] == ["fetch_origin"]
    assert [req.op for req in _parse_git_ops([{"op": "merge_origin_default_branch"}])] == [
        "merge_origin_default_branch"
    ]

    with pytest.raises(RuntimeError, match="must be a list"):
        _parse_git_ops("bad")
    with pytest.raises(RuntimeError, match="must be an object"):
        _parse_git_ops(["bad"])
    with pytest.raises(RuntimeError, match="must be one of"):
        _parse_git_ops([{"op": "unknown"}])


def test_parse_flaky_test_report_validation() -> None:
    assert _parse_flaky_test_report(None) is None
    parsed = _parse_flaky_test_report(
        {
            "run_id": 7001,
            "title": "Flaky unit test in scheduler shard",
            "summary": "Fails intermittently without code changes.",
            "relevant_log_excerpt": "AssertionError: expected 1 got 0",
        }
    )
    assert parsed is not None
    assert parsed.run_id == 7001
    assert parsed.title == "Flaky unit test in scheduler shard"

    with pytest.raises(RuntimeError, match="object or null"):
        _parse_flaky_test_report("bad")
    with pytest.raises(RuntimeError, match="must be an integer"):
        _parse_flaky_test_report({"run_id": "7001"})
    with pytest.raises(RuntimeError, match="must be >= 1"):
        _parse_flaky_test_report(
            {
                "run_id": 0,
                "title": "x",
                "summary": "y",
                "relevant_log_excerpt": "z",
            }
        )
    with pytest.raises(RuntimeError, match="title must be a non-empty string"):
        _parse_flaky_test_report(
            {
                "run_id": 1,
                "title": " ",
                "summary": "y",
                "relevant_log_excerpt": "z",
            }
        )
    with pytest.raises(RuntimeError, match="summary must be a non-empty string"):
        _parse_flaky_test_report(
            {
                "run_id": 1,
                "title": "x",
                "summary": " ",
                "relevant_log_excerpt": "z",
            }
        )
    with pytest.raises(RuntimeError, match="relevant_log_excerpt must be a non-empty string"):
        _parse_flaky_test_report(
            {
                "run_id": 1,
                "title": "x",
                "summary": "y",
                "relevant_log_excerpt": " ",
            }
        )


def test_parse_roadmap_revision_escalation_validation() -> None:
    assert _parse_roadmap_revision_escalation(None) is None
    escalation = _parse_roadmap_revision_escalation(
        {
            "kind": "roadmap_revision",
            "summary": "Assumption failed",
            "details": "Need to revise the plan.",
        }
    )
    assert escalation is not None
    assert escalation.kind == "roadmap_revision"

    with pytest.raises(RuntimeError, match="object or null"):
        _parse_roadmap_revision_escalation("bad")
    with pytest.raises(RuntimeError, match="kind must be roadmap_revision"):
        _parse_roadmap_revision_escalation(
            {
                "kind": "unknown",
                "summary": "x",
                "details": "y",
            }
        )
    with pytest.raises(RuntimeError, match="summary must be a non-empty string"):
        _parse_roadmap_revision_escalation(
            {
                "kind": "roadmap_revision",
                "summary": " ",
                "details": "y",
            }
        )
    with pytest.raises(RuntimeError, match="details must be a non-empty string"):
        _parse_roadmap_revision_escalation(
            {
                "kind": "roadmap_revision",
                "summary": "x",
                "details": " ",
            }
        )


def test_parse_direct_result_payload_preserves_escalation() -> None:
    result = _parse_direct_result_payload(
        {
            "pr_title": "Repair reviewer findings",
            "pr_summary": "Summary",
            "commit_message": "fix: repair",
            "blocked_reason": None,
            "escalation": {
                "kind": "roadmap_revision",
                "summary": "Assumption failed",
                "details": "Need to change the plan.",
            },
        }
    )

    assert result.pr_title == "Repair reviewer findings"
    assert result.escalation is not None
    assert result.escalation.kind == "roadmap_revision"


def test_parse_review_findings_validation() -> None:
    findings = _parse_review_findings(
        [
            {
                "finding_id": "R1",
                "path": "src/mergexo/codex_adapter.py",
                "line": 101,
                "title": "Validate reviewer output",
                "details": "Reject invalid outcome combinations.",
            }
        ]
    )
    assert findings[0].finding_id == "R1"

    with pytest.raises(RuntimeError, match="must be a list"):
        _parse_review_findings("bad")
    with pytest.raises(RuntimeError, match="must be an object"):
        _parse_review_findings(["bad"])
    with pytest.raises(RuntimeError, match="finding_id must be a non-empty string"):
        _parse_review_findings(
            [
                {
                    "finding_id": " ",
                    "path": "src/a.py",
                    "line": 1,
                    "title": "One",
                    "details": "First",
                }
            ]
        )
    with pytest.raises(RuntimeError, match="finding_id values must be unique"):
        _parse_review_findings(
            [
                {
                    "finding_id": "R1",
                    "path": "src/a.py",
                    "line": 1,
                    "title": "One",
                    "details": "First",
                },
                {
                    "finding_id": "R1",
                    "path": "src/b.py",
                    "line": 2,
                    "title": "Two",
                    "details": "Second",
                },
            ]
        )
    with pytest.raises(RuntimeError, match="path must be a non-empty string"):
        _parse_review_findings(
            [
                {
                    "finding_id": "R1",
                    "path": " ",
                    "line": 1,
                    "title": "One",
                    "details": "First",
                }
            ]
        )
    with pytest.raises(RuntimeError, match="line must be an integer >= 1 or null"):
        _parse_review_findings(
            [
                {
                    "finding_id": "R1",
                    "path": "src/a.py",
                    "line": 0,
                    "title": "One",
                    "details": "First",
                }
            ]
        )
    with pytest.raises(RuntimeError, match="title must be a non-empty string"):
        _parse_review_findings(
            [
                {
                    "finding_id": "R1",
                    "path": "src/a.py",
                    "line": 1,
                    "title": " ",
                    "details": "First",
                }
            ]
        )
    with pytest.raises(RuntimeError, match="details must be a non-empty string"):
        _parse_review_findings(
            [
                {
                    "finding_id": "R1",
                    "path": "src/a.py",
                    "line": 1,
                    "title": "One",
                    "details": " ",
                }
            ]
        )


def test_parse_pre_pr_review_result_payload_validation() -> None:
    approved = _parse_pre_pr_review_result_payload(
        {
            "outcome": "approved",
            "summary": "Looks good.",
            "findings": [],
            "escalation_reason": None,
        }
    )
    assert approved.outcome == "approved"

    changes_requested = _parse_pre_pr_review_result_payload(
        {
            "outcome": "changes_requested",
            "summary": "Needs a repair.",
            "findings": [
                {
                    "finding_id": "R1",
                    "path": "src/a.py",
                    "line": 10,
                    "title": "Fix this",
                    "details": "Repair the invalid contract combination.",
                }
            ],
            "escalation_reason": None,
        }
    )
    assert changes_requested.outcome == "changes_requested"
    assert changes_requested.findings[0].path == "src/a.py"

    escalated = _parse_pre_pr_review_result_payload(
        {
            "outcome": "escalate",
            "summary": "Needs human judgment.",
            "findings": [],
            "escalation_reason": "The candidate changes an ambiguous product contract.",
        }
    )
    assert escalated.outcome == "escalate"
    assert escalated.escalation_reason is not None

    with pytest.raises(RuntimeError, match="outcome must be one of"):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "maybe",
                "summary": "Needs a repair.",
                "findings": [],
                "escalation_reason": None,
            }
        )
    with pytest.raises(RuntimeError, match="approved outcome must not include findings"):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "approved",
                "summary": "Looks good.",
                "findings": [
                    {
                        "finding_id": "R1",
                        "path": "src/a.py",
                        "line": 10,
                        "title": "Fix this",
                        "details": "Repair the invalid contract combination.",
                    }
                ],
                "escalation_reason": None,
            }
        )
    with pytest.raises(RuntimeError, match="approved outcome must set escalation_reason to null"):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "approved",
                "summary": "Looks good.",
                "findings": [],
                "escalation_reason": "Needs a human sign-off anyway.",
            }
        )
    with pytest.raises(RuntimeError, match="requires at least one finding"):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "changes_requested",
                "summary": "Needs a repair.",
                "findings": [],
                "escalation_reason": None,
            }
        )
    with pytest.raises(
        RuntimeError, match="changes_requested outcome must set escalation_reason to null"
    ):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "changes_requested",
                "summary": "Needs a repair.",
                "findings": [
                    {
                        "finding_id": "R1",
                        "path": "src/a.py",
                        "line": 10,
                        "title": "Fix this",
                        "details": "Repair the invalid contract combination.",
                    }
                ],
                "escalation_reason": "Also asking for human review.",
            }
        )
    with pytest.raises(RuntimeError, match="requires a non-null escalation_reason"):
        _parse_pre_pr_review_result_payload(
            {
                "outcome": "escalate",
                "summary": "Needs human judgment.",
                "findings": [],
                "escalation_reason": None,
            }
        )


def test_respond_to_feedback_rejects_flaky_report_with_commit_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
        **_kwargs: object,
    ) -> str:
        _ = cmd, cwd, input_text, check
        message_payload = json.dumps(
            {
                "review_replies": [],
                "general_comment": None,
                "commit_message": "fix: should not be present",
                "flaky_test_report": {
                    "run_id": 7001,
                    "title": "Flaky run",
                    "summary": "Likely unrelated flake",
                    "relevant_log_excerpt": "traceback",
                },
            }
        )
        return (
            '{"type":"thread.started","thread_id":"thread-resumed"}\n'
            + json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": message_payload},
                }
            )
            + "\n"
        )

    monkeypatch.setattr("mergexo.codex_adapter.run", fake_run)
    adapter = CodexAdapter(_enabled_config())
    with pytest.raises(RuntimeError, match="cannot set commit_message"):
        adapter.respond_to_feedback(
            session=AgentSession(adapter="codex", thread_id="thread-abc"),
            turn=_feedback_turn(),
            cwd=tmp_path,
        )
