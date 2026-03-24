from __future__ import annotations

from pathlib import Path
import json

import pytest

from mergexo.agent_adapter import AgentSession, FeedbackTurn
from mergexo.codex_adapter import (
    CodexAdapter,
    _as_object_dict,
    _extract_final_agent_message,
    _extract_thread_id,
    _filter_resume_extra_args,
    _is_context_window_exhaustion_error,
    _parse_flaky_test_report,
    _parse_git_ops,
    _optional_output_text,
    _parse_event_line,
    _parse_json_payload,
    _parse_roadmap_revision_escalation,
    _parse_review_replies,
    _require_str,
    _require_str_list,
)
from mergexo.config import CodexConfig
from mergexo.models import (
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
)
from mergexo.observability import configure_logging
from mergexo.shell import CommandError


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


def test_start_roadmap_from_issue_happy_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "roadmap agent" in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Roadmap",
                    "summary": "Summary",
                    "roadmap_markdown": "# Roadmap",
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
    assert result.session is not None
    assert result.session.thread_id == "thread-roadmap"


def test_start_roadmap_from_issue_requires_graph_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "title": "Roadmap",
                    "summary": "Summary",
                    "roadmap_markdown": "# Roadmap",
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
    ) -> str:
        _ = cwd, check
        assert input_text is not None
        assert "roadmap-adjustment agent" in input_text
        assert '["n2","n3"]' in input_text
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "request_revision",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
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
        roadmap_status_report="status report",
        roadmap_markdown="# Roadmap",
        canonical_graph_json='{"roadmap_issue_number":1}',
        cwd=tmp_path,
    )

    assert result.action == "request_revision"
    assert result.summary == "Need revision"
    assert "revised roadmap" in result.details


def test_evaluate_roadmap_adjustment_rejects_invalid_action(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        idx = cmd.index("--output-last-message")
        Path(cmd[idx + 1]).write_text(
            json.dumps(
                {
                    "action": "maybe",
                    "summary": "Need revision",
                    "details": "The current frontier needs a revised roadmap.",
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


def test_respond_to_feedback_rejects_flaky_report_with_commit_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
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
