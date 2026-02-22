from __future__ import annotations

from pathlib import Path

from mergexo.agent_adapter import (
    AgentAdapter,
    DirectStartResult,
    AgentSession,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    ReviewReply,
)
from mergexo.models import (
    GeneratedDesign,
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
)


class DummyAdapter(AgentAdapter):
    def start_design_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        design_doc_path: str,
        default_branch: str,
        cwd: Path,
    ) -> DesignStartResult:
        _ = issue, repo_full_name, design_doc_path, default_branch, cwd
        return DesignStartResult(
            design=GeneratedDesign(
                title="t",
                design_doc_markdown="doc",
                touch_paths=("x",),
                summary="s",
            ),
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def respond_to_feedback(
        self,
        *,
        session: AgentSession,
        turn: FeedbackTurn,
        cwd: Path,
    ) -> FeedbackResult:
        _ = session, turn, cwd
        return FeedbackResult(
            session=AgentSession(adapter="dummy", thread_id="th"),
            review_replies=(ReviewReply(review_comment_id=1, body="ok"),),
            general_comment="done",
            commit_message="commit",
        )

    def start_bugfix_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        cwd: Path,
    ) -> DirectStartResult:
        _ = issue, repo_full_name, default_branch, cwd
        return DirectStartResult(
            pr_title="Fix bug",
            pr_summary="Summary",
            commit_message="fix: bug",
            blocked_reason=None,
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def start_small_job_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        cwd: Path,
    ) -> DirectStartResult:
        _ = issue, repo_full_name, default_branch, cwd
        return DirectStartResult(
            pr_title="Small job",
            pr_summary="Summary",
            commit_message="feat: small",
            blocked_reason=None,
            session=AgentSession(adapter="dummy", thread_id="th"),
        )


def test_agent_adapter_data_model() -> None:
    issue = Issue(number=1, title="t", body="b", html_url="u", labels=("x",))
    turn = FeedbackTurn(
        turn_key="turn-1",
        issue=issue,
        pull_request=PullRequestSnapshot(
            number=2,
            title="pr",
            body="desc",
            head_sha="h",
            base_sha="b",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(
            PullRequestReviewComment(
                comment_id=1,
                body="line",
                path="src/a.py",
                line=1,
                side="RIGHT",
                in_reply_to_id=None,
                user_login="r",
                html_url="u",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        issue_comments=(
            PullRequestIssueComment(
                comment_id=2,
                body="general",
                user_login="r",
                html_url="u2",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        changed_files=("src/a.py",),
    )

    adapter = DummyAdapter()
    start = adapter.start_design_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/1-t.md",
        default_branch="main",
        cwd=Path("."),
    )
    bugfix = adapter.start_bugfix_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        cwd=Path("."),
    )
    small_job = adapter.start_small_job_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        cwd=Path("."),
    )
    feedback = adapter.respond_to_feedback(
        session=AgentSession(adapter="dummy", thread_id="th"),
        turn=turn,
        cwd=Path("."),
    )

    assert start.session is not None
    assert start.session.thread_id == "th"
    assert bugfix.pr_title == "Fix bug"
    assert small_job.pr_title == "Small job"
    assert feedback.review_replies[0].review_comment_id == 1
