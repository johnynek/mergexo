from __future__ import annotations

from mergexo.agent_adapter import FeedbackTurn
from mergexo import __version__
from mergexo.models import (
    GeneratedDesign,
    Issue,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    WorkResult,
)
from mergexo.prompts import (
    build_bugfix_prompt,
    build_design_prompt,
    build_feedback_prompt,
    build_implementation_prompt,
    build_small_job_prompt,
)


def test_model_dataclasses_and_version() -> None:
    issue = Issue(number=1, title="t", body="b", html_url="u", labels=("x",))
    pr = PullRequest(number=2, html_url="pr")
    snapshot = PullRequestSnapshot(
        number=2,
        title="pr",
        body="desc",
        head_sha="h",
        base_sha="b",
        draft=False,
        state="open",
        merged=False,
    )
    review_comment = PullRequestReviewComment(
        comment_id=1,
        body="comment",
        path="src/a.py",
        line=7,
        side="RIGHT",
        in_reply_to_id=None,
        user_login="dev",
        html_url="u",
        created_at="t1",
        updated_at="t2",
    )
    issue_comment = PullRequestIssueComment(
        comment_id=2,
        body="note",
        user_login="dev",
        html_url="u",
        created_at="t1",
        updated_at="t2",
    )
    gen = GeneratedDesign(
        title="Title", design_doc_markdown="# Doc", touch_paths=("a.py",), summary="sum"
    )
    result = WorkResult(issue_number=1, branch="b", pr_number=2, pr_url="u")

    assert __version__ == "0.1.0"
    assert issue.labels == ("x",)
    assert pr.number == 2
    assert snapshot.head_sha == "h"
    assert review_comment.path == "src/a.py"
    assert issue_comment.body == "note"
    assert gen.touch_paths == ("a.py",)
    assert result.branch == "b"


def test_build_design_prompt_contains_required_contract() -> None:
    issue = Issue(
        number=12,
        title="Improve scheduler",
        body="Need to optimize queueing.",
        html_url="https://example/issue/12",
        labels=("agent:design",),
    )

    prompt = build_design_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/12-improve-scheduler.md",
        default_branch="main",
    )

    assert "You MUST report likely implementation file paths in touch_paths." in prompt
    assert "Return JSON only." in prompt
    assert "issue #12" in prompt.lower()
    assert "docs/design/12-improve-scheduler.md" in prompt


def test_build_feedback_prompt_contains_structured_sections() -> None:
    turn = FeedbackTurn(
        turn_key="turn-key-1",
        issue=Issue(number=9, title="t", body="b", html_url="u", labels=("x",)),
        pull_request=PullRequestSnapshot(
            number=5,
            title="PR",
            body="desc",
            head_sha="head",
            base_sha="base",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(
            PullRequestReviewComment(
                comment_id=11,
                body="Please rename",
                path="src/a.py",
                line=3,
                side="RIGHT",
                in_reply_to_id=None,
                user_login="reviewer",
                html_url="u",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        issue_comments=(
            PullRequestIssueComment(
                comment_id=21,
                body="General note",
                user_login="reviewer",
                html_url="u2",
                created_at="t3",
                updated_at="t4",
            ),
        ),
        changed_files=("src/a.py",),
    )

    prompt = build_feedback_prompt(turn=turn)

    assert "review_comment_id" in prompt
    assert "issue comments on the pr" in prompt.lower()
    assert "src/a.py" in prompt
    assert "turn_key" in prompt
    assert "If you provide commit_message" in prompt
    assert "Primary objective: resolve review feedback by editing repository files" in prompt
    assert "For comments on design docs" in prompt
    assert "Only skip file edits when blocked by genuine ambiguity" in prompt


def test_build_bugfix_prompt_requires_regression_tests() -> None:
    issue = Issue(
        number=21,
        title="Fix flaky scheduler",
        body="Queue occasionally stalls after retries.",
        html_url="https://example/issue/21",
        labels=("agent:bugfix",),
    )

    prompt = build_bugfix_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
    )

    assert "regression tests in tests/" in prompt
    assert "docs/python_style.md" in prompt
    assert "blocked_reason" in prompt
    assert "issue #21" in prompt.lower()


def test_build_small_job_prompt_is_scoped() -> None:
    issue = Issue(
        number=22,
        title="Add docs index",
        body="Create docs/index.md and link from README.",
        html_url="https://example/issue/22",
        labels=("agent:small-job",),
    )

    prompt = build_small_job_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
    )

    assert "small-job agent" in prompt
    assert "docs/python_style.md" in prompt
    assert "blocked_reason" in prompt
    assert "Keep scope tight" in prompt


def test_build_implementation_prompt_links_design_context() -> None:
    issue = Issue(
        number=23,
        title="Ship worker pool scheduling",
        body="Implement the accepted design and rollout checks.",
        html_url="https://example/issue/23",
        labels=("agent:design",),
    )

    prompt = build_implementation_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        design_doc_path="docs/design/23-ship-worker-pool.md",
        design_doc_markdown="# Design\n\nDo the thing.",
        design_pr_number=144,
        design_pr_url="https://example/pr/144",
    )

    assert "implementation agent" in prompt
    assert "docs/python_style.md" in prompt
    assert "docs/design/23-ship-worker-pool.md" in prompt
    assert "Source design PR: #144 (https://example/pr/144)" in prompt
    assert "blocked_reason" in prompt
    assert "Merged design doc markdown" in prompt
