from __future__ import annotations

from mergexo import __version__
from mergexo.models import GeneratedDesign, Issue, PullRequest, WorkResult
from mergexo.prompts import build_design_prompt


def test_model_dataclasses_and_version() -> None:
    issue = Issue(number=1, title="t", body="b", html_url="u", labels=("x",))
    pr = PullRequest(number=2, html_url="pr")
    gen = GeneratedDesign(title="Title", design_doc_markdown="# Doc", touch_paths=("a.py",), summary="sum")
    result = WorkResult(issue_number=1, branch="b", pr_number=2, pr_url="u")

    assert __version__ == "0.1.0"
    assert issue.labels == ("x",)
    assert pr.number == 2
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
