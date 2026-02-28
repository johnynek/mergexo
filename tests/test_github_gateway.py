from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest

from mergexo.github_gateway import (
    CompareCommitsStatus,
    GitHubAuthenticationError,
    GitHubGateway,
    GitHubPollingError,
    _as_bool,
    _as_int,
    _as_optional_int,
    _as_optional_str,
    _as_object_dict,
    _as_string,
    _normalize_optional_lower_str,
    _parse_http_response,
    _preview_for_log,
    _tail_lines,
)
from mergexo.models import Issue, WorkflowJobSnapshot
from mergexo.observability import configure_logging
from mergexo.shell import CommandError


@pytest.fixture(autouse=True)
def _disable_gateway_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("mergexo.github_gateway.time.sleep", lambda _: None)


def test_list_open_issues_with_any_labels_dedupes(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_list(self: GitHubGateway, label: str) -> list[Issue]:
        _ = self
        if label == "agent:design":
            return [
                Issue(
                    number=2,
                    title="Two",
                    body="b",
                    html_url="u2",
                    labels=("agent:design",),
                    author_login="alice",
                ),
                Issue(
                    number=1,
                    title="One",
                    body="b",
                    html_url="u1",
                    labels=("agent:design",),
                    author_login="bob",
                ),
            ]
        if label == "agent:bugfix":
            return [
                Issue(
                    number=2,
                    title="Two",
                    body="b",
                    html_url="u2",
                    labels=("agent:bugfix",),
                    author_login="",
                ),
            ]
        return []

    monkeypatch.setattr(GitHubGateway, "list_open_issues_with_label", fake_list)

    issues = gateway.list_open_issues_with_any_labels(("agent:design", "agent:bugfix"))
    assert [issue.number for issue in issues] == [1, 2]
    assert issues[1].labels == ("agent:design", "agent:bugfix")
    assert issues[1].author_login == "alice"


def test_list_open_issues_with_label_filters_and_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {
            "number": 1,
            "title": "Issue",
            "body": "Body",
            "html_url": "u",
            "user": {"login": "  Alice  "},
            "labels": [{"name": "x"}, 9],
        },
        {"pull_request": {"url": "pr"}, "number": 2},
        "skip",
    ]

    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway, method: str, path: str, payload: dict[str, object] | None = None
    ) -> object:
        _ = self
        _ = payload
        assert method == "GET"
        assert "/repos/o/r/issues?" in path
        return payload_for_test

    payload_for_test = payload
    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    issues = gateway.list_open_issues_with_label("agent:design")
    assert len(issues) == 1
    assert issues[0].number == 1
    assert issues[0].labels == ("x",)
    assert issues[0].author_login == "alice"


def test_list_open_issues_rejects_non_list(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: {"bad": "shape"},
    )

    with pytest.raises(RuntimeError, match="expected list"):
        gateway.list_open_issues_with_label("l")


def test_create_pull_request_and_comment(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_api(
        self: GitHubGateway, method: str, path: str, payload: dict[str, object] | None = None
    ) -> object:
        _ = self
        calls.append((method, path, payload))
        if path.endswith("/pulls"):
            return {"number": "123", "html_url": "https://example/pr/123"}
        return {"ok": True}

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    pr = gateway.create_pull_request("t", "head", "main", "body")
    gateway.post_issue_comment(7, "hello")
    gateway.post_review_comment_reply(7, 55, "reply")

    assert pr.number == 123
    assert pr.html_url == "https://example/pr/123"
    assert calls[0][0] == "POST"
    assert calls[1][1].endswith("/issues/7/comments")
    assert calls[2][1].endswith("/pulls/7/comments")
    assert calls[2][2] == {"body": "reply", "in_reply_to": 55}


def test_find_pull_request_by_head(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        parsed = urlparse(path)
        params = parse_qs(parsed.query)
        assert params["head"] == ["o:agent/design/7-worker"]
        assert params["base"] == ["main"]
        assert params["state"] == ["open"]
        return [
            {"number": 99, "html_url": "https://example/pr/99"},
            {"number": 101, "html_url": "https://example/pr/101"},
        ]

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    pr = gateway.find_pull_request_by_head(head="agent/design/7-worker", base="main")
    assert pr is not None
    assert pr.number == 101
    assert pr.html_url == "https://example/pr/101"


def test_find_pull_request_by_head_returns_none_when_no_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])
    assert gateway.find_pull_request_by_head(head="agent/design/7-worker", base="main") is None


def test_find_pull_request_by_head_rejects_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: {"bad": "shape"},
    )
    with pytest.raises(RuntimeError, match="expected list for pull request lookup"):
        gateway.find_pull_request_by_head(head="agent/design/7-worker", base="main")


def test_find_pull_request_by_head_skips_non_object_items(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: [
            1,
            {"number": 8, "html_url": "https://example/pr/8"},
        ],
    )
    pr = gateway.find_pull_request_by_head(head="agent/design/7-worker", base="main")
    assert pr is not None
    assert pr.number == 8
    assert pr.html_url == "https://example/pr/8"


def test_find_pull_request_by_head_rejects_invalid_state() -> None:
    gateway = GitHubGateway("o", "r")
    with pytest.raises(ValueError, match="state must be 'open' or 'all'"):
        gateway.find_pull_request_by_head(head="h", state="closed")  # type: ignore[arg-type]


def test_get_issue_parses_object(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, method, payload
        assert path.endswith("/issues/9")
        return {
            "number": 9,
            "title": "Issue title",
            "body": "Issue body",
            "html_url": "https://example/issue/9",
            "user": {"login": "Bob"},
            "labels": [{"name": "agent:design"}, "skip"],
        }

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    issue = gateway.get_issue(9)
    assert issue.number == 9
    assert issue.labels == ("agent:design",)
    assert issue.author_login == "bob"


def test_get_issue_rejects_non_object(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])

    with pytest.raises(RuntimeError, match="expected object for issue"):
        gateway.get_issue(1)


def test_create_pull_request_rejects_non_object(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])

    with pytest.raises(RuntimeError, match="expected object"):
        gateway.create_pull_request("t", "h", "b", "x")


def test_post_write_failures_emit_failed_events(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    gateway = GitHubGateway("o", "r")
    configure_logging(verbose=True)

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, path, payload
        if method == "POST":
            raise RuntimeError("boom")
        return {"ok": True}

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    with pytest.raises(RuntimeError, match="boom"):
        gateway.post_issue_comment(7, "hello")
    with pytest.raises(RuntimeError, match="boom"):
        gateway.post_review_comment_reply(7, 55, "reply")

    text = capsys.readouterr().err
    assert "event=github_issue_comment_failed" in text
    assert "issue_number=7" in text
    assert "repo_full_name=o/r" in text
    assert "event=github_review_reply_failed" in text
    assert "pr_number=7" in text
    assert "review_comment_id=55" in text


def test_pull_request_related_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, method, payload
        if path.endswith("/pulls/9"):
            return {
                "number": 9,
                "title": "PR title",
                "body": "desc",
                "head": {"sha": "headsha"},
                "base": {"sha": "basesha"},
                "draft": False,
                "state": "open",
                "merged": False,
            }
        if path.endswith("/pulls/9/files?per_page=100"):
            return ["skip", {"filename": "src/a.py"}, {"filename": "README.md"}]
        if path.endswith("/pulls/9/comments?per_page=100&page=1"):
            return [
                "skip",
                {
                    "id": 11,
                    "body": "line comment",
                    "path": "src/a.py",
                    "line": 10,
                    "side": "RIGHT",
                    "in_reply_to_id": None,
                    "user": {"login": "reviewer"},
                    "html_url": "http://review",
                    "created_at": "t1",
                    "updated_at": "t2",
                },
            ]
        if path.endswith("/pulls/9/reviews?per_page=100&page=1"):
            return [
                "skip",
                {
                    "id": 33,
                    "body": "Top-level review summary",
                    "state": "COMMENTED",
                    "user": {"login": "reviewer"},
                    "html_url": "http://review-summary",
                    "submitted_at": "t2.5",
                },
            ]
        if path.endswith("/issues/9/comments?per_page=100&page=1"):
            return [
                "skip",
                {
                    "id": 22,
                    "body": "general",
                    "user": {"login": "reviewer"},
                    "html_url": "http://issue",
                    "created_at": "t3",
                    "updated_at": "t4",
                },
            ]
        raise AssertionError(path)

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    pr = gateway.get_pull_request(9)
    files = gateway.list_pull_request_files(9)
    review_comments = gateway.list_pull_request_review_comments(9)
    review_summaries = gateway.list_pull_request_review_summaries(9)
    issue_comments = gateway.list_pull_request_issue_comments(9)
    issue_comments_direct = gateway.list_issue_comments(9)

    assert pr.number == 9
    assert pr.head_sha == "headsha"
    assert pr.state == "open"
    assert pr.merged is False
    assert files == ("src/a.py", "README.md")
    assert review_comments[0].comment_id == 11
    assert review_comments[0].user_login == "reviewer"
    assert review_summaries[0].comment_id == 33
    assert review_summaries[0].body == "Top-level review summary"
    assert issue_comments[0].comment_id == 22
    assert issue_comments_direct[0].comment_id == 22


def test_list_pull_request_review_comments_paginates_with_since(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    seen_pages: list[int] = []

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert path.startswith("/repos/o/r/pulls/9/comments?")
        parsed = urlparse(path)
        query = parse_qs(parsed.query)
        assert query["per_page"] == ["100"]
        assert query["since"] == ["2026-02-22T00:00:00Z"]
        page = int(query["page"][0])
        seen_pages.append(page)
        if page == 1:
            return [
                {
                    "id": item_id,
                    "body": f"review-{item_id}",
                    "path": "src/a.py",
                    "line": 1,
                    "side": "RIGHT",
                    "in_reply_to_id": None,
                    "user": {"login": "reviewer"},
                    "html_url": f"https://example/review/{item_id}",
                    "created_at": "2026-02-22T00:00:00Z",
                    "updated_at": "2026-02-22T00:00:00Z",
                }
                for item_id in range(1, 101)
            ]
        if page == 2:
            return [
                {
                    "id": 101,
                    "body": "review-101",
                    "path": "src/a.py",
                    "line": 1,
                    "side": "RIGHT",
                    "in_reply_to_id": None,
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/review/101",
                    "created_at": "2026-02-22T00:00:01Z",
                    "updated_at": "2026-02-22T00:00:01Z",
                },
                {
                    "id": 102,
                    "body": "review-102",
                    "path": "src/a.py",
                    "line": 1,
                    "side": "RIGHT",
                    "in_reply_to_id": None,
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/review/102",
                    "created_at": "2026-02-22T00:00:02Z",
                    "updated_at": "2026-02-22T00:00:02Z",
                },
            ]
        raise AssertionError(path)

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    comments = gateway.list_pull_request_review_comments(9, since="2026-02-22T00:00:00Z")
    assert len(comments) == 102
    assert comments[0].comment_id == 1
    assert comments[-1].comment_id == 102
    assert seen_pages == [1, 2]


def test_list_pull_request_review_summaries_paginates_and_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    seen_pages: list[int] = []

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert path.startswith("/repos/o/r/pulls/9/reviews?")
        parsed = urlparse(path)
        query = parse_qs(parsed.query)
        assert query["per_page"] == ["100"]
        page = int(query["page"][0])
        seen_pages.append(page)
        if page == 1:
            return [
                {
                    "id": 10,
                    "body": "needs work",
                    "state": "COMMENTED",
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/reviews/10",
                    "submitted_at": "2026-02-22T00:00:00Z",
                },
                {
                    "id": 11,
                    "body": "",
                    "state": "COMMENTED",
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/reviews/11",
                    "submitted_at": "2026-02-22T00:00:01Z",
                },
                {
                    "id": 12,
                    "body": "looks good",
                    "state": "APPROVED",
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/reviews/12",
                    "submitted_at": "2026-02-22T00:00:02Z",
                },
            ] + [
                {
                    "id": item_id,
                    "body": f"summary-{item_id}",
                    "state": "COMMENTED",
                    "user": {"login": "reviewer"},
                    "html_url": f"https://example/reviews/{item_id}",
                    "submitted_at": "2026-02-22T00:00:03Z",
                }
                for item_id in range(13, 111)
            ]
        if page == 2:
            return [
                {
                    "id": 111,
                    "body": "changes requested",
                    "state": "CHANGES_REQUESTED",
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/reviews/111",
                    "submitted_at": "2026-02-22T00:00:04Z",
                }
            ]
        raise AssertionError(path)

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    summaries = gateway.list_pull_request_review_summaries(9)
    assert len(summaries) == 100
    assert summaries[0].comment_id == 10
    assert summaries[-1].comment_id == 111
    assert all(summary.body for summary in summaries)
    assert seen_pages == [1, 2]


def test_list_issue_comments_paginates_with_since(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    seen_pages: list[int] = []

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert path.startswith("/repos/o/r/issues/7/comments?")
        parsed = urlparse(path)
        query = parse_qs(parsed.query)
        assert query["per_page"] == ["100"]
        assert query["since"] == ["2026-02-22T00:00:00Z"]
        page = int(query["page"][0])
        seen_pages.append(page)
        if page == 1:
            return [
                {
                    "id": item_id,
                    "body": f"issue-{item_id}",
                    "user": {"login": "reviewer"},
                    "html_url": f"https://example/issue/{item_id}",
                    "created_at": "2026-02-22T00:00:00Z",
                    "updated_at": "2026-02-22T00:00:00Z",
                }
                for item_id in range(1, 101)
            ]
        if page == 2:
            return [
                {
                    "id": 101,
                    "body": "issue-101",
                    "user": {"login": "reviewer"},
                    "html_url": "https://example/issue/101",
                    "created_at": "2026-02-22T00:00:01Z",
                    "updated_at": "2026-02-22T00:00:01Z",
                }
            ]
        raise AssertionError(path)

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    comments = gateway.list_issue_comments(7, since="2026-02-22T00:00:00Z")
    assert len(comments) == 101
    assert comments[0].comment_id == 1
    assert comments[-1].comment_id == 101
    assert seen_pages == [1, 2]


def test_get_pull_request_requires_head_and_base(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, method, path, payload
        return {
            "number": 1,
            "title": "t",
            "body": "b",
            "draft": False,
            "state": "open",
            "merged": False,
        }

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    with pytest.raises(RuntimeError, match="missing pull request head/base"):
        gateway.get_pull_request(1)


@pytest.mark.parametrize(
    "status",
    ("ahead", "identical", "behind", "diverged"),
)
def test_compare_commits_parses_known_statuses(
    monkeypatch: pytest.MonkeyPatch, status: CompareCommitsStatus
) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert path == "/repos/o/r/compare/base123...head456"
        return {"status": status}

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    assert gateway.compare_commits("base123", "head456") == status


def test_compare_commits_rejects_bad_payload_and_status(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])
    with pytest.raises(RuntimeError, match="expected object for compare commits"):
        gateway.compare_commits("a", "b")

    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: {"status": "unknown_status"},
    )
    with pytest.raises(RuntimeError, match="Unexpected GitHub compare status"):
        gateway.compare_commits("a", "b")


def test_list_workflow_runs_for_head_parses_and_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert "/actions/runs?" in path
        assert "event=pull_request" in path
        assert "head_sha=head123" in path
        return {
            "workflow_runs": [
                {
                    "id": 101,
                    "name": "ci",
                    "status": "completed",
                    "conclusion": "failure",
                    "html_url": "https://example/runs/101",
                    "head_sha": "head123",
                    "created_at": "t1",
                    "updated_at": "t2",
                    "pull_requests": [{"number": 7}],
                },
                {
                    "id": 102,
                    "name": "other",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://example/runs/102",
                    "head_sha": "head123",
                    "created_at": "t1",
                    "updated_at": "t2",
                    "pull_requests": [{"number": 999}],
                },
            ]
        }

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    runs = gateway.list_workflow_runs_for_head(7, "head123")
    assert len(runs) == 1
    assert runs[0].run_id == 101
    assert runs[0].conclusion == "failure"


def test_list_workflow_runs_for_head_rejects_bad_payload_and_skips_non_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])
    with pytest.raises(RuntimeError, match="expected object for workflow runs"):
        gateway.list_workflow_runs_for_head(7, "head123")

    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: {})
    with pytest.raises(RuntimeError, match="expected workflow_runs list"):
        gateway.list_workflow_runs_for_head(7, "head123")

    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: {"workflow_runs": ["skip"]},
    )
    assert gateway.list_workflow_runs_for_head(7, "head123") == ()


def test_list_workflow_jobs_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        assert method == "GET"
        assert path.endswith("/actions/runs/101/jobs?per_page=100")
        return {
            "jobs": [
                {
                    "id": 201,
                    "name": "lint",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://example/jobs/201",
                },
                {
                    "id": 202,
                    "name": "tests",
                    "status": "completed",
                    "conclusion": "failure",
                    "html_url": "https://example/jobs/202",
                },
            ]
        }

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    jobs = gateway.list_workflow_jobs(101)
    assert len(jobs) == 2
    assert jobs[1].job_id == 202
    assert jobs[1].conclusion == "failure"


def test_list_workflow_jobs_rejects_bad_payload_and_skips_non_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])
    with pytest.raises(RuntimeError, match="expected object for workflow jobs"):
        gateway.list_workflow_jobs(101)

    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: {})
    with pytest.raises(RuntimeError, match="expected jobs list"):
        gateway.list_workflow_jobs(101)

    monkeypatch.setattr(
        GitHubGateway,
        "_api_json",
        lambda self, method, path, payload=None: {"jobs": ["skip"]},
    )
    assert gateway.list_workflow_jobs(101) == ()


def test_get_failed_run_log_tails_extracts_tails_and_handles_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")

    def fake_jobs(self: GitHubGateway, run_id: int) -> tuple[WorkflowJobSnapshot, ...]:
        _ = self, run_id
        return (
            WorkflowJobSnapshot(
                job_id=10,
                name="tests",
                status="completed",
                conclusion="failure",
                html_url="https://example/jobs/10",
            ),
            WorkflowJobSnapshot(
                job_id=11,
                name="tests",
                status="completed",
                conclusion="timed_out",
                html_url="https://example/jobs/11",
            ),
            WorkflowJobSnapshot(
                job_id=12,
                name="lint",
                status="completed",
                conclusion="success",
                html_url="https://example/jobs/12",
            ),
        )

    def fake_api_text(self: GitHubGateway, method: str, path: str) -> str:
        _ = self
        assert method == "GET"
        if path.endswith("/actions/jobs/10/logs"):
            return "a\nb\nc\nd"
        raise RuntimeError("logs unavailable")

    monkeypatch.setattr(GitHubGateway, "list_workflow_jobs", fake_jobs)
    monkeypatch.setattr(GitHubGateway, "_api_text", fake_api_text)

    tails = gateway.get_failed_run_log_tails(101, tail_lines_per_action=2)
    assert tails == {
        "tests [job 10]": "c\nd",
        "tests [job 11]": None,
    }
    with pytest.raises(ValueError, match="tail_lines_per_action"):
        gateway.get_failed_run_log_tails(101, tail_lines_per_action=0)


def test_get_failed_run_log_tails_returns_empty_when_no_failed_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = GitHubGateway("o", "r")

    monkeypatch.setattr(
        GitHubGateway,
        "list_workflow_jobs",
        lambda self, run_id: (
            WorkflowJobSnapshot(
                job_id=10,
                name="lint",
                status="completed",
                conclusion="success",
                html_url="https://example/jobs/10",
            ),
        ),
    )
    assert gateway.get_failed_run_log_tails(101, tail_lines_per_action=50) == {}


@pytest.mark.parametrize(
    "method_name, bad_payload, expected",
    [
        ("get_pull_request", [], "expected object"),
        ("list_pull_request_files", {}, "expected list"),
        ("list_pull_request_review_comments", {}, "expected list"),
        ("list_pull_request_review_summaries", {}, "expected list"),
        ("list_pull_request_issue_comments", {}, "expected list"),
        ("list_issue_comments", {}, "expected list"),
    ],
)
def test_pull_request_related_fetch_errors(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    bad_payload: object,
    expected: str,
) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(
        GitHubGateway, "_api_json", lambda self, method, path, payload=None: bad_payload
    )

    method = getattr(gateway, method_name)
    with pytest.raises(RuntimeError, match=expected):
        method(1)


def test_api_json_invokes_gh_api(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[list[str], str | None, bool]] = []

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cwd
        calls.append((cmd, input_text, check))
        if "--include" in cmd:
            return "\n".join(
                (
                    "HTTP/2.0 200 OK",
                    'ETag: "etag-1"',
                    "",
                    '{"ok": true}',
                )
            )
        return json.dumps({"ok": True})

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")

    out_get = gateway._api_json("GET", "/path")
    out_post = gateway._api_json("POST", "/path", payload={"k": "v"})

    assert out_get == {"ok": True}
    assert out_post == {"ok": True}
    assert calls[0][0] == ["gh", "api", "--method", "GET", "--include", "/path"]
    assert calls[0][2] is False
    assert "--input" in calls[1][0]
    assert calls[1][1] == '{"k": "v"}'
    assert calls[1][2] is True


def test_api_json_get_uses_if_none_match_and_reuses_cached_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cwd, input_text
        assert check is False
        calls.append(cmd)
        if len(calls) == 1:
            return "\n".join(
                (
                    "HTTP/2.0 200 OK",
                    'ETag: "etag-2"',
                    "",
                    '{"value": 7}',
                )
            )
        assert len(calls) == 2
        return "\n".join(("HTTP/2.0 304 Not Modified", "", ""))

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")
    first = gateway._api_json("GET", "/path")
    second = gateway._api_json("GET", "/path")

    assert first == {"value": 7}
    assert second == {"value": 7}
    assert "--header" not in calls[0]
    assert "--header" in calls[1]
    header_index = calls[1].index("--header")
    assert calls[1][header_index + 1] == 'If-None-Match: "etag-2"'


def test_api_json_get_rejects_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cmd, cwd, input_text, check
        return "\n".join(
            (
                "HTTP/2.0 403 Forbidden",
                "",
                '{"message":"forbidden"}',
            )
        )

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")
    with pytest.raises(RuntimeError, match="status 403"):
        gateway._api_json("GET", "/path")


def test_api_json_get_wraps_malformed_http_as_polling_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(verbose=True)

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cmd, cwd, input_text, check
        return "not-http"

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")
    with pytest.raises(GitHubPollingError, match="GitHub polling GET failed for path /path"):
        gateway._api_json("GET", "/path")

    text = capsys.readouterr().err
    assert "event=github_poll_get_failed" in text
    assert "path=/path" in text
    assert "raw_preview=not-http" in text


def test_api_json_get_raises_authentication_error_after_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"api": 0, "auth": 0}

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cwd, input_text, check
        if cmd[:3] == ["gh", "auth", "status"]:
            calls["auth"] += 1
            raise CommandError("gh: not logged into any GitHub hosts. Run gh auth login")
        calls["api"] += 1
        return "not-http"

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)
    gateway = GitHubGateway("o", "r")

    with pytest.raises(GitHubAuthenticationError, match="not authenticated"):
        gateway._api_json("GET", "/path")

    assert calls["api"] == 3
    assert calls["auth"] == 1


def test_api_json_post_retries_command_failures_and_wraps_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cmd, cwd, input_text, check
        nonlocal calls
        calls += 1
        raise CommandError("Authentication failed. Run gh auth login")

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)
    gateway = GitHubGateway("o", "r")

    with pytest.raises(GitHubAuthenticationError, match="not authenticated"):
        gateway._api_json("POST", "/path", payload={"k": "v"})

    assert calls == 3


def test_api_helpers_raise_unreachable_runtime_when_max_attempts_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("mergexo.github_gateway._GH_API_MAX_ATTEMPTS", 0)
    gateway = GitHubGateway("o", "r")

    with pytest.raises(RuntimeError, match="Unreachable GitHub API text retry state"):
        gateway._api_text("GET", "/path")
    with pytest.raises(RuntimeError, match="Unreachable GitHub API JSON GET retry state"):
        gateway._api_json("GET", "/path")
    with pytest.raises(RuntimeError, match="Unreachable GitHub API JSON retry state"):
        gateway._api_json("POST", "/path")


def test_classify_github_failure_handles_raw_auth_text_and_non_get_methods() -> None:
    gateway = GitHubGateway("o", "r")

    from_raw = gateway._classify_github_failure(
        method="POST",
        path="/x",
        exc=RuntimeError("boom"),
        raw="Authentication failed. Run gh auth login",
        probe_auth=False,
    )
    assert isinstance(from_raw, GitHubAuthenticationError)

    non_get = gateway._classify_github_failure(
        method="PATCH",
        path="/x",
        exc=RuntimeError("boom"),
        raw="",
        probe_auth=False,
    )
    assert isinstance(non_get, GitHubPollingError)
    assert "GitHub API PATCH failed for path /x" in str(non_get)

    assert gateway._is_authentication_error_text("") is False


def test_api_json_get_rejects_not_modified_without_cached_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cmd, cwd, input_text, check
        return "\n".join(("HTTP/2.0 304 Not Modified", "", ""))

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")
    gateway._etags_by_path["/path"] = '"etag-3"'
    with pytest.raises(RuntimeError, match="uncached path"):
        gateway._api_json("GET", "/path")


def test_api_text_get_success_and_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cwd, input_text
        calls.append(cmd)
        assert check is False
        if len(calls) == 1:
            return "\n".join(("HTTP/2.0 200 OK", "", "line1\nline2"))
        return "\n".join(("HTTP/2.0 404 Not Found", "", "missing"))

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")
    with pytest.raises(ValueError, match="only supports GET"):
        gateway._api_text("POST", "/path")
    assert gateway._api_text("GET", "/path") == "line1\nline2"
    with pytest.raises(GitHubPollingError, match="GitHub polling GET failed for path /path"):
        gateway._api_text("GET", "/path")


def test_parse_http_response_handles_multiple_status_lines() -> None:
    status, headers, body = _parse_http_response(
        "\n".join(
            (
                "HTTP/2.0 301 Moved Permanently",
                "Location: somewhere",
                "",
                "HTTP/2.0 200 OK",
                'ETag: "abc"',
                "X-Ignored",
                "",
                '{"ok": true}',
            )
        )
    )

    assert status == 200
    assert headers == {"etag": '"abc"'}
    assert body == '{"ok": true}'


def test_parse_http_response_rejects_bad_status_output() -> None:
    with pytest.raises(RuntimeError, match="missing HTTP status"):
        _parse_http_response("not-http")

    with pytest.raises(RuntimeError, match="status line"):
        _parse_http_response("HTTP/2.0\n\n{}")

    with pytest.raises(RuntimeError, match="status line"):
        _parse_http_response("HTTP/2.0 okay\n\n{}")


def test_preview_for_log_handles_empty_and_long_values() -> None:
    assert _preview_for_log("") == "<empty>"
    assert _preview_for_log("abcdef", limit=4) == "abcd..."


def test_gateway_emits_read_and_write_logs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    gateway = GitHubGateway("o", "r")
    configure_logging(verbose=True)

    def fake_api(
        self: GitHubGateway,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> object:
        _ = self, payload
        if method == "GET" and path.startswith("/repos/o/r/issues?"):
            return [{"number": 1, "title": "Issue", "body": "", "html_url": "u", "labels": []}]
        if method == "GET" and path.endswith("/pulls/9/files?per_page=100"):
            return [{"filename": "src/a.py"}]
        if method == "GET" and path.endswith("/pulls/9/comments?per_page=100&page=1"):
            return []
        if method == "GET" and path.endswith("/pulls/9/reviews?per_page=100&page=1"):
            return []
        if method == "GET" and path.endswith("/issues/9/comments?per_page=100&page=1"):
            return []
        if method == "GET" and path.endswith("/compare/base...head"):
            return {"status": "ahead"}
        if method == "POST" and path.endswith("/pulls"):
            return {"number": 9, "html_url": "https://example/pr/9"}
        if method == "POST":
            return {"ok": True}
        raise AssertionError((method, path))

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    gateway.list_open_issues_with_label("agent:design")
    gateway.list_pull_request_files(9)
    gateway.list_pull_request_review_comments(9)
    gateway.list_pull_request_review_summaries(9)
    gateway.list_pull_request_issue_comments(9)
    gateway.list_issue_comments(9)
    gateway.compare_commits("base", "head")
    gateway.create_pull_request("t", "h", "b", "body")
    gateway.post_issue_comment(9, "hello")
    gateway.post_review_comment_reply(9, 10, "reply")

    text = capsys.readouterr().err
    assert "event=github_read count=1 endpoint=issues" in text
    assert "endpoint=issue_comments" in text
    assert "endpoint=compare_commits" in text
    assert "event=github_pr_created" in text
    assert "event=github_issue_comment_posted issue_number=9" in text
    assert "event=github_review_reply_posted pr_number=9 review_comment_id=10" in text


def test_helper_conversion_functions() -> None:
    assert _as_object_dict({"x": 1}) == {"x": 1}
    assert _as_object_dict({1: "x"}) is None
    assert _as_object_dict([1, 2]) is None

    assert _as_string("x") == "x"
    assert _as_string(None) == ""
    assert _as_string(3) == "3"

    assert _as_optional_str(None) is None
    assert _as_optional_str("x") == "x"
    assert _as_optional_str(4) == "4"

    assert _as_int(3, field="n") == 3
    assert _as_int("4", field="n") == 4
    with pytest.raises(RuntimeError, match="type"):
        _as_int(True, field="n")
    with pytest.raises(RuntimeError, match="value"):
        _as_int("bad", field="n")
    with pytest.raises(RuntimeError, match="type"):
        _as_int(2.5, field="n")

    assert _as_optional_int(None) is None
    assert _as_optional_int(3) == 3
    assert _as_optional_int("4") == 4
    with pytest.raises(RuntimeError, match="type"):
        _as_optional_int(True)
    with pytest.raises(RuntimeError, match="value"):
        _as_optional_int("bad")
    with pytest.raises(RuntimeError, match="type"):
        _as_optional_int(3.3)

    assert _as_bool(True) is True
    with pytest.raises(RuntimeError, match="bool"):
        _as_bool("x")

    assert _normalize_optional_lower_str(" FAILure  ") == "failure"
    assert _normalize_optional_lower_str("   ") is None
    assert _normalize_optional_lower_str(None) is None

    assert _tail_lines("a\nb\nc", max_lines=2) == "b\nc"
    assert _tail_lines("a\nb", max_lines=2) == "a\nb"
    assert _tail_lines("", max_lines=2) == "<empty>"
    with pytest.raises(ValueError, match="max_lines"):
        _tail_lines("x", max_lines=0)
