from __future__ import annotations

import json

import pytest

from mergexo.github_gateway import (
    GitHubGateway,
    _as_bool,
    _as_int,
    _as_optional_int,
    _as_optional_str,
    _as_object_dict,
    _as_string,
)


def test_list_open_issues_with_label_filters_and_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {
            "number": 1,
            "title": "Issue",
            "body": "Body",
            "html_url": "u",
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
            "labels": [{"name": "agent:design"}],
        }

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)
    issue = gateway.get_issue(9)
    assert issue.number == 9
    assert issue.labels == ("agent:design",)


def test_create_pull_request_rejects_non_object(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])

    with pytest.raises(RuntimeError, match="expected object"):
        gateway.create_pull_request("t", "h", "b", "x")


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
        if path.endswith("/pulls/9/comments?per_page=100"):
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
        if path.endswith("/issues/9/comments?per_page=100"):
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
    issue_comments = gateway.list_pull_request_issue_comments(9)

    assert pr.number == 9
    assert pr.head_sha == "headsha"
    assert pr.state == "open"
    assert pr.merged is False
    assert files == ("src/a.py", "README.md")
    assert review_comments[0].comment_id == 11
    assert review_comments[0].user_login == "reviewer"
    assert issue_comments[0].comment_id == 22


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
    "method_name, bad_payload, expected",
    [
        ("get_pull_request", [], "expected object"),
        ("list_pull_request_files", {}, "expected list"),
        ("list_pull_request_review_comments", {}, "expected list"),
        ("list_pull_request_issue_comments", {}, "expected list"),
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
    calls: list[tuple[list[str], str | None]] = []

    def fake_run(
        cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True
    ) -> str:
        _ = cwd, check
        calls.append((cmd, input_text))
        return json.dumps({"ok": True})

    monkeypatch.setattr("mergexo.github_gateway.run", fake_run)

    gateway = GitHubGateway("o", "r")

    out_get = gateway._api_json("GET", "/path")
    out_post = gateway._api_json("POST", "/path", payload={"k": "v"})

    assert out_get == {"ok": True}
    assert out_post == {"ok": True}
    assert calls[0][0] == ["gh", "api", "--method", "GET", "/path"]
    assert "--input" in calls[1][0]
    assert calls[1][1] == '{"k": "v"}'


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
