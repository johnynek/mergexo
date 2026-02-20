from __future__ import annotations

import json

import pytest

from mergexo.github_gateway import (
    GitHubGateway,
    _as_int,
    _as_object_dict,
    _as_string,
)


def test_list_open_issues_with_label_filters_and_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"number": 1, "title": "Issue", "body": "Body", "html_url": "u", "labels": [{"name": "x"}, 9]},
        {"pull_request": {"url": "pr"}, "number": 2},
        "skip",
    ]

    gateway = GitHubGateway("o", "r")

    def fake_api(self: GitHubGateway, method: str, path: str, payload: dict[str, object] | None = None) -> object:
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

    def fake_api(self: GitHubGateway, method: str, path: str, payload: dict[str, object] | None = None) -> object:
        _ = self
        calls.append((method, path, payload))
        if path.endswith("/pulls"):
            return {"number": "123", "html_url": "https://example/pr/123"}
        return {"ok": True}

    monkeypatch.setattr(GitHubGateway, "_api_json", fake_api)

    pr = gateway.create_pull_request("t", "head", "main", "body")
    gateway.post_issue_comment(7, "hello")

    assert pr.number == 123
    assert pr.html_url == "https://example/pr/123"
    assert calls[0][0] == "POST"
    assert calls[1][1].endswith("/issues/7/comments")


def test_create_pull_request_rejects_non_object(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GitHubGateway("o", "r")
    monkeypatch.setattr(GitHubGateway, "_api_json", lambda self, method, path, payload=None: [])

    with pytest.raises(RuntimeError, match="expected object"):
        gateway.create_pull_request("t", "h", "b", "x")


def test_api_json_invokes_gh_api(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[list[str], str | None]] = []

    def fake_run(cmd: list[str], *, cwd=None, input_text: str | None = None, check: bool = True) -> str:
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

    assert _as_int(3, field="n") == 3
    assert _as_int("4", field="n") == 4
    with pytest.raises(RuntimeError, match="type"):
        _as_int(True, field="n")
    with pytest.raises(RuntimeError, match="value"):
        _as_int("bad", field="n")
    with pytest.raises(RuntimeError, match="type"):
        _as_int(2.5, field="n")
