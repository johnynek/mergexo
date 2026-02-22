from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from typing import cast
from urllib.parse import urlencode

from mergexo.models import (
    Issue,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
)
from mergexo.observability import log_event
from mergexo.shell import run


LOGGER = logging.getLogger("mergexo.github_gateway")


@dataclass(frozen=True)
class GitHubGateway:
    owner: str
    name: str

    def list_open_issues_with_any_labels(self, labels: tuple[str, ...]) -> list[Issue]:
        deduped: dict[int, Issue] = {}
        for label in labels:
            for issue in self.list_open_issues_with_label(label):
                existing = deduped.get(issue.number)
                if existing is None:
                    deduped[issue.number] = issue
                    continue
                merged_labels: list[str] = list(existing.labels)
                for candidate in issue.labels:
                    if candidate not in merged_labels:
                        merged_labels.append(candidate)
                deduped[issue.number] = Issue(
                    number=issue.number,
                    title=issue.title,
                    body=issue.body,
                    html_url=issue.html_url,
                    labels=tuple(merged_labels),
                )

        issues = [deduped[number] for number in sorted(deduped)]
        log_event(
            LOGGER,
            "issues_deduped",
            fetched_label_count=len(labels),
            deduped_issue_count=len(issues),
        )
        return issues

    def list_open_issues_with_label(self, label: str) -> list[Issue]:
        query = urlencode({"state": "open", "labels": label, "per_page": "100"})
        path = f"/repos/{self.owner}/{self.name}/issues?{query}"
        payload = self._api_json("GET", path)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected GitHub response: expected list for issues")

        issues: list[Issue] = []
        for item in payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            # GitHub returns pull requests in the issues endpoint; ignore those.
            if "pull_request" in item_obj:
                continue
            number = _as_int(item_obj.get("number"), field="number")
            title = _as_string(item_obj.get("title"))
            body = _as_string(item_obj.get("body"))
            html_url = _as_string(item_obj.get("html_url"))
            labels_obj = item_obj.get("labels")
            label_names: list[str] = []
            if isinstance(labels_obj, list):
                for entry in labels_obj:
                    entry_obj = _as_object_dict(entry)
                    if entry_obj is None:
                        continue
                    name = entry_obj.get("name")
                    if isinstance(name, str):
                        label_names.append(name)
            issues.append(
                Issue(
                    number=number,
                    title=title,
                    body=body,
                    html_url=html_url,
                    labels=tuple(label_names),
                )
            )
        log_event(
            LOGGER,
            "github_read",
            endpoint="issues",
            count=len(issues),
        )
        return issues

    def create_pull_request(self, title: str, head: str, base: str, body: str) -> PullRequest:
        path = f"/repos/{self.owner}/{self.name}/pulls"
        payload = self._api_json(
            "POST",
            path,
            payload={"title": title, "head": head, "base": base, "body": body},
        )
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for PR")
        number = _as_int(payload_obj.get("number"), field="number")
        html_url = _as_string(payload_obj.get("html_url"))
        log_event(
            LOGGER,
            "github_pr_created",
            pr_number=number,
            base=base,
            head=head,
        )
        return PullRequest(number=number, html_url=html_url)

    def get_issue(self, issue_number: int) -> Issue:
        path = f"/repos/{self.owner}/{self.name}/issues/{issue_number}"
        payload = self._api_json("GET", path)
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for issue")
        number = _as_int(payload_obj.get("number"), field="number")
        title = _as_string(payload_obj.get("title"))
        body = _as_string(payload_obj.get("body"))
        html_url = _as_string(payload_obj.get("html_url"))
        labels_obj = payload_obj.get("labels")
        label_names: list[str] = []
        if isinstance(labels_obj, list):
            for entry in labels_obj:
                entry_obj = _as_object_dict(entry)
                if entry_obj is None:
                    continue
                label = entry_obj.get("name")
                if isinstance(label, str):
                    label_names.append(label)
        log_event(
            LOGGER,
            "github_read",
            endpoint="issue",
            issue_number=number,
        )
        return Issue(
            number=number, title=title, body=body, html_url=html_url, labels=tuple(label_names)
        )

    def get_pull_request(self, pr_number: int) -> PullRequestSnapshot:
        path = f"/repos/{self.owner}/{self.name}/pulls/{pr_number}"
        payload = self._api_json("GET", path)
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for pull request")

        head = _as_object_dict(payload_obj.get("head"))
        base = _as_object_dict(payload_obj.get("base"))
        if head is None or base is None:
            raise RuntimeError("Unexpected GitHub response: missing pull request head/base")

        snapshot = PullRequestSnapshot(
            number=_as_int(payload_obj.get("number"), field="number"),
            title=_as_string(payload_obj.get("title")),
            body=_as_string(payload_obj.get("body")),
            head_sha=_as_string(head.get("sha")),
            base_sha=_as_string(base.get("sha")),
            draft=_as_bool(payload_obj.get("draft")),
            state=_as_string(payload_obj.get("state")),
            merged=_as_bool(payload_obj.get("merged")),
        )
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request",
            pr_number=snapshot.number,
        )
        return snapshot

    def list_pull_request_files(self, pr_number: int) -> tuple[str, ...]:
        path = f"/repos/{self.owner}/{self.name}/pulls/{pr_number}/files?per_page=100"
        payload = self._api_json("GET", path)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected GitHub response: expected list of pull request files")

        files: list[str] = []
        for item in payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            filename = item_obj.get("filename")
            if isinstance(filename, str) and filename:
                files.append(filename)
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_files",
            pr_number=pr_number,
            count=len(files),
        )
        return tuple(files)

    def list_pull_request_review_comments(self, pr_number: int) -> list[PullRequestReviewComment]:
        path = f"/repos/{self.owner}/{self.name}/pulls/{pr_number}/comments?per_page=100"
        payload = self._api_json("GET", path)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected GitHub response: expected list of review comments")

        comments: list[PullRequestReviewComment] = []
        for item in payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            user_obj = _as_object_dict(item_obj.get("user"))
            comments.append(
                PullRequestReviewComment(
                    comment_id=_as_int(item_obj.get("id"), field="id"),
                    body=_as_string(item_obj.get("body")),
                    path=_as_string(item_obj.get("path")),
                    line=_as_optional_int(item_obj.get("line")),
                    side=_as_optional_str(item_obj.get("side")),
                    in_reply_to_id=_as_optional_int(item_obj.get("in_reply_to_id")),
                    user_login=_as_string(user_obj.get("login") if user_obj else None),
                    html_url=_as_string(item_obj.get("html_url")),
                    created_at=_as_string(item_obj.get("created_at")),
                    updated_at=_as_string(item_obj.get("updated_at")),
                )
            )
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_review_comments",
            pr_number=pr_number,
            count=len(comments),
        )
        return comments

    def list_pull_request_issue_comments(self, pr_number: int) -> list[PullRequestIssueComment]:
        path = f"/repos/{self.owner}/{self.name}/issues/{pr_number}/comments?per_page=100"
        payload = self._api_json("GET", path)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected GitHub response: expected list of issue comments")

        comments: list[PullRequestIssueComment] = []
        for item in payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            user_obj = _as_object_dict(item_obj.get("user"))
            comments.append(
                PullRequestIssueComment(
                    comment_id=_as_int(item_obj.get("id"), field="id"),
                    body=_as_string(item_obj.get("body")),
                    user_login=_as_string(user_obj.get("login") if user_obj else None),
                    html_url=_as_string(item_obj.get("html_url")),
                    created_at=_as_string(item_obj.get("created_at")),
                    updated_at=_as_string(item_obj.get("updated_at")),
                )
            )
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_issue_comments",
            pr_number=pr_number,
            count=len(comments),
        )
        return comments

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        path = f"/repos/{self.owner}/{self.name}/issues/{issue_number}/comments"
        self._api_json("POST", path, payload={"body": body})
        log_event(LOGGER, "github_issue_comment_posted", issue_number=issue_number)

    def post_review_comment_reply(self, pr_number: int, review_comment_id: int, body: str) -> None:
        path = f"/repos/{self.owner}/{self.name}/pulls/{pr_number}/comments"
        self._api_json(
            "POST",
            path,
            payload={"body": body, "in_reply_to": review_comment_id},
        )
        log_event(
            LOGGER,
            "github_review_reply_posted",
            pr_number=pr_number,
            review_comment_id=review_comment_id,
        )

    def _api_json(self, method: str, path: str, payload: dict[str, object] | None = None) -> object:
        cmd = ["gh", "api", "--method", method, path]
        stdin_payload: str | None = None
        if payload is not None:
            cmd.extend(["--input", "-"])
            stdin_payload = json.dumps(payload)
        raw = run(cmd, input_text=stdin_payload)
        return json.loads(raw)


def _as_object_dict(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    if not all(isinstance(key, str) for key in value.keys()):
        return None
    return cast(dict[str, object], value)


def _as_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"Unexpected GitHub response type for {field}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f"Unexpected GitHub response value for {field}: {value}") from exc
    raise RuntimeError(f"Unexpected GitHub response type for {field}")


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError("Unexpected GitHub response type for optional int field")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(
                f"Unexpected GitHub response value for optional int field: {value}"
            ) from exc
    raise RuntimeError("Unexpected GitHub response type for optional int field")


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raise RuntimeError("Unexpected GitHub response type for bool field")
