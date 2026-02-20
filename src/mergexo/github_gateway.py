from __future__ import annotations

from dataclasses import dataclass
import json
from typing import cast
from urllib.parse import urlencode

from mergexo.models import Issue, PullRequest
from mergexo.shell import run


@dataclass(frozen=True)
class GitHubGateway:
    owner: str
    name: str

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
        return PullRequest(number=number, html_url=html_url)

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        path = f"/repos/{self.owner}/{self.name}/issues/{issue_number}/comments"
        self._api_json("POST", path, payload={"body": body})

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
