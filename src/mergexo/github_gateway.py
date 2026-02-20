from __future__ import annotations

from dataclasses import dataclass
import json
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
            if not isinstance(item, dict):
                continue
            # GitHub returns pull requests in the issues endpoint; ignore those.
            if "pull_request" in item:
                continue
            number = int(item.get("number"))
            title = str(item.get("title") or "")
            body = str(item.get("body") or "")
            html_url = str(item.get("html_url") or "")
            labels_obj = item.get("labels")
            label_names: list[str] = []
            if isinstance(labels_obj, list):
                for entry in labels_obj:
                    if isinstance(entry, dict):
                        name = entry.get("name")
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
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected GitHub response: expected object for PR")
        number = int(payload.get("number"))
        html_url = str(payload.get("html_url") or "")
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
