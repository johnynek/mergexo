from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
import logging
from typing import Literal, cast
from urllib.parse import urlencode

from mergexo.models import (
    Issue,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    WorkflowJobSnapshot,
    WorkflowRunSnapshot,
)
from mergexo.observability import log_event
from mergexo.shell import run


LOGGER = logging.getLogger("mergexo.github_gateway")
CompareCommitsStatus = Literal["ahead", "identical", "behind", "diverged"]
_ACTIONS_GREEN_CONCLUSIONS = {"success", "neutral", "skipped"}


class GitHubPollingError(RuntimeError):
    """Recoverable GitHub polling failure; caller should retry next poll."""


@dataclass(frozen=True)
class GitHubGateway:
    owner: str
    name: str
    _etags_by_path: dict[str, str] = field(
        default_factory=dict,
        init=False,
        repr=False,
        compare=False,
    )
    _cached_get_payload_by_path: dict[str, object] = field(
        default_factory=dict,
        init=False,
        repr=False,
        compare=False,
    )

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
                    author_login=existing.author_login or issue.author_login,
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
            user_obj = _as_object_dict(item_obj.get("user"))
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
                    author_login=_as_login(user_obj.get("login") if user_obj else None),
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
        try:
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
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "github_pr_create_failed",
                repo_full_name=f"{self.owner}/{self.name}",
                base=base,
                head=head,
                error_type=type(exc).__name__,
            )
            raise
        log_event(
            LOGGER,
            "github_pr_created",
            repo_full_name=f"{self.owner}/{self.name}",
            pr_number=number,
            pr_url=html_url,
            base=base,
            head=head,
        )
        return PullRequest(number=number, html_url=html_url)

    def find_pull_request_by_head(
        self,
        *,
        head: str,
        base: str | None = None,
        state: Literal["open", "all"] = "open",
    ) -> PullRequest | None:
        if state not in {"open", "all"}:
            raise ValueError("state must be 'open' or 'all'")
        query_items: dict[str, str] = {
            "state": state,
            "head": f"{self.owner}:{head}",
            "per_page": "100",
        }
        if base is not None:
            query_items["base"] = base
        path = f"/repos/{self.owner}/{self.name}/pulls?{urlencode(query_items)}"
        payload = self._api_json("GET", path)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected GitHub response: expected list for pull request lookup")

        candidates: list[PullRequest] = []
        for item in payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            number = _as_int(item_obj.get("number"), field="number")
            html_url = _as_string(item_obj.get("html_url"))
            candidates.append(PullRequest(number=number, html_url=html_url))

        if not candidates:
            log_event(
                LOGGER,
                "github_read",
                endpoint="pull_request_lookup_by_head",
                head=head,
                base=base,
                state=state,
                found=False,
            )
            return None

        selected = max(candidates, key=lambda pr: pr.number)
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_lookup_by_head",
            head=head,
            base=base,
            state=state,
            found=True,
            pr_number=selected.number,
        )
        return selected

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
        user_obj = _as_object_dict(payload_obj.get("user"))
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
            number=number,
            title=title,
            body=body,
            html_url=html_url,
            labels=tuple(label_names),
            author_login=_as_login(user_obj.get("login") if user_obj else None),
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

    def compare_commits(self, base_sha: str, head_sha: str) -> CompareCommitsStatus:
        path = f"/repos/{self.owner}/{self.name}/compare/{base_sha}...{head_sha}"
        payload = self._api_json("GET", path)
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for compare commits")

        status_raw = _as_string(payload_obj.get("status")).strip().lower()
        valid_statuses = {"ahead", "identical", "behind", "diverged"}
        if status_raw not in valid_statuses:
            raise RuntimeError(f"Unexpected GitHub compare status: {status_raw!r}")
        status = cast(CompareCommitsStatus, status_raw)

        log_event(
            LOGGER,
            "github_read",
            endpoint="compare_commits",
            base_sha=base_sha,
            head_sha=head_sha,
            status=status,
        )
        return status

    def list_workflow_runs_for_head(
        self, pr_number: int, head_sha: str
    ) -> tuple[WorkflowRunSnapshot, ...]:
        query = urlencode(
            {
                "event": "pull_request",
                "head_sha": head_sha,
                "per_page": "100",
            }
        )
        path = f"/repos/{self.owner}/{self.name}/actions/runs?{query}"
        payload = self._api_json("GET", path)
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for workflow runs")
        runs_payload = payload_obj.get("workflow_runs")
        if not isinstance(runs_payload, list):
            raise RuntimeError("Unexpected GitHub response: expected workflow_runs list")

        runs: list[WorkflowRunSnapshot] = []
        for item in runs_payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue

            pull_requests_payload = item_obj.get("pull_requests")
            if isinstance(pull_requests_payload, list) and pull_requests_payload:
                linked_numbers = {
                    _as_int(pr_obj.get("number"), field="number")
                    for raw in pull_requests_payload
                    if (pr_obj := _as_object_dict(raw)) is not None and "number" in pr_obj
                }
                if pr_number not in linked_numbers:
                    continue

            conclusion = _normalize_optional_lower_str(item_obj.get("conclusion"))
            runs.append(
                WorkflowRunSnapshot(
                    run_id=_as_int(item_obj.get("id"), field="id"),
                    name=_as_string(item_obj.get("name")),
                    status=_as_string(item_obj.get("status")).strip().lower(),
                    conclusion=conclusion,
                    html_url=_as_string(item_obj.get("html_url")),
                    head_sha=_as_string(item_obj.get("head_sha")),
                    created_at=_as_string(item_obj.get("created_at")),
                    updated_at=_as_string(item_obj.get("updated_at")),
                )
            )

        log_event(
            LOGGER,
            "github_read",
            endpoint="workflow_runs",
            pr_number=pr_number,
            head_sha=head_sha,
            count=len(runs),
        )
        return tuple(sorted(runs, key=lambda run: run.run_id))

    def list_workflow_jobs(self, run_id: int) -> tuple[WorkflowJobSnapshot, ...]:
        path = f"/repos/{self.owner}/{self.name}/actions/runs/{run_id}/jobs?per_page=100"
        payload = self._api_json("GET", path)
        payload_obj = _as_object_dict(payload)
        if payload_obj is None:
            raise RuntimeError("Unexpected GitHub response: expected object for workflow jobs")
        jobs_payload = payload_obj.get("jobs")
        if not isinstance(jobs_payload, list):
            raise RuntimeError("Unexpected GitHub response: expected jobs list")

        jobs: list[WorkflowJobSnapshot] = []
        for item in jobs_payload:
            item_obj = _as_object_dict(item)
            if item_obj is None:
                continue
            jobs.append(
                WorkflowJobSnapshot(
                    job_id=_as_int(item_obj.get("id"), field="id"),
                    name=_as_string(item_obj.get("name")),
                    status=_as_string(item_obj.get("status")).strip().lower(),
                    conclusion=_normalize_optional_lower_str(item_obj.get("conclusion")),
                    html_url=_as_string(item_obj.get("html_url")),
                )
            )

        log_event(
            LOGGER,
            "github_read",
            endpoint="workflow_jobs",
            run_id=run_id,
            count=len(jobs),
        )
        return tuple(sorted(jobs, key=lambda job: job.job_id))

    def get_failed_run_log_tails(
        self,
        run_id: int,
        tail_lines_per_action: int = 500,
    ) -> dict[str, str | None]:
        if tail_lines_per_action < 1:
            raise ValueError("tail_lines_per_action must be >= 1")

        jobs = self.list_workflow_jobs(run_id)
        failed_jobs = [
            job
            for job in jobs
            if job.status == "completed" and job.conclusion not in _ACTIONS_GREEN_CONCLUSIONS
        ]
        if not failed_jobs:
            return {}

        base_names = [_normalized_action_name(job.name) for job in failed_jobs]
        name_counts = Counter(base_names)
        log_tails: dict[str, str | None] = {}
        for job, base_name in zip(failed_jobs, base_names, strict=True):
            action_name = (
                base_name if name_counts[base_name] == 1 else f"{base_name} [job {job.job_id}]"
            )
            path = f"/repos/{self.owner}/{self.name}/actions/jobs/{job.job_id}/logs"
            try:
                raw_log = self._api_text("GET", path)
                log_tails[action_name] = _tail_lines(raw_log, max_lines=tail_lines_per_action)
            except Exception as exc:  # noqa: BLE001
                log_event(
                    LOGGER,
                    "actions_failure_logs_unavailable",
                    run_id=run_id,
                    job_id=job.job_id,
                    action_name=action_name,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                log_tails[action_name] = None

        return log_tails

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

    def list_pull_request_review_comments(
        self,
        pr_number: int,
        *,
        since: str | None = None,
    ) -> list[PullRequestReviewComment]:
        comments: list[PullRequestReviewComment] = []
        page = 1
        while True:
            query_items: dict[str, object] = {"per_page": 100, "page": page}
            if since is not None:
                query_items["since"] = since
            path = (
                f"/repos/{self.owner}/{self.name}/pulls/{pr_number}/comments?"
                f"{urlencode(query_items)}"
            )
            payload = self._api_json("GET", path)
            if not isinstance(payload, list):
                raise RuntimeError("Unexpected GitHub response: expected list of review comments")

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
            if len(payload) < 100:
                break
            page += 1
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_review_comments",
            pr_number=pr_number,
            since=since,
            count=len(comments),
        )
        return comments

    def list_pull_request_issue_comments(
        self,
        pr_number: int,
        *,
        since: str | None = None,
    ) -> list[PullRequestIssueComment]:
        comments = self.list_issue_comments(pr_number, since=since)
        log_event(
            LOGGER,
            "github_read",
            endpoint="pull_request_issue_comments",
            pr_number=pr_number,
            since=since,
            count=len(comments),
        )
        return comments

    def list_issue_comments(
        self,
        issue_number: int,
        *,
        since: str | None = None,
    ) -> list[PullRequestIssueComment]:
        comments: list[PullRequestIssueComment] = []
        page = 1
        while True:
            query_items: dict[str, object] = {"per_page": 100, "page": page}
            if since is not None:
                query_items["since"] = since
            path = (
                f"/repos/{self.owner}/{self.name}/issues/{issue_number}/comments?"
                f"{urlencode(query_items)}"
            )
            payload = self._api_json("GET", path)
            if not isinstance(payload, list):
                raise RuntimeError("Unexpected GitHub response: expected list of issue comments")

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
            if len(payload) < 100:
                break
            page += 1
        log_event(
            LOGGER,
            "github_read",
            endpoint="issue_comments",
            issue_number=issue_number,
            since=since,
            count=len(comments),
        )
        return comments

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        path = f"/repos/{self.owner}/{self.name}/issues/{issue_number}/comments"
        try:
            self._api_json("POST", path, payload={"body": body})
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "github_issue_comment_failed",
                repo_full_name=f"{self.owner}/{self.name}",
                issue_number=issue_number,
                error_type=type(exc).__name__,
            )
            raise
        log_event(LOGGER, "github_issue_comment_posted", issue_number=issue_number)

    def post_review_comment_reply(self, pr_number: int, review_comment_id: int, body: str) -> None:
        path = f"/repos/{self.owner}/{self.name}/pulls/{pr_number}/comments"
        try:
            self._api_json(
                "POST",
                path,
                payload={"body": body, "in_reply_to": review_comment_id},
            )
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "github_review_reply_failed",
                repo_full_name=f"{self.owner}/{self.name}",
                pr_number=pr_number,
                review_comment_id=review_comment_id,
                error_type=type(exc).__name__,
            )
            raise
        log_event(
            LOGGER,
            "github_review_reply_posted",
            pr_number=pr_number,
            review_comment_id=review_comment_id,
        )

    def _api_text(self, method: str, path: str) -> str:
        method_upper = method.upper()
        if method_upper != "GET":
            raise ValueError("_api_text currently only supports GET")
        cmd = ["gh", "api", "--method", method_upper, "--include", path]
        raw = run(cmd, check=False)
        try:
            status_code, _headers, body = _parse_http_response(raw)
            if status_code < 200 or status_code >= 300:
                message = body.strip() or "<empty>"
                raise RuntimeError(
                    f"GitHub API text request failed with status {status_code}: {message}"
                )
            return body
        except Exception as exc:
            log_event(
                LOGGER,
                "github_poll_get_failed",
                path=path,
                error_type=type(exc).__name__,
                error=str(exc),
                raw_preview=_preview_for_log(raw),
            )
            raise GitHubPollingError(f"GitHub polling GET failed for path {path}: {exc}") from exc

    def _api_json(self, method: str, path: str, payload: dict[str, object] | None = None) -> object:
        method_upper = method.upper()
        if method_upper == "GET":
            cmd = ["gh", "api", "--method", method_upper]
            etag = self._etags_by_path.get(path)
            if etag:
                cmd.extend(["--header", f"If-None-Match: {etag}"])
            cmd.extend(["--include", path])

            raw = run(cmd, check=False)
            try:
                status_code, headers, body = _parse_http_response(raw)

                if status_code == 304:
                    cached_payload = self._cached_get_payload_by_path.get(path)
                    if cached_payload is None:
                        raise RuntimeError(f"GitHub returned 304 for uncached path: {path}")
                    return cached_payload

                if status_code < 200 or status_code >= 300:
                    message = body.strip() or "<empty>"
                    raise RuntimeError(
                        f"GitHub API request failed with status {status_code}: {message}"
                    )

                payload_obj = json.loads(body)
                etag = headers.get("etag")
                if etag:
                    self._etags_by_path[path] = etag
                    self._cached_get_payload_by_path[path] = payload_obj
                return payload_obj
            except Exception as exc:
                log_event(
                    LOGGER,
                    "github_poll_get_failed",
                    path=path,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    raw_preview=_preview_for_log(raw),
                )
                raise GitHubPollingError(
                    f"GitHub polling GET failed for path {path}: {exc}"
                ) from exc

        cmd = ["gh", "api", "--method", method_upper, path]
        stdin_payload: str | None = None
        if payload is not None:
            cmd.extend(["--input", "-"])
            stdin_payload = json.dumps(payload)
        raw = run(cmd, input_text=stdin_payload)
        return json.loads(raw)


def _parse_http_response(raw: str) -> tuple[int, dict[str, str], str]:
    normalized = raw.replace("\r\n", "\n")
    lines = normalized.split("\n")

    status_line_index = -1
    for index, line in enumerate(lines):
        if line.startswith("HTTP/"):
            status_line_index = index

    if status_line_index < 0:
        raise RuntimeError("Unexpected GitHub response: missing HTTP status line")

    status_line = lines[status_line_index]
    status_parts = status_line.split(" ", 2)
    if len(status_parts) < 2:
        raise RuntimeError(f"Unexpected GitHub response status line: {status_line!r}")

    try:
        status_code = int(status_parts[1])
    except ValueError as exc:
        raise RuntimeError(f"Unexpected GitHub response status line: {status_line!r}") from exc

    headers: dict[str, str] = {}
    body_start = len(lines)
    for index in range(status_line_index + 1, len(lines)):
        line = lines[index]
        if line == "":
            body_start = index + 1
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    body = "\n".join(lines[body_start:])
    return status_code, headers, body


def _preview_for_log(text: str, *, limit: int = 240) -> str:
    compact = text.replace("\n", "\\n").strip()
    if not compact:
        return "<empty>"
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit]}..."


def _normalize_optional_lower_str(value: object) -> str | None:
    raw = _as_optional_str(value)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    return normalized or None


def _normalized_action_name(raw_name: str) -> str:
    normalized = raw_name.strip()
    return normalized or "unnamed-action"


def _tail_lines(raw_text: str, *, max_lines: int) -> str:
    if max_lines < 1:
        raise ValueError("max_lines must be >= 1")
    lines = raw_text.splitlines()
    if not lines:
        return "<empty>"
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


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


def _as_login(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


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
