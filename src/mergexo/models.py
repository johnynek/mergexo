from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    html_url: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class PullRequest:
    number: int
    html_url: str


@dataclass(frozen=True)
class PullRequestSnapshot:
    number: int
    title: str
    body: str
    head_sha: str
    base_sha: str
    draft: bool
    state: str
    merged: bool


@dataclass(frozen=True)
class PullRequestReviewComment:
    comment_id: int
    body: str
    path: str
    line: int | None
    side: str | None
    in_reply_to_id: int | None
    user_login: str
    html_url: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class PullRequestIssueComment:
    comment_id: int
    body: str
    user_login: str
    html_url: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class GeneratedDesign:
    title: str
    design_doc_markdown: str
    touch_paths: tuple[str, ...]
    summary: str


@dataclass(frozen=True)
class WorkResult:
    issue_number: int
    branch: str
    pr_number: int
    pr_url: str
