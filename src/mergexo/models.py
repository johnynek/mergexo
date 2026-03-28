from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


IssueFlow = Literal["reference_doc", "design_doc", "bugfix", "small_job", "roadmap"]
RoadmapNodeKind = Literal["reference_doc", "design_doc", "small_job", "roadmap"]
RoadmapDependencyRequirement = Literal["planned", "implemented"]
OperatorCommandName = Literal["unblock", "retry", "restart", "help", "invalid"]
OperatorCommandStatus = Literal["applied", "rejected", "failed"]
OperatorReplyStatus = Literal["applied", "rejected", "failed", "help"]
RestartMode = Literal["git_checkout", "pypi"]
RuntimeOperationStatus = Literal["pending", "running", "failed", "completed"]
PrActionsFeedbackPolicy = Literal["never", "first_fail", "all_complete"]


ROADMAP_NODE_KINDS: tuple[RoadmapNodeKind, ...] = (
    "reference_doc",
    "design_doc",
    "small_job",
    "roadmap",
)


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    html_url: str
    labels: tuple[str, ...]
    author_login: str = ""
    state: str = "open"


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
class WorkflowRunSnapshot:
    run_id: int
    name: str
    status: str
    conclusion: str | None
    html_url: str
    head_sha: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class WorkflowJobSnapshot:
    job_id: int
    name: str
    status: str
    conclusion: str | None
    html_url: str


@dataclass(frozen=True)
class FlakyTestReport:
    run_id: int
    title: str
    summary: str
    relevant_log_excerpt: str


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
class RoadmapDependency:
    node_id: str
    requires: RoadmapDependencyRequirement = "implemented"


@dataclass(frozen=True)
class RoadmapNode:
    node_id: str
    kind: RoadmapNodeKind
    title: str
    body_markdown: str
    depends_on: tuple[RoadmapDependency, ...] = ()


@dataclass(frozen=True)
class GeneratedRoadmap:
    title: str
    summary: str
    roadmap_markdown: str
    roadmap_issue_number: int
    version: int
    graph_nodes: tuple[RoadmapNode, ...]
    canonical_graph_json: str


@dataclass(frozen=True)
class RoadmapRevisionEscalation:
    kind: Literal["roadmap_revision"]
    summary: str
    details: str


@dataclass(frozen=True)
class WorkResult:
    issue_number: int
    branch: str
    pr_number: int
    pr_url: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class OperatorCommandRecord:
    command_key: str
    issue_number: int
    pr_number: int | None
    comment_id: int
    author_login: str
    command: OperatorCommandName
    args_json: str
    status: OperatorCommandStatus
    result: str
    created_at: str
    updated_at: str
    repo_full_name: str = ""


@dataclass(frozen=True)
class RuntimeOperationRecord:
    op_name: str
    status: RuntimeOperationStatus
    requested_by: str
    request_command_key: str
    mode: RestartMode
    detail: str | None
    created_at: str
    updated_at: str
    request_repo_full_name: str = ""
