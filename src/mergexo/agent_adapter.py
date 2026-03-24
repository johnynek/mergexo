from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mergexo.models import (
    FlakyTestReport,
    GeneratedDesign,
    GeneratedRoadmap,
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    RoadmapDependencyRequirement,
    RoadmapNodeKind,
    RoadmapRevisionEscalation,
)


@dataclass(frozen=True)
class AgentSession:
    adapter: str
    thread_id: str | None


@dataclass(frozen=True)
class DesignStartResult:
    design: GeneratedDesign
    session: AgentSession | None


@dataclass(frozen=True)
class RoadmapStartResult:
    roadmap: GeneratedRoadmap
    session: AgentSession | None


@dataclass(frozen=True)
class DirectStartResult:
    pr_title: str
    pr_summary: str
    commit_message: str | None
    blocked_reason: str | None
    session: AgentSession | None
    escalation: RoadmapRevisionEscalation | None = None


@dataclass(frozen=True)
class ReviewReply:
    review_comment_id: int
    body: str


GitOpName = Literal["fetch_origin", "merge_origin_default_branch"]
RoadmapAdjustmentAction = Literal["proceed", "request_revision", "abandon"]


@dataclass(frozen=True)
class GitOpRequest:
    op: GitOpName


@dataclass(frozen=True)
class RoadmapDependencyReference:
    ready_node_id: str
    requires: RoadmapDependencyRequirement


@dataclass(frozen=True)
class RoadmapDependencyArtifact:
    dependency_node_id: str
    dependency_kind: RoadmapNodeKind
    dependency_title: str
    frontier_references: tuple[RoadmapDependencyReference, ...]
    child_issue_number: int | None
    child_issue_url: str | None
    child_issue_title: str | None
    child_issue_body: str | None
    issue_run_status: str | None
    issue_run_branch: str | None
    issue_run_error: str | None
    resolution_markers: tuple[str, ...]
    pr_number: int | None
    pr_url: str | None
    pr_title: str | None
    pr_body: str | None
    pr_state: str | None
    pr_merged: bool | None
    changed_files: tuple[str, ...]
    review_summaries: tuple[PullRequestIssueComment, ...]
    issue_comments: tuple[PullRequestIssueComment, ...]


@dataclass(frozen=True)
class RoadmapAdjustmentResult:
    action: RoadmapAdjustmentAction
    summary: str
    details: str


@dataclass(frozen=True)
class FeedbackResult:
    session: AgentSession
    review_replies: tuple[ReviewReply, ...]
    general_comment: str | None
    commit_message: str | None
    git_ops: tuple[GitOpRequest, ...]
    flaky_test_report: FlakyTestReport | None = None
    escalation: RoadmapRevisionEscalation | None = None


@dataclass(frozen=True)
class FeedbackTurn:
    turn_key: str
    issue: Issue
    pull_request: PullRequestSnapshot
    review_comments: tuple[PullRequestReviewComment, ...]
    issue_comments: tuple[PullRequestIssueComment, ...]
    changed_files: tuple[str, ...]


class AgentAdapter(ABC):
    @abstractmethod
    def start_design_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        design_doc_path: str,
        default_branch: str,
        cwd: Path,
    ) -> DesignStartResult:
        """Start a PR lifecycle from an issue and produce an initial design artifact."""

    @abstractmethod
    def start_roadmap_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        roadmap_docs_dir: str,
        recommended_node_count: int,
        cwd: Path,
    ) -> RoadmapStartResult:
        """Start a roadmap PR lifecycle from an issue and produce roadmap artifacts."""

    @abstractmethod
    def evaluate_roadmap_adjustment(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        roadmap_doc_path: str,
        graph_path: str,
        graph_version: int,
        ready_node_ids: tuple[str, ...],
        dependency_artifacts: tuple[RoadmapDependencyArtifact, ...],
        roadmap_status_report: str,
        roadmap_markdown: str,
        canonical_graph_json: str,
        cwd: Path,
    ) -> RoadmapAdjustmentResult:
        """Decide whether a ready roadmap frontier should proceed, revise, or abandon."""

    @abstractmethod
    def start_bugfix_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        """Start a direct bugfix PR flow from an issue."""

    @abstractmethod
    def start_small_job_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        """Start a direct small-job PR flow from an issue."""

    @abstractmethod
    def start_implementation_from_design(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        design_doc_path: str,
        design_doc_markdown: str,
        design_pr_number: int | None,
        design_pr_url: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        """Start an implementation PR flow from a merged design doc."""

    @abstractmethod
    def respond_to_feedback(
        self,
        *,
        session: AgentSession,
        turn: FeedbackTurn,
        cwd: Path,
    ) -> FeedbackResult:
        """Produce reply actions for a review feedback turn within the same PR session."""
