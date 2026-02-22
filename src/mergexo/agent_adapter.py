from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mergexo.models import (
    GeneratedDesign,
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
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
class ReviewReply:
    review_comment_id: int
    body: str


GitOpName = Literal["fetch_origin", "merge_origin_default_branch"]


@dataclass(frozen=True)
class GitOpRequest:
    op: GitOpName


@dataclass(frozen=True)
class FeedbackResult:
    session: AgentSession
    review_replies: tuple[ReviewReply, ...]
    general_comment: str | None
    commit_message: str | None
    git_ops: tuple[GitOpRequest, ...]


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
    def respond_to_feedback(
        self,
        *,
        session: AgentSession,
        turn: FeedbackTurn,
        cwd: Path,
    ) -> FeedbackResult:
        """Produce reply actions for a review feedback turn within the same PR session."""
