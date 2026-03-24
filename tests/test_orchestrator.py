from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import logging
import re
import sqlite3
import time
from typing import cast

from hypothesis import given, strategies as st
import pytest

from mergexo import observability_queries as oq
from mergexo.agent_adapter import (
    AgentSession,
    DirectStartResult,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
    RoadmapDependencyArtifact,
    RoadmapAdjustmentResult,
    RoadmapStartResult,
    ReviewReply,
)
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import (
    FeedbackEventRecord,
    ParsedOperatorCommand,
    compute_roadmap_node_issue_token,
    compute_pre_pr_checkpoint_token,
    compute_source_issue_redirect_token,
    parse_operator_command,
)
from mergexo.github_gateway import GitHubAuthenticationError, GitHubGateway, GitHubPollingError
from mergexo.models import (
    FlakyTestReport,
    GeneratedDesign,
    GeneratedRoadmap,
    Issue,
    IssueFlow,
    OperatorCommandRecord,
    PrActionsFeedbackPolicy,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    RestartMode,
    RoadmapRevisionEscalation,
    RoadmapNode,
    WorkflowJobSnapshot,
    WorkflowRunSnapshot,
    WorkResult,
)
from mergexo.orchestrator import (
    CheckpointedPrePrBlockedError,
    DirectFlowBlockedError,
    DirectFlowValidationError,
    FeedbackTransientGitError,
    GlobalWorkLimiter,
    Phase1Orchestrator,
    RestartRequested,
    SlotPool,
    _FeedbackFuture,
    _RunningIssueMetadata,
    _branch_for_implementation_candidate,
    _branch_for_pre_pr_followup,
    _design_branch_slug,
    _design_doc_url,
    _default_commit_message,
    _flow_trigger_label,
    _feedback_transient_git_retry_delay_seconds,
    _flow_label_from_branch,
    _has_regression_test_changes,
    _is_transient_feedback_git_command_error,
    _is_transient_git_remote_error,
    _is_mergexo_status_comment,
    _is_merge_conflict_error,
    _normalize_feedback_terminal_status,
    _normalize_timestamp_for_compare,
    _implementation_candidate_from_json_dict,
    _implementation_candidate_to_json_dict,
    _infer_pre_pr_flow_from_issue_and_error,
    _failure_class_for_exception,
    _is_recoverable_pre_pr_error,
    _is_recoverable_pre_pr_exception,
    _issue_from_json_dict,
    _issue_to_json_dict,
    _issue_branch,
    _operator_args_payload,
    _operator_normalized_command,
    _operator_reply_issue_number,
    _operator_reply_status_for_record,
    _operator_source_comment_url,
    _pre_pr_flow_label,
    _pull_request_url,
    _parse_utc_timestamp,
    _normalize_design_doc_body,
    _render_source_issue_redirect_comment,
    _render_design_doc,
    _render_operator_command_result,
    _render_roadmap_child_issue_body,
    _ready_frontier_dependency_references,
    _key_roadmap_dependency_comments,
    _roadmap_dependency_changed_files,
    _roadmap_dependency_resolution_markers,
    _render_roadmap_status_report,
    _render_regex_patterns,
    _recovery_pr_payload_for_issue,
    _roadmap_child_label_for_kind,
    _parse_superseding_roadmap_parent,
    _resolve_issue_flow,
    _summarize_git_error,
    _slugify,
    _trigger_labels,
    _truncate_feedback_text,
)
from mergexo.observability import configure_logging
from mergexo.roadmap_parser import parse_roadmap_graph_json
from mergexo.shell import CommandError
from mergexo.state import (
    ActionTokenScopeKind,
    ActionTokenState,
    GitHubCallOutboxState,
    GitHubCommentSurface,
    GitHubCommentPollCursorState,
    ImplementationCandidateState,
    PendingFeedbackEvent,
    PollCursorUpdate,
    PrFlakeState,
    PrFlakeStatus,
    PrePrFollowupState,
    RoadmapBlockerRow,
    RoadmapDependencyState,
    RoadmapNodeGraphInput,
    RoadmapNodeRecord,
    RoadmapStatusSnapshotRow,
    IssueRunRecord,
    StateStore,
    TrackedPullRequestState,
)


@dataclass
class FakeLease:
    slot: int
    path: Path


class FakeGitManager:
    def __init__(self, checkout_root: Path) -> None:
        self.checkout_root = checkout_root
        self.ensure_layout_called = False
        self.prepare_calls: list[Path] = []
        self.branch_calls: list[tuple[Path, str]] = []
        self.commit_calls: list[tuple[Path, str]] = []
        self.push_calls: list[tuple[Path, str]] = []
        self.checkpoint_persist_calls: list[tuple[Path, str, str]] = []
        self.cleanup_calls: list[Path] = []
        self.ensure_checkout_calls: list[int] = []
        self.recover_quarantined_slot_calls: list[int] = []
        self.restore_calls: list[tuple[Path, str, str]] = []
        self.fetch_calls: list[Path] = []
        self.merge_calls: list[Path] = []
        self.current_head_calls: list[Path] = []
        self.is_ancestor_calls: list[tuple[Path, str, str]] = []
        self.push_results: list[bool] = []
        self.restore_feedback_result = True
        self.staged_files: tuple[str, ...] = ("src/a.py", "tests/test_a.py")
        self.current_head_sha_value = "headsha"
        self.checkpoint_head_sha_value = "checkpointsha"
        self.persist_checkpoint_should_fail = False
        self.is_ancestor_results: dict[tuple[str, str], bool] = {}

    def ensure_layout(self) -> None:
        self.ensure_layout_called = True

    def ensure_checkout(self, slot: int) -> Path:
        self.ensure_checkout_calls.append(slot)
        path = self.checkout_root / f"worker-{slot}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def prepare_checkout(self, checkout_path: Path) -> None:
        self.prepare_calls.append(checkout_path)

    def create_or_reset_branch(self, checkout_path: Path, branch: str) -> None:
        self.branch_calls.append((checkout_path, branch))

    def commit_all(self, checkout_path: Path, message: str) -> None:
        self.commit_calls.append((checkout_path, message))

    def list_staged_files(self, checkout_path: Path) -> tuple[str, ...]:
        _ = checkout_path
        return self.staged_files

    def push_branch(self, checkout_path: Path, branch: str) -> bool:
        self.push_calls.append((checkout_path, branch))
        if self.push_results:
            return self.push_results.pop(0)
        return False

    def persist_checkpoint_branch(
        self,
        checkout_path: Path,
        branch: str,
        *,
        commit_message: str,
    ) -> str:
        self.checkpoint_persist_calls.append((checkout_path, branch, commit_message))
        if self.persist_checkpoint_should_fail:
            raise RuntimeError("checkpoint push failed")
        return self.checkpoint_head_sha_value

    def cleanup_slot(self, checkout_path: Path) -> None:
        self.cleanup_calls.append(checkout_path)

    def recover_quarantined_slot(self, slot: int) -> Path:
        self.recover_quarantined_slot_calls.append(slot)
        return self.ensure_checkout(slot)

    def fetch_origin(self, checkout_path: Path) -> None:
        self.fetch_calls.append(checkout_path)

    def merge_origin_default_branch(self, checkout_path: Path) -> None:
        self.merge_calls.append(checkout_path)

    def restore_feedback_branch(
        self, checkout_path: Path, branch: str, expected_head_sha: str
    ) -> bool:
        self.restore_calls.append((checkout_path, branch, expected_head_sha))
        if self.restore_feedback_result:
            self.current_head_sha_value = expected_head_sha
        return self.restore_feedback_result

    def current_head_sha(self, checkout_path: Path) -> str:
        self.current_head_calls.append(checkout_path)
        return self.current_head_sha_value

    def is_ancestor(self, checkout_path: Path, older_sha: str, newer_sha: str) -> bool:
        self.is_ancestor_calls.append((checkout_path, older_sha, newer_sha))
        return self.is_ancestor_results.get((older_sha, newer_sha), True)


class FakeGitHub:
    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
        self.requested_labels: tuple[str, ...] | None = None
        self.created_prs: list[tuple[str, str, str, str]] = []
        self.created_issues: list[tuple[str, str, tuple[str, ...] | None]] = []
        self.rerun_requests: list[int] = []
        self.comments: list[tuple[int, str]] = []
        self.review_replies: list[tuple[int, int, str]] = []
        self.review_comments: list[PullRequestReviewComment] = []
        self.review_summaries: list[PullRequestIssueComment] = []
        self.issue_comments: list[PullRequestIssueComment] = []
        self.changed_files: tuple[str, ...] = ()
        self.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="PR body",
            head_sha="headsha",
            base_sha="basesha",
            draft=False,
            state="open",
            merged=False,
        )
        self._next_comment_id = 1000
        self._next_issue_number = 1000
        self.compare_calls: list[tuple[str, str]] = []
        self.compare_statuses: dict[tuple[str, str], str] = {}
        self.workflow_runs_by_head: dict[tuple[int, str], tuple[WorkflowRunSnapshot, ...]] = {}
        self.workflow_jobs_by_run_id: dict[int, tuple[WorkflowJobSnapshot, ...]] = {}
        self.failed_run_log_tails: dict[int, dict[str, str | None]] = {}
        self.failed_log_tail_calls: list[tuple[int, int]] = []
        self.pr_lookup_by_head_base: dict[tuple[str, str], PullRequest] = {}
        self.closed_issue_numbers: list[int] = []

    def list_open_issues_with_any_labels(self, labels: tuple[str, ...]) -> list[Issue]:
        self.requested_labels = labels
        return self.issues

    def list_open_issues_with_label(self, label: str) -> list[Issue]:
        return [issue for issue in self.issues if label in issue.labels]

    def create_pull_request(self, title: str, head: str, base: str, body: str) -> PullRequest:
        self.created_prs.append((title, head, base, body))
        self.pr_snapshot = PullRequestSnapshot(
            number=101,
            title=title,
            body=body,
            head_sha="headsha",
            base_sha="basesha",
            draft=False,
            state="open",
            merged=False,
        )
        pr = PullRequest(number=101, html_url="https://example/pr/101")
        self.pr_lookup_by_head_base[(head, base)] = pr
        return pr

    def create_issue(
        self,
        *,
        title: str,
        body: str,
        labels: tuple[str, ...] | None = None,
    ) -> Issue:
        self.created_issues.append((title, body, labels))
        issue_number = self._next_issue_number
        self._next_issue_number += 1
        issue = Issue(
            number=issue_number,
            title=title,
            body=body,
            html_url=f"https://example/issues/{issue_number}",
            labels=labels or (),
            author_login="mergexo[bot]",
        )
        self.issues.append(issue)
        return issue

    def close_issue(self, issue_number: int) -> None:
        self.closed_issue_numbers.append(issue_number)

    def find_pull_request_by_head(
        self,
        *,
        head: str,
        base: str | None = None,
        state: str = "open",
    ) -> PullRequest | None:
        _ = state
        if base is None:
            for (candidate_head, _candidate_base), pr in self.pr_lookup_by_head_base.items():
                if candidate_head == head:
                    return pr
            return None
        return self.pr_lookup_by_head_base.get((head, base))

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        self.comments.append((issue_number, body))
        self._next_comment_id += 1
        self.issue_comments.append(
            PullRequestIssueComment(
                comment_id=self._next_comment_id,
                body=body,
                user_login="mergexo[bot]",
                html_url=f"https://example/comment/{self._next_comment_id}",
                created_at="now",
                updated_at="now",
            )
        )

    def get_issue(self, issue_number: int) -> Issue:
        for issue in self.issues:
            if issue.number == issue_number:
                return issue
        return Issue(
            number=issue_number,
            title=f"Issue {issue_number}",
            body="Body",
            html_url=f"https://example/issues/{issue_number}",
            labels=("agent:design",),
            author_login="issue-author",
        )

    def get_pull_request(self, pr_number: int) -> PullRequestSnapshot:
        if pr_number == self.pr_snapshot.number:
            return self.pr_snapshot
        return replace(self.pr_snapshot, number=pr_number)

    def list_pull_request_files(self, pr_number: int) -> tuple[str, ...]:
        _ = pr_number
        return self.changed_files

    def list_pull_request_review_comments(
        self, pr_number: int, *, since: str | None = None
    ) -> list[PullRequestReviewComment]:
        _ = pr_number, since
        return list(self.review_comments)

    def list_pull_request_review_summaries(self, pr_number: int) -> list[PullRequestIssueComment]:
        _ = pr_number
        return list(self.review_summaries)

    def list_pull_request_issue_comments(
        self, pr_number: int, *, since: str | None = None
    ) -> list[PullRequestIssueComment]:
        _ = pr_number, since
        return list(self.issue_comments)

    def list_issue_comments(
        self, issue_number: int, *, since: str | None = None
    ) -> list[PullRequestIssueComment]:
        _ = issue_number, since
        return list(self.issue_comments)

    def list_workflow_runs_for_head(
        self, pr_number: int, head_sha: str
    ) -> tuple[WorkflowRunSnapshot, ...]:
        return self.workflow_runs_by_head.get((pr_number, head_sha), ())

    def list_workflow_jobs(self, run_id: int) -> tuple[WorkflowJobSnapshot, ...]:
        return self.workflow_jobs_by_run_id.get(run_id, ())

    def get_failed_run_log_tails(
        self, run_id: int, tail_lines_per_action: int = 500
    ) -> dict[str, str | None]:
        self.failed_log_tail_calls.append((run_id, tail_lines_per_action))
        return dict(self.failed_run_log_tails.get(run_id, {}))

    def rerun_workflow_run_failed_jobs(self, run_id: int) -> None:
        self.rerun_requests.append(run_id)

    def post_review_comment_reply(self, pr_number: int, review_comment_id: int, body: str) -> None:
        self.review_replies.append((pr_number, review_comment_id, body))
        self._next_comment_id += 1
        self.review_comments.append(
            PullRequestReviewComment(
                comment_id=self._next_comment_id,
                body=body,
                path="src/a.py",
                line=1,
                side="RIGHT",
                in_reply_to_id=review_comment_id,
                user_login="mergexo[bot]",
                html_url=f"https://example/review/{self._next_comment_id}",
                created_at="now",
                updated_at="now",
            )
        )

    def compare_commits(self, base_sha: str, head_sha: str) -> str:
        self.compare_calls.append((base_sha, head_sha))
        if (base_sha, head_sha) in self.compare_statuses:
            return self.compare_statuses[(base_sha, head_sha)]
        if base_sha == head_sha:
            return "identical"
        return "ahead"


class FakeAgent:
    def __init__(self, generated: GeneratedDesign | None = None, fail: bool = False) -> None:
        self.generated = generated or GeneratedDesign(
            title="Design Title",
            summary="Summary",
            touch_paths=("src/a.py",),
            design_doc_markdown="## Body\n\nDetails",
        )
        self.fail = fail
        self.calls: list[tuple[Issue, Path]] = []
        self.roadmap_calls: list[tuple[Issue, Path]] = []
        self.roadmap_adjustment_calls: list[
            tuple[Issue, tuple[str, ...], tuple[RoadmapDependencyArtifact, ...], Path]
        ] = []
        self.bugfix_calls: list[tuple[Issue, Path]] = []
        self.small_job_calls: list[tuple[Issue, Path]] = []
        self.implementation_calls: list[tuple[Issue, Path, str]] = []
        self.bugfix_guidelines_paths: list[str | None] = []
        self.small_job_guidelines_paths: list[str | None] = []
        self.implementation_guidelines_paths: list[str | None] = []
        self.feedback_calls: list[tuple[AgentSession, FeedbackTurn, Path]] = []
        self.bugfix_result = DirectStartResult(
            pr_title="Fix bug",
            pr_summary="Applies bugfix changes.",
            commit_message="fix: resolve issue",
            blocked_reason=None,
            session=AgentSession(adapter="codex", thread_id="thread-123"),
        )
        self.small_job_result = DirectStartResult(
            pr_title="Implement small job",
            pr_summary="Applies scoped change.",
            commit_message="feat: complete small job",
            blocked_reason=None,
            session=AgentSession(adapter="codex", thread_id="thread-123"),
        )
        self.implementation_result = DirectStartResult(
            pr_title="Implement merged design",
            pr_summary="Implements the merged design document.",
            commit_message="feat: implement merged design",
            blocked_reason=None,
            session=AgentSession(adapter="codex", thread_id="thread-123"),
        )
        self.roadmap_result = GeneratedRoadmap(
            title="Roadmap Title",
            summary="Roadmap summary",
            roadmap_markdown="# Roadmap\n\nBody",
            roadmap_issue_number=7,
            version=1,
            graph_nodes=(
                RoadmapNode(
                    node_id="n1",
                    kind="small_job",
                    title="Ship",
                    body_markdown="Do it",
                ),
            ),
            canonical_graph_json=(
                '{"nodes":[{"body_markdown":"Do it","depends_on":[],"kind":"small_job",'
                '"node_id":"n1","title":"Ship"}],"roadmap_issue_number":7,"version":1}'
            ),
        )
        self.feedback_results: list[FeedbackResult] = []
        self.feedback_result = FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-123"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(),
        )
        self.roadmap_adjustment_result = RoadmapAdjustmentResult(
            action="proceed",
            summary="Proceed",
            details="The ready frontier can proceed.",
        )

    def start_design_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        design_doc_path: str,
        default_branch: str,
        cwd: Path,
    ) -> DesignStartResult:
        _ = repo_full_name, design_doc_path, default_branch
        self.calls.append((issue, cwd))
        if self.fail:
            raise RuntimeError("codex failed")
        return DesignStartResult(
            design=self.generated,
            session=AgentSession(adapter="codex", thread_id="thread-123"),
        )

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
        _ = repo_full_name, default_branch, roadmap_docs_dir, recommended_node_count
        self.roadmap_calls.append((issue, cwd))
        if self.fail:
            raise RuntimeError("codex failed")
        return RoadmapStartResult(
            roadmap=GeneratedRoadmap(
                title=self.roadmap_result.title,
                summary=self.roadmap_result.summary,
                roadmap_markdown=self.roadmap_result.roadmap_markdown,
                roadmap_issue_number=issue.number,
                version=self.roadmap_result.version,
                graph_nodes=self.roadmap_result.graph_nodes,
                canonical_graph_json=json.dumps(
                    {
                        "nodes": [
                            {
                                "body_markdown": "Do it",
                                "depends_on": [],
                                "kind": "small_job",
                                "node_id": "n1",
                                "title": "Ship",
                            }
                        ],
                        "roadmap_issue_number": issue.number,
                        "version": 1,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
            session=AgentSession(adapter="codex", thread_id="thread-123"),
        )

    def respond_to_feedback(
        self, *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        self.feedback_calls.append((session, turn, cwd))
        if self.feedback_results:
            return self.feedback_results.pop(0)
        return self.feedback_result

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
        _ = (
            repo_full_name,
            default_branch,
            coding_guidelines_path,
            roadmap_doc_path,
            graph_path,
            graph_version,
            dependency_artifacts,
            roadmap_status_report,
            roadmap_markdown,
            canonical_graph_json,
        )
        self.roadmap_adjustment_calls.append((issue, ready_node_ids, dependency_artifacts, cwd))
        return self.roadmap_adjustment_result

    def start_bugfix_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = repo_full_name, default_branch
        self.bugfix_guidelines_paths.append(coding_guidelines_path)
        self.bugfix_calls.append((issue, cwd))
        if self.fail:
            raise RuntimeError("codex failed")
        return self.bugfix_result

    def start_small_job_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = repo_full_name, default_branch
        self.small_job_guidelines_paths.append(coding_guidelines_path)
        self.small_job_calls.append((issue, cwd))
        if self.fail:
            raise RuntimeError("codex failed")
        return self.small_job_result

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
        _ = (
            repo_full_name,
            default_branch,
            design_doc_markdown,
            design_pr_number,
            design_pr_url,
        )
        self.implementation_guidelines_paths.append(coding_guidelines_path)
        self.implementation_calls.append((issue, cwd, design_doc_path))
        if self.fail:
            raise RuntimeError("codex failed")
        return self.implementation_result


class FakeState:
    def __init__(self, *, allowed: set[int] | None = None) -> None:
        self.allowed = allowed or set()
        self.running: list[int] = []
        self.completed: list[WorkResult] = []
        self.failed: list[tuple[int, str]] = []
        self.saved_sessions: list[tuple[int, str, str | None]] = []
        self.tracked: list[TrackedPullRequestState] = []
        self.implementation_candidates: list[ImplementationCandidateState] = []
        self.status_updates: list[tuple[int, int, str, str | None, str | None]] = []
        self.run_starts: list[tuple[str, str, int, int | None]] = []
        self.run_finishes: list[tuple[str, str, str | None, str | None]] = []
        self._pr_status_by_pr: dict[int, str] = {}
        self._pr_flake_state_by_pr: dict[int, PrFlakeState] = {}
        self._action_tokens: dict[str, ActionTokenState] = {}
        self._poll_cursors: dict[tuple[str, int], GitHubCommentPollCursorState] = {}
        self._issue_comment_cursors: dict[int, tuple[int, int]] = {}
        self._takeover_active_issue_numbers: set[int] = set()
        self._takeover_comment_floors_by_pr: dict[int, tuple[int, int]] = {}
        self._next_github_call_id = 1
        self._github_calls_by_id: dict[int, dict[str, object]] = {}
        self._github_call_id_by_dedupe: dict[str, int] = {}
        self.claim_blocked_implementation_issue_numbers: set[int] = set()
        self.claim_blocked_pre_pr_followup_issue_numbers: set[int] = set()
        self.claim_blocked_feedback_pr_numbers: set[int] = set()
        self.released_feedback_claims: list[tuple[int, str]] = []
        self.cleared_feedback_event_pr_numbers: list[int] = []

    def _github_call_state(self, row: dict[str, object]) -> GitHubCallOutboxState:
        return GitHubCallOutboxState(
            call_id=cast(int, row["call_id"]),
            call_kind=cast(str, row["call_kind"]),
            dedupe_key=cast(str, row["dedupe_key"]),
            payload_json=cast(str, row["payload_json"]),
            status=cast(str, row["status"]),
            state_applied=cast(bool, row["state_applied"]),
            attempt_count=cast(int, row["attempt_count"]),
            last_error=cast(str | None, row["last_error"]),
            result_json=cast(str | None, row["result_json"]),
            run_id=cast(str | None, row["run_id"]),
            issue_number=cast(int | None, row["issue_number"]),
            branch=cast(str | None, row["branch"]),
            pr_number=cast(int | None, row["pr_number"]),
            created_at=cast(str, row["created_at"]),
            updated_at=cast(str, row["updated_at"]),
            repo_full_name=cast(str, row["repo_full_name"]),
        )

    def upsert_github_call_intent(
        self,
        *,
        call_kind: str,
        dedupe_key: str,
        payload_json: str,
        run_id: str | None = None,
        issue_number: int | None = None,
        branch: str | None = None,
        repo_full_name: str | None = None,
    ) -> GitHubCallOutboxState:
        _ = repo_full_name
        existing_id = self._github_call_id_by_dedupe.get(dedupe_key)
        if existing_id is not None:
            return self._github_call_state(self._github_calls_by_id[existing_id])
        call_id = self._next_github_call_id
        self._next_github_call_id += 1
        row: dict[str, object] = {
            "call_id": call_id,
            "call_kind": call_kind,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "pending",
            "state_applied": False,
            "attempt_count": 0,
            "last_error": None,
            "result_json": None,
            "run_id": run_id,
            "issue_number": issue_number,
            "branch": branch,
            "pr_number": None,
            "created_at": "now",
            "updated_at": "now",
            "repo_full_name": "",
        }
        self._github_calls_by_id[call_id] = row
        self._github_call_id_by_dedupe[dedupe_key] = call_id
        return self._github_call_state(row)

    def mark_github_call_in_progress(
        self,
        *,
        call_id: int,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = repo_full_name
        row = self._github_calls_by_id.get(call_id)
        if row is None:
            return False
        status = cast(str, row["status"])
        if status not in {"pending", "in_progress"}:
            return False
        row["status"] = "in_progress"
        row["attempt_count"] = cast(int, row["attempt_count"]) + 1
        row["updated_at"] = "now"
        return True

    def mark_github_call_pending_retry(
        self,
        *,
        call_id: int,
        error: str,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = repo_full_name
        row = self._github_calls_by_id.get(call_id)
        if row is None:
            return False
        row["status"] = "pending"
        row["last_error"] = error
        row["updated_at"] = "now"
        return True

    def mark_github_call_succeeded(
        self,
        *,
        call_id: int,
        result_json: str,
        pr_number: int | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = repo_full_name
        row = self._github_calls_by_id.get(call_id)
        if row is None:
            return False
        row["status"] = "succeeded"
        row["result_json"] = result_json
        row["pr_number"] = pr_number
        row["last_error"] = None
        row["updated_at"] = "now"
        return True

    def list_replayable_github_calls(
        self,
        *,
        call_kind: str,
        repo_full_name: str | None = None,
    ) -> tuple[GitHubCallOutboxState, ...]:
        _ = repo_full_name
        entries: list[GitHubCallOutboxState] = []
        for row in self._github_calls_by_id.values():
            if row["call_kind"] != call_kind:
                continue
            status = cast(str, row["status"])
            if status in {"pending", "in_progress"} or (
                status == "succeeded" and cast(bool, row["state_applied"]) is False
            ):
                entries.append(self._github_call_state(row))
        entries.sort(key=lambda item: item.call_id)
        return tuple(entries)

    def mark_create_pr_call_state_applied(
        self,
        *,
        issue_number: int,
        branch: str,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> int:
        _ = repo_full_name
        updated = 0
        for row in self._github_calls_by_id.values():
            if row["call_kind"] != "create_pull_request":
                continue
            if row["issue_number"] != issue_number:
                continue
            if row["branch"] != branch:
                continue
            if row["pr_number"] != pr_number:
                continue
            if row["status"] != "succeeded":
                continue
            if cast(bool, row["state_applied"]):
                continue
            row["state_applied"] = True
            row["updated_at"] = "now"
            updated += 1
        return updated

    def apply_succeeded_create_pr_call(
        self,
        *,
        call_id: int,
        issue_number: int,
        branch: str,
        pr_number: int,
        pr_url: str,
        run_id: str | None = None,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = run_id, repo_full_name
        row = self._github_calls_by_id.get(call_id)
        if row is None:
            return False
        if row["status"] != "succeeded" or cast(bool, row["state_applied"]):
            return False
        row["state_applied"] = True
        row["updated_at"] = "now"
        self.mark_completed(
            issue_number=issue_number,
            branch=branch,
            pr_number=pr_number,
            pr_url=pr_url,
            repo_full_name=repo_full_name,
        )
        return True

    def can_enqueue(self, issue_number: int, *, repo_full_name: str | None = None) -> bool:
        _ = repo_full_name
        return issue_number in self.allowed

    def claim_new_issue_run_start(
        self,
        *,
        issue_number: int,
        flow: str,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        _ = flow, branch, meta_json, started_at, repo_full_name
        if issue_number not in self.allowed:
            return None
        self.allowed.discard(issue_number)
        self.running.append(issue_number)
        run_key = run_id or f"issue_flow:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append(("issue_flow", run_key, issue_number, None))
        return run_key

    def claim_implementation_issue_run_start(
        self,
        *,
        issue_number: int,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        _ = branch, meta_json, started_at, repo_full_name
        if issue_number in self.claim_blocked_implementation_issue_numbers:
            return None
        self.running.append(issue_number)
        run_key = run_id or f"implementation_flow:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append(("implementation_flow", run_key, issue_number, None))
        return run_key

    def claim_pre_pr_followup_run_start(
        self,
        *,
        issue_number: int,
        flow: str,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        _ = flow, branch, meta_json, started_at, repo_full_name
        if issue_number in self.claim_blocked_pre_pr_followup_issue_numbers:
            return None
        self.running.append(issue_number)
        run_key = run_id or f"pre_pr_followup:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append(("pre_pr_followup", run_key, issue_number, None))
        return run_key

    def claim_feedback_turn_start(
        self,
        *,
        pr_number: int,
        issue_number: int,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str | None:
        _ = branch, meta_json, started_at, repo_full_name
        if pr_number in self.claim_blocked_feedback_pr_numbers:
            return None
        run_key = run_id or f"feedback_turn:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append(("feedback_turn", run_key, issue_number, pr_number))
        return run_key

    def release_feedback_turn_claim(
        self,
        *,
        pr_number: int,
        run_id: str,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = repo_full_name
        self.released_feedback_claims.append((pr_number, run_id))
        return True

    def mark_running(self, issue_number: int, *, repo_full_name: str | None = None) -> None:
        _ = repo_full_name
        self.running.append(issue_number)

    def mark_completed(
        self,
        *,
        issue_number: int,
        branch: str,
        pr_number: int,
        pr_url: str,
        repo_full_name: str | None = None,
    ) -> None:
        _ = repo_full_name
        self.completed.append(
            WorkResult(issue_number=issue_number, branch=branch, pr_number=pr_number, pr_url=pr_url)
        )
        self._pr_status_by_pr[pr_number] = "awaiting_feedback"

    def mark_failed(
        self,
        *,
        issue_number: int,
        error: str,
        failure_class: str | None = None,
        retryable: bool = False,
        repo_full_name: str | None = None,
    ) -> None:
        _ = repo_full_name, failure_class, retryable
        self.failed.append((issue_number, error))

    def save_agent_session(
        self,
        *,
        issue_number: int,
        adapter: str,
        thread_id: str | None,
        repo_full_name: str | None = None,
    ) -> None:
        _ = repo_full_name
        self.saved_sessions.append((issue_number, adapter, thread_id))

    def get_agent_session(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> tuple[str, str | None] | None:
        _ = repo_full_name
        for num, adapter, thread_id in self.saved_sessions:
            if num == issue_number:
                return adapter, thread_id
        return None

    def list_tracked_pull_requests(
        self, *, repo_full_name: str | None = None
    ) -> tuple[TrackedPullRequestState, ...]:
        _ = repo_full_name
        return tuple(self.tracked)

    def list_implementation_candidates(
        self, *, repo_full_name: str | None = None
    ) -> tuple[ImplementationCandidateState, ...]:
        _ = repo_full_name
        return tuple(self.implementation_candidates)

    def list_pre_pr_followups(
        self, *, repo_full_name: str | None = None
    ) -> tuple[PrePrFollowupState, ...]:
        _ = repo_full_name
        return ()

    def get_issue_takeover_active(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> bool:
        _ = repo_full_name
        return issue_number in self._takeover_active_issue_numbers

    def set_issue_takeover_active(
        self,
        *,
        issue_number: int,
        ignore_active: bool,
        repo_full_name: str | None = None,
    ) -> object:
        _ = repo_full_name
        if ignore_active:
            self._takeover_active_issue_numbers.add(issue_number)
        else:
            self._takeover_active_issue_numbers.discard(issue_number)
        return cast(
            object,
            type(
                "IssueTakeoverState",
                (),
                {
                    "repo_full_name": repo_full_name or "",
                    "issue_number": issue_number,
                    "ignore_active": ignore_active,
                    "updated_at": "now",
                },
            )(),
        )

    def list_active_issue_takeovers(self, *, repo_full_name: str | None = None) -> tuple[int, ...]:
        _ = repo_full_name
        return tuple(sorted(self._takeover_active_issue_numbers))

    def list_feedback_pr_numbers_for_issue(
        self,
        *,
        issue_number: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, ...]:
        _ = repo_full_name
        return tuple(
            sorted(
                tracked.pr_number
                for tracked in self.tracked
                if tracked.issue_number == issue_number
            )
        )

    def get_pr_takeover_comment_floors(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, int]:
        _ = repo_full_name
        return self._takeover_comment_floors_by_pr.get(pr_number, (0, 0))

    def advance_pr_takeover_comment_floors(
        self,
        *,
        pr_number: int,
        review_floor_comment_id: int,
        issue_floor_comment_id: int,
        repo_full_name: str | None = None,
    ) -> tuple[int, int]:
        _ = repo_full_name
        review_floor, issue_floor = self._takeover_comment_floors_by_pr.get(pr_number, (0, 0))
        next_review_floor = max(review_floor, review_floor_comment_id)
        next_issue_floor = max(issue_floor, issue_floor_comment_id)
        self._takeover_comment_floors_by_pr[pr_number] = (next_review_floor, next_issue_floor)
        return next_review_floor, next_issue_floor

    def mark_pending_feedback_events_processed_for_pr(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> int:
        _ = repo_full_name
        self.cleared_feedback_event_pr_numbers.append(pr_number)
        return 0

    def list_legacy_failed_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[object, ...]:
        _ = repo_full_name
        return ()

    def list_legacy_running_issue_runs_without_pr(
        self, *, repo_full_name: str | None = None
    ) -> tuple[object, ...]:
        _ = repo_full_name
        return ()

    def reconcile_unfinished_agent_runs(self, *, repo_full_name: str | None = None) -> int:
        _ = repo_full_name
        return 0

    def reconcile_stale_running_issue_runs_with_followups(
        self, *, repo_full_name: str | None = None
    ) -> int:
        _ = repo_full_name
        return 0

    def prune_observability_history(
        self, *, retention_days: int, repo_full_name: str | None = None
    ) -> tuple[int, int]:
        _ = retention_days, repo_full_name
        return 0, 0

    def record_issue_run_start(
        self,
        *,
        run_kind: str,
        issue_number: int,
        flow: str,
        branch: str,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str:
        _ = flow, branch, meta_json, started_at, repo_full_name
        self.running.append(issue_number)
        run_key = run_id or f"{run_kind}:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append((run_kind, run_key, issue_number, None))
        return run_key

    def record_agent_run_start(
        self,
        *,
        run_kind: str,
        issue_number: int,
        pr_number: int | None,
        flow: str | None,
        branch: str | None,
        meta_json: str = "{}",
        run_id: str | None = None,
        started_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> str:
        _ = flow, branch, meta_json, started_at, repo_full_name
        run_key = run_id or f"{run_kind}:{issue_number}:{len(self.run_starts) + 1}"
        self.run_starts.append((run_kind, run_key, issue_number, pr_number))
        return run_key

    def finish_agent_run(
        self,
        *,
        run_id: str,
        terminal_status: str,
        failure_class: str | None = None,
        error: str | None = None,
        finished_at: str | None = None,
    ) -> bool:
        _ = finished_at
        self.run_finishes.append((run_id, terminal_status, failure_class, error))
        return True

    def get_runtime_operation(self, op_name: str):  # type: ignore[no-untyped-def]
        _ = op_name
        return None

    def list_blocked_pull_requests(
        self, *, repo_full_name: str | None = None
    ) -> tuple[object, ...]:
        _ = repo_full_name
        return ()

    def get_operator_command(self, command_key: str, *, repo_full_name: str | None = None):  # type: ignore[no-untyped-def]
        _ = command_key, repo_full_name
        return None

    def record_operator_command(self, **kwargs):  # type: ignore[no-untyped-def]
        return OperatorCommandRecord(
            command_key=str(kwargs.get("command_key", "k")),
            issue_number=int(kwargs.get("issue_number", 0)),
            pr_number=kwargs.get("pr_number"),
            comment_id=int(kwargs.get("comment_id", 0)),
            author_login=str(kwargs.get("author_login", "alice")),
            command=cast(str, kwargs.get("command", "help")),
            args_json=str(kwargs.get("args_json", "{}")),
            status=cast(str, kwargs.get("status", "applied")),
            result=str(kwargs.get("result", "")),
            created_at="now",
            updated_at="now",
            repo_full_name=str(kwargs.get("repo_full_name", "")),
        )

    def update_operator_command_result(self, **kwargs):  # type: ignore[no-untyped-def]
        _ = kwargs
        return None

    def request_runtime_restart(self, **kwargs):  # type: ignore[no-untyped-def]
        repo_full_name = str(kwargs.get("request_repo_full_name", ""))
        return (
            cast(
                object,
                type(
                    "RuntimeOp",
                    (),
                    {
                        "op_name": "restart",
                        "status": "pending",
                        "requested_by": str(kwargs.get("requested_by", "alice")),
                        "request_command_key": str(kwargs.get("request_command_key", "k")),
                        "request_repo_full_name": repo_full_name,
                        "mode": kwargs.get("mode", "git_checkout"),
                        "detail": None,
                    },
                )(),
            ),
            True,
        )

    def set_runtime_operation_status(self, **kwargs):  # type: ignore[no-untyped-def]
        _ = kwargs
        return None

    def reset_blocked_pull_requests(self, **kwargs):  # type: ignore[no-untyped-def]
        _ = kwargs
        return 0

    def get_pull_request_status(self, pr_number: int, *, repo_full_name: str | None = None):  # type: ignore[no-untyped-def]
        _ = repo_full_name
        status = self._pr_status_by_pr.get(pr_number)
        if status is None:
            return None
        return cast(
            object,
            type(
                "PullRequestStatusState",
                (),
                {"status": status},
            )(),
        )

    def mark_pr_status(
        self,
        *,
        pr_number: int,
        issue_number: int,
        status: str,
        last_seen_head_sha: str | None = None,
        error: str | None = None,
        reason: str | None = None,
        detail: str | None = None,
        repo_full_name: str | None = None,
    ) -> None:
        _ = repo_full_name, reason, detail
        self.status_updates.append((pr_number, issue_number, status, last_seen_head_sha, error))
        self._pr_status_by_pr[pr_number] = status
        if status in {"merged", "closed"}:
            self._pr_flake_state_by_pr.pop(pr_number, None)

    def upsert_pr_flake_state(
        self,
        *,
        pr_number: int,
        issue_number: int,
        head_sha: str,
        run_id: int,
        initial_run_updated_at: str,
        status: PrFlakeStatus,
        flake_issue_number: int,
        flake_issue_url: str,
        report_title: str,
        report_summary: str,
        report_excerpt: str,
        full_log_context_markdown: str,
        rerun_requested_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> PrFlakeState:
        existing = self._pr_flake_state_by_pr.get(pr_number)
        next_state = PrFlakeState(
            repo_full_name=repo_full_name or "",
            pr_number=pr_number,
            issue_number=issue_number,
            head_sha=head_sha,
            run_id=run_id,
            initial_run_updated_at=initial_run_updated_at,
            status=status,
            flake_issue_number=flake_issue_number,
            flake_issue_url=flake_issue_url,
            report_title=report_title,
            report_summary=report_summary,
            report_excerpt=report_excerpt,
            full_log_context_markdown=full_log_context_markdown,
            rerun_requested_at=(
                existing.rerun_requested_at
                if existing is not None and rerun_requested_at is None
                else rerun_requested_at
            ),
            created_at="now" if existing is None else existing.created_at,
            updated_at="now",
        )
        self._pr_flake_state_by_pr[pr_number] = next_state
        return next_state

    def get_pr_flake_state(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> PrFlakeState | None:
        _ = repo_full_name
        return self._pr_flake_state_by_pr.get(pr_number)

    def get_active_pr_flake_state(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> PrFlakeState | None:
        _ = repo_full_name
        state = self._pr_flake_state_by_pr.get(pr_number)
        if state is None:
            return None
        if state.status != "awaiting_rerun_result":
            return None
        return state

    def set_pr_flake_state_status(
        self,
        *,
        pr_number: int,
        status: PrFlakeStatus,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        _ = repo_full_name
        state = self._pr_flake_state_by_pr.get(pr_number)
        if state is None:
            return None
        updated = PrFlakeState(
            repo_full_name=state.repo_full_name,
            pr_number=state.pr_number,
            issue_number=state.issue_number,
            head_sha=state.head_sha,
            run_id=state.run_id,
            initial_run_updated_at=state.initial_run_updated_at,
            status=status,
            flake_issue_number=state.flake_issue_number,
            flake_issue_url=state.flake_issue_url,
            report_title=state.report_title,
            report_summary=state.report_summary,
            report_excerpt=state.report_excerpt,
            full_log_context_markdown=state.full_log_context_markdown,
            rerun_requested_at=state.rerun_requested_at,
            created_at=state.created_at,
            updated_at="now",
        )
        self._pr_flake_state_by_pr[pr_number] = updated
        return updated

    def mark_pr_flake_rerun_requested(
        self,
        *,
        pr_number: int,
        rerun_requested_at: str | None = None,
        repo_full_name: str | None = None,
    ) -> PrFlakeState | None:
        _ = repo_full_name
        state = self._pr_flake_state_by_pr.get(pr_number)
        if state is None:
            return None
        updated = PrFlakeState(
            repo_full_name=state.repo_full_name,
            pr_number=state.pr_number,
            issue_number=state.issue_number,
            head_sha=state.head_sha,
            run_id=state.run_id,
            initial_run_updated_at=state.initial_run_updated_at,
            status=state.status,
            flake_issue_number=state.flake_issue_number,
            flake_issue_url=state.flake_issue_url,
            report_title=state.report_title,
            report_summary=state.report_summary,
            report_excerpt=state.report_excerpt,
            full_log_context_markdown=state.full_log_context_markdown,
            rerun_requested_at=state.rerun_requested_at or rerun_requested_at or "now",
            created_at=state.created_at,
            updated_at="now",
        )
        self._pr_flake_state_by_pr[pr_number] = updated
        return updated

    def clear_pr_flake_state(
        self,
        *,
        pr_number: int,
        repo_full_name: str | None = None,
    ) -> bool:
        _ = repo_full_name
        return self._pr_flake_state_by_pr.pop(pr_number, None) is not None

    def get_issue_comment_cursor(
        self, issue_number: int, *, repo_full_name: str | None = None
    ) -> object:
        _ = repo_full_name
        pre_pr, post_pr = self._issue_comment_cursors.get(issue_number, (0, 0))
        return cast(
            object,
            type(
                "IssueCommentCursorState",
                (),
                {
                    "repo_full_name": repo_full_name or "",
                    "issue_number": issue_number,
                    "pre_pr_last_consumed_comment_id": pre_pr,
                    "post_pr_last_redirected_comment_id": post_pr,
                    "updated_at": "now",
                },
            )(),
        )

    def advance_pre_pr_last_consumed_comment_id(
        self,
        *,
        issue_number: int,
        comment_id: int,
        repo_full_name: str | None = None,
    ) -> object:
        _ = repo_full_name
        pre_pr, post_pr = self._issue_comment_cursors.get(issue_number, (0, 0))
        self._issue_comment_cursors[issue_number] = (max(pre_pr, comment_id), post_pr)
        return self.get_issue_comment_cursor(issue_number, repo_full_name=repo_full_name)

    def advance_post_pr_last_redirected_comment_id(
        self,
        *,
        issue_number: int,
        comment_id: int,
        repo_full_name: str | None = None,
    ) -> object:
        _ = repo_full_name
        pre_pr, post_pr = self._issue_comment_cursors.get(issue_number, (0, 0))
        self._issue_comment_cursors[issue_number] = (pre_pr, max(post_pr, comment_id))
        return self.get_issue_comment_cursor(issue_number, repo_full_name=repo_full_name)

    def ingest_feedback_events(self, events: object, *, repo_full_name: str | None = None) -> None:
        _ = events, repo_full_name

    def get_poll_cursor(
        self,
        *,
        surface: str,
        scope_number: int,
        repo_full_name: str | None = None,
    ) -> GitHubCommentPollCursorState | None:
        _ = repo_full_name
        return self._poll_cursors.get((surface, scope_number))

    def upsert_poll_cursor(
        self,
        *,
        surface: str,
        scope_number: int,
        last_updated_at: str,
        last_comment_id: int,
        bootstrap_complete: bool,
        repo_full_name: str | None = None,
    ) -> GitHubCommentPollCursorState:
        cursor = GitHubCommentPollCursorState(
            repo_full_name=repo_full_name or "johnynek/mergexo",
            surface=cast(GitHubCommentSurface, surface),
            scope_number=scope_number,
            last_updated_at=last_updated_at,
            last_comment_id=last_comment_id,
            bootstrap_complete=bootstrap_complete,
            updated_at="now",
        )
        self._poll_cursors[(surface, scope_number)] = cursor
        return cursor

    def ingest_feedback_scan_batch(
        self,
        *,
        events: object,
        cursor_updates: object,
        token_observations: object = (),
        repo_full_name: str | None = None,
    ) -> None:
        _ = events
        for update in cast(tuple[PollCursorUpdate, ...], cursor_updates):
            self.upsert_poll_cursor(
                surface=update.surface,
                scope_number=update.scope_number,
                last_updated_at=update.last_updated_at,
                last_comment_id=update.last_comment_id,
                bootstrap_complete=update.bootstrap_complete,
                repo_full_name=repo_full_name,
            )
        for observation in cast(tuple[object, ...], tuple(token_observations)):
            token = getattr(observation, "token")
            self.record_action_token_observed(
                token=token,
                scope_kind=getattr(observation, "scope_kind"),
                scope_number=getattr(observation, "scope_number"),
                source=getattr(observation, "source"),
                observed_comment_id=getattr(observation, "comment_id"),
                observed_updated_at=getattr(observation, "updated_at"),
                repo_full_name=repo_full_name,
            )

    def get_action_token(
        self, token: str, *, repo_full_name: str | None = None
    ) -> ActionTokenState | None:
        _ = repo_full_name
        return self._action_tokens.get(token)

    def record_action_token_planned(
        self,
        *,
        token: str,
        scope_kind: str,
        scope_number: int,
        source: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        existing = self._action_tokens.get(token)
        if existing is not None and existing.status in {"observed", "posted"}:
            return existing
        state = ActionTokenState(
            repo_full_name=repo_full_name or "johnynek/mergexo",
            token=token,
            scope_kind=cast(ActionTokenScopeKind, scope_kind),
            scope_number=scope_number,
            source=source,
            status="planned",
            attempt_count=0 if existing is None else existing.attempt_count,
            observed_comment_id=None,
            observed_updated_at=None,
            created_at="now",
            updated_at="now",
        )
        self._action_tokens[token] = state
        return state

    def record_action_token_posted(
        self,
        *,
        token: str,
        scope_kind: str,
        scope_number: int,
        source: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        existing = self._action_tokens.get(token)
        attempts = 1 if existing is None else existing.attempt_count + 1
        state = ActionTokenState(
            repo_full_name=repo_full_name or "johnynek/mergexo",
            token=token,
            scope_kind=cast(ActionTokenScopeKind, scope_kind),
            scope_number=scope_number,
            source=source,
            status="posted",
            attempt_count=attempts,
            observed_comment_id=None if existing is None else existing.observed_comment_id,
            observed_updated_at=None if existing is None else existing.observed_updated_at,
            created_at="now" if existing is None else existing.created_at,
            updated_at="now",
        )
        self._action_tokens[token] = state
        return state

    def record_action_token_observed(
        self,
        *,
        token: str,
        scope_kind: str,
        scope_number: int,
        source: str,
        observed_comment_id: int,
        observed_updated_at: str,
        repo_full_name: str | None = None,
    ) -> ActionTokenState:
        existing = self._action_tokens.get(token)
        state = ActionTokenState(
            repo_full_name=repo_full_name or "johnynek/mergexo",
            token=token,
            scope_kind=cast(ActionTokenScopeKind, scope_kind),
            scope_number=scope_number,
            source=source,
            status="observed",
            attempt_count=0 if existing is None else existing.attempt_count,
            observed_comment_id=observed_comment_id,
            observed_updated_at=observed_updated_at,
            created_at="now" if existing is None else existing.created_at,
            updated_at="now",
        )
        self._action_tokens[token] = state
        return state

    def list_pending_feedback_events(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> tuple[object, ...]:
        _ = pr_number, repo_full_name
        return ()

    def mark_feedback_events_processed(
        self,
        *,
        event_keys: tuple[str, ...],
        repo_full_name: str | None = None,
    ) -> None:
        _ = event_keys, repo_full_name

    def finalize_feedback_turn(
        self,
        *,
        pr_number: int,
        issue_number: int,
        processed_event_keys: tuple[str, ...],
        session: AgentSession,
        head_sha: str,
        repo_full_name: str | None = None,
    ) -> None:
        _ = pr_number, issue_number, processed_event_keys, session, head_sha, repo_full_name


def _config(
    tmp_path: Path,
    *,
    worker_count: int = 1,
    enable_github_operations: bool = False,
    enable_roadmaps: bool = False,
    enable_issue_comment_routing: bool = False,
    enable_pr_actions_monitoring: bool = False,
    pr_actions_feedback_policy: PrActionsFeedbackPolicy | None = None,
    enable_incremental_comment_fetch: bool = False,
    comment_fetch_overlap_seconds: int = 5,
    comment_fetch_safe_backfill_seconds: int = 86400,
    pr_actions_log_tail_lines: int = 500,
    restart_drain_timeout_seconds: int = 900,
    restart_default_mode: str = "git_checkout",
    restart_supported_modes: tuple[str, ...] = ("git_checkout",),
    operations_issue_number: int | None = None,
    operator_logins: tuple[str, ...] = (),
    ignore_label: str = "agent:ignore",
    allowed_users: tuple[str, ...] = ("issue-author", "reviewer"),
    required_tests: str | None = None,
    test_file_regex: tuple[str, ...] | None = None,
) -> AppConfig:
    compiled_test_file_regex = (
        tuple(re.compile(pattern) for pattern in test_file_regex)
        if test_file_regex is not None
        else None
    )
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=worker_count,
            poll_interval_seconds=1,
            enable_github_operations=enable_github_operations,
            enable_roadmaps=enable_roadmaps,
            enable_issue_comment_routing=enable_issue_comment_routing,
            enable_pr_actions_monitoring=enable_pr_actions_monitoring,
            enable_incremental_comment_fetch=enable_incremental_comment_fetch,
            comment_fetch_overlap_seconds=comment_fetch_overlap_seconds,
            comment_fetch_safe_backfill_seconds=comment_fetch_safe_backfill_seconds,
            pr_actions_log_tail_lines=pr_actions_log_tail_lines,
            restart_drain_timeout_seconds=restart_drain_timeout_seconds,
            restart_default_mode=cast(RestartMode, restart_default_mode),
            restart_supported_modes=cast(tuple[RestartMode, ...], restart_supported_modes),
        ),
        repos=(
            RepoConfig(
                repo_id="mergexo",
                owner="johnynek",
                name="mergexo",
                default_branch="main",
                trigger_label="agent:design",
                bugfix_label="agent:bugfix",
                small_job_label="agent:small-job",
                coding_guidelines_path="docs/python_style.md",
                design_docs_dir="docs/design",
                allowed_users=frozenset(login.lower() for login in allowed_users),
                local_clone_source=None,
                remote_url=None,
                required_tests=required_tests,
                test_file_regex=compiled_test_file_regex,
                pr_actions_feedback_policy=pr_actions_feedback_policy,
                operations_issue_number=operations_issue_number,
                operator_logins=operator_logins,
                ignore_label=ignore_label,
            ),
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def _issue(
    number: int = 7,
    title: str = "Add worker scheduler",
    labels: tuple[str, ...] = ("agent:design",),
    author_login: str = "issue-author",
) -> Issue:
    return Issue(
        number=number,
        title=title,
        body="Body",
        html_url=f"https://example/issues/{number}",
        labels=labels,
        author_login=author_login,
    )


def _write_checkout_file(
    checkout_root: Path,
    relative_path: str,
    *,
    content: str = "",
    slot: int = 0,
) -> Path:
    path = checkout_root / f"worker-{slot}" / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_required_tests_script(checkout_root: Path, *, slot: int = 0) -> Path:
    return _write_checkout_file(
        checkout_root,
        "scripts/required-tests.sh",
        content="#!/usr/bin/env bash\nexit 0\n",
        slot=slot,
    )


def _issue_run_row(
    db_path: Path,
    issue_number: int,
) -> tuple[str, str | None, int | None, str | None, str | None]:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, branch, pr_number, pr_url, error FROM issue_runs WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
        assert row is not None
        status, branch, pr_number, pr_url, error = row
        assert isinstance(status, str)
        assert branch is None or isinstance(branch, str)
        assert pr_number is None or isinstance(pr_number, int)
        assert pr_url is None or isinstance(pr_url, str)
        assert error is None or isinstance(error, str)
        return status, branch, pr_number, pr_url, error
    finally:
        conn.close()


def test_slot_pool_acquire_release(tmp_path: Path) -> None:
    manager = FakeGitManager(tmp_path / "checkouts")
    pool = SlotPool(manager, worker_count=1)

    lease = pool.acquire()
    assert lease.slot == 0
    assert lease.path.exists()

    pool.release(lease)
    lease2 = pool.acquire()
    assert lease2.slot == 0


def test_slot_pool_release_quarantines_and_recovers_slot(tmp_path: Path) -> None:
    manager = FakeGitManager(tmp_path / "checkouts")
    pool = SlotPool(manager, worker_count=1)

    lease = pool.acquire()
    pool.release(lease, quarantine_reason="cleanup failed")
    assert manager.recover_quarantined_slot_calls == [0]

    lease2 = pool.acquire()
    assert lease2.slot == 0


def test_slot_pool_release_still_releases_when_quarantine_recovery_fails(tmp_path: Path) -> None:
    manager = FakeGitManager(tmp_path / "checkouts")
    pool = SlotPool(manager, worker_count=1)
    lease = pool.acquire()

    def fail_recovery(slot: int) -> Path:
        _ = slot
        raise RuntimeError("recovery failed")

    manager.recover_quarantined_slot = fail_recovery
    pool.release(lease, quarantine_reason="cleanup failed")

    lease2 = pool.acquire()
    assert lease2.slot == 0


def test_global_work_limiter_guards_bounds() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        GlobalWorkLimiter(0)

    limiter = GlobalWorkLimiter(1)
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is False
    assert limiter.in_flight() == 1
    assert limiter.capacity() == 1

    limiter.release()
    assert limiter.in_flight() == 0
    with pytest.raises(RuntimeError, match="no work is in flight"):
        limiter.release()


def test_orchestrator_queue_count_accessors(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    running: Future[WorkResult] = Future()
    feedback: Future[str] = Future()
    orch._running = {1: running}
    orch._running_feedback = {101: _FeedbackFuture(issue_number=1, run_id="run-1", future=feedback)}
    assert orch.pending_work_count() == 2
    assert orch.queue_counts() == (1, 1)
    assert orch.in_flight_work_count() == 0


def test_enqueue_paths_record_run_history_starts(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=3)
    issue = _issue(labels=("agent:design",))
    state = FakeState(allowed={issue.number})
    github = FakeGitHub([issue])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    class NoopPool:
        def submit(self, fn, *args):  # type: ignore[no-untyped-def]
            _ = fn, args
            fut: Future[WorkResult] = Future()
            return fut

    orch._enqueue_new_work(NoopPool())
    assert state.run_starts[0][0] == "issue_flow"

    state.implementation_candidates = [
        ImplementationCandidateState(
            issue_number=issue.number,
            design_branch="agent/design/7-add-worker-scheduler",
            design_pr_number=11,
            design_pr_url="https://example/pr/11",
        )
    ]
    orch._running = {}
    orch._enqueue_implementation_work(NoopPool())
    assert any(kind == "implementation_flow" for kind, *_ in state.run_starts)

    state.tracked = [
        TrackedPullRequestState(
            pr_number=101,
            issue_number=issue.number,
            branch="agent/design/7-add-worker-scheduler",
            status="awaiting_feedback",
            last_seen_head_sha=None,
        )
    ]

    class FeedbackPool:
        def submit(self, fn, *args):  # type: ignore[no-untyped-def]
            _ = fn, args
            fut: Future[str] = Future()
            return fut

    orch._enqueue_feedback_work(FeedbackPool())
    assert any(kind == "feedback_turn" for kind, *_ in state.run_starts)


def test_reap_finished_completes_issue_run_once(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    done: Future[WorkResult] = Future()
    done.set_result(
        WorkResult(
            issue_number=7,
            branch="agent/design/7-worker",
            pr_number=101,
            pr_url="https://example/pr/101",
        )
    )
    orch._running = {7: done}
    orch._running_issue_metadata = {
        7: _RunningIssueMetadata(
            run_id="run-7",
            flow="design_doc",
            branch="agent/design/7-worker",
            context_json="{}",
            source_issue_number=7,
            consumed_comment_id_max=0,
        )
    }

    orch._reap_finished()
    orch._reap_finished()
    assert state.run_finishes == [("run-7", "completed", None, None)]


def test_reap_finished_marks_failed_run_with_metadata(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    bad: Future[WorkResult] = Future()
    bad.set_exception(RuntimeError("boom"))
    orch._running = {7: bad}
    orch._running_issue_metadata = {
        7: _RunningIssueMetadata(
            run_id="run-7",
            flow="design_doc",
            branch="agent/design/7-worker",
            context_json="{}",
            source_issue_number=7,
            consumed_comment_id_max=0,
        )
    }

    orch._reap_finished()
    assert state.run_finishes == [("run-7", "failed", "unknown", "boom")]


def test_feedback_terminal_status_from_state_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7-worker",
        status="awaiting_feedback",
        last_seen_head_sha=None,
    )
    assert (
        orch._feedback_terminal_status_from_state(tracked=tracked, fallback="blocked") == "blocked"
    )
    state._pr_status_by_pr[101] = "awaiting_feedback"
    assert (
        orch._feedback_terminal_status_from_state(tracked=tracked, fallback="blocked")
        == "completed"
    )


def test_ensure_poll_setup_logs_when_stale_runs_reconciled(tmp_path: Path) -> None:
    cfg = _config(tmp_path)

    class ReconState(FakeState):
        def reconcile_unfinished_agent_runs(self, *, repo_full_name: str | None = None) -> int:
            _ = repo_full_name
            return 1

        def reconcile_stale_running_issue_runs_with_followups(
            self, *, repo_full_name: str | None = None
        ) -> int:
            _ = repo_full_name
            return 1

    state = ReconState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._ensure_poll_setup()


def test_ensure_poll_setup_replays_pending_create_pr_calls(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    issue = _issue()
    run_id = state.record_issue_run_start(
        run_kind="issue_flow",
        issue_number=issue.number,
        flow="design_doc",
        branch="agent/design/7-add-worker-scheduler",
        run_id="run-7",
        repo_full_name=cfg.repo.full_name,
    )
    assert run_id == "run-7"
    state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key=("create_pr:7:main:agent/design/7-add-worker-scheduler:run-7"),
        payload_json=(
            '{"base":"main","body":"Design doc.\\n\\nRefs #7",'
            '"head":"agent/design/7-add-worker-scheduler",'
            '"issue_number":7,'
            '"title":"Design doc for #7: Design Title"}'
        ),
        run_id="run-7",
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        repo_full_name=cfg.repo.full_name,
    )
    github = FakeGitHub([issue])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    orch._ensure_poll_setup()

    tracked = state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name)
    assert len(tracked) == 1
    assert tracked[0].issue_number == issue.number
    assert tracked[0].pr_number == 101
    assert github.created_prs == [
        (
            "Design doc for #7: Design Title",
            "agent/design/7-add-worker-scheduler",
            "main",
            "Design doc.\n\nRefs #7",
        )
    ]
    replayable = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )
    assert replayable == ()
    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        run_row = conn.execute(
            """
            SELECT terminal_status
            FROM agent_run_history
            WHERE run_id = ?
            """,
            ("run-7",),
        ).fetchone()
    finally:
        conn.close()
    assert run_row is not None
    assert run_row[0] == "completed"


def test_ensure_poll_setup_replays_in_progress_create_pr_calls_without_duplicates(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    issue = _issue()
    state.record_issue_run_start(
        run_kind="issue_flow",
        issue_number=issue.number,
        flow="design_doc",
        branch="agent/design/7-add-worker-scheduler",
        run_id="run-7",
        repo_full_name=cfg.repo.full_name,
    )
    intent = state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key=("create_pr:7:main:agent/design/7-add-worker-scheduler:run-7"),
        payload_json=(
            '{"base":"main","body":"Design doc.\\n\\nRefs #7",'
            '"head":"agent/design/7-add-worker-scheduler",'
            '"issue_number":7,'
            '"title":"Design doc for #7: Design Title"}'
        ),
        run_id="run-7",
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        repo_full_name=cfg.repo.full_name,
    )
    assert state.mark_github_call_in_progress(
        call_id=intent.call_id,
        repo_full_name=cfg.repo.full_name,
    )
    github = FakeGitHub([issue])
    github.pr_lookup_by_head_base[("agent/design/7-add-worker-scheduler", "main")] = PullRequest(
        number=212,
        html_url="https://example/pr/212",
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    orch._ensure_poll_setup()

    assert github.created_prs == []
    tracked = state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name)
    assert len(tracked) == 1
    assert tracked[0].pr_number == 212
    replayable = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )
    assert replayable == ()


@pytest.mark.parametrize(
    ("payload_json", "message"),
    [
        ("[]", "Invalid create_pull_request outbox payload"),
        (
            '{"title":"Design doc","head":"agent/design/7-worker","base":"main","body":"Design body"}',
            "missing issue_number",
        ),
        (
            '{"issue_number":7,"head":"agent/design/7-worker","base":"main","body":"Design body"}',
            "missing title",
        ),
        (
            '{"issue_number":7,"title":"Design doc","base":"main","body":"Design body"}',
            "missing head",
        ),
        (
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker","body":"Design body"}',
            "missing base",
        ),
        (
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker","base":"main"}',
            "missing body",
        ),
    ],
)
def test_parse_create_pr_outbox_payload_validation(
    tmp_path: Path, payload_json: str, message: str
) -> None:
    orch = Phase1Orchestrator(
        _config(tmp_path),
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    with pytest.raises(RuntimeError, match=message):
        orch._parse_create_pr_outbox_payload(payload_json)


def test_pull_request_from_outbox_result_validation_paths(tmp_path: Path) -> None:
    orch = Phase1Orchestrator(
        _config(tmp_path),
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    assert orch._pull_request_from_outbox_result(None) is None
    assert orch._pull_request_from_outbox_result("[]") is None
    assert (
        orch._pull_request_from_outbox_result('{"pr_number":"bad","pr_url":"https://example/pr/1"}')
        is None
    )
    assert orch._pull_request_from_outbox_result('{"pr_number":1,"pr_url":1}') is None
    parsed = orch._pull_request_from_outbox_result(
        '{"pr_number":123,"pr_url":"https://example/pr/123"}'
    )
    assert parsed is not None
    assert parsed.number == 123
    assert parsed.html_url == "https://example/pr/123"


def test_find_existing_pull_request_for_branch_handles_missing_or_legacy_finder(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    github = FakeGitHub([])
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    setattr(github, "find_pull_request_by_head", 7)
    assert (
        orch._find_existing_pull_request_for_branch(
            head="agent/design/7-worker",
            base="main",
        )
        is None
    )

    legacy_gh = FakeGitHub([])

    def legacy_finder_without_state(*, head: str, base: str | None = None) -> PullRequest | None:
        _ = head, base
        return PullRequest(number=222, html_url="https://example/pr/222")

    setattr(legacy_gh, "find_pull_request_by_head", legacy_finder_without_state)
    legacy_orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=legacy_gh,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    pr = legacy_orch._find_existing_pull_request_for_branch(
        head="agent/design/7-worker",
        base="main",
    )
    assert pr is not None
    assert pr.number == 222

    class LegacyAllFallbackGitHub(FakeGitHub):
        def __init__(self) -> None:
            super().__init__([])
            self.call_count = 0

        def find_pull_request_by_head(
            self,
            *,
            head: str,
            base: str | None = None,
            state: str = "open",
        ) -> PullRequest | None:
            _ = head, base, state
            self.call_count += 1
            if self.call_count == 1:
                return None
            if self.call_count == 2:
                raise TypeError("state not supported")
            return PullRequest(number=333, html_url="https://example/pr/333")

    all_fallback_gh = LegacyAllFallbackGitHub()
    all_fallback_orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=all_fallback_gh,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    pr = all_fallback_orch._find_existing_pull_request_for_branch(
        head="agent/design/7-worker",
        base="main",
    )
    assert pr is not None
    assert pr.number == 333
    assert all_fallback_gh.call_count == 3


def test_execute_create_pr_outbox_call_uses_succeeded_result_without_github_call(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    github = FakeGitHub([_issue(7, "Design issue")])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    entry = GitHubCallOutboxState(
        call_id=1,
        call_kind="create_pull_request",
        dedupe_key="create_pr:7:main:agent/design/7-worker:run-7",
        payload_json=(
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker",'
            '"base":"main","body":"Design body"}'
        ),
        status="succeeded",
        state_applied=False,
        attempt_count=1,
        last_error=None,
        result_json='{"pr_number":101,"pr_url":"https://example/pr/101"}',
        run_id="run-7",
        issue_number=7,
        branch="agent/design/7-worker",
        pr_number=101,
        created_at="2026-02-26T00:00:00.000Z",
        updated_at="2026-02-26T00:00:00.000Z",
    )

    pr = orch._execute_create_pr_outbox_call(entry)
    assert pr.number == 101
    assert pr.html_url == "https://example/pr/101"
    assert github.created_prs == []


def test_execute_create_pr_outbox_call_repairs_succeeded_rows_missing_result_payload(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key="create_pr:7:main:agent/design/7-worker:run-7",
        payload_json=(
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker",'
            '"base":"main","body":"Design body"}'
        ),
        run_id="run-7",
        issue_number=7,
        branch="agent/design/7-worker",
        repo_full_name=cfg.repo.full_name,
    )
    entry = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )[0]
    assert state.mark_github_call_succeeded(
        call_id=entry.call_id,
        result_json="[]",
        repo_full_name=cfg.repo.full_name,
    )
    succeeded_entry = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )[0]

    github = FakeGitHub([_issue(7, "Design issue")])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    pr = orch._execute_create_pr_outbox_call(succeeded_entry)
    assert pr.number == 101
    replayable = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )
    assert len(replayable) == 1
    assert replayable[0].status == "succeeded"
    assert replayable[0].result_json == '{"pr_number": 101, "pr_url": "https://example/pr/101"}'


def test_execute_create_pr_outbox_call_recovers_after_create_error(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    issue = _issue(7, "Design issue")
    state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key="create_pr:7:main:agent/design/7-worker:run-7",
        payload_json=(
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker",'
            '"base":"main","body":"Design body"}'
        ),
        run_id="run-7",
        issue_number=7,
        branch="agent/design/7-worker",
        repo_full_name=cfg.repo.full_name,
    )
    entry = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )[0]

    class RecoverAfterCreateErrorGitHub(FakeGitHub):
        def __init__(self) -> None:
            super().__init__([issue])
            self.lookup_call_count = 0

        def find_pull_request_by_head(
            self, *, head: str, base: str | None = None, state: str = "open"
        ) -> PullRequest | None:
            _ = head, base, state
            self.lookup_call_count += 1
            if self.lookup_call_count == 3:
                return PullRequest(number=212, html_url="https://example/pr/212")
            return None

        def create_pull_request(self, title: str, head: str, base: str, body: str) -> PullRequest:
            _ = title, head, base, body
            raise RuntimeError("create failed after side effects")

    github = RecoverAfterCreateErrorGitHub()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    pr = orch._execute_create_pr_outbox_call(entry)
    assert pr.number == 212
    replayable = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )
    assert len(replayable) == 1
    assert replayable[0].status == "succeeded"
    assert replayable[0].pr_number == 212


def test_replay_pending_create_pr_calls_reraises_github_polling_errors(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key="create_pr:7:main:agent/design/7-worker:run-7",
        payload_json=(
            '{"issue_number":7,"title":"Design doc","head":"agent/design/7-worker",'
            '"base":"main","body":"Design body"}'
        ),
        run_id="run-7",
        issue_number=7,
        branch="agent/design/7-worker",
        repo_full_name=cfg.repo.full_name,
    )

    class PollingErrorGitHub(FakeGitHub):
        def find_pull_request_by_head(
            self, *, head: str, base: str | None = None, state: str = "open"
        ) -> PullRequest | None:
            _ = head, base, state
            return None

        def create_pull_request(self, title: str, head: str, base: str, body: str) -> PullRequest:
            _ = title, head, base, body
            raise GitHubPollingError("temporary outage")

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=PollingErrorGitHub([_issue(7, "Design issue")]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    with pytest.raises(GitHubPollingError, match="temporary outage"):
        orch._replay_pending_create_pr_calls()


def test_replay_pending_create_pr_calls_logs_and_continues_on_parse_error(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = StateStore(tmp_path / "state.db")
    state.upsert_github_call_intent(
        call_kind="create_pull_request",
        dedupe_key="create_pr:7:main:agent/design/7-worker:run-7",
        payload_json="[]",
        run_id="run-7",
        issue_number=7,
        branch="agent/design/7-worker",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([_issue(7, "Design issue")]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    assert orch._replay_pending_create_pr_calls() == 0
    replayable = state.list_replayable_github_calls(
        call_kind="create_pull_request",
        repo_full_name=cfg.repo.full_name,
    )
    assert len(replayable) == 1
    assert replayable[0].status == "pending"


@given(st.text())
def test_slugify_is_safe(text: str) -> None:
    slug = _slugify(text)
    assert slug
    assert re.fullmatch(r"[a-z0-9-]+", slug)


def test_flow_helpers() -> None:
    cfg = RepoConfig(
        repo_id="r",
        owner="o",
        name="r",
        default_branch="main",
        trigger_label="agent:design",
        bugfix_label="agent:bugfix",
        small_job_label="agent:small-job",
        coding_guidelines_path="docs/python_style.md",
        design_docs_dir="docs/design",
        allowed_users=frozenset({"o"}),
        local_clone_source=None,
        remote_url=None,
    )
    assert _trigger_labels(cfg) == (
        "agent:roadmap",
        "agent:bugfix",
        "agent:small-job",
        "agent:design",
    )
    assert _trigger_labels(cfg, enable_roadmaps=False) == (
        "agent:bugfix",
        "agent:small-job",
        "agent:design",
    )
    assert _trigger_labels(cfg, enable_roadmaps=True) == (
        "agent:roadmap",
        "agent:bugfix",
        "agent:small-job",
        "agent:design",
    )

    issue = _issue(labels=("agent:design", "agent:bugfix", "agent:small-job"))
    assert (
        _resolve_issue_flow(
            issue=issue,
            design_label=cfg.trigger_label,
            roadmap_label=cfg.roadmap_label,
            bugfix_label=cfg.bugfix_label,
            small_job_label=cfg.small_job_label,
            ignore_label=cfg.ignore_label,
        )
        == "bugfix"
    )
    assert (
        _resolve_issue_flow(
            issue=_issue(labels=("agent:bugfix", cfg.ignore_label)),
            design_label=cfg.trigger_label,
            roadmap_label=cfg.roadmap_label,
            bugfix_label=cfg.bugfix_label,
            small_job_label=cfg.small_job_label,
            ignore_label=cfg.ignore_label,
        )
        is None
    )
    assert (
        _resolve_issue_flow(
            issue=_issue(labels=("agent:roadmap", "agent:bugfix")),
            design_label=cfg.trigger_label,
            roadmap_label=cfg.roadmap_label,
            bugfix_label=cfg.bugfix_label,
            small_job_label=cfg.small_job_label,
            ignore_label=cfg.ignore_label,
        )
        == "roadmap"
    )
    assert _issue_branch(flow="design_doc", issue_number=7, slug="x") == "agent/design/7-x"
    assert _issue_branch(flow="roadmap", issue_number=7, slug="x") == "agent/roadmap/7-x"
    assert _issue_branch(flow="bugfix", issue_number=7, slug="x") == "agent/bugfix/7-x"
    assert _issue_branch(flow="small_job", issue_number=7, slug="x") == "agent/small/7-x"
    assert _default_commit_message(flow="bugfix", issue_number=7) == "fix: resolve issue #7"
    assert _default_commit_message(flow="small_job", issue_number=7) == "feat: implement issue #7"
    assert (
        _resolve_issue_flow(
            issue=_issue(labels=("triage",)),
            design_label=cfg.trigger_label,
            roadmap_label=cfg.roadmap_label,
            bugfix_label=cfg.bugfix_label,
            small_job_label=cfg.small_job_label,
        )
        is None
    )
    assert (
        _has_regression_test_changes(("tests/test_a.py", "src/a.py"), (re.compile(r"^tests/"),))
        is True
    )
    assert _has_regression_test_changes(("src/a.py",), (re.compile(r"^tests/"),)) is False
    assert (
        _has_regression_test_changes(
            ("integration/FooSpec.scala",),
            (re.compile(r"^tests/"), re.compile(r"^integration/")),
        )
        is True
    )
    assert _render_regex_patterns(()) == "<none>"
    assert _render_regex_patterns((re.compile(r"^tests/"),)) == "`^tests/`"
    assert _render_regex_patterns((re.compile(r"^tests/"), re.compile(r"^integration/"))) == (
        "`^tests/` or `^integration/`"
    )
    assert (
        _render_regex_patterns(
            (re.compile(r"^tests/"), re.compile(r"^integration/"), re.compile(r"\.scala$"))
        )
        == "`^tests/`, `^integration/`, or `\\.scala$`"
    )
    assert _design_branch_slug("agent/design/7-x") == "7-x"
    assert _design_branch_slug("agent/small/7-x") is None
    assert _design_branch_slug("agent/design/   ") is None
    assert _parse_superseding_roadmap_parent("Supersedes roadmap #41") == 41
    assert _parse_superseding_roadmap_parent("no parent") is None
    assert _roadmap_child_label_for_kind(kind="design_doc", repo=cfg) == cfg.trigger_label
    assert _roadmap_child_label_for_kind(kind="small_job", repo=cfg) == cfg.small_job_label
    assert _roadmap_child_label_for_kind(kind="roadmap", repo=cfg) == cfg.roadmap_label
    with pytest.raises(RuntimeError, match="Unsupported roadmap node kind"):
        _roadmap_child_label_for_kind(kind="unknown", repo=cfg)
    child_body = _render_roadmap_child_issue_body(
        roadmap_issue_number=7,
        node_id="n1",
        dependencies_json='[{"node_id":"n0","requires":"planned"}]',
        body_markdown="Body",
    )
    assert "Parent roadmap: #7" in child_body
    assert "n0 (planned)" in child_body
    invalid_child_body = _render_roadmap_child_issue_body(
        roadmap_issue_number=7,
        node_id="n2",
        dependencies_json="{",
        body_markdown="Body",
    )
    assert "- none" in invalid_child_body
    mixed_child_body = _render_roadmap_child_issue_body(
        roadmap_issue_number=7,
        node_id="n3",
        dependencies_json='[1, {"node_id":"n1","requires":"implemented"}]',
        body_markdown="Body",
    )
    assert "n1 (implemented)" in mixed_child_body
    status_body = _render_roadmap_status_report(
        roadmap_status="active",
        graph_version=1,
        adjustment_state="idle",
        rows=(
            RoadmapStatusSnapshotRow(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="n1",
                kind="small_job",
                status="issued",
                dependency_summary="-",
                child_issue_number=10,
                child_issue_url="https://example/issues/10",
                last_progress_at="2026-03-01T00:00:00.000Z",
                blocked_since_at=None,
            ),
        ),
        blockers=(
            RoadmapBlockerRow(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="n2",
                status="blocked",
                blocked_since_at="2026-03-01T00:00:00.000Z",
                child_issue_number=11,
                child_issue_url="https://example/issues/11",
                last_progress_at="2026-03-01T00:00:00.000Z",
            ),
        ),
        request_comment_id=123,
    )
    assert "request_comment_id: 123" in status_body
    assert "n1 [small_job]" in status_body
    assert "n2: blocked_since" in status_body
    empty_status_body = _render_roadmap_status_report(
        roadmap_status="active",
        graph_version=1,
        adjustment_state="idle",
        rows=(),
        blockers=(),
        request_comment_id=321,
    )
    assert "(no roadmap nodes found)" in empty_status_body
    assert "Blockers (oldest first):\n- none" in empty_status_body
    assert "graph_version: 1" in empty_status_body
    assert "adjustment_state: idle" in empty_status_body
    dependency_refs = _ready_frontier_dependency_references(
        nodes_by_id={
            "n2": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="n2",
                kind="small_job",
                title="Ready",
                body_markdown="Ready",
                dependencies_json='[{"node_id":"n1","requires":"implemented"},1]',
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
            "n3": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="n3",
                kind="small_job",
                title="Also ready",
                body_markdown="Also ready",
                dependencies_json='[{"node_id":"n1","requires":"planned"}]',
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
            "bad": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="bad",
                kind="small_job",
                title="Bad",
                body_markdown="Bad",
                dependencies_json="{",
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
            "bad-shape": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="bad-shape",
                kind="small_job",
                title="Bad shape",
                body_markdown="Bad shape",
                dependencies_json='"not-a-list"',
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
            "bad-fields": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="bad-fields",
                kind="small_job",
                title="Bad fields",
                body_markdown="Bad fields",
                dependencies_json='[{"node_id":1,"requires":"implemented"},{"node_id":"n1"}]',
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
            "bad-requires": RoadmapNodeRecord(
                repo_full_name="o/r",
                roadmap_issue_number=7,
                node_id="bad-requires",
                kind="small_job",
                title="Bad requires",
                body_markdown="Bad requires",
                dependencies_json='[{"node_id":"n1","requires":"reviewed"}]',
                introduced_in_version=1,
                retired_in_version=None,
                is_active=True,
                child_issue_number=None,
                child_issue_url=None,
                status="pending",
                planned_at=None,
                implemented_at=None,
                status_changed_at=None,
                last_progress_at=None,
                blocked_since_at=None,
                claim_token=None,
                updated_at="t",
            ),
        },
        ready_node_ids=("n2", "n3", "missing", "bad", "bad-shape", "bad-fields", "bad-requires"),
    )
    assert tuple(reference.ready_node_id for reference in dependency_refs["n1"]) == ("n2", "n3")
    assert _roadmap_dependency_changed_files(tuple(f"src/file_{i}.py" for i in range(30))) == tuple(
        f"src/file_{i}.py" for i in range(25)
    )
    key_comments = _key_roadmap_dependency_comments(
        (
            PullRequestIssueComment(
                comment_id=1,
                body="bot",
                user_login="mergexo[bot]",
                html_url="https://example/comment/1",
                created_at="t1",
                updated_at="t1",
            ),
            PullRequestIssueComment(
                comment_id=2,
                body="keep-1",
                user_login="alice",
                html_url="https://example/comment/2",
                created_at="t2",
                updated_at="t2",
            ),
            PullRequestIssueComment(
                comment_id=3,
                body="keep-2",
                user_login="bob",
                html_url="https://example/comment/3",
                created_at="t3",
                updated_at="t3",
            ),
            PullRequestIssueComment(
                comment_id=4,
                body="keep-3",
                user_login="carol",
                html_url="https://example/comment/4",
                created_at="t4",
                updated_at="t4",
            ),
            PullRequestIssueComment(
                comment_id=5,
                body="keep-4",
                user_login="dave",
                html_url="https://example/comment/5",
                created_at="t5",
                updated_at="t5",
            ),
        )
    )
    assert tuple(comment.body for comment in key_comments) == ("keep-2", "keep-3", "keep-4")
    resolution_markers = _roadmap_dependency_resolution_markers(
        node=RoadmapNodeRecord(
            repo_full_name="o/r",
            roadmap_issue_number=7,
            node_id="n1",
            kind="small_job",
            title="Dependency",
            body_markdown="Dependency",
            dependencies_json="[]",
            introduced_in_version=1,
            retired_in_version=None,
            is_active=True,
            child_issue_number=10,
            child_issue_url="https://example/issues/10",
            status="completed",
            planned_at="t1",
            implemented_at="t2",
            status_changed_at="t2",
            last_progress_at="t2",
            blocked_since_at=None,
            claim_token=None,
            updated_at="t2",
        ),
        issue_run=IssueRunRecord(
            repo_full_name="o/r",
            issue_number=10,
            status="merged",
            branch="agent/impl/10-dependency",
            pr_number=101,
            pr_url="https://example/pr/101",
            error=None,
        ),
    )
    assert "issue_run_status=merged" in resolution_markers
    assert "planned_at=set" in resolution_markers
    assert "issue_run_pr=set" in resolution_markers
    error_markers = _roadmap_dependency_resolution_markers(
        node=RoadmapNodeRecord(
            repo_full_name="o/r",
            roadmap_issue_number=7,
            node_id="n5",
            kind="small_job",
            title="Errored issue run",
            body_markdown="Errored issue run",
            dependencies_json="[]",
            introduced_in_version=1,
            retired_in_version=None,
            is_active=True,
            child_issue_number=12,
            child_issue_url="https://example/issues/12",
            status="blocked",
            planned_at=None,
            implemented_at=None,
            status_changed_at=None,
            last_progress_at=None,
            blocked_since_at="t",
            claim_token=None,
            updated_at="t",
        ),
        issue_run=IssueRunRecord(
            repo_full_name="o/r",
            issue_number=12,
            status="blocked",
            branch=None,
            pr_number=None,
            pr_url=None,
            error="boom",
        ),
    )
    assert "issue_run_error=boom" in error_markers
    missing_issue_run_markers = _roadmap_dependency_resolution_markers(
        node=RoadmapNodeRecord(
            repo_full_name="o/r",
            roadmap_issue_number=7,
            node_id="n4",
            kind="small_job",
            title="No issue run",
            body_markdown="No issue run",
            dependencies_json="[]",
            introduced_in_version=1,
            retired_in_version=None,
            is_active=True,
            child_issue_number=None,
            child_issue_url=None,
            status="pending",
            planned_at=None,
            implemented_at=None,
            status_changed_at=None,
            last_progress_at=None,
            blocked_since_at=None,
            claim_token=None,
            updated_at="t",
        ),
        issue_run=None,
    )
    assert "issue_run=missing" in missing_issue_run_markers
    assert _pre_pr_flow_label("roadmap") == "roadmap"
    assert _flow_trigger_label(flow="roadmap", repo=cfg) == cfg.roadmap_label
    roadmap_recovery = _recovery_pr_payload_for_issue(
        issue=_issue(8, "Plan"), branch="agent/roadmap/8-plan"
    )
    assert roadmap_recovery[0].startswith("Roadmap for #8:")
    assert _flow_label_from_branch("agent/roadmap/8-plan") == "roadmap"
    assert (
        _design_doc_url(
            repo_full_name="johnynek/mergexo",
            default_branch="main",
            design_doc_path="docs/design/7-x.md",
        )
        == "https://github.com/johnynek/mergexo/blob/main/docs/design/7-x.md"
    )


def test_observability_failure_and_terminal_helpers() -> None:
    assert _normalize_feedback_terminal_status("completed") == "completed"
    assert _normalize_feedback_terminal_status("bad-value") == "completed"
    assert _failure_class_for_exception(DirectFlowBlockedError("x flow blocked: waiting")) == (
        "policy_block"
    )
    assert _failure_class_for_exception(RuntimeError("required pre-push tests failed")) == (
        "tests_failed"
    )
    assert _failure_class_for_exception(RuntimeError("non-fast-forward transition")) == (
        "history_rewrite"
    )
    assert _failure_class_for_exception(GitHubPollingError("x")) == "github_error"
    assert _failure_class_for_exception(RuntimeError("github API failure")) == "github_error"
    assert _failure_class_for_exception(CommandError(["git"], 1, "", "")) == "agent_error"
    assert (
        _failure_class_for_exception(
            CommandError(
                "Command failed\n"
                "cmd: git push origin branch\n"
                "exit: 1\n"
                "stdout:\n\n"
                "stderr:\nERROR: no healthy upstream\nfatal: Could not read from remote repository.\n"
            )
        )
        == "github_error"
    )
    assert _failure_class_for_exception(FeedbackTransientGitError("cannot reach github")) == (
        "github_error"
    )
    assert _is_transient_feedback_git_command_error(
        "Command failed\n"
        "cmd: git clone foo\n"
        "exit: 128\n"
        "stdout:\n\n"
        "stderr:\nERROR: no healthy upstream\nfatal: Could not read from remote repository.\n"
    )
    assert not _is_transient_feedback_git_command_error(
        "Command failed\n"
        "cmd: git push origin branch\n"
        "exit: 1\n"
        "stdout:\n\n"
        "stderr:\nAutomatic merge failed; merge conflict in src/app.py\n"
    )
    assert not _is_transient_feedback_git_command_error("   \n\t")
    assert not _is_transient_feedback_git_command_error(
        "Command failed\n"
        "cmd: /tmp/custom-wrapper git fetch origin\n"
        "exit: 128\n"
        "stdout:\n\n"
        "stderr:\nERROR: no healthy upstream\n"
    )
    assert not _is_transient_git_remote_error("   ")
    assert _is_transient_git_remote_error("remote: Internal Server Error")
    assert _feedback_transient_git_retry_delay_seconds(1) == 5
    assert _feedback_transient_git_retry_delay_seconds(2) == 10
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        _feedback_transient_git_retry_delay_seconds(0)


def test_render_design_doc_includes_frontmatter_and_summary() -> None:
    issue = _issue()
    design = GeneratedDesign(
        title="Design",
        summary="Summary",
        touch_paths=("src/a.py", "src/b.py"),
        design_doc_markdown="## Section\n\nContent\n",
    )

    doc = _render_design_doc(issue=issue, design=design)

    assert "issue: 7" in doc
    assert "touch_paths:" in doc
    assert "  - src/a.py" in doc
    assert "## Summary" in doc
    assert "## Section" in doc


def test_render_design_doc_strips_duplicate_generated_preamble() -> None:
    issue = _issue()
    design = GeneratedDesign(
        title="Design",
        summary="Summary",
        touch_paths=("src/a.py",),
        design_doc_markdown=(
            "---\n"
            "issue: 7\n"
            "priority: 2\n"
            "touch_paths:\n"
            "  - src/a.py\n"
            "---\n\n"
            "# Design\n\n"
            "_Issue: #7 (https://example/issues/7)_\n\n"
            "## Summary\n\n"
            "Repeated summary.\n\n"
            "## Context\n\n"
            "Real content.\n"
        ),
    )

    doc = _render_design_doc(issue=issue, design=design)

    assert doc.count("touch_paths:") == 1
    assert doc.count("# Design") == 1
    assert doc.count("## Summary") == 1
    assert "## Context\n\nReal content." in doc
    assert "Repeated summary." not in doc


def test_normalize_design_doc_body_keeps_malformed_frontmatter_literal() -> None:
    body = _normalize_design_doc_body("---\nissue: 7\npriority: 2")

    assert body == "---\nissue: 7\npriority: 2"


def test_normalize_design_doc_body_strips_leading_blank_lines() -> None:
    body = _normalize_design_doc_body("\n\n## Context\n\nReal content.\n")

    assert body == "## Context\n\nReal content."


def test_process_issue_happy_path(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    result = orch._process_issue(_issue(), "design_doc")

    assert result.issue_number == 7
    assert result.pr_number == 101
    assert github.created_prs
    assert "Refs #7" in github.created_prs[0][3]
    assert "Source issue:" not in github.created_prs[0][3]
    assert github.comments == [
        (7, "MergeXO assigned an agent and started design work for issue #7."),
        (7, "Opened design PR: https://example/pr/101"),
    ]
    assert git.prepare_calls
    assert git.branch_calls
    assert git.commit_calls
    assert git.push_calls
    assert git.cleanup_calls

    branch = result.branch
    generated_path = git.branch_calls[0][0] / f"docs/design/7-{_slugify('Add worker scheduler')}.md"
    assert generated_path.exists()
    contents = generated_path.read_text(encoding="utf-8")
    assert f"# {agent.generated.title}" in contents
    assert "touch_paths" in contents
    assert branch.startswith("agent/design/7-")
    assert state.saved_sessions == [(7, "codex", "thread-123")]


def test_process_issue_rejects_unauthorized_author_before_side_effects(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="not allowed by repo.allowed_users"):
        orch._process_issue(_issue(author_login="outsider"), "design_doc")

    assert git.ensure_checkout_calls == []
    assert git.prepare_calls == []
    assert git.branch_calls == []
    assert github.created_prs == []
    assert github.comments == []


def test_process_issue_bugfix_flow_happy_path(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:bugfix",))
    result = orch._process_issue(issue, "bugfix")

    assert result.branch.startswith("agent/bugfix/7-")
    assert len(agent.bugfix_calls) == 1
    assert len(agent.small_job_calls) == 0
    assert git.commit_calls[-1][1] == "fix: resolve issue"
    assert github.created_prs[0][0] == "Fix bug"
    assert "Fixes #7" in github.created_prs[0][3]
    assert "Source issue:" not in github.created_prs[0][3]
    assert github.comments == [
        (7, "MergeXO assigned an agent and started bugfix PR work for issue #7."),
        (7, "Opened bugfix PR: https://example/pr/101"),
    ]


def test_process_issue_bugfix_omits_missing_coding_guidelines_file(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert agent.bugfix_guidelines_paths == [None]


def test_process_issue_bugfix_uses_coding_guidelines_file_when_present(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    _write_checkout_file(git.checkout_root, "docs/python_style.md", content="# style\n")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert agent.bugfix_guidelines_paths == ["docs/python_style.md"]


def test_resolve_checkout_path_returns_absolute_path_unchanged(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    absolute_path = tmp_path / "required-tests.sh"
    resolved = orch._resolve_checkout_path(
        configured_path=str(absolute_path),
        checkout_path=tmp_path / "checkout",
    )

    assert resolved == absolute_path


def test_required_tests_command_for_checkout_none_uses_configured_command(tmp_path: Path) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    assert (
        orch._required_tests_command_for_checkout(checkout_path=None) == "scripts/required-tests.sh"
    )


def test_process_issue_bugfix_skips_missing_required_tests_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cmd, cwd, input_text, check
        raise AssertionError("required tests command should be skipped when the file is missing")

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert len(agent.bugfix_calls) == 1
    assert "Required pre-push test command" not in agent.bugfix_calls[0][0].body
    assert len(git.commit_calls) == 1
    assert len(git.push_calls) == 1


def test_process_issue_bugfix_retries_when_required_tests_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    run_calls = 0

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        nonlocal run_calls
        _ = cwd, input_text, check
        run_calls += 1
        assert cmd[-1].endswith("scripts/required-tests.sh")
        if run_calls == 1:
            raise CommandError(
                "Command failed\n"
                "cmd: /tmp/required-tests.sh\n"
                "exit: 1\n"
                "stdout:\nfirst stdout\n"
                "stderr:\nfirst stderr\n"
            )
        return ""

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:bugfix",))
    result = orch._process_issue(issue, "bugfix")

    assert result.pr_number == 101
    assert len(agent.bugfix_calls) == 2
    assert "Required pre-push test command" in agent.bugfix_calls[0][0].body
    assert "Failure output:" in agent.bugfix_calls[1][0].body
    assert "stdout:" in agent.bugfix_calls[1][0].body
    assert "stderr:" in agent.bugfix_calls[1][0].body
    assert len(git.commit_calls) == 2
    assert len(git.push_calls) == 1
    assert run_calls == 2


def test_process_issue_design_flow_blocks_when_required_tests_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[-1].endswith("scripts/required-tests.sh")
        raise CommandError(
            "Command failed\ncmd: /tmp/required-tests.sh\nexit: 1\nstdout:\nout\nstderr:\nerr\n"
        )

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="required pre-push tests failed before pushing"):
        orch._process_issue(_issue(), "design_doc")

    assert len(git.commit_calls) == 1
    assert git.push_calls == []
    assert github.created_prs == []
    assert any("design flow could not push" in body for _, body in github.comments)


def test_process_issue_bugfix_blocks_when_required_tests_repair_limit_exceeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    run_calls = 0

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        nonlocal run_calls
        _ = cwd, input_text, check
        run_calls += 1
        assert cmd[-1].endswith("scripts/required-tests.sh")
        raise CommandError(
            "Command failed\ncmd: /tmp/required-tests.sh\nexit: 1\nstdout:\nout\nstderr:\nerr\n"
        )

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="required pre-push tests did not pass"):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert len(agent.bugfix_calls) == 4
    assert len(git.commit_calls) == 4
    assert git.push_calls == []
    assert run_calls == 4
    assert any(
        "could not satisfy the required pre-push test" in body for _, body in github.comments
    )


def test_process_issue_small_job_flow_uses_default_commit_message(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    agent.small_job_result = DirectStartResult(
        pr_title="Do a small thing",
        pr_summary="Summary",
        commit_message=None,
        blocked_reason=None,
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:small-job",))
    result = orch._process_issue(issue, "small_job")

    assert result.branch.startswith("agent/small/7-")
    assert len(agent.small_job_calls) == 1
    assert git.commit_calls[-1][1] == "feat: implement issue #7"
    assert "Fixes #7" in github.created_prs[0][3]
    assert "Source issue:" not in github.created_prs[0][3]
    assert github.comments == [
        (7, "MergeXO assigned an agent and started small-job PR work for issue #7."),
        (7, "Opened small-job PR: https://example/pr/101"),
    ]


def test_process_issue_bugfix_requires_regression_tests(tmp_path: Path) -> None:
    cfg = _config(tmp_path, test_file_regex=(r"^tests/",))
    git = FakeGitManager(tmp_path / "checkouts")
    git.staged_files = ("src/a.py",)
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="regression test"):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert github.created_prs == []
    assert git.commit_calls == []
    assert any(
        "requires at least one staged regression test" in body for _, body in github.comments
    )


def test_process_issue_bugfix_without_test_file_regex_allows_non_test_changes(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    git.staged_files = ("src/a.py",)
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    result = orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert result.branch.startswith("agent/bugfix/7-")
    assert git.commit_calls != []
    assert github.created_prs != []


def test_process_issue_repairs_push_merge_conflict_with_agent(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:small-job",))

    original_push = git.push_branch
    push_attempts = 0

    def push_with_merge_conflict_once(checkout_path: Path, branch: str) -> None:
        nonlocal push_attempts
        push_attempts += 1
        if push_attempts == 1:
            raise CommandError(
                "Command failed\n"
                "cmd: git merge\n"
                "exit: 1\n"
                "stdout:\n\n"
                "stderr:\n"
                "CONFLICT (content): Merge conflict in src/a.py\n"
                "Automatic merge failed; fix conflicts and then commit the result.\n"
            )
        original_push(checkout_path, branch)

    git.push_branch = push_with_merge_conflict_once  # type: ignore[method-assign]

    result = orch._process_issue(issue, "small_job")

    assert result.pr_number == 101
    assert push_attempts == 2
    assert len(agent.small_job_calls) == 2
    assert "merge conflict" in agent.small_job_calls[1][0].body.lower()
    assert len(git.commit_calls) == 2
    assert github.created_prs != []


def test_process_issue_blocks_after_repeated_push_merge_conflicts(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:small-job",))

    def push_always_conflicts(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError(
            "Command failed\n"
            "cmd: git merge\n"
            "exit: 1\n"
            "stdout:\n\n"
            "stderr:\n"
            "CONFLICT (content): Merge conflict in src/a.py\n"
            "Automatic merge failed; fix conflicts and then commit the result.\n"
        )

    git.push_branch = push_always_conflicts  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="flow blocked"):
        orch._process_issue(issue, "small_job")

    assert github.created_prs == []
    assert any(
        "could not resolve push-time merge conflicts" in body.lower() for _, body in github.comments
    )
    assert len(agent.small_job_calls) == 4


def test_process_issue_push_non_conflict_error_propagates(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=("agent:small-job",))

    def push_non_conflict_failure(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("push rejected for unrelated reason")

    git.push_branch = push_non_conflict_failure  # type: ignore[method-assign]

    with pytest.raises(CommandError, match="unrelated reason"):
        orch._process_issue(issue, "small_job")

    assert len(agent.small_job_calls) == 1
    assert github.created_prs == []


def test_process_issue_small_job_revalidates_required_tests_after_push_auto_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    git.push_results = [True]
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=(cfg.repo.small_job_label,))
    required_test_calls: list[Path] = []
    required_test_results = iter((None, "post-merge required-tests failure"))

    def fake_run_required_tests(*, checkout_path: Path) -> str | None:
        required_test_calls.append(checkout_path)
        return next(required_test_results)

    monkeypatch.setattr(orch, "_run_required_tests_before_push", fake_run_required_tests)

    with pytest.raises(
        DirectFlowValidationError,
        match="required pre-push tests failed after push reconciled remote branch updates",
    ):
        orch._process_issue(issue, "small_job")

    assert len(required_test_calls) == 2
    assert len(git.push_calls) == 1
    assert github.created_prs == []
    assert any(
        issue_number == issue.number
        and "small-job flow pushed" in body
        and "required pre-push tests then failed on the merged branch" in body
        for issue_number, body in github.comments
    )


def test_process_issue_small_job_allows_push_auto_merge_when_recheck_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    git.push_results = [True]
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    issue = _issue(labels=(cfg.repo.small_job_label,))
    required_test_calls: list[Path] = []

    def fake_run_required_tests(*, checkout_path: Path) -> str | None:
        required_test_calls.append(checkout_path)
        return None

    monkeypatch.setattr(orch, "_run_required_tests_before_push", fake_run_required_tests)

    result = orch._process_issue(issue, "small_job")

    assert result.pr_number == 101
    assert len(required_test_calls) == 2
    assert len(git.push_calls) == 1
    assert github.created_prs != []


def test_process_issue_roadmap_revalidates_required_tests_after_push_auto_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True, required_tests="scripts/test.sh")
    issue = _issue(120, "Roadmap", labels=(cfg.repo.roadmap_label,))
    github = FakeGitHub([issue])
    git = FakeGitManager(tmp_path / "checkouts")
    git.push_results = [True]
    state = StateStore(tmp_path / "state.db")
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    required_test_calls: list[Path] = []
    required_test_results = iter((None, "post-merge required-tests failure"))

    def fake_run_required_tests(*, checkout_path: Path) -> str | None:
        required_test_calls.append(checkout_path)
        return next(required_test_results)

    monkeypatch.setattr(orch, "_run_required_tests_before_push", fake_run_required_tests)

    with pytest.raises(
        DirectFlowValidationError,
        match="required pre-push tests failed after push reconciled remote branch updates",
    ):
        orch._process_issue(issue, "roadmap")

    assert len(required_test_calls) == 2
    assert len(git.push_calls) == 1
    assert github.created_prs == []
    assert any(
        issue_number == issue.number
        and "roadmap flow pushed" in body
        and "required pre-push tests then failed on the merged branch" in body
        for issue_number, body in github.comments
    )


def test_process_implementation_candidate_happy_path(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    state = FakeState()

    checkout_path = git.ensure_checkout(0)
    design_doc = checkout_path / "docs/design/7-add-worker-scheduler.md"
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    design_doc.write_text("# Design\n\nImplement queue scheduler.", encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    result = orch._process_implementation_candidate(
        ImplementationCandidateState(
            issue_number=7,
            design_branch="agent/design/7-add-worker-scheduler",
            design_pr_number=44,
            design_pr_url="https://example/pr/44",
        )
    )

    assert result.branch == "agent/impl/7-add-worker-scheduler"
    assert len(agent.implementation_calls) == 1
    assert agent.implementation_calls[0][2] == "docs/design/7-add-worker-scheduler.md"
    assert github.created_prs[0][0] == "Implement merged design"
    assert "Fixes #7" in github.created_prs[0][3]
    assert (
        "Implements design doc: [docs/design/7-add-worker-scheduler.md]"
        "(https://github.com/johnynek/mergexo/blob/main/docs/design/7-add-worker-scheduler.md)"
        in github.created_prs[0][3]
    )
    assert "Design source PR: https://example/pr/44" in github.created_prs[0][3]
    assert "Source issue:" not in github.created_prs[0][3]
    assert github.comments == [
        (7, "MergeXO assigned an agent and started implementation PR work for issue #7."),
        (7, "Opened implementation PR: https://example/pr/101"),
    ]


def test_process_implementation_candidate_marks_codex_finished_on_agent_exception(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent(fail=True)
    state = FakeState()

    checkout_path = git.ensure_checkout(0)
    design_doc = checkout_path / "docs/design/7-add-worker-scheduler.md"
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    design_doc.write_text("# Design\n\nImplement queue scheduler.", encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="codex failed"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )

    assert len(agent.implementation_calls) == 1


def test_process_implementation_candidate_rejects_unauthorized_author(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue(author_login="outsider")])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="not allowed by repo.allowed_users"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )

    assert git.ensure_checkout_calls == []
    assert git.prepare_calls == []
    assert github.created_prs == []
    assert github.comments == []
    assert len(agent.implementation_calls) == 0


def test_process_implementation_candidate_requires_design_doc(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="merged design doc"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )

    assert github.created_prs == []
    assert len(agent.implementation_calls) == 0
    assert any("requires a merged design doc" in body for _, body in github.comments)


def test_process_implementation_candidate_rejects_invalid_design_branch(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="valid design branch suffix"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/small/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )


def test_process_implementation_candidate_blocked_posts_comment(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    agent.implementation_result = DirectStartResult(
        pr_title="N/A",
        pr_summary="N/A",
        commit_message=None,
        blocked_reason="Need migration strategy from humans.",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    state = FakeState()

    checkout_path = git.ensure_checkout(0)
    design_doc = checkout_path / "docs/design/7-add-worker-scheduler.md"
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    design_doc.write_text("# Design\n\nImplement queue scheduler.", encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="blocked"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )

    assert github.created_prs == []
    assert any("Need migration strategy from humans." in body for _, body in github.comments)


def test_process_implementation_candidate_recoverable_blocked_checkpoints_with_default_branch(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    git.checkpoint_head_sha_value = "implsha1"
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    agent.implementation_result = DirectStartResult(
        pr_title="N/A",
        pr_summary="N/A",
        commit_message=None,
        blocked_reason="Need migration strategy from humans.",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    state = StateStore(tmp_path / "state.db")

    checkout_path = git.ensure_checkout(0)
    design_doc = checkout_path / "docs/design/7-add-worker-scheduler.md"
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    design_doc.write_text("# Design\n\nImplement queue scheduler.", encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="implementation flow blocked"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            )
        )

    assert len(git.checkpoint_persist_calls) == 1
    assert git.checkpoint_persist_calls[0][1] == "agent/impl/7-add-worker-scheduler"


def test_process_implementation_candidate_recoverable_blocked_checkpoints_with_branch_override(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    git.checkpoint_head_sha_value = "implsha2"
    github = FakeGitHub(issues=[_issue()])
    agent = FakeAgent()
    agent.implementation_result = DirectStartResult(
        pr_title="N/A",
        pr_summary="N/A",
        commit_message=None,
        blocked_reason="Need migration strategy from humans.",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    state = StateStore(tmp_path / "state.db")

    checkout_path = git.ensure_checkout(0)
    design_doc = checkout_path / "docs/design/7-add-worker-scheduler.md"
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    design_doc.write_text("# Design\n\nImplement queue scheduler.", encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="implementation flow blocked"):
        orch._process_implementation_candidate(
            ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-add-worker-scheduler",
                design_pr_number=44,
                design_pr_url="https://example/pr/44",
            ),
            branch_override="agent/impl/custom-resume-branch",
        )

    assert len(git.checkpoint_persist_calls) == 1
    assert git.checkpoint_persist_calls[0][1] == "agent/impl/custom-resume-branch"


def test_process_issue_direct_flow_blocked_posts_comment(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    agent.bugfix_result = DirectStartResult(
        pr_title="Fix bug",
        pr_summary="Summary",
        commit_message=None,
        blocked_reason="Missing reproduction steps",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="blocked"):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert github.created_prs == []
    assert git.commit_calls == []
    assert any("Missing reproduction steps" in body for _, body in github.comments)


def test_process_issue_recoverable_blocked_checkpoints_and_posts_checkpoint_comment(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    git.checkpoint_head_sha_value = "abc1234"
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    agent.bugfix_result = DirectStartResult(
        pr_title="Fix bug",
        pr_summary="Summary",
        commit_message=None,
        blocked_reason="Need reproducible environment details",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=github,
        git_manager=git,
        agent=agent,
    )

    with pytest.raises(RuntimeError, match="bugfix flow blocked"):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert len(git.checkpoint_persist_calls) == 1
    checkpoint_call = git.checkpoint_persist_calls[0]
    assert checkpoint_call[1] == "agent/bugfix/7-add-worker-scheduler"

    checkpoint_comments = [body for _, body in github.comments if "checkpoint branch" in body]
    assert len(checkpoint_comments) == 1
    checkpoint_body = checkpoint_comments[0]
    assert "blocked reason: bugfix flow blocked: Need reproducible environment details" in (
        checkpoint_body
    )
    assert "checkpoint branch: `agent/bugfix/7-add-worker-scheduler`" in checkpoint_body
    assert "checkpoint commit: `abc1234`" in checkpoint_body
    assert "tree/abc1234" in checkpoint_body
    assert "compare/main...abc1234" in checkpoint_body
    token = compute_pre_pr_checkpoint_token(issue_number=7, checkpoint_sha="abc1234")
    assert token in checkpoint_body


def test_process_issue_recoverable_blocked_checkpoint_comment_is_idempotent(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    git.checkpoint_head_sha_value = "abc1234"
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    agent.bugfix_result = DirectStartResult(
        pr_title="Fix bug",
        pr_summary="Summary",
        commit_message=None,
        blocked_reason="Need reproducible environment details",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    existing_token = compute_pre_pr_checkpoint_token(issue_number=7, checkpoint_sha="abc1234")
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=99,
            body=f"existing\n\n<!-- mergexo-action:{existing_token} -->",
            user_login="mergexo[bot]",
            html_url="https://example/issues/7#issuecomment-99",
            created_at="2026-02-24T00:00:00Z",
            updated_at="2026-02-24T00:00:00Z",
        )
    ]
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=github,
        git_manager=git,
        agent=agent,
    )

    with pytest.raises(RuntimeError, match="bugfix flow blocked"):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    checkpoint_comments = [body for _, body in github.comments if "checkpoint branch" in body]
    assert checkpoint_comments == []


def test_process_issue_recoverable_blocked_checkpoint_failure_is_not_silent(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    git.persist_checkpoint_should_fail = True
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    agent.bugfix_result = DirectStartResult(
        pr_title="Fix bug",
        pr_summary="Summary",
        commit_message=None,
        blocked_reason="Need reproducible environment details",
        session=AgentSession(adapter="codex", thread_id="thread-123"),
    )
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=github,
        git_manager=git,
        agent=agent,
    )

    with pytest.raises(
        RuntimeError, match="checkpoint persistence failed before follow-up handoff"
    ):
        orch._process_issue(_issue(labels=("agent:bugfix",)), "bugfix")

    assert any(
        "could not persist a recoverable pre-pr checkpoint" in body.lower()
        for _, body in github.comments
    )


def test_process_direct_issue_rejects_unsupported_flow(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="Unsupported direct flow"):
        orch._process_direct_issue(
            issue=_issue(labels=("agent:design",)),
            flow="design_doc",
            checkout_path=tmp_path / "checkout",
            branch="agent/design/7-add-worker-scheduler",
        )


def test_process_issue_emits_lifecycle_logs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    configure_logging(verbose=True)
    _ = orch._process_issue(_issue(), "design_doc")

    text = capsys.readouterr().err
    assert "event=slot_acquired" in text
    assert "event=issue_processing_started flow=design_doc issue_number=7" in text
    assert (
        "event=design_turn_started branch=agent/design/7-add-worker-scheduler issue_number=7"
        in text
    )
    assert "event=design_turn_completed issue_number=7" in text
    assert "event=issue_processing_completed" in text
    assert "branch=agent/design/7-add-worker-scheduler" in text
    assert "flow=design_doc" in text
    assert "pr_number=101" in text
    assert "event=slot_released" in text


def test_repo_logging_context_wrappers_delegate_to_underlying_methods(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    issue_result = WorkResult(issue_number=7, branch="b1", pr_number=11, pr_url="u1")
    impl_result = WorkResult(issue_number=7, branch="b2", pr_number=12, pr_url="u2")
    observed: dict[str, object] = {}

    def fake_process_issue(
        issue: Issue,
        flow: IssueFlow,
        *,
        branch_override: str | None = None,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        observed["issue_number"] = issue.number
        observed["flow"] = flow
        observed["branch_override"] = branch_override
        observed["pre_pr_last_consumed_comment_id"] = pre_pr_last_consumed_comment_id
        logging.getLogger("mergexo.tests.wrapper").info("event=wrapper_probe")
        return issue_result

    def fake_process_implementation_candidate(
        candidate: ImplementationCandidateState,
        *,
        issue_override: Issue | None = None,
        branch_override: str | None = None,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        observed["implementation_issue_number"] = candidate.issue_number
        observed["implementation_issue_override"] = (
            issue_override.number if issue_override else None
        )
        observed["implementation_branch_override"] = branch_override
        observed["implementation_pre_pr_last_consumed_comment_id"] = pre_pr_last_consumed_comment_id
        return impl_result

    def fake_process_feedback_turn(tracked: TrackedPullRequestState) -> None:
        observed["feedback_pr_number"] = tracked.pr_number

    monkeypatch.setattr(orch, "_process_issue", fake_process_issue)
    monkeypatch.setattr(
        orch,
        "_process_implementation_candidate",
        fake_process_implementation_candidate,
    )
    monkeypatch.setattr(orch, "_process_feedback_turn", fake_process_feedback_turn)
    configure_logging(verbose=True)

    issue = _issue()
    assert (
        orch._process_issue_worker(issue, "design_doc", "agent/design/7-add-worker-scheduler", 17)
        == issue_result
    )

    candidate = ImplementationCandidateState(
        issue_number=issue.number,
        design_branch="agent/design/7-add-worker-scheduler",
        design_pr_number=101,
        design_pr_url="https://example/pr/101",
    )
    assert (
        orch._process_implementation_candidate_worker(
            candidate, issue, "agent/impl/7-add-worker-scheduler", 23
        )
        == impl_result
    )

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    orch._process_feedback_turn_worker(tracked)

    assert observed == {
        "issue_number": issue.number,
        "flow": "design_doc",
        "branch_override": "agent/design/7-add-worker-scheduler",
        "pre_pr_last_consumed_comment_id": 17,
        "implementation_issue_number": issue.number,
        "implementation_issue_override": issue.number,
        "implementation_branch_override": "agent/impl/7-add-worker-scheduler",
        "implementation_pre_pr_last_consumed_comment_id": 23,
        "feedback_pr_number": 101,
    }
    assert "repo_full_name=johnynek/mergexo event=wrapper_probe" in capsys.readouterr().err


def test_active_run_id_helpers_and_prompt_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    future: Future[str] = Future()
    orch._running_issue_metadata[7] = _RunningIssueMetadata(
        run_id="run-issue",
        flow="design_doc",
        branch="agent/design/7",
        context_json="{}",
        source_issue_number=7,
        consumed_comment_id_max=0,
    )
    orch._running_feedback[70] = _FeedbackFuture(
        issue_number=7, run_id="run-feedback", future=future
    )

    assert orch._active_run_id_for_issue(7) == "run-issue"
    assert orch._active_run_id_for_pr(70) == "run-feedback"
    assert orch._active_run_id_for_pr_now(70) == "run-feedback"
    assert orch._active_run_id_for_pr_now(999) is None
    monkeypatch.setattr(time, "sleep", lambda _: None)
    assert orch._active_run_id_for_pr(999) is None

    recorded_meta: list[tuple[str, str]] = []

    def fake_update_agent_run_meta(*, run_id: str, meta_json: str) -> bool:
        recorded_meta.append((run_id, meta_json))
        return True

    monkeypatch.setattr(
        orch._state, "update_agent_run_meta", fake_update_agent_run_meta, raising=False
    )

    orch._record_run_prompt(None, "ignored")
    assert recorded_meta == []

    orch._record_run_prompt("run-issue", "Prompt text")
    assert recorded_meta == [("run-issue", '{"last_prompt": "Prompt text"}')]


def test_process_feedback_turn_worker_retries_transient_git_errors_before_succeeding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha="head-1",
    )
    attempts = 0

    def flaky_feedback_turn(_: TrackedPullRequestState) -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise CommandError(
                "Command failed\n"
                "cmd: git clone foo\n"
                "exit: 128\n"
                "stdout:\n\n"
                "stderr:\nERROR: no healthy upstream\n"
                "fatal: Could not read from remote repository.\n"
            )
        return "completed"

    sleep_calls: list[float] = []
    monkeypatch.setattr(orch, "_process_feedback_turn", flaky_feedback_turn)
    monkeypatch.setattr(time, "sleep", lambda seconds: sleep_calls.append(seconds))

    assert orch._process_feedback_turn_worker(tracked) == "completed"
    assert attempts == 3
    assert sleep_calls == [5, 10]


def test_process_feedback_turn_worker_raises_concise_error_after_transient_retry_exhaustion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha="head-1",
    )

    def always_failing_feedback_turn(_: TrackedPullRequestState) -> str:
        raise CommandError(
            "Command failed\n"
            "cmd: git -C /tmp/worker fetch origin --prune --tags\n"
            "exit: 128\n"
            "stdout:\n\n"
            "stderr:\nERROR: no healthy upstream\n"
            "fatal: Could not read from remote repository.\n"
        )

    monkeypatch.setattr(orch, "_process_feedback_turn", always_failing_feedback_turn)
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(
        FeedbackTransientGitError,
        match=r"Unable to reach GitHub during feedback operations after 4 attempts",
    ):
        orch._process_feedback_turn_worker(tracked)


def test_process_feedback_turn_worker_reraises_non_transient_git_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha="head-1",
    )

    def merge_conflict_feedback_turn(_: TrackedPullRequestState) -> str:
        raise CommandError(
            "Command failed\n"
            "cmd: git push origin branch\n"
            "exit: 1\n"
            "stdout:\n\n"
            "stderr:\nAutomatic merge failed; merge conflict in src/app.py\n"
        )

    monkeypatch.setattr(orch, "_process_feedback_turn", merge_conflict_feedback_turn)

    with pytest.raises(CommandError, match="merge conflict"):
        orch._process_feedback_turn_worker(tracked)


def test_update_run_meta_clears_cache_when_update_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._initialize_run_meta_cache("run-1")
    assert "run-1" in orch._run_meta_cache

    def fake_update_agent_run_meta(*, run_id: str, meta_json: str) -> bool:
        _ = run_id, meta_json
        return False

    monkeypatch.setattr(
        orch._state, "update_agent_run_meta", fake_update_agent_run_meta, raising=False
    )

    orch._update_run_meta("run-1", last_prompt="meta")
    assert "run-1" not in orch._run_meta_cache


def test_process_issue_releases_slot_on_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent(fail=True)
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="codex failed"):
        orch._process_issue(_issue(), "design_doc")

    assert git.cleanup_calls
    # Slot should have been released; re-acquire should not block and should return slot 0.
    lease = orch._slot_pool.acquire()
    assert lease.slot == 0


def test_process_issue_worker_cleanup_failure_does_not_override_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    expected = WorkResult(
        issue_number=7,
        branch="agent/design/7-cleanup",
        pr_number=101,
        pr_url="https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )

    def fake_process_design_issue(
        *,
        issue: Issue,
        checkout_path: Path,
        branch: str,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        _ = issue, checkout_path, branch, pre_pr_last_consumed_comment_id
        return expected

    def failing_cleanup(checkout_path: Path) -> None:
        git.cleanup_calls.append(checkout_path)
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(orch, "_process_design_issue", fake_process_design_issue)
    monkeypatch.setattr(git, "cleanup_slot", failing_cleanup)

    result = orch._process_issue(_issue(), "design_doc", branch_override=expected.branch)
    assert result == expected
    assert git.recover_quarantined_slot_calls == [0]
    assert git.cleanup_calls
    lease = orch._slot_pool.acquire()
    assert lease.slot == 0


@pytest.mark.parametrize(
    ("flow", "branch"), [("bugfix", "agent/bugfix/7"), ("small_job", "agent/small/7")]
)
def test_process_direct_issue_marks_codex_finished_on_agent_exception(
    tmp_path: Path, flow: IssueFlow, branch: str
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent(fail=True)
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="codex failed"):
        orch._process_direct_issue(
            issue=_issue(),
            flow=flow,
            checkout_path=tmp_path,
            branch=branch,
        )


def test_enqueue_new_work_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=2)
    git = FakeGitManager(tmp_path / "checkouts")
    issue1 = _issue(1, "One")
    issue2 = _issue(2, "Two")
    github = FakeGitHub([issue1, issue2])
    agent = FakeAgent()
    state = FakeState(allowed={1})

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[tuple[int, IssueFlow]] = []

        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, extra
            fut: Future[WorkResult] = Future()
            fut.set_result(
                WorkResult(
                    issue_number=issue.number,
                    branch="b",
                    pr_number=9,
                    pr_url="u",
                )
            )
            self.submitted.append((issue.number, flow))
            return fut

    pool = FakePool()

    orch._enqueue_new_work(pool)
    assert state.running == [1]
    assert pool.submitted == [(1, "design_doc")]
    assert github.requested_labels == ("agent:bugfix", "agent:small-job", "agent:design")

    # Already-running path: skip existing issue number.
    already_running_future: Future[WorkResult] = Future()
    orch._running = {1: already_running_future}
    orch._enqueue_new_work(pool)

    # Full queue path: when running reaches max, function returns early.
    running_future: Future[WorkResult] = Future()
    orch._running = {99: running_future, 100: running_future}
    orch._enqueue_new_work(pool)


def test_enqueue_new_work_passes_existing_issue_comments_on_first_invocation(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, worker_count=1, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = Issue(
        number=1,
        title="One",
        body="Issue body",
        html_url="https://example/issues/1",
        labels=("agent:design",),
        author_login="issue-author",
    )
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=5,
            body="first comment",
            user_login="reviewer",
            html_url="https://example/issues/1#issuecomment-5",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=6,
            body="second comment",
            user_login="reviewer",
            html_url="https://example/issues/1#issuecomment-6",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
    ]
    state = FakeState(allowed={1})
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[tuple[Issue, IssueFlow, int]] = []

        def submit(
            self,
            fn,
            issue_arg,
            flow_arg,
            branch_arg,
            consumed_comment_id_max,
        ):  # type: ignore[no-untyped-def]
            _ = fn, branch_arg
            fut: Future[WorkResult] = Future()
            fut.set_result(
                WorkResult(
                    issue_number=issue_arg.number,
                    branch="b",
                    pr_number=9,
                    pr_url="u",
                )
            )
            self.submitted.append((issue_arg, flow_arg, consumed_comment_id_max))
            return fut

    pool = FakePool()
    orch._enqueue_new_work(pool)

    assert len(pool.submitted) == 1
    submitted_issue, submitted_flow, consumed_comment_id_max = pool.submitted[0]
    assert submitted_flow == "design_doc"
    assert consumed_comment_id_max == 6
    assert "Issue body" in submitted_issue.body
    assert "Ordered issue comments at first invocation:" in submitted_issue.body
    assert "first comment" in submitted_issue.body
    assert "second comment" in submitted_issue.body
    assert submitted_issue.body.index("first comment") < submitted_issue.body.index(
        "second comment"
    )

    cursor = state.get_issue_comment_cursor(1)
    assert cursor.pre_pr_last_consumed_comment_id == 6


def test_enqueue_new_work_does_not_advance_cursor_when_issue_already_processed(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, worker_count=1, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(7, labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=11,
            body="Please open the PR now",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-11",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json="{}",
        waiting_reason="new_issue_comments_pending",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=10,
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class NoopPool:
        def submit(self, *args: object, **kwargs: object) -> Future[WorkResult]:
            _ = args, kwargs
            raise AssertionError("submit should not be called when claim is lost")

    orch._enqueue_new_work(NoopPool())

    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 10


def test_enqueue_new_work_skips_unauthorized_issue_authors(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=2, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(1, "One", author_login="outsider")])
    agent = FakeAgent()
    state = FakeState(allowed={1})
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, extra
            raise AssertionError("submit should not be called for unauthorized issues")

    orch._enqueue_new_work(FakePool())

    assert state.running == []
    assert orch._running == {}


def test_enqueue_implementation_work_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=2)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    state.implementation_candidates = [
        ImplementationCandidateState(
            issue_number=7,
            design_branch="agent/design/7-a",
            design_pr_number=41,
            design_pr_url="https://example/pr/41",
        ),
        ImplementationCandidateState(
            issue_number=8,
            design_branch="agent/design/8-b",
            design_pr_number=42,
            design_pr_url="https://example/pr/42",
        ),
    ]
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[int] = []

        def submit(self, fn, candidate, *extra):  # type: ignore[no-untyped-def]
            _ = fn, extra
            fut: Future[WorkResult] = Future()
            fut.set_result(
                WorkResult(
                    issue_number=candidate.issue_number,
                    branch="agent/impl/x",
                    pr_number=9,
                    pr_url="u",
                )
            )
            self.submitted.append(candidate.issue_number)
            return fut

    pool = FakePool()
    orch._enqueue_implementation_work(pool)
    assert state.running == [7, 8]
    assert pool.submitted == [7, 8]

    # Already-running path: skip candidate issue number currently in-flight.
    running_future: Future[WorkResult] = Future()
    orch._running = {7: running_future}
    state.running.clear()
    pool.submitted.clear()
    state.implementation_candidates = [state.implementation_candidates[0]]
    orch._enqueue_implementation_work(pool)
    assert state.running == []
    assert pool.submitted == []

    # Capacity guard should short-circuit before submitting.
    orch._running = {1: running_future, 2: running_future}
    state.running.clear()
    pool.submitted.clear()
    orch._enqueue_implementation_work(pool)
    assert state.running == []
    assert pool.submitted == []

    # Cross-process claim contention path: capacity is available but state claim loses.
    orch._running = {}
    state.running.clear()
    pool.submitted.clear()
    state.implementation_candidates = [state.implementation_candidates[0]]
    state.claim_blocked_implementation_issue_numbers = {7}
    starts_before = len(state.run_starts)
    orch._enqueue_implementation_work(pool)
    assert state.running == []
    assert pool.submitted == []
    assert len(state.run_starts) == starts_before


def test_enqueue_implementation_work_skips_takeover_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=1)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([issue])
    state = FakeState()
    state.implementation_candidates = [
        ImplementationCandidateState(
            issue_number=issue.number,
            design_branch="agent/design/7-a",
            design_pr_number=41,
            design_pr_url="https://example/pr/41",
        )
    ]
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def submit(self, fn, candidate, *extra):  # type: ignore[no-untyped-def]
            _ = fn, candidate, extra
            raise AssertionError("implementation worker should not be submitted for takeover issue")

    orch._enqueue_implementation_work(FakePool())
    assert state.running == []


def test_shared_global_work_limiter_caps_enqueue_across_orchestrators(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=1)
    git = FakeGitManager(tmp_path / "checkouts")
    limiter = GlobalWorkLimiter(1)

    first = Phase1Orchestrator(
        cfg,
        state=FakeState(allowed={1}),
        github=FakeGitHub([_issue(1, "One")]),
        git_manager=git,
        agent=FakeAgent(),
        work_limiter=limiter,
    )
    second = Phase1Orchestrator(
        cfg,
        state=FakeState(allowed={1}),
        github=FakeGitHub([_issue(1, "One")]),
        git_manager=git,
        agent=FakeAgent(),
        work_limiter=limiter,
    )

    class PendingPool:
        def __init__(self) -> None:
            self.submitted = 0
            self.last_future: Future[WorkResult] | None = None

        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, extra
            fut: Future[WorkResult] = Future()
            self.last_future = fut
            self.submitted += 1
            return fut

    pool = PendingPool()

    first._enqueue_new_work(pool)
    assert pool.submitted == 1
    second._enqueue_new_work(pool)
    assert pool.submitted == 1

    assert pool.last_future is not None
    pool.last_future.set_result(WorkResult(issue_number=1, branch="b", pr_number=1, pr_url="u"))
    second._enqueue_new_work(pool)
    assert pool.submitted == 2


def test_enqueue_new_work_releases_capacity_when_submit_fails(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=1)
    limiter = GlobalWorkLimiter(1)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(allowed={1}),
        github=FakeGitHub([_issue(1, "One")]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
        work_limiter=limiter,
    )

    class FailingPool:
        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, extra
            raise RuntimeError("submit failed")

    with pytest.raises(RuntimeError, match="submit failed"):
        orch._enqueue_new_work(FailingPool())

    assert limiter.in_flight() == 0


def test_enqueue_new_work_uses_flow_precedence(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=3)
    git = FakeGitManager(tmp_path / "checkouts")
    issues = [
        _issue(1, "Bug", labels=("agent:design", "agent:bugfix")),
        _issue(2, "Small", labels=("agent:design", "agent:small-job")),
        _issue(3, "Design", labels=("agent:design",)),
    ]
    github = FakeGitHub(issues)
    agent = FakeAgent()
    state = FakeState(allowed={1, 2, 3})
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[tuple[int, IssueFlow]] = []

        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, extra
            self.submitted.append((issue.number, flow))
            fut: Future[WorkResult] = Future()
            fut.set_result(
                WorkResult(issue_number=issue.number, branch="b", pr_number=1, pr_url="u")
            )
            return fut

    pool = FakePool()
    orch._enqueue_new_work(pool)

    assert pool.submitted == [(1, "bugfix"), (2, "small_job"), (3, "design_doc")]


def test_enqueue_new_work_skips_issues_without_matching_labels(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=1)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(1, "Bad label", labels=("triage",))])
    agent = FakeAgent()
    state = FakeState(allowed={1})
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, extra
            raise AssertionError("submit should not be called")

    orch._enqueue_new_work(FakePool())
    assert state.running == []


def test_enqueue_new_work_skips_issues_with_ignore_label(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=1)
    git = FakeGitManager(tmp_path / "checkouts")
    ignored_issue = _issue(1, "Ignored", labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([ignored_issue])
    state = FakeState(allowed={1})
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def submit(self, fn, issue, flow, *extra):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, extra
            raise AssertionError("submit should not be called for ignored issues")

    orch._enqueue_new_work(FakePool())

    assert state.running == []
    assert 1 in state._takeover_active_issue_numbers


def test_sync_takeover_states_processes_known_issue_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, worker_count=1)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    state = FakeState()
    state._takeover_active_issue_numbers.add(7)
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    seen: list[int] = []

    def _record_sync(*, issue_number: int, issue: Issue | None = None) -> bool:
        _ = issue
        seen.append(issue_number)
        return False

    monkeypatch.setattr(orch, "_sync_takeover_state_for_issue", _record_sync)
    orch._sync_takeover_states()
    assert seen == [7]


def test_reap_finished_marks_success_and_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    ok: Future[WorkResult] = Future()
    ok.set_result(WorkResult(issue_number=1, branch="b", pr_number=2, pr_url="u"))
    bad: Future[WorkResult] = Future()
    bad.set_exception(RuntimeError("boom"))

    orch._running = {1: ok, 2: bad}
    orch._reap_finished()

    assert [w.issue_number for w in state.completed] == [1]
    assert state.failed[0][0] == 2
    assert orch._running == {}


def test_reap_finished_recoverable_pre_pr_failure_moves_to_followup_waiting(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    bad: Future[WorkResult] = Future()
    bad.set_exception(RuntimeError("bugfix flow blocked: need reporter follow-up"))
    orch._running = {7: bad}
    orch._running_issue_metadata = {
        7: _RunningIssueMetadata(
            run_id="run-7",
            flow="bugfix",
            branch="agent/bugfix/7-worker",
            context_json='{"flow":"bugfix"}',
            source_issue_number=7,
            consumed_comment_id_max=19,
        )
    }

    orch._reap_finished()

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "awaiting_issue_followup"
    followups = state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name)
    assert len(followups) == 1
    assert followups[0].flow == "bugfix"
    assert "reporter follow-up" in followups[0].waiting_reason
    assert followups[0].last_checkpoint_sha is None
    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 19


def test_reap_finished_checkpointed_pre_pr_failure_persists_checkpoint_sha(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    bad: Future[WorkResult] = Future()
    bad.set_exception(
        CheckpointedPrePrBlockedError(
            waiting_reason="bugfix flow blocked: waiting",
            checkpoint_branch="agent/bugfix/7-checkpoint",
            checkpoint_sha="abc123",
        )
    )
    orch._running = {7: bad}
    orch._running_issue_metadata = {
        7: _RunningIssueMetadata(
            run_id="run-7",
            flow="bugfix",
            branch="agent/bugfix/7-worker",
            context_json='{"flow":"bugfix"}',
            source_issue_number=7,
            consumed_comment_id_max=21,
        )
    }

    orch._reap_finished()

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "awaiting_issue_followup"
    assert row[1] == "agent/bugfix/7-checkpoint"
    followups = state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name)
    assert len(followups) == 1
    assert followups[0].last_checkpoint_sha == "abc123"
    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 21


def test_process_issue_pre_pr_ordering_gate_blocks_pr_creation_until_comments_processed(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=22,
            body="Please include a regression test.",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-22",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="new_issue_comments_pending"):
        orch._process_issue(
            _issue(labels=("agent:bugfix",)),
            "bugfix",
            pre_pr_last_consumed_comment_id=0,
        )

    assert github.created_prs == []


def test_pending_source_issue_followups_ignore_mergexo_status_comments(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_issue_comment_routing=True,
        allowed_users=("johnynek", "reviewer"),
    )
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    comments = [
        PullRequestIssueComment(
            comment_id=1,
            body="MergeXO assigned an agent and started small-job PR work for issue #7.",
            user_login="johnynek",
            html_url="https://example/issues/7#issuecomment-1",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=2,
            body="MergeXO small-job flow was blocked for issue #7: waiting for details.",
            user_login="johnynek",
            html_url="https://example/issues/7#issuecomment-2",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
        PullRequestIssueComment(
            comment_id=3,
            body="Please include one integration test.",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-3",
            created_at="2026-02-23T00:00:02Z",
            updated_at="2026-02-23T00:00:02Z",
        ),
    ]

    pending = orch._pending_source_issue_followups(comments=comments, after_comment_id=0)

    assert tuple(comment.comment_id for comment in pending) == (3,)


def test_is_mergexo_status_comment_detects_prefixed_status_messages() -> None:
    assert (
        _is_mergexo_status_comment(
            "MergeXO assigned an agent and started small-job PR work for issue #7."
        )
        is True
    )
    assert (
        _is_mergexo_status_comment(
            "MergeXO small-job flow was blocked for issue #7: waiting for details."
        )
        is True
    )
    assert _is_mergexo_status_comment("Please retry after rebasing on main.") is False
    assert _is_mergexo_status_comment("   ") is False


def test_repair_failed_no_staged_change_run_opens_recovery_pr(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:small-job",))
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    branch = "agent/small/7-add-worker-scheduler"
    state.mark_awaiting_issue_followup(
        issue_number=issue.number,
        flow="small_job",
        branch=branch,
        context_json='{"flow":"small_job"}',
        waiting_reason="temporary placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_failed(
        issue.number,
        "No staged changes to commit",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._repair_failed_no_staged_change_runs()

    assert len(github.created_prs) == 1
    _, head, base, body = github.created_prs[0]
    assert head == branch
    assert base == "main"
    assert "Recovered small-job PR from a previously pushed branch." in body
    row = _issue_run_row(tmp_path / "state.db", issue.number)
    assert row[0] == "awaiting_feedback"
    assert row[1] == branch
    assert row[2] == 101
    assert row[3] == "https://example/pr/101"
    assert row[4] is None
    assert any("Opened recovered small-job PR" in comment for _, comment in github.comments)


def test_repair_failed_no_staged_change_runs_skips_non_recoverable_rows(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(number=7), _issue(number=8), _issue(number=9)])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    # Empty branch -> skipped.
    state.mark_failed(7, "No staged changes to commit", repo_full_name=cfg.repo.full_name)

    # Non-matching error -> skipped even with branch.
    state.mark_awaiting_issue_followup(
        issue_number=8,
        flow="small_job",
        branch="agent/small/8-one",
        context_json='{"flow":"small_job"}',
        waiting_reason="placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_failed(8, "push rejected", repo_full_name=cfg.repo.full_name)

    # Recoverable row -> repaired.
    state.mark_awaiting_issue_followup(
        issue_number=9,
        flow="small_job",
        branch="agent/small/9-two",
        context_json='{"flow":"small_job"}',
        waiting_reason="placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_failed(9, "No staged changes to commit", repo_full_name=cfg.repo.full_name)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._repair_failed_no_staged_change_runs()

    assert len(github.created_prs) == 1
    assert github.created_prs[0][1] == "agent/small/9-two"


def test_repair_stale_running_run_opens_recovery_pr(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design",))
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    branch = "agent/design/7-add-worker-scheduler"
    state.mark_awaiting_issue_followup(
        issue_number=issue.number,
        flow="design_doc",
        branch=branch,
        context_json='{"flow":"design_doc"}',
        waiting_reason="temporary placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_running(issue.number, repo_full_name=cfg.repo.full_name)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._repair_stale_running_runs()

    assert len(github.created_prs) == 1
    _, head, base, body = github.created_prs[0]
    assert head == branch
    assert base == "main"
    assert "Recovered design PR from a previously pushed branch." in body
    row = _issue_run_row(tmp_path / "state.db", issue.number)
    assert row[0] == "awaiting_feedback"
    assert row[1] == branch
    assert row[2] == 101
    assert row[3] == "https://example/pr/101"
    assert row[4] is None
    assert any("Opened recovered design PR" in comment for _, comment in github.comments)


def test_repair_stale_running_runs_skips_active_and_empty_branch_rows(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue1 = _issue(number=1)
    issue2 = _issue(number=2)
    issue3 = _issue(number=3)
    github = FakeGitHub([issue1, issue2, issue3])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")

    state.mark_awaiting_issue_followup(
        issue_number=1,
        flow="design_doc",
        branch="agent/design/1-one",
        context_json='{"flow":"design_doc"}',
        waiting_reason="placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_running(1, repo_full_name=cfg.repo.full_name)

    # Empty branch.
    state.mark_running(2, repo_full_name=cfg.repo.full_name)

    state.mark_awaiting_issue_followup(
        issue_number=3,
        flow="design_doc",
        branch="agent/design/3-three",
        context_json='{"flow":"design_doc"}',
        waiting_reason="placeholder",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_running(3, repo_full_name=cfg.repo.full_name)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    active: Future[WorkResult] = Future()
    orch._running = {1: active}

    orch._repair_stale_running_runs()

    assert len(github.created_prs) == 1
    assert github.created_prs[0][1] == "agent/design/3-three"


def test_recover_missing_pr_branch_returns_for_unauthorized_issue_author(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(number=7, author_login="outsider")])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    orch._recover_missing_pr_branch(issue_number=7, branch="agent/small/7-one")

    assert github.created_prs == []
    assert state.completed == []


def test_recover_missing_pr_branch_skips_takeover_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, ignore_label="agent:ignore")
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(number=7, labels=("agent:bugfix", "agent:ignore"))])
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._recover_missing_pr_branch(issue_number=7, branch="agent/bugfix/7-one")

    assert github.created_prs == []
    assert 7 in state._takeover_active_issue_numbers


def test_recover_missing_pr_branch_handles_pr_create_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(number=7)])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def fail_create(*args: object, **kwargs: object) -> PullRequest:
        _ = args, kwargs
        raise RuntimeError("boom")

    github.create_pull_request = fail_create  # type: ignore[method-assign]

    orch._recover_missing_pr_branch(issue_number=7, branch="agent/small/7-one")

    assert state.completed == []


def test_recover_missing_pr_branch_tolerates_issue_comment_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(number=7)
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def fail_comment(issue_number: int, body: str) -> None:
        _ = issue_number, body
        raise RuntimeError("comment failed")

    github.post_issue_comment = fail_comment  # type: ignore[method-assign]

    orch._recover_missing_pr_branch(issue_number=7, branch="agent/small/7-one")

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "awaiting_feedback"
    assert row[1] == "agent/small/7-one"
    assert row[2] == 101
    assert row[3] == "https://example/pr/101"


def test_recovery_payload_covers_impl_and_bugfix_branches() -> None:
    issue = _issue(number=11, title="Handle edge case")

    design_title, design_body, design_flow = _recovery_pr_payload_for_issue(
        issue=issue,
        branch="agent/design/11-handle-edge-case",
    )
    impl_title, impl_body, impl_flow = _recovery_pr_payload_for_issue(
        issue=issue,
        branch="agent/impl/11-handle-edge-case",
    )
    bug_title, bug_body, bug_flow = _recovery_pr_payload_for_issue(
        issue=issue,
        branch="agent/bugfix/11-handle-edge-case",
    )

    assert design_flow == "design"
    assert design_title.startswith("Design doc for #11:")
    assert "Recovered design PR from a previously pushed branch." in design_body
    assert "Refs #11" in design_body
    assert "Source issue:" not in design_body

    assert impl_flow == "implementation"
    assert impl_title.startswith("Implementation for #11:")
    assert "Recovered implementation PR from a previously pushed branch." in impl_body
    assert "Fixes #11" in impl_body
    assert "Source issue:" not in impl_body

    assert bug_flow == "bugfix"
    assert bug_title.startswith("Bugfix for #11:")
    assert "Recovered bugfix PR from a previously pushed branch." in bug_body
    assert "Fixes #11" in bug_body
    assert "Source issue:" not in bug_body

    assert _flow_label_from_branch("agent/design/11-handle-edge-case") == "design"
    assert _flow_label_from_branch("agent/impl/11-handle-edge-case") == "implementation"
    assert _flow_label_from_branch("agent/bugfix/11-handle-edge-case") == "bugfix"


def test_enqueue_pre_pr_followup_work_enqueues_pending_comments_in_order(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=5,
            body="old",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-5",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=6,
            body="new direction",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-6",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json='{"flow":"bugfix","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:bugfix"],"author_login":"issue-author"}}',
        waiting_reason="bugfix flow blocked: waiting for clarifications",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=5,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[tuple[Issue, IssueFlow, str, int]] = []

        def submit(self, fn, issue_arg, flow_arg, branch_arg, consumed):  # type: ignore[no-untyped-def]
            _ = fn
            self.submitted.append((issue_arg, flow_arg, branch_arg, consumed))
            fut: Future[WorkResult] = Future()
            return fut

    pool = FakePool()
    orch._enqueue_pre_pr_followup_work(pool)

    assert len(pool.submitted) == 1
    submitted_issue, submitted_flow, submitted_branch, consumed_comment_id = pool.submitted[0]
    assert submitted_flow == "bugfix"
    assert submitted_branch == "agent/bugfix/7-add-worker-scheduler"
    assert consumed_comment_id == 6
    assert "new direction" in submitted_issue.body
    assert "waiting for clarifications" in submitted_issue.body


def test_enqueue_pre_pr_followup_work_skips_when_claim_is_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=5,
            body="old",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-5",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=6,
            body="new direction",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-6",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json='{"flow":"bugfix","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:bugfix"],"author_login":"issue-author"}}',
        waiting_reason="bugfix flow blocked: waiting for clarifications",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=5,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    monkeypatch.setattr(
        state,
        "claim_pre_pr_followup_run_start",
        lambda **kwargs: None,
    )

    class NoopPool:
        def submit(self, *args: object, **kwargs: object) -> Future[WorkResult]:
            _ = args, kwargs
            raise AssertionError("submit should not be called when claim is lost")

    orch._enqueue_pre_pr_followup_work(NoopPool())

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "awaiting_issue_followup"


def test_enqueue_pre_pr_followup_work_skips_takeover_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix", "agent:ignore"))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=45,
            body="Please continue",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-45",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=issue.number,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json="{}",
        waiting_reason="bugfix flow blocked: waiting for details",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class NoopPool:
        def submit(self, *args: object, **kwargs: object) -> Future[WorkResult]:
            _ = args, kwargs
            raise AssertionError("submit should not be called for takeover followups")

    orch._enqueue_pre_pr_followup_work(NoopPool())
    followups = state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name)
    assert len(followups) == 1
    cursor = state.get_issue_comment_cursor(issue.number, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 45


def test_takeover_resume_processes_only_comments_after_resume_boundary(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    active_issue = _issue(labels=("agent:bugfix", "agent:ignore"))
    github = FakeGitHub([active_issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=40,
            body="takeover comment",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-40",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json="{}",
        waiting_reason="bugfix flow blocked: waiting for details",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    assert orch._is_takeover_active(issue_number=7, issue=active_issue) is True
    cursor_during_takeover = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor_during_takeover.pre_pr_last_consumed_comment_id == 40

    resumed_issue = _issue(labels=("agent:bugfix",))
    github.issues = [resumed_issue]
    orch._poll_issue_cache.clear()
    orch._poll_takeover_synced_issue_numbers.clear()
    orch._poll_takeover_active_issue_numbers.clear()
    assert orch._is_takeover_active(issue_number=7, issue=resumed_issue) is False

    github.issue_comments.append(
        PullRequestIssueComment(
            comment_id=41,
            body="resume comment",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-41",
            created_at="2026-02-23T00:01:00Z",
            updated_at="2026-02-23T00:01:00Z",
        )
    )

    class FakePool:
        def __init__(self) -> None:
            self.submitted_comment_ids: list[int] = []

        def submit(self, fn, issue, flow, branch, consumed_comment_id_max):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow, branch
            self.submitted_comment_ids.append(consumed_comment_id_max)
            fut: Future[WorkResult] = Future()
            fut.set_result(
                WorkResult(
                    issue_number=7,
                    branch="agent/bugfix/7-add-worker-scheduler",
                    pr_number=101,
                    pr_url="https://example/pr/101",
                )
            )
            return fut

    pool = FakePool()
    orch._enqueue_pre_pr_followup_work(pool)
    assert pool.submitted_comment_ids == [41]


def test_snapshot_takeover_boundaries_updates_review_summary_cursor(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_summaries = [
        PullRequestIssueComment(
            comment_id=30,
            body="older review summary",
            user_login="reviewer",
            html_url="https://example/reviews/30",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=31,
            body="newer review summary",
            user_login="reviewer",
            html_url="https://example/reviews/31",
            created_at="2026-02-23T00:01:00Z",
            updated_at="2026-02-23T00:01:00Z",
        ),
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._snapshot_takeover_comment_boundaries(
        issue_number=issue.number,
        clear_pending_feedback=False,
    )

    summary_cursor = state.get_poll_cursor(
        surface="pr_review_summaries",
        scope_number=101,
        repo_full_name=cfg.repo.full_name,
    )
    assert summary_cursor is not None
    assert summary_cursor.last_comment_id == 31
    assert summary_cursor.last_updated_at == "2026-02-23T00:01:00Z"


def test_enqueue_pre_pr_followup_work_legacy_mode_records_token_observation(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    token = "f" * 64
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=6,
            body=f"old direction\n\n<!-- mergexo-action:{token} -->",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-6",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
    ]

    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json='{"flow":"bugfix","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:bugfix"],"author_login":"issue-author"}}',
        waiting_reason="bugfix flow blocked: waiting for clarifications",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=6,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    class NoopPool:
        def submit(self, *args: object, **kwargs: object) -> Future[WorkResult]:
            _ = args, kwargs
            raise AssertionError("submit should not be called")

    orch._enqueue_pre_pr_followup_work(NoopPool())
    observed = state.get_action_token(token, repo_full_name=cfg.repo.full_name)
    assert observed is not None
    assert observed.status == "observed"


def test_enqueue_pre_pr_followup_work_incremental_bootstrap_processes_pending_comments(
    tmp_path: Path,
) -> None:
    cfg = _config(
        tmp_path,
        enable_issue_comment_routing=True,
        enable_incremental_comment_fetch=True,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=4,
            body="historical comment",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-4",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=6,
            body="first follow-up",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-6",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
        PullRequestIssueComment(
            comment_id=7,
            body="please open the PR",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-7",
            created_at="2026-02-23T00:00:02Z",
            updated_at="2026-02-23T00:00:02Z",
        ),
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-add-worker-scheduler",
        context_json='{"flow":"bugfix","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:bugfix"],"author_login":"issue-author"}}',
        waiting_reason="bugfix flow blocked: waiting for clarifications",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=5,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[tuple[Issue, IssueFlow, str, int]] = []

        def submit(self, fn, issue_arg, flow_arg, branch_arg, consumed):  # type: ignore[no-untyped-def]
            _ = fn
            self.submitted.append((issue_arg, flow_arg, branch_arg, consumed))
            fut: Future[WorkResult] = Future()
            return fut

    pool = FakePool()
    orch._enqueue_pre_pr_followup_work(pool)
    assert len(pool.submitted) == 1
    submitted_issue, submitted_flow, submitted_branch, consumed_comment_id = pool.submitted[0]
    assert submitted_flow == "bugfix"
    assert submitted_branch == "agent/bugfix/7-add-worker-scheduler"
    assert consumed_comment_id == 7
    assert "first follow-up" in submitted_issue.body
    assert "please open the PR" in submitted_issue.body
    assert "historical comment" not in submitted_issue.body


def test_post_pr_source_issue_comment_redirects_are_idempotent_and_filtered(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=30,
            body="Could you revise this?",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-30",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        ),
        PullRequestIssueComment(
            comment_id=31,
            body="bot-noise",
            user_login="mergexo[bot]",
            html_url="https://example/issues/7#issuecomment-31",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        ),
        PullRequestIssueComment(
            comment_id=32,
            body="outsider",
            user_login="outsider",
            html_url="https://example/issues/7#issuecomment-32",
            created_at="2026-02-23T00:00:02Z",
            updated_at="2026-02-23T00:00:02Z",
        ),
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/bugfix/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._scan_post_pr_source_issue_comment_redirects()
    assert len(github.comments) == 1
    redirected_body = github.comments[0][1]
    token = compute_source_issue_redirect_token(issue_number=7, pr_number=101, comment_id=30)
    assert token in redirected_body
    assert "no longer actioned" in redirected_body
    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.post_pr_last_redirected_comment_id == 32

    # Second scan should skip all comments at or below the stored cursor.
    orch._scan_post_pr_source_issue_comment_redirects()
    assert len(github.comments) == 1


def test_post_pr_source_issue_comment_redirects_skip_takeover_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix", "agent:ignore"))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=30,
            body="Please update status.",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-30",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/bugfix/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._scan_post_pr_source_issue_comment_redirects()
    assert github.comments == []
    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.post_pr_last_redirected_comment_id == 30
    assert state.get_issue_takeover_active(7, repo_full_name=cfg.repo.full_name) is True

    orch._scan_post_pr_source_issue_comment_redirects()
    assert github.comments == []


def test_post_pr_source_redirects_ingest_action_token_observations(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    token = "a" * 64
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=30,
            body=f"automation marker\n\n<!-- mergexo-action:{token} -->",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-30",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/bugfix/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._scan_post_pr_source_issue_comment_redirects()
    observed = state.get_action_token(token, repo_full_name=cfg.repo.full_name)
    assert observed is not None
    assert observed.status == "observed"


def test_post_pr_source_redirects_incremental_bootstrap_skips_history(
    tmp_path: Path,
) -> None:
    cfg = _config(
        tmp_path,
        enable_issue_comment_routing=True,
        enable_incremental_comment_fetch=True,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:bugfix",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=30,
            body="old comment",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-30",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/bugfix/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._scan_post_pr_source_issue_comment_redirects()
    assert github.comments == []

    github.issue_comments.append(
        PullRequestIssueComment(
            comment_id=31,
            body="new comment",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-31",
            created_at="2026-02-23T00:00:01Z",
            updated_at="2026-02-23T00:00:01Z",
        )
    )
    orch._scan_post_pr_source_issue_comment_redirects()
    assert len(github.comments) == 1
    assert "no longer actioned" in github.comments[0][1]


def test_adopt_legacy_failed_pre_pr_runs_moves_recoverable_rows_to_followup_waiting(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:small-job",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=99,
            body="latest context",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-99",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_failed(
        issue_number=7,
        error="small-job flow blocked: environment issue",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._adopt_legacy_failed_pre_pr_runs()

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "awaiting_issue_followup"
    followups = state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name)
    assert len(followups) == 1
    assert followups[0].flow == "small_job"
    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 99


def test_adopt_legacy_failed_pre_pr_runs_skips_takeover_issues(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:small-job", "agent:ignore"))
    github = FakeGitHub([issue])
    state = StateStore(tmp_path / "state.db")
    state.mark_failed(
        issue_number=7,
        error="small-job flow blocked: environment issue",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._adopt_legacy_failed_pre_pr_runs()

    row = _issue_run_row(tmp_path / "state.db", 7)
    assert row[0] == "failed"
    assert state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name) == ()


def test_run_once_with_issue_comment_routing_invokes_followup_and_redirect_scans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    calls = {"adopt": 0, "followup": 0, "redirect": 0}

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_wait_for_all", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: None)
    monkeypatch.setattr(
        orch,
        "_enqueue_pre_pr_followup_work",
        lambda pool: calls.__setitem__("followup", calls["followup"] + 1),
    )
    monkeypatch.setattr(
        orch,
        "_scan_post_pr_source_issue_comment_redirects",
        lambda: calls.__setitem__("redirect", calls["redirect"] + 1),
    )
    monkeypatch.setattr(
        orch,
        "_adopt_legacy_failed_pre_pr_runs",
        lambda: calls.__setitem__("adopt", calls["adopt"] + 1),
    )

    orch.run(once=True)
    assert calls == {"adopt": 1, "followup": 1, "redirect": 2}


def test_enqueue_pre_pr_followup_work_handles_capacity_and_missing_context_paths(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, worker_count=2, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:small-job",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=40,
            body="please continue",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-40",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")

    # No followups path: should no-op.
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class NoopPool:
        def submit(self, *args: object, **kwargs: object) -> Future[WorkResult]:
            _ = args, kwargs
            raise AssertionError("submit should not be called")

    orch._enqueue_pre_pr_followup_work(NoopPool())

    # Capacity-full path: should return without submit.
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="small_job",
        branch="agent/small/7-add-worker-scheduler",
        context_json='{"flow":"small_job","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:small-job"],"author_login":"issue-author"}}',
        waiting_reason="small-job flow blocked: waiting",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=39,
        repo_full_name=cfg.repo.full_name,
    )
    full_future: Future[WorkResult] = Future()
    orch._running = {1: full_future}
    feedback_future: Future[str] = Future()
    orch._running_feedback = {
        2: _FeedbackFuture(issue_number=8, run_id="run-feedback", future=feedback_future)
    }
    orch._enqueue_pre_pr_followup_work(NoopPool())

    # Already-running path.
    orch._running = {7: Future()}
    orch._running_feedback = {}
    orch._enqueue_pre_pr_followup_work(NoopPool())

    # Pending-empty path.
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=40,
        repo_full_name=cfg.repo.full_name,
    )
    orch._running = {}
    orch._enqueue_pre_pr_followup_work(NoopPool())
    state.clear_pre_pr_followup_state(7, repo_full_name=cfg.repo.full_name)

    # Missing implementation context path: skip and keep waiting.
    state.mark_awaiting_issue_followup(
        issue_number=8,
        flow="implementation",
        branch="agent/impl/8-add-worker-scheduler",
        context_json='{"flow":"implementation"}',
        waiting_reason="implementation flow blocked: waiting",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=8,
        comment_id=39,
        repo_full_name=cfg.repo.full_name,
    )
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=40,
            body="continue",
            user_login="reviewer",
            html_url="https://example/issues/8#issuecomment-40",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    orch._running = {}
    orch._running_feedback = {}
    orch._enqueue_pre_pr_followup_work(NoopPool())
    followups = state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name)
    assert any(item.issue_number == 8 for item in followups)


def test_enqueue_pre_pr_followup_work_implementation_resume_path_submits_worker(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:small-job",))
    github = FakeGitHub([issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=11,
            body="resume",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-11",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="implementation",
        branch="agent/impl/7-add-worker-scheduler",
        context_json='{"flow":"implementation","issue":{"number":7,"title":"Add worker scheduler","body":"Body","html_url":"https://example/issues/7","labels":["agent:small-job"],"author_login":"issue-author"},"candidate":{"issue_number":7,"design_branch":"agent/design/7-add-worker-scheduler","design_pr_number":44,"design_pr_url":"https://example/pr/44","repo_full_name":""}}',
        waiting_reason="implementation flow blocked: waiting",
        repo_full_name=cfg.repo.full_name,
    )
    state.advance_pre_pr_last_consumed_comment_id(
        issue_number=7,
        comment_id=10,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    class CapturePool:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def submit(self, fn, *args):  # type: ignore[no-untyped-def]
            self.calls.append((fn, *args))
            fut: Future[WorkResult] = Future()
            return fut

    pool = CapturePool()
    orch._enqueue_pre_pr_followup_work(pool)

    assert len(pool.calls) == 1
    submitted = pool.calls[0]
    assert submitted[0] == orch._process_implementation_candidate_worker
    assert isinstance(submitted[1], ImplementationCandidateState)
    assert isinstance(submitted[2], Issue)
    assert submitted[3] == "agent/impl/7-add-worker-scheduler"
    assert submitted[4] == 11


def test_scan_post_pr_source_issue_comment_redirects_handles_no_targets_and_blocked_targets(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    state = StateStore(tmp_path / "state.db")
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    # No targets path.
    orch._scan_post_pr_source_issue_comment_redirects()
    assert github.comments == []

    # Blocked PR target path.
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=issue.number,
        status="blocked",
        last_seen_head_sha="head",
        error="blocked",
        repo_full_name=cfg.repo.full_name,
    )
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=15,
            body="please handle this",
            user_login="reviewer",
            html_url="https://example/issues/7#issuecomment-15",
            created_at="2026-02-23T00:00:00Z",
            updated_at="2026-02-23T00:00:00Z",
        )
    ]
    orch._scan_post_pr_source_issue_comment_redirects()
    assert len(github.comments) == 1


def test_adopt_legacy_failed_pre_pr_runs_skips_non_recoverable_unknown_and_implementation(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue_unknown = _issue(number=8, labels=("triage",))
    issue_impl = _issue(number=9, labels=("triage",))
    github = FakeGitHub([issue_unknown, issue_impl])
    state = StateStore(tmp_path / "state.db")
    state.mark_failed(
        issue_number=7,
        error="plain failure",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_failed(
        issue_number=8,
        error="required pre-push tests did not pass after automated repair attempts",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_failed(
        issue_number=9,
        error="implementation flow blocked: waiting for info",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._adopt_legacy_failed_pre_pr_runs()
    assert state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name) == ()

    # Second pass returns immediately when already adopted.
    orch._adopt_legacy_failed_pre_pr_runs()


def test_reap_finished_success_with_metadata_updates_followup_cursor(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    state = StateStore(tmp_path / "state.db")
    state.mark_awaiting_issue_followup(
        issue_number=7,
        flow="bugfix",
        branch="agent/bugfix/7-worker",
        context_json='{"flow":"bugfix"}',
        waiting_reason="blocked",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    ok: Future[WorkResult] = Future()
    ok.set_result(
        WorkResult(
            issue_number=7,
            branch="agent/bugfix/7-worker",
            pr_number=101,
            pr_url="https://example/pr/101",
        )
    )
    orch._running = {7: ok}
    orch._running_issue_metadata = {
        7: _RunningIssueMetadata(
            run_id="run-7",
            flow="bugfix",
            branch="agent/bugfix/7-worker",
            context_json='{"flow":"bugfix"}',
            source_issue_number=7,
            consumed_comment_id_max=55,
        )
    }

    orch._reap_finished()

    cursor = state.get_issue_comment_cursor(7, repo_full_name=cfg.repo.full_name)
    assert cursor.pre_pr_last_consumed_comment_id == 55
    assert state.list_pre_pr_followups(repo_full_name=cfg.repo.full_name) == ()


def test_worker_wrappers_delegate_with_consumed_comment_id(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    issue = _issue()
    candidate = ImplementationCandidateState(
        issue_number=7,
        design_branch="agent/design/7-add-worker-scheduler",
        design_pr_number=44,
        design_pr_url="https://example/pr/44",
    )

    def fake_process_issue(
        arg_issue: Issue,
        flow: IssueFlow,
        *,
        branch_override: str | None = None,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        assert arg_issue.number == 7
        assert flow == "design_doc"
        assert branch_override == "agent/design/7-add-worker-scheduler"
        assert pre_pr_last_consumed_comment_id == 12
        return WorkResult(issue_number=7, branch="b", pr_number=1, pr_url="u")

    def fake_process_impl(
        arg_candidate: ImplementationCandidateState,
        *,
        issue_override: Issue | None = None,
        branch_override: str | None = None,
        pre_pr_last_consumed_comment_id: int = 0,
    ) -> WorkResult:
        assert arg_candidate.issue_number == 7
        assert issue_override is issue
        assert branch_override == "agent/impl/7-add-worker-scheduler"
        assert pre_pr_last_consumed_comment_id == 13
        return WorkResult(issue_number=7, branch="b", pr_number=1, pr_url="u")

    orch._process_issue = fake_process_issue  # type: ignore[method-assign]
    orch._process_implementation_candidate = fake_process_impl  # type: ignore[method-assign]

    assert (
        orch._process_issue_worker(
            issue, "design_doc", "agent/design/7-add-worker-scheduler", 12
        ).issue_number
        == 7
    )
    assert (
        orch._process_implementation_candidate_worker(
            candidate,
            issue,
            "agent/impl/7-add-worker-scheduler",
            13,
        ).issue_number
        == 7
    )


def test_pre_pr_comment_helpers_filter_tokenized_and_allow_empty_pending(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    github = FakeGitHub([])
    marker = "<!-- mergexo-action:" + ("a" * 64) + " -->"
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=1,
            body=marker,
            user_login="reviewer",
            html_url="u",
            created_at="now",
            updated_at="now",
        )
    ]
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    # Tokenized comment is filtered out, so no pending comments at gate.
    orch._run_pre_pr_ordering_gate(issue_number=7, last_consumed_comment_id=0)
    assert (
        orch._pending_source_issue_followups(comments=github.issue_comments, after_comment_id=0)
        == []
    )


def test_decode_pre_pr_context_invalid_json_falls_back_to_live_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_issue_comment_routing=True)
    issue = _issue()
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=FakeGitHub([issue]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    parsed_issue, parsed_candidate = orch._decode_pre_pr_context(
        followup=PrePrFollowupState(
            issue_number=7,
            flow="bugfix",
            branch="b",
            context_json="{not-json",
            waiting_reason="x",
            last_checkpoint_sha=None,
            updated_at="now",
            repo_full_name=cfg.repo.full_name,
        )
    )
    assert parsed_issue.number == 7
    assert parsed_candidate is None


def test_pre_pr_helper_functions_cover_fallback_paths() -> None:
    issue = _issue(labels=("triage",))
    assert _pre_pr_flow_label("design_doc") == "design"
    assert _pre_pr_flow_label("small_job") == "small-job"
    assert _issue_to_json_dict(issue)["number"] == 7
    assert _issue_from_json_dict({1: "bad"}) is None  # type: ignore[arg-type]
    assert _issue_from_json_dict({"number": "7"}) is None  # type: ignore[dict-item]
    assert _issue_from_json_dict({"number": 7, "title": 1}) is None  # type: ignore[dict-item]
    assert _issue_from_json_dict({"number": 7, "title": "x", "body": 1}) is None  # type: ignore[dict-item]
    assert _issue_from_json_dict({"number": 7, "title": "x", "body": "y", "html_url": 1}) is None  # type: ignore[dict-item]
    assert (
        _issue_from_json_dict(
            {"number": 7, "title": "x", "body": "y", "html_url": "u", "author_login": 1}
        )
        is None
    )  # type: ignore[dict-item]
    assert (
        _issue_from_json_dict(
            {
                "number": 7,
                "title": "x",
                "body": "y",
                "html_url": "u",
                "author_login": "a",
                "labels": "bad",
            }
        )
        is None
    )
    assert (
        _issue_from_json_dict(
            {
                "number": 7,
                "title": "x",
                "body": "y",
                "html_url": "u",
                "labels": ["ok", 2],
                "author_login": "a",
            }
        )
        is None
    )
    valid_issue = _issue_from_json_dict(_issue_to_json_dict(issue))
    assert isinstance(valid_issue, Issue)

    candidate = ImplementationCandidateState(
        issue_number=7,
        design_branch="bad-branch",
        design_pr_number=None,
        design_pr_url=None,
    )
    assert _implementation_candidate_to_json_dict(candidate)["issue_number"] == 7
    assert _implementation_candidate_from_json_dict({1: "x"}) is None  # type: ignore[arg-type]
    assert _implementation_candidate_from_json_dict({"issue_number": "7"}) is None  # type: ignore[dict-item]
    assert _implementation_candidate_from_json_dict({"issue_number": 7, "design_branch": 1}) is None  # type: ignore[dict-item]
    assert (
        _implementation_candidate_from_json_dict(
            {"issue_number": 7, "design_branch": "b", "design_pr_number": "x"}
        )
        is None
    )  # type: ignore[dict-item]
    assert (
        _implementation_candidate_from_json_dict(
            {"issue_number": 7, "design_branch": "b", "design_pr_number": 1, "design_pr_url": 2}
        )
        is None
    )  # type: ignore[dict-item]
    assert (
        _implementation_candidate_from_json_dict(
            {
                "issue_number": 7,
                "design_branch": "b",
                "design_pr_number": 1,
                "design_pr_url": "u",
                "repo_full_name": 3,
            }
        )
        is None
    )  # type: ignore[dict-item]
    assert _branch_for_implementation_candidate(candidate) == "bad-branch"
    assert (
        _branch_for_pre_pr_followup(
            flow="implementation",
            issue=issue,
            stored_branch="",
            candidate=None,
        )
        == "agent/impl/unknown"
    )
    assert (
        _branch_for_pre_pr_followup(
            flow="implementation",
            issue=issue,
            stored_branch="",
            candidate=ImplementationCandidateState(
                issue_number=7,
                design_branch="agent/design/7-x",
                design_pr_number=None,
                design_pr_url=None,
            ),
        )
        == "agent/impl/7-x"
    )
    assert _branch_for_pre_pr_followup(
        flow="bugfix",
        issue=issue,
        stored_branch="",
        candidate=None,
    ).startswith("agent/bugfix/")

    assert (
        _infer_pre_pr_flow_from_issue_and_error(
            issue=issue,
            error="small-job flow blocked: waiting",
            design_label="agent:design",
            roadmap_label="agent:roadmap",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
        )
        == "small_job"
    )
    assert (
        _infer_pre_pr_flow_from_issue_and_error(
            issue=issue,
            error="implementation flow blocked: waiting",
            design_label="agent:design",
            roadmap_label="agent:roadmap",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
        )
        == "implementation"
    )
    assert (
        _infer_pre_pr_flow_from_issue_and_error(
            issue=issue,
            error="plain failure",
            design_label="agent:design",
            roadmap_label="agent:roadmap",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
        )
        is None
    )
    assert _is_recoverable_pre_pr_error("") is False
    assert _is_merge_conflict_error("") is False
    assert _is_merge_conflict_error("Automatic merge failed; fix conflicts and then commit") is True
    assert _is_merge_conflict_error("plain push rejection") is False
    assert _is_recoverable_pre_pr_exception(DirectFlowBlockedError("x")) is True
    assert (
        _is_recoverable_pre_pr_exception(
            DirectFlowValidationError(
                "required pre-push tests did not pass after automated repair attempts"
            )
        )
        is True
    )
    assert _is_recoverable_pre_pr_exception(RuntimeError("plain")) is False
    assert _pull_request_url(repo_full_name="o/r", pr_number=9) == "https://github.com/o/r/pull/9"
    assert "no longer actioned" in _render_source_issue_redirect_comment(
        pr_number=9,
        pr_url="https://github.com/o/r/pull/9",
        source_comment_url="https://example/comment/1",
    )


def test_issue_with_push_merge_conflict_helper_truncates_and_handles_empty_output(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    issue = _issue(labels=("agent:small-job",))
    checkout_path = git.checkout_root / "worker-0"

    long_output = "x" * 13000
    with_long_output = orch._issue_with_push_merge_conflict(
        issue=issue,
        branch="agent/small/7-add-worker-scheduler",
        failure_output=long_output,
        attempt=1,
        checkout_path=checkout_path,
    )
    assert "... [truncated by MergeXO]" in with_long_output.body

    with_empty_output = orch._issue_with_push_merge_conflict(
        issue=issue,
        branch="agent/small/7-add-worker-scheduler",
        failure_output="   \n\t",
        attempt=2,
        checkout_path=checkout_path,
    )
    assert "Merge output:\n<empty>" in with_empty_output.body


def test_wait_for_all_calls_sleep_when_needed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    fut: Future[WorkResult] = Future()
    orch._running = {1: fut}

    calls = {"reap": 0, "sleep": 0}

    def fake_reap() -> None:
        calls["reap"] += 1
        if calls["reap"] >= 2:
            orch._running.clear()

    def fake_sleep(seconds: float) -> None:
        _ = seconds
        calls["sleep"] += 1

    monkeypatch.setattr(orch, "_reap_finished", fake_reap)
    monkeypatch.setattr(time, "sleep", fake_sleep)

    orch._wait_for_all(pool=object())
    assert calls["sleep"] == 1


def test_poll_once_respects_allow_enqueue_and_restart_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_github_operations=True)
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    calls = {"scan": 0, "new": 0, "impl": 0, "feedback": 0}

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(
        orch, "_scan_operator_commands", lambda: calls.__setitem__("scan", calls["scan"] + 1)
    )
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: calls.__setitem__("new", 1))
    monkeypatch.setattr(
        orch, "_enqueue_implementation_work", lambda pool: calls.__setitem__("impl", 1)
    )
    monkeypatch.setattr(
        orch, "_enqueue_feedback_work", lambda pool: calls.__setitem__("feedback", 1)
    )

    orch.poll_once(pool=cast(object, object()), allow_enqueue=False)  # type: ignore[arg-type]
    assert calls == {"scan": 1, "new": 0, "impl": 0, "feedback": 0}

    monkeypatch.setattr(orch, "_is_restart_pending", lambda: True)
    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]
    assert calls == {"scan": 1, "new": 0, "impl": 0, "feedback": 0}


def test_poll_once_disables_enqueue_after_repeated_github_auth_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_github_operations=True)
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    calls = {"scan": 0, "new": 0}

    def fail_scan() -> None:
        calls["scan"] += 1
        raise GitHubAuthenticationError("gh is not authenticated")

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_scan_operator_commands", fail_scan)
    monkeypatch.setattr(orch, "_repair_stale_running_runs", lambda: None)
    monkeypatch.setattr(orch, "_repair_failed_no_staged_change_runs", lambda: None)
    monkeypatch.setattr(
        orch, "_enqueue_new_work", lambda pool: calls.__setitem__("new", calls["new"] + 1)
    )
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: None)

    for _ in range(3):
        orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]

    assert calls["scan"] == 3
    assert calls["new"] == 2
    assert orch.github_auth_shutdown_pending() is True
    assert "not authenticated" in orch.github_auth_shutdown_reason().lower()


def test_github_auth_shutdown_reason_defaults_when_empty(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_github_operations=True)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    assert "not authenticated" in orch.github_auth_shutdown_reason().lower()


def test_poll_once_runs_actions_monitor_before_feedback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_pr_actions_monitoring=True)
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    order: list[str] = []

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_monitor_pr_actions", lambda: order.append("monitor"))
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: order.append("feedback"))

    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]
    assert order == ["monitor", "feedback"]


def test_poll_once_runs_actions_monitor_when_repo_policy_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    order: list[str] = []

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_monitor_pr_actions", lambda: order.append("monitor"))
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: order.append("feedback"))

    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]
    assert order == ["monitor", "feedback"]


def test_poll_once_skips_actions_monitor_when_repo_policy_is_never(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(
        tmp_path,
        enable_pr_actions_monitoring=True,
        pr_actions_feedback_policy="never",
    )
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    order: list[str] = []

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_monitor_pr_actions", lambda: order.append("monitor"))
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: order.append("feedback"))

    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]
    assert order == ["feedback"]


def test_poll_once_runs_roadmap_steps_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    steps: list[str] = []

    def fake_run_poll_step(*, step_name: str, fn) -> bool:  # type: ignore[no-untyped-def]
        _ = fn
        steps.append(step_name)
        return True

    monkeypatch.setattr(orch, "_run_poll_step", fake_run_poll_step)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]

    assert "activate_merged_roadmaps" in steps
    assert "advance_roadmap_nodes" in steps
    assert "publish_roadmap_status_reports" in steps


def test_poll_once_skips_roadmap_steps_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=False)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    steps: list[str] = []

    def fake_run_poll_step(*, step_name: str, fn) -> bool:  # type: ignore[no-untyped-def]
        _ = fn
        steps.append(step_name)
        return True

    monkeypatch.setattr(orch, "_run_poll_step", fake_run_poll_step)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]

    assert "activate_merged_roadmaps" not in steps
    assert "advance_roadmap_nodes" not in steps
    assert "publish_roadmap_status_reports" not in steps


def test_poll_once_marks_github_error_when_roadmap_steps_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    def fake_run_poll_step(*, step_name: str, fn) -> bool:  # type: ignore[no-untyped-def]
        _ = fn
        return step_name not in {
            "activate_merged_roadmaps",
            "advance_roadmap_nodes",
            "publish_roadmap_status_reports",
        }

    monkeypatch.setattr(orch, "_run_poll_step", fake_run_poll_step)
    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]


def test_effective_pr_actions_feedback_policy_uses_repo_override_then_runtime_fallback(
    tmp_path: Path,
) -> None:
    runtime_enabled_cfg = _config(tmp_path / "runtime", enable_pr_actions_monitoring=True)
    runtime_enabled_orch = Phase1Orchestrator(
        runtime_enabled_cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts-runtime"),
        agent=FakeAgent(),
    )
    assert runtime_enabled_orch._effective_pr_actions_feedback_policy() == "all_complete"

    repo_override_cfg = _config(
        tmp_path / "override",
        enable_pr_actions_monitoring=True,
        pr_actions_feedback_policy="first_fail",
    )
    repo_override_orch = Phase1Orchestrator(
        repo_override_cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts-override"),
        agent=FakeAgent(),
    )
    assert repo_override_orch._effective_pr_actions_feedback_policy() == "first_fail"

    runtime_disabled_cfg = _config(tmp_path / "disabled", enable_pr_actions_monitoring=False)
    runtime_disabled_orch = Phase1Orchestrator(
        runtime_disabled_cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts-disabled"),
        agent=FakeAgent(),
    )
    assert runtime_disabled_orch._effective_pr_actions_feedback_policy() == "never"


def test_poll_once_records_error_when_actions_monitor_step_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _config(tmp_path, enable_pr_actions_monitoring=True)
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=git,
        agent=FakeAgent(),
    )
    configure_logging(verbose=True)

    steps: list[str] = []

    def fake_run_poll_step(*, step_name: str, fn) -> bool:  # type: ignore[no-untyped-def]
        _ = fn
        steps.append(step_name)
        return step_name != "monitor_pr_actions"

    monkeypatch.setattr(orch, "_run_poll_step", fake_run_poll_step)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    orch.poll_once(pool=cast(object, object()), allow_enqueue=True)  # type: ignore[arg-type]
    assert "monitor_pr_actions" in steps
    assert "enqueue_feedback_work" in steps
    text = capsys.readouterr().err
    assert "event=poll_completed" in text
    assert "poll_had_github_errors=true" in text


def test_run_once_and_continuous_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    once_calls = {"enqueue": 0, "wait": 0}

    monkeypatch.setattr(
        orch, "_enqueue_new_work", lambda pool: once_calls.__setitem__("enqueue", 1)
    )
    monkeypatch.setattr(orch, "_wait_for_all", lambda pool: once_calls.__setitem__("wait", 1))
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    orch.run(once=True)
    assert git.ensure_layout_called is True
    assert once_calls == {"enqueue": 1, "wait": 1}

    class StopLoop(RuntimeError):
        pass

    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    def stop_sleep(seconds: float) -> None:
        _ = seconds
        raise StopLoop("stop")

    monkeypatch.setattr(time, "sleep", stop_sleep)

    with pytest.raises(StopLoop):
        orch.run(once=False)


def test_run_continues_after_github_polling_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    configure_logging(verbose=True)

    calls = {"enqueue": 0, "sleep": 0}

    def fail_enqueue(pool) -> None:  # type: ignore[no-untyped-def]
        _ = pool
        calls["enqueue"] += 1
        raise GitHubPollingError("malformed response")

    class StopLoop(RuntimeError):
        pass

    def stop_sleep(seconds: float) -> None:
        _ = seconds
        calls["sleep"] += 1
        raise StopLoop("stop")

    monkeypatch.setattr(orch, "_enqueue_new_work", fail_enqueue)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_feedback_work", lambda pool: None)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(time, "sleep", stop_sleep)

    with pytest.raises(StopLoop, match="stop"):
        orch.run(once=False)

    assert calls["enqueue"] == 1
    assert calls["sleep"] == 1
    text = capsys.readouterr().err
    assert "event=poll_step_failed" in text
    assert "step=enqueue_new_work" in text
    assert "error_type=GitHubPollingError" in text


def test_run_marks_poll_error_when_multiple_poll_steps_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        enable_issue_comment_routing=True,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    configure_logging(verbose=True)

    steps: list[str] = []

    def fail_poll_step(*, step_name: str, fn) -> bool:  # type: ignore[no-untyped-def]
        _ = fn
        steps.append(step_name)
        return False

    monkeypatch.setattr(orch, "_run_poll_step", fail_poll_step)
    monkeypatch.setattr(orch, "_wait_for_all", lambda pool: None)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    orch.run(once=True)

    assert "scan_operator_commands" in steps
    assert "enqueue_implementation_work" in steps
    assert "enqueue_pre_pr_followup_work" in steps
    assert "enqueue_feedback_work" in steps
    assert "scan_post_pr_source_issue_comment_redirects" in steps
    text = capsys.readouterr().err
    assert "event=poll_completed" in text
    assert "poll_had_github_errors=true" in text


def test_run_once_with_github_ops_scans_commands_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_github_operations=True)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    calls = {"scan": 0, "drain": 0, "feedback": 0}

    monkeypatch.setattr(orch, "_reap_finished", lambda: None)
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(orch, "_enqueue_implementation_work", lambda pool: None)
    monkeypatch.setattr(
        orch, "_enqueue_feedback_work", lambda pool: calls.__setitem__("feedback", 1)
    )
    monkeypatch.setattr(
        orch, "_scan_operator_commands", lambda: calls.__setitem__("scan", calls["scan"] + 1)
    )
    monkeypatch.setattr(
        orch,
        "_drain_for_pending_restart_if_needed",
        lambda: calls.__setitem__("drain", calls["drain"] + 1) or False,
    )
    monkeypatch.setattr(orch, "_wait_for_all", lambda pool: None)

    orch.run(once=True)
    assert calls == {"scan": 2, "drain": 2, "feedback": 1}


def _review_comment(
    comment_id: int = 11, updated_at: str = "2026-02-21T00:00:00Z"
) -> PullRequestReviewComment:
    return PullRequestReviewComment(
        comment_id=comment_id,
        body="Please update docs",
        path="docs/design/7-add-worker-scheduler.md",
        line=12,
        side="RIGHT",
        in_reply_to_id=None,
        user_login="reviewer",
        html_url=f"https://example/review/{comment_id}",
        created_at=updated_at,
        updated_at=updated_at,
    )


def _tracked_state_from_store(store: StateStore) -> TrackedPullRequestState:
    tracked = store.list_tracked_pull_requests()
    assert len(tracked) == 1
    return tracked[0]


def _issue_comment(comment_id: int = 22) -> PullRequestIssueComment:
    return PullRequestIssueComment(
        comment_id=comment_id,
        body="General feedback",
        user_login="reviewer",
        html_url=f"https://example/issue-comment/{comment_id}",
        created_at="2026-02-21T00:00:00Z",
        updated_at="2026-02-21T00:00:00Z",
    )


def _feedback_commit_push_context(
    tmp_path: Path,
) -> tuple[
    Phase1Orchestrator,
    FakeGitManager,
    FakeGitHub,
    FakeState,
    TrackedPullRequestState,
    FeedbackTurn,
    FeedbackResult,
]:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="head-1",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-1"),
        review_replies=(),
        general_comment=None,
        commit_message="feat: update",
        git_ops=(),
    )
    return orch, git, github, state, tracked, turn, result


def _workflow_run(
    *,
    run_id: int,
    status: str,
    conclusion: str | None,
    updated_at: str,
    name: str = "CI",
    head_sha: str = "head-1",
) -> WorkflowRunSnapshot:
    return WorkflowRunSnapshot(
        run_id=run_id,
        name=name,
        status=status,
        conclusion=conclusion,
        html_url=f"https://example/runs/{run_id}",
        head_sha=head_sha,
        created_at="2026-02-21T00:00:00Z",
        updated_at=updated_at,
    )


def _workflow_job(
    *,
    job_id: int,
    name: str,
    conclusion: str,
    status: str = "completed",
) -> WorkflowJobSnapshot:
    return WorkflowJobSnapshot(
        job_id=job_id,
        name=name,
        status=status,
        conclusion=conclusion,
        html_url=f"https://example/jobs/{job_id}",
    )


def test_required_tests_helpers_handle_exception_idempotence_and_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    script_path = tmp_path / "scripts" / "required-tests.sh"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cmd, cwd, input_text, check
        raise RuntimeError("boom")

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)
    required_error = orch._run_required_tests_before_push(checkout_path=tmp_path)
    assert required_error == "RuntimeError: boom"

    issue = Issue(
        number=7,
        title="Issue",
        body="Body",
        html_url="https://example/issues/7",
        labels=("agent:bugfix",),
        author_login="issue-author",
    )
    with_reminder = orch._issue_with_required_tests_reminder(issue, checkout_path=tmp_path)
    assert "Required pre-push test command" in with_reminder.body
    assert (
        orch._issue_with_required_tests_reminder(with_reminder, checkout_path=tmp_path)
        is with_reminder
    )

    empty_failure = orch._issue_with_required_tests_failure(
        issue=issue,
        failure_output="   ",
        attempt=1,
        checkout_path=tmp_path,
    )
    assert "Failure output:\n<empty>" in empty_failure.body

    large_failure = orch._issue_with_required_tests_failure(
        issue=issue,
        failure_output="x" * 13000,
        attempt=2,
        checkout_path=tmp_path,
    )
    assert "... [truncated by MergeXO]" in large_failure.body

    cfg_without_required_tests = _config(tmp_path)
    orch_without_required_tests = Phase1Orchestrator(
        cfg_without_required_tests,
        state=state,
        github=github,
        git_manager=git,
        agent=agent,
    )
    untouched_issue = orch_without_required_tests._issue_with_required_tests_failure(
        issue=issue,
        failure_output="failure",
        attempt=1,
    )
    assert untouched_issue is issue


def test_required_tests_helpers_skip_when_required_tests_file_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    run_called = False

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        nonlocal run_called
        _ = cmd, cwd, input_text, check
        run_called = True
        return ""

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    required_error = orch._run_required_tests_before_push(checkout_path=tmp_path)
    assert required_error is None
    assert run_called is False

    issue = Issue(
        number=8,
        title="Issue",
        body="Body",
        html_url="https://example/issues/8",
        labels=("agent:small-job",),
        author_login="issue-author",
    )
    assert orch._issue_with_required_tests_reminder(issue, checkout_path=tmp_path) is issue
    assert (
        orch._issue_with_required_tests_failure(
            issue=issue,
            failure_output="failure",
            attempt=1,
            checkout_path=tmp_path,
        )
        is issue
    )

    comment = _issue_comment()
    assert orch._append_required_tests_feedback_reminder(
        (comment,),
        checkout_path=tmp_path,
    ) == (comment,)


def test_save_agent_session_if_present_skips_none(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    orch._save_agent_session_if_present(issue_number=7, session=None)

    assert state.saved_sessions == []


def test_feedback_turn_required_tests_failure_truncates_and_handles_empty_output(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.changed_files = ("src/a.py",)
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )

    truncated_turn = orch._feedback_turn_with_required_tests_failure(
        turn=turn,
        pull_request=github.pr_snapshot,
        repair_round=1,
        failure_output="x" * 13000,
    )
    assert "... [truncated by MergeXO]" in truncated_turn.issue_comments[-1].body
    assert truncated_turn.changed_files == ("src/a.py",)

    empty_turn = orch._feedback_turn_with_required_tests_failure(
        turn=turn,
        pull_request=github.pr_snapshot,
        repair_round=2,
        failure_output="   ",
    )
    assert "Failure output:\n<empty>" in empty_turn.issue_comments[-1].body


def test_feedback_turn_push_merge_conflict_truncates_and_handles_empty_output(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.changed_files = ("src/a.py",)
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )

    truncated_turn = orch._feedback_turn_with_push_merge_conflict(
        turn=turn,
        pull_request=github.pr_snapshot,
        branch="agent/design/7-add-worker-scheduler",
        repair_round=1,
        failure_output="x" * 13000,
    )
    assert "... [truncated by MergeXO]" in truncated_turn.issue_comments[-1].body
    assert truncated_turn.changed_files == ("src/a.py",)

    empty_turn = orch._feedback_turn_with_push_merge_conflict(
        turn=turn,
        pull_request=github.pr_snapshot,
        branch="agent/design/7-add-worker-scheduler",
        repair_round=2,
        failure_output="   ",
    )
    assert "Merge output:\n<empty>" in empty_turn.issue_comments[-1].body


def test_commit_push_feedback_re_raises_non_merge_conflict_push_error(tmp_path: Path) -> None:
    orch, git, github, _state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    _ = github

    def raise_push_error(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("transport failed")

    git.push_branch = raise_push_error  # type: ignore[method-assign]

    with pytest.raises(CommandError, match="transport failed"):
        orch._commit_push_feedback_with_required_tests(
            tracked=tracked,
            checkout_path=tmp_path,
            turn=turn,
            result=result,
            pull_request=turn.pull_request,
            turn_start_head="head-1",
        )


def test_commit_push_feedback_blocks_when_merge_conflict_repair_limit_exceeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)

    def raise_merge_conflict(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("Automatic merge failed; merge conflict in src/app.py")

    git.push_branch = raise_merge_conflict  # type: ignore[method-assign]
    repair_calls = {"count": 0}

    def fake_run_feedback_agent_with_git_ops(
        **kwargs: object,
    ) -> tuple[FeedbackResult, PullRequestSnapshot]:
        _ = kwargs
        repair_calls["count"] += 1
        return result, github.pr_snapshot

    monkeypatch.setattr(
        orch, "_run_feedback_agent_with_git_ops", fake_run_feedback_agent_with_git_ops
    )

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert repair_calls["count"] == 3
    assert state.status_updates[-1][2] == "blocked"
    assert "push-time merge conflict unresolved" in str(state.status_updates[-1][4]).lower()
    assert any("attempts: 3" in body for _, body in github.comments)


def test_commit_push_feedback_marks_merged_when_pr_merges_after_push_conflict(
    tmp_path: Path,
) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-merged",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=True,
    )

    def raise_merge_conflict(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("Automatic merge failed; merge conflict in src/app.py")

    git.push_branch = raise_merge_conflict  # type: ignore[method-assign]

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert state.status_updates == [(101, 7, "merged", "head-merged", None)]


def test_commit_push_feedback_marks_closed_when_pr_closes_after_push_conflict(
    tmp_path: Path,
) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-closed",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=False,
    )

    def raise_merge_conflict(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("Automatic merge failed; merge conflict in src/app.py")

    git.push_branch = raise_merge_conflict  # type: ignore[method-assign]

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert state.status_updates == [(101, 7, "closed", "head-closed", None)]


def test_commit_push_feedback_returns_none_when_merge_conflict_repair_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orch, git, _github, _state, tracked, turn, result = _feedback_commit_push_context(tmp_path)

    def raise_merge_conflict(checkout_path: Path, branch: str) -> None:
        _ = checkout_path, branch
        raise CommandError("Automatic merge failed; merge conflict in src/app.py")

    git.push_branch = raise_merge_conflict  # type: ignore[method-assign]
    monkeypatch.setattr(orch, "_run_feedback_agent_with_git_ops", lambda **kwargs: None)

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None


def test_commit_push_feedback_pushes_git_op_local_commit_without_commit_message(
    tmp_path: Path,
) -> None:
    orch, git, _github, _state, tracked, turn, _result = _feedback_commit_push_context(tmp_path)
    result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-1"),
        review_replies=(),
        general_comment="Merged main and validated.",
        commit_message=None,
        git_ops=(),
    )
    git.current_head_sha_value = "head-2"
    git.is_ancestor_results[("head-1", "head-2")] = True

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is not None
    pushed_result, _ = outcome
    assert pushed_result.commit_message is None
    assert git.commit_calls == []
    assert len(git.push_calls) == 1


def test_commit_push_feedback_blocks_for_invalid_review_reply_target(tmp_path: Path) -> None:
    orch, git, github, state, tracked, turn, _result = _feedback_commit_push_context(tmp_path)
    result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-1"),
        review_replies=(ReviewReply(review_comment_id=21, body="Done"),),
        general_comment=None,
        commit_message="feat: update",
        git_ops=(),
    )

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert git.commit_calls == []
    assert git.push_calls == []
    assert state.status_updates == [
        (
            101,
            7,
            "blocked",
            "head-1",
            "agent returned review_replies for non-review comment ids: 21",
        )
    ]
    assert len(github.comments) == 1
    assert "non-review comment ids" in github.comments[0][1].lower()


def test_commit_push_feedback_replays_agent_after_remote_push_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orch, git, github, _state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    git.current_head_sha_value = "head-1"
    git.push_results = [True]
    github.changed_files = ("src/a.py",)
    replay_calls: list[FeedbackTurn] = []

    replay_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-2"),
        review_replies=(),
        general_comment="Validated merged remote updates.",
        commit_message=None,
        git_ops=(),
    )

    def fake_run_feedback_agent_with_git_ops(
        **kwargs: object,
    ) -> tuple[FeedbackResult, PullRequestSnapshot]:
        replay_calls.append(cast(FeedbackTurn, kwargs["turn"]))
        return replay_result, github.pr_snapshot

    monkeypatch.setattr(
        orch, "_run_feedback_agent_with_git_ops", fake_run_feedback_agent_with_git_ops
    )

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is not None
    final_result, _ = outcome
    assert final_result.general_comment == "Validated merged remote updates."
    assert len(replay_calls) == 1
    repair_turn = replay_calls[0]
    assert (
        "concurrent remote branch update while pushing feedback"
        in repair_turn.issue_comments[-1].body
    )
    assert "origin/agent/design/7-add-worker-scheduler" in repair_turn.issue_comments[-1].body
    assert repair_turn.changed_files == ("src/a.py",)


def test_commit_push_feedback_blocks_after_repeated_remote_push_races(tmp_path: Path) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    git.current_head_sha_value = "head-1"
    git.push_results = [True, True, True, True]

    def replay_result_with_followup(
        **kwargs: object,
    ) -> tuple[FeedbackResult, PullRequestSnapshot]:
        _ = kwargs
        return (
            FeedbackResult(
                session=AgentSession(adapter="codex", thread_id="thread-race"),
                review_replies=(),
                general_comment="Remote branch changed again; applying follow-up.",
                commit_message="fix: re-apply after remote branch merge",
                git_ops=(),
            ),
            github.pr_snapshot,
        )

    orch._run_feedback_agent_with_git_ops = replay_result_with_followup  # type: ignore[method-assign]
    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert state.status_updates[-1][2] == "blocked"
    assert "reconciliation exceeded safety limit" in str(state.status_updates[-1][4]).lower()
    assert any(
        "concurrent remote branch updates kept racing" in body.lower()
        for _, body in github.comments
    )


def test_commit_push_feedback_marks_merged_when_pr_merges_after_remote_push_race(
    tmp_path: Path,
) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    git.current_head_sha_value = "head-1"
    git.push_results = [True]
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-merged",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=True,
    )

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert state.status_updates == [(101, 7, "merged", "head-merged", None)]


def test_commit_push_feedback_marks_closed_when_pr_closes_after_remote_push_race(
    tmp_path: Path,
) -> None:
    orch, git, github, state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    git.current_head_sha_value = "head-1"
    git.push_results = [True]
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-closed",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=False,
    )

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None
    assert state.status_updates == [(101, 7, "closed", "head-closed", None)]


def test_commit_push_feedback_returns_none_when_remote_push_race_repair_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orch, git, _github, _state, tracked, turn, result = _feedback_commit_push_context(tmp_path)
    git.current_head_sha_value = "head-1"
    git.push_results = [True]
    monkeypatch.setattr(orch, "_run_feedback_agent_with_git_ops", lambda **kwargs: None)

    outcome = orch._commit_push_feedback_with_required_tests(
        tracked=tracked,
        checkout_path=tmp_path,
        turn=turn,
        result=result,
        pull_request=turn.pull_request,
        turn_start_head="head-1",
    )

    assert outcome is None


def test_feedback_turn_processes_once_for_same_comment(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]
    github.changed_files = ("docs/design/7-add-worker-scheduler.md",)

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    assert state.list_pending_feedback_events(101) == ()

    tracked_again = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_again)
    assert len(agent.feedback_calls) == 1


def test_feedback_turn_incremental_scan_handles_same_second_boundaries(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment(comment_id=11, updated_at="2026-02-21T00:00:00Z")]
    github.changed_files = ("docs/design/7-add-worker-scheduler.md",)

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert len(agent.feedback_calls) == 1

    review_cursor = state.get_poll_cursor(
        surface="pr_review_comments",
        scope_number=101,
        repo_full_name=cfg.repo.full_name,
    )
    assert review_cursor is not None
    assert review_cursor.last_comment_id == 11

    github.review_comments.append(_review_comment(comment_id=12, updated_at="2026-02-21T00:00:00Z"))
    tracked_again = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_again)
    assert len(agent.feedback_calls) == 2
    review_cursor_again = state.get_poll_cursor(
        surface="pr_review_comments",
        scope_number=101,
        repo_full_name=cfg.repo.full_name,
    )
    assert review_cursor_again is not None
    assert review_cursor_again.last_comment_id == 12


def test_feedback_turn_incremental_includes_new_pr_issue_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment(comment_id=11)]
    github.issue_comments = [_issue_comment(comment_id=21)]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    first_turn = agent.feedback_calls[0][1]
    assert tuple(comment.comment_id for comment in first_turn.review_comments) == (11,)
    assert tuple(comment.comment_id for comment in first_turn.issue_comments) == (21,)

    github.issue_comments.append(_issue_comment(comment_id=22))
    tracked_again = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_again)

    assert len(agent.feedback_calls) == 2
    second_turn = agent.feedback_calls[1][1]
    assert tuple(comment.comment_id for comment in second_turn.issue_comments) == (22,)


def test_feedback_turn_ingests_review_summary_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_summaries = [
        PullRequestIssueComment(
            comment_id=31,
            body="Please split this feedback into the cache package.",
            user_login="reviewer",
            html_url="https://example/review/31",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        )
    ]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    turn = agent.feedback_calls[0][1]
    assert turn.review_comments == ()
    assert any(
        comment.comment_id == 31
        and "Please split this feedback into the cache package." in comment.body
        for comment in turn.issue_comments
    )

    summary_cursor = state.get_poll_cursor(
        surface="pr_review_summaries",
        scope_number=101,
        repo_full_name=cfg.repo.full_name,
    )
    assert summary_cursor is not None
    assert summary_cursor.last_comment_id == 31


def test_feedback_turn_incremental_resets_invalid_future_cursor(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = []
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.upsert_poll_cursor(
        surface="pr_review_comments",
        scope_number=101,
        last_updated_at="9999-01-01T00:00:00Z",
        last_comment_id=9,
        bootstrap_complete=True,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    cursor = state.get_poll_cursor(
        surface="pr_review_comments",
        scope_number=101,
        repo_full_name=cfg.repo.full_name,
    )
    assert cursor is not None
    assert cursor.last_updated_at != "9999-01-01T00:00:00Z"


def test_load_poll_cursor_normalizes_timestamp_format(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    state = StateStore(tmp_path / "state.db")
    state.upsert_poll_cursor(
        surface="pr_review_comments",
        scope_number=101,
        last_updated_at="2026-02-24T00:00:00+00:00",
        last_comment_id=9,
        bootstrap_complete=True,
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    normalized = orch._load_poll_cursor(surface="pr_review_comments", scope_number=101)
    assert normalized.last_updated_at == "2026-02-24T00:00:00Z"
    assert normalized.last_comment_id == 9


def test_since_for_cursor_invalid_and_floor_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    monkeypatch.setattr(
        "mergexo.orchestrator._parse_utc_timestamp",
        lambda value: None,  # type: ignore[no-untyped-def]
    )
    since_invalid = orch._since_for_cursor(last_updated_at="bad-value")
    assert since_invalid.endswith("Z")


def test_since_for_cursor_clamps_to_epoch(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    assert orch._since_for_cursor(last_updated_at="1970-01-01T00:00:00Z") == "1970-01-01T00:00:00Z"


def test_token_reconcile_since_applies_safe_floor_and_epoch(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_incremental_comment_fetch=True)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    before = datetime.now(timezone.utc) - timedelta(days=2)
    clamped = _parse_utc_timestamp(orch._token_reconcile_since(created_at="2000-01-01T00:00:00Z"))
    assert clamped is not None
    assert clamped >= before

    epoch_cfg = _config(
        tmp_path / "epoch",
        enable_incremental_comment_fetch=True,
        comment_fetch_safe_backfill_seconds=4_000_000_000,
    )
    epoch_orch = Phase1Orchestrator(
        epoch_cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts-epoch"),
        agent=FakeAgent(),
    )
    assert (
        epoch_orch._token_reconcile_since(created_at="1970-01-01T00:00:02Z")
        == "1970-01-01T00:00:00Z"
    )


def test_monitor_pr_actions_skips_enqueue_while_runs_active(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="all_complete")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7000,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T00:50:00Z",
        ),
        _workflow_run(
            run_id=7001,
            status="in_progress",
            conclusion=None,
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_monitor_pr_actions_skips_takeover_issues(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_monitor_pr_actions_never_policy_skips_enqueue(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_pr_actions_monitoring=True,
        pr_actions_feedback_policy="never",
    )
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_monitor_pr_actions_first_fail_enqueues_even_while_runs_active(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:00:00Z",
        ),
        _workflow_run(
            run_id=7002,
            status="in_progress",
            conclusion=None,
            updated_at="2026-02-21T01:01:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    pending = state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name)
    assert len(pending) == 1
    assert pending[0].event_key == "101:actions:7001:2026-02-21T01:00:00Z"


def test_monitor_pr_actions_enqueues_failed_runs_once_per_update(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_pr_actions_monitoring=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    orch._monitor_pr_actions()
    pending_once = state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name)
    assert len(pending_once) == 1
    assert pending_once[0].event_key == "101:actions:7001:2026-02-21T01:00:00Z"

    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:10:00Z",
        ),
    )
    orch._monitor_pr_actions()
    pending_twice = state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name)
    assert {event.event_key for event in pending_twice} == {
        "101:actions:7001:2026-02-21T01:00:00Z",
        "101:actions:7001:2026-02-21T01:10:00Z",
    }


def test_monitor_pr_actions_skips_when_runs_are_green(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_pr_actions_monitoring=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="success",
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._monitor_pr_actions()
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_monitor_pr_actions_skips_closed_or_merged_prs(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_pr_actions_monitoring=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T01:00:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    orch._monitor_pr_actions()

    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_feedback_turn_resolves_stale_actions_events_without_agent(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7001,
            status="completed",
            conclusion="success",
            updated_at="2026-02-21T01:10:00Z",
        ),
    )

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7001:2026-02-21T01:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7001,
                updated_at="2026-02-21T01:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()
    assert len(state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name)) == 1


def test_resolve_actions_feedback_events_marks_all_stale_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
        _workflow_run(
            run_id=7003,
            status="in_progress",
            conclusion=None,
            updated_at="2026-02-21T02:00:00Z",
        ),
        _workflow_run(
            run_id=7004,
            status="completed",
            conclusion="success",
            updated_at="2026-02-21T02:00:00Z",
        ),
    )
    github.workflow_jobs_by_run_id[7002] = (
        _workflow_job(job_id=8001, name="tests", conclusion="failure"),
    )
    github.failed_run_log_tails[7002] = {"tests": "tail"}

    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    actionable, synthetic_comments, stale_keys = orch._resolve_actions_feedback_events(
        pr_number=101,
        head_sha="head-1",
        action_events=(
            PendingFeedbackEvent(
                event_key="missing",
                kind="actions",
                comment_id=7001,
                updated_at="2026-02-21T02:00:00Z",
            ),
            PendingFeedbackEvent(
                event_key="updated-mismatch",
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T01:59:00Z",
            ),
            PendingFeedbackEvent(
                event_key="still-running",
                kind="actions",
                comment_id=7003,
                updated_at="2026-02-21T02:00:00Z",
            ),
            PendingFeedbackEvent(
                event_key="now-green",
                kind="actions",
                comment_id=7004,
                updated_at="2026-02-21T02:00:00Z",
            ),
            PendingFeedbackEvent(
                event_key="actionable",
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
    )

    assert tuple(event.event_key for event in actionable) == ("actionable",)
    assert set(stale_keys) == {"missing", "updated-mismatch", "still-running", "now-green"}
    assert len(synthetic_comments) == 1


def test_feedback_turn_includes_actions_failure_context_without_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
            name="CI",
        ),
    )
    github.workflow_jobs_by_run_id[7002] = (
        _workflow_job(job_id=8001, name="tests", conclusion="failure"),
        _workflow_job(job_id=8003, name="tests", conclusion="failure"),
        _workflow_job(job_id=8002, name="lint", conclusion="success"),
    )
    github.failed_run_log_tails[7002] = {"tests [job 8001]": "line a\nline b"}

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7002:2026-02-21T02:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    turn = agent.feedback_calls[0][1]
    assert turn.review_comments == ()
    assert len(turn.issue_comments) == 1
    assert "MergeXO GitHub Actions failure context" in turn.issue_comments[0].body
    assert "action: tests [job 8001]" in turn.issue_comments[0].body
    assert "<logs unavailable for this action>" in turn.issue_comments[0].body
    assert "last 500 log lines" in turn.issue_comments[0].body
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()
    assert github.failed_log_tail_calls == [(7002, 500)]


def test_feedback_turn_actions_context_handles_missing_failed_jobs(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7004,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:05:00Z",
        ),
    )
    github.workflow_jobs_by_run_id[7004] = (
        _workflow_job(job_id=8101, name="lint", conclusion="success"),
    )

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7004:2026-02-21T02:05:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7004,
                updated_at="2026-02-21T02:05:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    turn = agent.feedback_calls[0][1]
    assert (
        "No failed actions were returned by the jobs API for this run."
        in turn.issue_comments[0].body
    )


def test_feedback_turn_flake_report_files_issue_and_requests_rerun(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
    )
    github.workflow_jobs_by_run_id[7002] = (
        _workflow_job(job_id=8001, name="tests", conclusion="failure"),
    )
    github.failed_run_log_tails[7002] = {"tests": "AssertionError: expected 1 got 0"}

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-555"),
        review_replies=(),
        general_comment=None,
        commit_message=None,
        git_ops=(),
        flaky_test_report=FlakyTestReport(
            run_id=7002,
            title="Flaky scheduler integration test",
            summary="Failure appears unrelated to this PR and reproduces intermittently.",
            relevant_log_excerpt="AssertionError: expected 1 got 0",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7002:2026-02-21T02:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)

    assert result == "completed"
    assert len(github.created_issues) == 1
    created_title, created_body, created_labels = github.created_issues[0]
    assert created_title == "Flaky scheduler integration test"
    assert created_labels is None
    assert "Full Captured Actions Context" in created_body
    assert "AssertionError: expected 1 got 0" in created_body

    assert github.rerun_requests == [7002]
    assert any("likely unrelated flaky CI failure" in body for _, body in github.comments)
    assert any("rerun the failed jobs once" in body.lower() for _, body in github.comments)

    flake_state = state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name)
    assert flake_state is not None
    assert flake_state.status == "awaiting_rerun_result"
    assert flake_state.rerun_requested_at is not None
    assert flake_state.flake_issue_number == 1000
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_monitor_pr_actions_resolves_flake_after_green_rerun(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="success",
            updated_at="2026-02-21T02:30:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=issue.number,
        head_sha="head-1",
        run_id=7002,
        initial_run_updated_at="2026-02-21T02:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=88,
        flake_issue_url="https://example/issues/88",
        report_title="Flaky scheduler integration test",
        report_summary="Looks flaky.",
        report_excerpt="AssertionError",
        full_log_context_markdown="MergeXO GitHub Actions failure context",
        rerun_requested_at="2026-02-21T02:05:00Z",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    orch._monitor_pr_actions()

    flake_state = state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name)
    assert flake_state is not None
    assert flake_state.status == "resolved_after_rerun"
    tracked = state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name)
    assert len(tracked) == 1
    assert tracked[0].pr_number == 101


def test_monitor_pr_actions_blocks_after_second_flake_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:30:00Z",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=issue.number,
        head_sha="head-1",
        run_id=7002,
        initial_run_updated_at="2026-02-21T02:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=88,
        flake_issue_url="https://example/issues/88",
        report_title="Flaky scheduler integration test",
        report_summary="Looks flaky.",
        report_excerpt="AssertionError",
        full_log_context_markdown="MergeXO GitHub Actions failure context",
        rerun_requested_at="2026-02-21T02:05:00Z",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    orch._monitor_pr_actions()

    flake_state = state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name)
    assert flake_state is not None
    assert flake_state.status == "blocked_after_second_failure"
    blocked = state.list_blocked_pull_requests(repo_full_name=cfg.repo.full_name)
    assert len(blocked) == 1
    assert blocked[0].pr_number == 101
    assert any("PR is now blocked" in body for _, body in github.comments)
    assert any("https://example/issues/88" in body for _, body in github.comments)


def test_feedback_turn_flake_idempotent_after_finalize_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
    )
    github.workflow_jobs_by_run_id[7002] = (
        _workflow_job(job_id=8001, name="tests", conclusion="failure"),
    )
    github.failed_run_log_tails[7002] = {"tests": "AssertionError: expected 1 got 0"}

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-555"),
        review_replies=(),
        general_comment=None,
        commit_message=None,
        git_ops=(),
        flaky_test_report=FlakyTestReport(
            run_id=7002,
            title="Flaky scheduler integration test",
            summary="Failure appears unrelated to this PR and reproduces intermittently.",
            relevant_log_excerpt="AssertionError: expected 1 got 0",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7002:2026-02-21T02:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    original_finalize = state.finalize_feedback_turn
    calls = {"count": 0}

    def crash_once(**kwargs: object) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("crash before finalize")
        original_finalize(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(state, "finalize_feedback_turn", crash_once)

    tracked = _tracked_state_from_store(state)
    with pytest.raises(RuntimeError, match="crash before finalize"):
        orch._process_feedback_turn(tracked)

    assert len(github.created_issues) == 1
    assert len(github.rerun_requests) == 1
    first_comment_count = len(github.comments)
    assert first_comment_count >= 1

    tracked_retry = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_retry)

    assert len(github.created_issues) == 1
    assert len(github.rerun_requests) == 1
    assert len(github.comments) == first_comment_count
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_reconcile_pr_flake_state_waiting_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    state = FakeState()
    orch = Phase1Orchestrator(
        cfg, state=state, github=FakeGitHub([]), git_manager=git, agent=FakeAgent()
    )
    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha=None,
    )
    pull_request = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )

    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=7,
        head_sha="old-head",
        run_id=7001,
        initial_run_updated_at="2026-02-21T02:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=88,
        flake_issue_url="https://example/issues/88",
        report_title="Flake",
        report_summary="summary",
        report_excerpt="excerpt",
        full_log_context_markdown="ctx",
        repo_full_name=cfg.repo.full_name,
    )
    assert not orch._reconcile_pr_flake_state(tracked=tracked, pull_request=pull_request, runs=())
    assert state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name) is None

    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=7,
        head_sha="head-1",
        run_id=7001,
        initial_run_updated_at="2026-02-21T02:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=88,
        flake_issue_url="https://example/issues/88",
        report_title="Flake",
        report_summary="summary",
        report_excerpt="excerpt",
        full_log_context_markdown="ctx",
        repo_full_name=cfg.repo.full_name,
    )
    assert orch._reconcile_pr_flake_state(tracked=tracked, pull_request=pull_request, runs=())
    assert orch._reconcile_pr_flake_state(
        tracked=tracked,
        pull_request=pull_request,
        runs=(
            _workflow_run(
                run_id=7001,
                status="completed",
                conclusion="failure",
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
    )
    assert orch._reconcile_pr_flake_state(
        tracked=tracked,
        pull_request=pull_request,
        runs=(
            _workflow_run(
                run_id=7001,
                status="in_progress",
                conclusion=None,
                updated_at="2026-02-21T02:05:00Z",
            ),
        ),
    )


def test_feedback_turn_clears_active_flake_state_on_head_change(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-new",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=issue.number,
        head_sha="head-old",
        run_id=7001,
        initial_run_updated_at="2026-02-21T02:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=88,
        flake_issue_url="https://example/issues/88",
        report_title="Flake",
        report_summary="summary",
        report_excerpt="excerpt",
        full_log_context_markdown="ctx",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)

    assert result == "completed"
    assert state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name) is None


def test_actions_context_and_flake_render_helpers(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    contexts = orch._actions_context_by_run_id(
        (
            PullRequestIssueComment(
                comment_id=1,
                body="- run_id: 7001",
                user_login="mergexo-system",
                html_url="",
                created_at="now",
                updated_at="now",
            ),
            PullRequestIssueComment(
                comment_id=2,
                body="- run_id: nope",
                user_login="mergexo-system",
                html_url="",
                created_at="now",
                updated_at="now",
            ),
            PullRequestIssueComment(
                comment_id=3,
                body="no run id here",
                user_login="mergexo-system",
                html_url="",
                created_at="now",
                updated_at="now",
            ),
        )
    )
    assert contexts == {7001: "- run_id: 7001"}
    assert orch._run_id_from_actions_context("- run_id: nope") is None
    assert orch._run_id_from_actions_context("missing") is None

    title = orch._render_flake_issue_title(
        report=FlakyTestReport(
            run_id=7001,
            title="",
            summary="",
            relevant_log_excerpt="x",
        ),
        pr_number=101,
    )
    assert title == "Flaky CI test on PR #101 (run 7001)"
    blocked_comment = orch._render_flake_blocked_pr_comment(
        run_id=7001,
        run_url="https://example/runs/7001",
        flake_issue_url="https://example/issues/88",
        conclusion="failure",
        reason="rerun request failed",
    )
    assert "detail: rerun request failed" in blocked_comment

    oversized_tail = "x" * 40000
    actions_context = orch._render_actions_failure_context_comment(
        run=_workflow_run(
            run_id=7001,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
        failed_jobs=(_workflow_job(job_id=8001, name="tests", conclusion="failure"),),
        log_tails_by_action={"tests": oversized_tail},
        tail_lines=500,
    )
    assert len(actions_context) <= 32000
    assert actions_context.endswith("... [truncated by MergeXO]")

    oversized_issue_body = orch._render_flake_issue_body(
        issue=_issue(),
        pull_request=PullRequestSnapshot(
            number=101,
            title="PR",
            body="Body",
            head_sha="head-1",
            base_sha="base-1",
            draft=False,
            state="open",
            merged=False,
        ),
        report=FlakyTestReport(
            run_id=7001,
            title="Flaky scheduler integration test",
            summary="Likely flaky; failure reproduces intermittently.",
            relevant_log_excerpt="AssertionError: expected 1 got 0",
        ),
        run_id=7001,
        run_url="https://example/runs/7001",
        full_log_context_markdown="y" * 50000,
    )
    assert len(oversized_issue_body) <= 32000
    assert oversized_issue_body.endswith("... [truncated by MergeXO]")


def test_truncate_feedback_text_guardrails() -> None:
    with pytest.raises(ValueError, match="soft_limit_chars"):
        _truncate_feedback_text("text", soft_limit_chars=0, hard_limit_chars=100)
    with pytest.raises(ValueError, match="hard_limit_chars"):
        _truncate_feedback_text("text", soft_limit_chars=100, hard_limit_chars=0)

    truncated = _truncate_feedback_text("x" * 10, soft_limit_chars=5, hard_limit_chars=5)
    assert truncated == "\n... [truncated by MergeXO]"[:5]


def test_handle_flaky_test_report_reject_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha=None,
    )
    issue = _issue()
    pr = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    report = FlakyTestReport(
        run_id=7001,
        title="Flake",
        summary="summary",
        relevant_log_excerpt="excerpt",
    )

    assert (
        orch._handle_flaky_test_report(
            tracked=tracked,
            issue=issue,
            pull_request=pr,
            turn_key="turn",
            session=AgentSession(adapter="codex", thread_id="t"),
            pending_event_keys=("k",),
            actions_only_turn=True,
            actionable_actions_by_run_id={7001: PendingFeedbackEvent("k", "actions", 7001, "u")},
            actions_context_by_run_id={7001: "ctx"},
            report=report,
            commit_message="bad",
        )
        is None
    )
    assert (
        orch._handle_flaky_test_report(
            tracked=tracked,
            issue=issue,
            pull_request=pr,
            turn_key="turn",
            session=AgentSession(adapter="codex", thread_id="t"),
            pending_event_keys=("k",),
            actions_only_turn=False,
            actionable_actions_by_run_id={7001: PendingFeedbackEvent("k", "actions", 7001, "u")},
            actions_context_by_run_id={7001: "ctx"},
            report=report,
            commit_message=None,
        )
        is None
    )
    assert (
        orch._handle_flaky_test_report(
            tracked=tracked,
            issue=issue,
            pull_request=pr,
            turn_key="turn",
            session=AgentSession(adapter="codex", thread_id="t"),
            pending_event_keys=("k",),
            actions_only_turn=True,
            actionable_actions_by_run_id={},
            actions_context_by_run_id={7001: "ctx"},
            report=report,
            commit_message=None,
        )
        is None
    )
    assert (
        orch._handle_flaky_test_report(
            tracked=tracked,
            issue=issue,
            pull_request=pr,
            turn_key="turn",
            session=AgentSession(adapter="codex", thread_id="t"),
            pending_event_keys=("k",),
            actions_only_turn=True,
            actionable_actions_by_run_id={7001: PendingFeedbackEvent("k", "actions", 7001, "u")},
            actions_context_by_run_id={},
            report=report,
            commit_message=None,
        )
        is None
    )


def test_feedback_turn_flake_rerun_request_failure_blocks(tmp_path: Path) -> None:
    cfg = _config(tmp_path, pr_actions_feedback_policy="first_fail")
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
    )
    github.workflow_jobs_by_run_id[7002] = (
        _workflow_job(job_id=8001, name="tests", conclusion="failure"),
    )
    github.failed_run_log_tails[7002] = {"tests": "AssertionError"}

    def fail_rerun(run_id: int) -> None:
        _ = run_id
        raise RuntimeError("forbidden")

    github.rerun_workflow_run_failed_jobs = fail_rerun  # type: ignore[method-assign]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-555"),
        review_replies=(),
        general_comment=None,
        commit_message=None,
        git_ops=(),
        flaky_test_report=FlakyTestReport(
            run_id=7002,
            title="Flaky scheduler integration test",
            summary="Failure appears unrelated to this PR and reproduces intermittently.",
            relevant_log_excerpt="AssertionError",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:actions:7002:2026-02-21T02:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="actions",
                comment_id=7002,
                updated_at="2026-02-21T02:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)

    assert result == "blocked"
    flake_state = state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name)
    assert flake_state is not None
    assert flake_state.status == "blocked_after_second_failure"
    blocked = state.list_blocked_pull_requests(repo_full_name=cfg.repo.full_name)
    assert len(blocked) == 1
    assert any("detail: Rerun request failed" in body for _, body in github.comments)


def test_handle_flaky_test_report_replaces_mismatched_existing_state(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    state = FakeState()
    github = FakeGitHub([_issue()])
    github.workflow_runs_by_head[(101, "head-1")] = (
        _workflow_run(
            run_id=7002,
            status="completed",
            conclusion="failure",
            updated_at="2026-02-21T02:00:00Z",
        ),
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha=None,
    )
    issue = _issue()
    pr = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    state.upsert_pr_flake_state(
        pr_number=101,
        issue_number=7,
        head_sha="head-old",
        run_id=7001,
        initial_run_updated_at="2026-02-21T01:00:00Z",
        status="awaiting_rerun_result",
        flake_issue_number=77,
        flake_issue_url="https://example/issues/77",
        report_title="old",
        report_summary="old",
        report_excerpt="old",
        full_log_context_markdown="old",
        repo_full_name=cfg.repo.full_name,
    )

    result = orch._handle_flaky_test_report(
        tracked=tracked,
        issue=issue,
        pull_request=pr,
        turn_key="turn",
        session=AgentSession(adapter="codex", thread_id="t"),
        pending_event_keys=("k",),
        actions_only_turn=True,
        actionable_actions_by_run_id={
            7002: PendingFeedbackEvent("k", "actions", 7002, "2026-02-21T02:00:00Z")
        },
        actions_context_by_run_id={7002: "MergeXO GitHub Actions failure context:\n- run_id: 7002"},
        report=FlakyTestReport(
            run_id=7002,
            title="new flake",
            summary="new summary",
            relevant_log_excerpt="new excerpt",
        ),
        commit_message=None,
    )
    assert result == "completed"
    flake_state = state.get_pr_flake_state(101, repo_full_name=cfg.repo.full_name)
    assert flake_state is not None
    assert flake_state.run_id == 7002
    assert len(github.created_issues) == 1


def test_feedback_turn_blocks_legacy_tracked_pr_when_issue_author_unauthorized(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(author_login="outsider")
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]
    github.issue_comments = [_issue_comment()]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert len(github.comments) == 1
    assert "not allowed by `repo.allowed_users`" in github.comments[0][1]
    assert github.review_replies == []
    assert state.list_pending_feedback_events(101) == ()
    assert state.list_tracked_pull_requests() == ()
    blocked = state.list_blocked_pull_requests()
    assert len(blocked) == 1
    assert blocked[0].pr_number == 101
    assert blocked[0].error is not None
    assert "not allowed" in blocked[0].error


def test_feedback_turn_retry_after_crash_does_not_duplicate_replies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=11, body="Done"),),
        general_comment="Updated in latest commit.",
        commit_message=None,
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    original_finalize = state.finalize_feedback_turn
    calls = {"count": 0}

    def crash_once(**kwargs: object) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("crash before finalize")
        original_finalize(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(state, "finalize_feedback_turn", crash_once)

    tracked = _tracked_state_from_store(state)
    with pytest.raises(RuntimeError, match="crash before finalize"):
        orch._process_feedback_turn(tracked)

    first_reply_count = len(github.review_replies)
    first_issue_comment_count = len(github.comments)
    assert first_reply_count == 1
    assert first_issue_comment_count >= 1
    assert len(state.list_pending_feedback_events(101)) == 1

    tracked_retry = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_retry)

    assert len(github.review_replies) == first_reply_count
    assert len(github.comments) == first_issue_comment_count
    assert state.list_pending_feedback_events(101) == ()


def test_feedback_turn_blocks_before_push_for_invalid_review_reply_target(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.issue_comments = [_issue_comment(comment_id=21)]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=21, body="Done"),),
        general_comment=None,
        commit_message="docs: apply feedback",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)

    assert result == "blocked"
    assert git.commit_calls == []
    assert git.push_calls == []
    assert github.review_replies == []
    assert len(github.comments) == 1
    assert "non-review comment ids" in github.comments[0][1].lower()
    assert "general_comment" in github.comments[0][1]
    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()


def test_feedback_turn_preserves_escalation_before_invalid_review_reply_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.issue_comments = [_issue_comment(comment_id=21)]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=21, body="Done"),),
        general_comment=None,
        commit_message="docs: apply feedback",
        git_ops=(),
        escalation=RoadmapRevisionEscalation(
            kind="roadmap_revision",
            summary="Foundational assumption failed",
            details="Need to revisit the plan before continuing.",
        ),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    handled_urls: list[str] = []
    monkeypatch.setattr(
        orch,
        "_handle_roadmap_revision_escalation",
        lambda source_issue, escalation, source_url: handled_urls.append(source_url),
    )

    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)

    assert result == "blocked"
    assert handled_urls == ["https://github.com/johnynek/mergexo/pull/101"]
    assert git.commit_calls == []
    assert git.push_calls == []
    assert github.review_replies == []
    assert len(github.comments) == 1
    assert "non-review comment ids" in github.comments[0][1].lower()
    assert len(state.list_pending_feedback_events(101)) == 1


def test_feedback_turn_skips_agent_when_branch_head_mismatch_and_retries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    git.restore_feedback_result = False

    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    configure_logging(verbose=True)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert len(state.list_pending_feedback_events(101)) == 1
    stderr = capsys.readouterr().err
    assert (
        "event=feedback_turn_blocked issue_number=7 pr_number=101 reason=head_mismatch_retry"
        in stderr
    )

    git.restore_feedback_result = True
    tracked_retry = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_retry)
    assert len(agent.feedback_calls) == 1
    assert state.list_pending_feedback_events(101) == ()


def test_feedback_turn_blocks_on_cross_cycle_history_rewrite_and_is_comment_idempotent(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-new",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.compare_statuses[("head-old", "head-new")] = "diverged"

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=issue.number,
        status="awaiting_feedback",
        last_seen_head_sha="head-old",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert len(github.comments) == 1
    assert "non-fast-forward" in github.comments[0][1].lower()
    assert agent.feedback_calls == []

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "non-fast-forward" in str(row[1]).lower()
    finally:
        conn.close()

    # Repeat the same blocked transition and verify we do not post duplicate operator comments.
    orch._process_feedback_turn(tracked)
    assert len(github.comments) == 1


def test_feedback_turn_allows_cross_cycle_fast_forward_drift(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-new",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.compare_statuses[("head-old", "head-new")] = "ahead"
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=issue.number,
        status="awaiting_feedback",
        last_seen_head_sha="head-old",
        repo_full_name=cfg.repo.full_name,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    assert state.list_pending_feedback_events(101) == ()
    tracked_after = _tracked_state_from_store(state)
    assert tracked_after.last_seen_head_sha == "head-new"
    assert ("head-old", "head-new") in github.compare_calls
    assert github.comments == []


def test_feedback_turn_blocks_when_agent_rewrites_local_history(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    git.is_ancestor_results[("head-1", "rewritten-head")] = False

    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    original_respond = agent.respond_to_feedback

    def respond_and_rewrite(
        *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        git.current_head_sha_value = "rewritten-head"
        return original_respond(session=session, turn=turn, cwd=cwd)

    agent.respond_to_feedback = respond_and_rewrite  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    assert git.push_calls == []
    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()
    assert len(github.comments) == 1
    assert "non-fast-forward" in github.comments[0][1].lower()

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "non-fast-forward" in str(row[1]).lower()
    finally:
        conn.close()


def test_feedback_turn_marks_closed_pr_and_stops_tracking(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=False,
    )

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert state.list_tracked_pull_requests() == ()
    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "closed"
    finally:
        conn.close()


def test_run_once_invokes_feedback_enqueue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    calls = {"feedback": 0}
    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: None)
    monkeypatch.setattr(
        orch, "_enqueue_feedback_work", lambda pool: calls.__setitem__("feedback", 1)
    )
    monkeypatch.setattr(orch, "_wait_for_all", lambda pool: None)
    monkeypatch.setattr(orch, "_reap_finished", lambda: None)

    orch.run(once=True)
    assert calls["feedback"] == 1


def test_enqueue_feedback_work_skips_takeover_issues(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue(labels=("agent:design", "agent:ignore"))])
    state = FakeState()
    state.tracked = [
        TrackedPullRequestState(
            pr_number=101,
            issue_number=7,
            branch="agent/design/7",
            status="awaiting_feedback",
            last_seen_head_sha=None,
        )
    ]
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def submit(self, fn, tracked):  # type: ignore[no-untyped-def]
            _ = fn, tracked
            raise AssertionError("feedback worker should not be submitted for takeover issues")

    starts_before = len(state.run_starts)
    orch._enqueue_feedback_work(FakePool())
    assert len(state.run_starts) == starts_before


def test_enqueue_feedback_work_marks_takeover_merged_pr_terminal(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-merged",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=True,
    )
    db_path = tmp_path / "state.db"
    state = StateStore(db_path)
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def submit(self, fn, tracked):  # type: ignore[no-untyped-def]
            _ = fn, tracked
            raise AssertionError("feedback worker should not be submitted for merged PRs")

    orch._enqueue_feedback_work(FakePool())

    assert state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name) == ()
    pr_status = state.get_pull_request_status(101, repo_full_name=cfg.repo.full_name)
    assert pr_status is not None
    assert pr_status.status == "merged"
    outcomes = oq.load_terminal_issue_outcomes(db_path, cfg.repo.full_name, limit=10)
    assert len(outcomes) == 1
    assert outcomes[0].issue_number == issue.number
    assert outcomes[0].pr_number == 101
    assert outcomes[0].status == "merged"


def test_enqueue_feedback_work_marks_takeover_closed_pr_terminal(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="PR",
        body="Body",
        head_sha="head-closed",
        base_sha="base-1",
        draft=False,
        state="closed",
        merged=False,
    )
    db_path = tmp_path / "state.db"
    state = StateStore(db_path)
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    class FakePool:
        def submit(self, fn, tracked):  # type: ignore[no-untyped-def]
            _ = fn, tracked
            raise AssertionError("feedback worker should not be submitted for closed PRs")

    orch._enqueue_feedback_work(FakePool())

    assert state.list_tracked_pull_requests(repo_full_name=cfg.repo.full_name) == ()
    pr_status = state.get_pull_request_status(101, repo_full_name=cfg.repo.full_name)
    assert pr_status is not None
    assert pr_status.status == "closed"
    outcomes = oq.load_terminal_issue_outcomes(db_path, cfg.repo.full_name, limit=10)
    assert len(outcomes) == 1
    assert outcomes[0].issue_number == issue.number
    assert outcomes[0].pr_number == 101
    assert outcomes[0].status == "closed"


def test_enqueue_feedback_work_handles_duplicate_and_capacity(tmp_path: Path) -> None:
    cfg = _config(tmp_path, worker_count=3)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    state.tracked = [
        TrackedPullRequestState(
            pr_number=101,
            issue_number=7,
            branch="agent/design/7",
            status="awaiting_feedback",
            last_seen_head_sha=None,
        ),
        TrackedPullRequestState(
            pr_number=102,
            issue_number=8,
            branch="agent/design/8",
            status="awaiting_feedback",
            last_seen_head_sha=None,
        ),
    ]
    github.get_pull_request = lambda pr_number: PullRequestSnapshot(  # type: ignore[method-assign]
        number=pr_number,
        title=f"PR {pr_number}",
        body="Body",
        head_sha=f"head-{pr_number}",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    class FakePool:
        def __init__(self) -> None:
            self.submitted: list[int] = []

        def submit(self, fn, tracked):  # type: ignore[no-untyped-def]
            _ = fn
            fut: Future[str] = Future()
            self.submitted.append(tracked.pr_number)
            return fut

    pool = FakePool()
    orch._enqueue_feedback_work(pool)
    assert pool.submitted == [101, 102]

    # Duplicate entry should be skipped when already running.
    duplicate_future: Future[str] = Future()
    orch._running_feedback = {
        101: _FeedbackFuture(issue_number=7, run_id="run-duplicate", future=duplicate_future)
    }
    pool.submitted.clear()
    state.tracked = [state.tracked[0]]
    orch._enqueue_feedback_work(pool)
    assert pool.submitted == []

    # Capacity guard should short-circuit before submitting.
    full_future: Future[WorkResult] = Future()
    orch._running = {1: full_future, 2: full_future, 3: full_future}
    pool.submitted.clear()
    orch._enqueue_feedback_work(pool)
    assert pool.submitted == []

    # Cross-process contention path: claim is lost after candidate listing.
    orch._running = {}
    orch._running_feedback = {}
    pool.submitted.clear()
    state.tracked = [state.tracked[0]]
    state.claim_blocked_feedback_pr_numbers = {101}
    starts_before = len(state.run_starts)
    orch._enqueue_feedback_work(pool)
    assert pool.submitted == []
    assert len(state.run_starts) == starts_before


def test_reap_finished_marks_feedback_failure_blocked(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    bad_feedback: Future[str] = Future()
    bad_feedback.set_exception(RuntimeError("feedback boom"))
    orch._running_feedback = {
        101: _FeedbackFuture(issue_number=7, run_id="run-bad", future=bad_feedback)
    }

    orch._reap_finished()
    assert state.status_updates == [(101, 7, "blocked", None, "feedback boom")]
    assert len(github.comments) == 1
    assert "unexpected error occurred" in github.comments[0][1].lower()
    assert state.released_feedback_claims == [(101, "run-bad")]


def test_reap_finished_blocks_with_concise_transient_git_retry_message(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    transient_failure: Future[str] = Future()
    transient_failure.set_exception(
        FeedbackTransientGitError(
            "Unable to reach GitHub during feedback operations after 4 attempts. "
            "Last error: ERROR: no healthy upstream"
        )
    )
    orch._running_feedback = {
        101: _FeedbackFuture(issue_number=7, run_id="run-transient", future=transient_failure)
    }

    orch._reap_finished()
    assert state.status_updates == [
        (
            101,
            7,
            "blocked",
            None,
            "Unable to reach GitHub during feedback operations after 4 attempts. "
            "Last error: ERROR: no healthy upstream",
        )
    ]
    assert state.run_finishes == [
        (
            "run-transient",
            "failed",
            "github_error",
            "Unable to reach GitHub during feedback operations after 4 attempts. "
            "Last error: ERROR: no healthy upstream",
        )
    ]
    assert len(github.comments) == 1
    assert (
        "unable to reach github during feedback operations after 4 attempts"
        in github.comments[0][1].lower()
    )
    assert state.released_feedback_claims == [(101, "run-transient")]


def test_mark_feedback_blocked_survives_comment_post_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([_issue()])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def raise_post_failure(issue_number: int, body: str) -> None:
        _ = issue_number, body
        raise RuntimeError("post failed")

    github.post_issue_comment = raise_post_failure  # type: ignore[method-assign]

    orch._mark_feedback_blocked(
        pr_number=101,
        issue_number=7,
        reason="test_block_reason",
        error="blocked for test",
        comment_body="test block message",
    )

    assert state.status_updates == [(101, 7, "blocked", None, "blocked for test")]


def test_reap_finished_keeps_feedback_tracked_on_github_polling_error(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    polling_failure: Future[str] = Future()
    polling_failure.set_exception(GitHubPollingError("GitHub polling GET failed for path /x: boom"))
    orch._running_feedback = {
        101: _FeedbackFuture(issue_number=7, run_id="run-poll", future=polling_failure)
    }

    orch._reap_finished()
    assert state.status_updates == [(101, 7, "awaiting_feedback", None, None)]
    assert state.run_finishes == [
        (
            "run-poll",
            "failed",
            "github_error",
            "GitHub polling GET failed for path /x: boom",
        )
    ]
    assert state.released_feedback_claims == [(101, "run-poll")]


def test_reap_finished_marks_feedback_success(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    ok_feedback: Future[str] = Future()
    ok_feedback.set_result("completed")
    orch._running_feedback = {
        101: _FeedbackFuture(issue_number=7, run_id="run-ok", future=ok_feedback)
    }

    orch._reap_finished()
    assert orch._running_feedback == {}
    assert state.status_updates == []
    assert state.released_feedback_claims == [(101, "run-ok")]


def test_feedback_turn_marks_merged_pr_and_stops_tracking(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=True,
    )

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert state.list_tracked_pull_requests() == ()


def test_feedback_turn_skips_when_takeover_ignore_label_is_active(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue(labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment(comment_id=13)]
    github.issue_comments = [_issue_comment(comment_id=23)]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.ingest_feedback_events(
        (
            FeedbackEventRecord(
                event_key="101:review:11:2026-02-21T00:00:00Z",
                pr_number=101,
                issue_number=issue.number,
                kind="review",
                comment_id=11,
                updated_at="2026-02-21T00:00:00Z",
            ),
        ),
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    tracked = _tracked_state_from_store(state)
    result = orch._process_feedback_turn(tracked)
    assert result == "completed"
    assert state.get_issue_takeover_active(issue.number, repo_full_name=cfg.repo.full_name) is True
    assert state.get_pr_takeover_comment_floors(
        pr_number=101,
        repo_full_name=cfg.repo.full_name,
    ) == (13, 23)
    assert state.list_pending_feedback_events(101, repo_full_name=cfg.repo.full_name) == ()


def test_feedback_turn_blocks_when_session_missing(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]
    github.issue_comments = [_issue_comment()]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert state.list_tracked_pull_requests() == ()
    assert any("blocked" in body.lower() for _, body in github.comments)


def test_feedback_turn_commit_no_staged_changes_blocks_and_keeps_event_pending(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=11, body="Done"),),
        general_comment="Updated in latest commit.",
        commit_message="chore: noop",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def raise_no_staged(checkout_path: Path, message: str) -> None:
        _ = checkout_path, message
        raise RuntimeError("No staged changes to commit")

    git.commit_all = raise_no_staged  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()
    assert github.review_replies == []
    assert len(github.comments) == 1
    assert "feedback automation is blocked" in github.comments[0][1].lower()
    assert "no new staged changes or local commits" in github.comments[0][1].lower()

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "no staged changes" in str(row[1]).lower()
    finally:
        conn.close()


def test_feedback_turn_commit_no_staged_changes_allows_agent_local_commit(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=11, body="Done"),),
        general_comment="Updated in latest commit.",
        commit_message="chore: noop",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def raise_no_staged_after_local_commit(checkout_path: Path, message: str) -> None:
        _ = checkout_path, message
        git.current_head_sha_value = "head-2"
        raise RuntimeError("No staged changes to commit")

    git.commit_all = raise_no_staged_after_local_commit  # type: ignore[method-assign]
    git.is_ancestor_results[("head-1", "head-2")] = True

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(state.list_pending_feedback_events(101)) == 0
    assert len(state.list_tracked_pull_requests()) == 1
    assert len(git.push_calls) == 1
    assert len(github.review_replies) == 1
    assert any("updated in latest commit" in body.lower() for _, body in github.comments)
    assert all("feedback automation is blocked" not in body.lower() for _, body in github.comments)


def test_feedback_turn_blocks_on_pre_finalize_remote_history_rewrite(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.compare_statuses[("head-1", "head-rewritten")] = "behind"
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(GitOpRequest(op="fetch_origin"),),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(),
        ),
    ]

    def fetch_and_rewrite(checkout_path: Path) -> None:
        git.fetch_calls.append(checkout_path)
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-rewritten",
            base_sha="base-1",
            draft=False,
            state="open",
            merged=False,
        )

    git.fetch_origin = fetch_and_rewrite  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(git.fetch_calls) == 1
    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()
    assert len(github.comments) == 1
    assert "non-fast-forward" in github.comments[0][1].lower()


def test_feedback_turn_marks_merged_when_pr_merges_before_finalize(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    original_respond = agent.respond_to_feedback

    def respond_and_merge(
        *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-merged",
            base_sha="base-1",
            draft=False,
            state="open",
            merged=True,
        )
        return original_respond(session=session, turn=turn, cwd=cwd)

    agent.respond_to_feedback = respond_and_merge  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert state.list_tracked_pull_requests() == ()
    assert len(state.list_pending_feedback_events(101)) == 1

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "merged"
    finally:
        conn.close()


def test_feedback_turn_marks_closed_when_pr_closes_before_finalize(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    original_respond = agent.respond_to_feedback

    def respond_and_close(
        *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-closed",
            base_sha="base-1",
            draft=False,
            state="closed",
            merged=False,
        )
        return original_respond(session=session, turn=turn, cwd=cwd)

    agent.respond_to_feedback = respond_and_close  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert state.list_tracked_pull_requests() == ()
    assert len(state.list_pending_feedback_events(101)) == 1

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "closed"
    finally:
        conn.close()


def test_feedback_turn_commit_unexpected_error_propagates(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(),
        general_comment=None,
        commit_message="chore: broken",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def raise_other(checkout_path: Path, message: str) -> None:
        _ = checkout_path, message
        raise RuntimeError("boom")

    git.commit_all = raise_other  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    with pytest.raises(RuntimeError, match="boom"):
        orch._process_feedback_turn(tracked)


def test_feedback_turn_commit_push_marks_closed(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(),
        general_comment=None,
        commit_message="feat: update",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    original_push = git.push_branch

    def push_and_close(checkout_path: Path, branch: str) -> None:
        original_push(checkout_path, branch)
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-2",
            base_sha="base-1",
            draft=False,
            state="closed",
            merged=False,
        )

    git.push_branch = push_and_close  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert state.list_tracked_pull_requests() == ()


def test_feedback_turn_commit_push_marks_merged(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(),
        general_comment=None,
        commit_message="feat: update",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    original_push = git.push_branch

    def push_and_merge(checkout_path: Path, branch: str) -> None:
        original_push(checkout_path, branch)
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-2",
            base_sha="base-1",
            draft=False,
            state="open",
            merged=True,
        )

    git.push_branch = push_and_merge  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert state.list_tracked_pull_requests() == ()


def test_feedback_turn_retries_required_tests_failure_before_push(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: first pass",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(ReviewReply(review_comment_id=11, body="Fixed required tests"),),
            general_comment="Required tests now pass.",
            commit_message="feat: repair required tests",
            git_ops=(),
        ),
    ]

    run_calls = 0

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        nonlocal run_calls
        _ = cwd, input_text, check
        run_calls += 1
        assert cmd[-1].endswith("scripts/required-tests.sh")
        if run_calls == 1:
            raise CommandError(
                "Command failed\n"
                "cmd: /tmp/required-tests.sh\n"
                "exit: 1\n"
                "stdout:\nfirst stdout\n"
                "stderr:\nfirst stderr\n"
            )
        return ""

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 2
    second_turn = agent.feedback_calls[1][1]
    assert any(
        "required pre-push test failed" in comment.body.lower()
        for comment in second_turn.issue_comments
    )
    assert len(git.commit_calls) == 2
    assert len(git.push_calls) == 1
    assert run_calls == 2


def test_feedback_turn_retries_push_merge_conflict_before_push(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: first pass",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(ReviewReply(review_comment_id=11, body="Resolved merge conflict"),),
            general_comment="Merged remote updates and resolved conflicts.",
            commit_message="feat: resolve merge conflict",
            git_ops=(),
        ),
    ]

    original_push = git.push_branch
    push_attempts = 0

    def push_with_merge_conflict(checkout_path: Path, branch: str) -> None:
        nonlocal push_attempts
        push_attempts += 1
        original_push(checkout_path, branch)
        if push_attempts == 1:
            raise CommandError("Automatic merge failed; fix conflicts and then commit the result")

    git.push_branch = push_with_merge_conflict  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert push_attempts == 2
    assert len(agent.feedback_calls) == 2
    second_turn = agent.feedback_calls[1][1]
    assert any(
        "push-time merge conflict detected" in comment.body.lower()
        for comment in second_turn.issue_comments
    )
    assert len(git.commit_calls) == 2
    assert len(git.push_calls) == 2
    assert state.list_pending_feedback_events(101) == ()


def test_feedback_turn_blocks_when_push_merge_conflict_reported_impossible(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: first pass",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(),
            general_comment="I cannot safely resolve these merge conflicts.",
            commit_message=None,
            git_ops=(),
        ),
    ]

    original_push = git.push_branch
    push_attempts = 0

    def push_with_merge_conflict(checkout_path: Path, branch: str) -> None:
        nonlocal push_attempts
        push_attempts += 1
        original_push(checkout_path, branch)
        raise CommandError("Automatic merge failed; merge conflict in src/app.py")

    git.push_branch = push_with_merge_conflict  # type: ignore[method-assign]

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert push_attempts == 1
    assert len(agent.feedback_calls) == 2
    second_turn = agent.feedback_calls[1][1]
    assert any(
        "push-time merge conflict detected" in comment.body.lower()
        for comment in second_turn.issue_comments
    )
    assert len(git.commit_calls) == 1
    assert len(git.push_calls) == 1
    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()
    assert any(
        "merge conflicts could not be resolved" in body.lower() for _, body in github.comments
    )

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "cannot safely resolve" in str(row[1]).lower()
    finally:
        conn.close()


def test_feedback_turn_blocks_when_required_tests_reported_impossible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: first pass",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(),
            general_comment="Impossible: fix would violate required invariant.",
            commit_message=None,
            git_ops=(),
        ),
    ]

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[-1].endswith("scripts/required-tests.sh")
        raise CommandError(
            "Command failed\ncmd: /tmp/required-tests.sh\nexit: 1\nstdout:\nout\nstderr:\nerr\n"
        )

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 2
    assert git.push_calls == []
    assert any(
        "required pre-push tests could not be satisfied" in body for _, body in github.comments
    )

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "Impossible: fix would violate required invariant." in str(row[1])
    finally:
        conn.close()


def test_feedback_turn_blocks_on_required_tests_repair_local_history_rewrite(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(),
        general_comment=None,
        commit_message="feat: update",
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    is_ancestor_calls = 0

    def is_ancestor_with_repair_rewrite(
        checkout_path: Path, older_sha: str, newer_sha: str
    ) -> bool:
        nonlocal is_ancestor_calls
        _ = checkout_path, older_sha, newer_sha
        is_ancestor_calls += 1
        return is_ancestor_calls == 1

    git.is_ancestor = is_ancestor_with_repair_rewrite  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert is_ancestor_calls == 2
    assert git.commit_calls == []
    assert git.push_calls == []
    assert any("required_tests_repair_local" in body for _, body in github.comments)


def test_feedback_turn_blocks_when_required_tests_repair_limit_exceeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: pass 1",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: pass 2",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-3"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: pass 3",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-4"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: pass 4",
            git_ops=(),
        ),
    ]

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[-1].endswith("scripts/required-tests.sh")
        raise CommandError(
            "Command failed\ncmd: /tmp/required-tests.sh\nexit: 1\nstdout:\nout\nstderr:\nerr\n"
        )

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 4
    assert len(git.commit_calls) == 4
    assert git.push_calls == []
    assert any("kept failing after 3 repair attempts" in body for _, body in github.comments)

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "required pre-push tests failed after automated repair attempts" in str(row[1])
    finally:
        conn.close()


def test_feedback_turn_blocks_when_required_tests_repair_outcome_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, required_tests="scripts/required-tests.sh")
    git = FakeGitManager(tmp_path / "checkouts")
    _write_required_tests_script(git.checkout_root)
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message="feat: initial",
            git_ops=(),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=tuple(GitOpRequest(op="fetch_origin") for _ in range(5)),
        ),
    ]

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        _ = cwd, input_text, check
        assert cmd[-1].endswith("scripts/required-tests.sh")
        raise CommandError(
            "Command failed\ncmd: /tmp/required-tests.sh\nexit: 1\nstdout:\nout\nstderr:\nerr\n"
        )

    monkeypatch.setattr("mergexo.orchestrator.run", fake_run)

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 2
    assert len(git.commit_calls) == 1
    assert git.push_calls == []

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        row = conn.execute(
            "SELECT status, error FROM issue_runs WHERE issue_number = ?", (issue.number,)
        ).fetchone()
        assert row is not None
        assert row[0] == "blocked"
        assert "too many git operations in one round" in str(row[1])
    finally:
        conn.close()


def test_feedback_turn_posts_reply_without_remote_full_token_scan(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(ReviewReply(review_comment_id=11, body="Done"),),
        general_comment=None,
        commit_message=None,
        git_ops=(),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(github.review_replies) == 1
    assert state.list_pending_feedback_events(101) == ()


def test_normalize_filters_tokenized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    marker = "<!-- mergexo-action:" + ("a" * 64) + " -->"
    review = PullRequestReviewComment(
        comment_id=1,
        body=f"done\n\n{marker}",
        path="src/a.py",
        line=1,
        side="RIGHT",
        in_reply_to_id=None,
        user_login="reviewer",
        html_url="u",
        created_at="t1",
        updated_at="t1",
    )
    issue = PullRequestIssueComment(
        comment_id=2,
        body="regular issue feedback",
        user_login="reviewer",
        html_url="u2",
        created_at="t2",
        updated_at="t2",
    )
    tokenized_issue = PullRequestIssueComment(
        comment_id=3,
        body=marker,
        user_login="reviewer",
        html_url="u3",
        created_at="t3",
        updated_at="t3",
    )

    normalized_review = orch._normalize_review_events(
        pr_number=10, issue_number=20, comments=[review]
    )
    normalized_issue = orch._normalize_issue_events(
        pr_number=10, issue_number=20, comments=[issue, tokenized_issue]
    )

    assert normalized_review == []
    assert len(normalized_issue) == 1
    assert normalized_issue[0][0].kind == "issue"


def test_normalize_filters_comments_at_or_below_takeover_comment_floors(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    orch = Phase1Orchestrator(
        cfg, state=FakeState(), github=github, git_manager=git, agent=FakeAgent()
    )

    normalized_review = orch._normalize_review_events(
        pr_number=10,
        issue_number=20,
        comments=[_review_comment(comment_id=4), _review_comment(comment_id=5)],
        takeover_review_floor_comment_id=4,
    )
    normalized_issue = orch._normalize_issue_events(
        pr_number=10,
        issue_number=20,
        comments=[_issue_comment(comment_id=8), _issue_comment(comment_id=9)],
        takeover_issue_floor_comment_id=8,
    )

    assert tuple(event.comment_id for event, _ in normalized_review) == (5,)
    assert tuple(event.comment_id for event, _ in normalized_issue) == (9,)


def test_normalize_review_summary_filters_bot_unauthorized_and_tokenized(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, allowed_users=("issue-author", "reviewer"))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    orch = Phase1Orchestrator(
        cfg, state=FakeState(), github=github, git_manager=git, agent=FakeAgent()
    )
    marker = "<!-- mergexo-action:" + ("a" * 64) + " -->"
    comments = [
        PullRequestIssueComment(
            comment_id=1,
            body="bot summary",
            user_login="mergexo[bot]",
            html_url="https://example/reviews/1",
            created_at="t1",
            updated_at="t1",
        ),
        PullRequestIssueComment(
            comment_id=2,
            body="outsider summary",
            user_login="outsider",
            html_url="https://example/reviews/2",
            created_at="t2",
            updated_at="t2",
        ),
        PullRequestIssueComment(
            comment_id=3,
            body=marker,
            user_login="reviewer",
            html_url="https://example/reviews/3",
            created_at="t3",
            updated_at="t3",
        ),
        PullRequestIssueComment(
            comment_id=4,
            body="valid summary feedback",
            user_login="reviewer",
            html_url="https://example/reviews/4",
            created_at="t4",
            updated_at="t4",
        ),
    ]

    normalized = orch._normalize_review_summary_events(
        pr_number=10,
        issue_number=20,
        comments=comments,
    )

    assert tuple(event.comment_id for event, _ in normalized) == (4,)
    assert normalized[0][0].kind == "review_summary"


def test_fetch_remote_action_tokens_reads_recent_pr_comment_surfaces(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    token_a = "a" * 64
    token_b = "b" * 64
    github.review_comments = [
        PullRequestReviewComment(
            comment_id=1,
            body=f"ok\n\n<!-- mergexo-action:{token_a} -->",
            path="src/a.py",
            line=1,
            side="RIGHT",
            in_reply_to_id=None,
            user_login="reviewer",
            html_url="u",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        )
    ]
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=2,
            body=f"ok\n\n<!-- mergexo-action:{token_b} -->",
            user_login="reviewer",
            html_url="u2",
            created_at="2026-02-21T00:00:01Z",
            updated_at="2026-02-21T00:00:01Z",
        )
    ]

    orch = Phase1Orchestrator(
        cfg,
        state=FakeState(),
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    tokens = orch._fetch_remote_action_tokens(101)
    assert tokens == {token_a, token_b}


def test_timestamp_parse_and_normalize_helpers_cover_invalid_and_naive_inputs() -> None:
    assert _parse_utc_timestamp("   ") is None
    assert _parse_utc_timestamp("not-a-timestamp") is None
    naive = _parse_utc_timestamp("2026-02-24T12:34:56")
    assert naive is not None
    assert naive.isoformat() == "2026-02-24T12:34:56+00:00"
    assert _normalize_timestamp_for_compare("bad-ts") == "bad-ts"


def test_normalize_filters_unauthorized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("issue-author", "reviewer"))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    unauthorized_review = PullRequestReviewComment(
        comment_id=2,
        body="Please change this",
        path="src/a.py",
        line=1,
        side="RIGHT",
        in_reply_to_id=None,
        user_login="outsider",
        html_url="u2",
        created_at="t2",
        updated_at="t2",
    )
    unauthorized_issue = PullRequestIssueComment(
        comment_id=4,
        body="Please update",
        user_login="outsider",
        html_url="u4",
        created_at="t4",
        updated_at="t4",
    )

    normalized_review = orch._normalize_review_events(
        pr_number=10,
        issue_number=20,
        comments=[_review_comment(comment_id=1), unauthorized_review],
    )
    normalized_issue = orch._normalize_issue_events(
        pr_number=10,
        issue_number=20,
        comments=[_issue_comment(comment_id=3), unauthorized_issue],
    )

    assert tuple(event.comment_id for event, _ in normalized_review) == (1,)
    assert tuple(event.comment_id for event, _ in normalized_issue) == (3,)


def test_feedback_turn_processes_only_authorized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("issue-author", "reviewer"))
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [
        _review_comment(comment_id=11),
        PullRequestReviewComment(
            comment_id=12,
            body="Unauthorized review",
            path="src/a.py",
            line=2,
            side="RIGHT",
            in_reply_to_id=None,
            user_login="outsider",
            html_url="https://example/review/12",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        ),
    ]
    github.issue_comments = [
        _issue_comment(comment_id=21),
        PullRequestIssueComment(
            comment_id=22,
            body="Unauthorized issue feedback",
            user_login="outsider",
            html_url="https://example/issue-comment/22",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        ),
    ]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    turn = agent.feedback_calls[0][1]
    assert tuple(comment.comment_id for comment in turn.review_comments) == (11,)
    assert tuple(comment.comment_id for comment in turn.issue_comments) == (21,)
    assert state.list_pending_feedback_events(101) == ()


def test_feedback_turn_ignores_all_unauthorized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("issue-author", "reviewer"))
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [
        PullRequestReviewComment(
            comment_id=12,
            body="Unauthorized review",
            path="src/a.py",
            line=2,
            side="RIGHT",
            in_reply_to_id=None,
            user_login="outsider",
            html_url="https://example/review/12",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        ),
    ]
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=22,
            body="Unauthorized issue feedback",
            user_login="outsider",
            html_url="https://example/issue-comment/22",
            created_at="2026-02-21T00:00:00Z",
            updated_at="2026-02-21T00:00:00Z",
        ),
    ]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert state.list_pending_feedback_events(101) == ()


def test_run_feedback_agent_with_git_ops_round_trip(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.changed_files = ("docs/design/7-add-worker-scheduler.md",)
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(GitOpRequest(op="fetch_origin"),),
        ),
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-2"),
            review_replies=(ReviewReply(review_comment_id=11, body="Updated"),),
            general_comment="Done",
            commit_message="docs: apply feedback",
            git_ops=(),
        ),
    ]

    outcome = orch._run_feedback_agent_with_git_ops(
        tracked=tracked,
        session=AgentSession(adapter="codex", thread_id="thread-0"),
        turn=turn,
        checkout_path=tmp_path,
        pull_request=github.pr_snapshot,
    )

    assert outcome is not None
    result, _ = outcome
    assert result.commit_message == "docs: apply feedback"
    assert git.fetch_calls == [tmp_path]
    assert len(agent.feedback_calls) == 2
    second_turn = agent.feedback_calls[1][1]
    assert len(second_turn.issue_comments) == 1
    assert "fetch_origin: ok (ok)" in second_turn.issue_comments[0].body


def test_run_feedback_agent_with_git_ops_marks_codex_finished_on_agent_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    running_future: Future[str] = Future()
    orch._running_feedback[tracked.pr_number] = _FeedbackFuture(
        issue_number=tracked.issue_number,
        run_id="run-feedback",
        future=running_future,
    )
    orch._initialize_run_meta_cache("run-feedback")

    recorded_meta: list[dict[str, object]] = []

    def fake_update_agent_run_meta(*, run_id: str, meta_json: str) -> bool:
        assert run_id == "run-feedback"
        recorded_meta.append(cast(dict[str, object], json.loads(meta_json)))
        return True

    monkeypatch.setattr(
        orch._state, "update_agent_run_meta", fake_update_agent_run_meta, raising=False
    )

    def raise_feedback_error(
        *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        _ = session, turn, cwd
        raise RuntimeError("feedback agent crashed")

    monkeypatch.setattr(agent, "respond_to_feedback", raise_feedback_error)

    with pytest.raises(RuntimeError, match="feedback agent crashed"):
        orch._run_feedback_agent_with_git_ops(
            tracked=tracked,
            session=AgentSession(adapter="codex", thread_id="thread-0"),
            turn=turn,
            checkout_path=tmp_path,
            pull_request=github.pr_snapshot,
        )

    assert len(recorded_meta) == 2
    assert recorded_meta[0]["codex_active"] is True
    assert recorded_meta[1]["codex_active"] is False
    assert recorded_meta[1]["codex_session_id"] == "thread-0"


def test_run_feedback_agent_with_git_ops_blocks_when_request_batch_too_large(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=tuple(GitOpRequest(op="fetch_origin") for _ in range(5)),
        )
    ]

    outcome = orch._run_feedback_agent_with_git_ops(
        tracked=tracked,
        session=AgentSession(adapter="codex", thread_id="thread-0"),
        turn=turn,
        checkout_path=tmp_path,
        pull_request=github.pr_snapshot,
    )

    assert outcome is None
    assert state.status_updates == [
        (
            101,
            7,
            "blocked",
            "headsha",
            "agent requested too many git operations in one round; max=4",
        )
    ]
    assert len(github.comments) == 1
    assert "too many git operations" in github.comments[0][1].lower()
    assert len(agent.feedback_calls) == 1
    assert git.fetch_calls == []


def test_run_feedback_agent_with_git_ops_blocks_when_round_limit_exceeded(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id=f"thread-{index}"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(GitOpRequest(op="fetch_origin"),),
        )
        for index in range(4)
    ]

    outcome = orch._run_feedback_agent_with_git_ops(
        tracked=tracked,
        session=AgentSession(adapter="codex", thread_id="thread-0"),
        turn=turn,
        checkout_path=tmp_path,
        pull_request=github.pr_snapshot,
    )

    assert outcome is None
    assert len(agent.feedback_calls) == 4
    assert len(git.fetch_calls) == 3
    assert state.status_updates == [
        (
            101,
            7,
            "blocked",
            "headsha",
            "agent exceeded maximum git-op follow-up rounds; max=3",
        )
    ]
    assert len(github.comments) == 1
    assert "follow-up rounds exceeded the safety limit" in github.comments[0][1].lower()


def test_run_feedback_agent_with_git_ops_stops_when_pr_merged_after_git_op(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(GitOpRequest(op="fetch_origin"),),
        )
    ]

    def fetch_and_mark_merged(checkout_path: Path) -> None:
        git.fetch_calls.append(checkout_path)
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-merged",
            base_sha="basesha",
            draft=False,
            state="open",
            merged=True,
        )

    git.fetch_origin = fetch_and_mark_merged  # type: ignore[method-assign]

    outcome = orch._run_feedback_agent_with_git_ops(
        tracked=tracked,
        session=AgentSession(adapter="codex", thread_id="thread-0"),
        turn=turn,
        checkout_path=tmp_path,
        pull_request=github.pr_snapshot,
    )

    assert outcome is None
    assert len(agent.feedback_calls) == 1
    assert state.status_updates == [(101, 7, "merged", "head-merged", None)]


def test_run_feedback_agent_with_git_ops_stops_when_pr_closed_after_git_op(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=issue.number,
        branch="agent/design/7-add-worker-scheduler",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )
    turn = FeedbackTurn(
        turn_key="turn-key",
        issue=issue,
        pull_request=github.pr_snapshot,
        review_comments=(),
        issue_comments=(),
        changed_files=(),
    )
    agent.feedback_results = [
        FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-1"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(GitOpRequest(op="fetch_origin"),),
        )
    ]

    def fetch_and_mark_closed(checkout_path: Path) -> None:
        git.fetch_calls.append(checkout_path)
        github.pr_snapshot = PullRequestSnapshot(
            number=101,
            title="Design PR",
            body="Body",
            head_sha="head-closed",
            base_sha="basesha",
            draft=False,
            state="closed",
            merged=False,
        )

    git.fetch_origin = fetch_and_mark_closed  # type: ignore[method-assign]

    outcome = orch._run_feedback_agent_with_git_ops(
        tracked=tracked,
        session=AgentSession(adapter="codex", thread_id="thread-0"),
        turn=turn,
        checkout_path=tmp_path,
        pull_request=github.pr_snapshot,
    )

    assert outcome is None
    assert len(agent.feedback_calls) == 1
    assert state.status_updates == [(101, 7, "closed", "head-closed", None)]


def test_execute_feedback_git_ops_collects_success_and_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = TrackedPullRequestState(
        pr_number=101,
        issue_number=7,
        branch="agent/design/7",
        status="awaiting_feedback",
        last_seen_head_sha="headsha",
    )

    def failing_fetch(checkout_path: Path) -> None:
        _ = checkout_path
        raise RuntimeError("conflict\nplease resolve manually")

    git.fetch_origin = failing_fetch  # type: ignore[method-assign]

    outcomes = orch._execute_feedback_git_ops(
        tracked=tracked,
        checkout_path=tmp_path,
        requests=(
            GitOpRequest(op="fetch_origin"),
            GitOpRequest(op="merge_origin_default_branch"),
        ),
    )

    assert len(outcomes) == 2
    assert outcomes[0].op == "fetch_origin"
    assert outcomes[0].success is False
    assert outcomes[0].detail == "conflict please resolve manually"
    assert outcomes[1].op == "merge_origin_default_branch"
    assert outcomes[1].success is True
    assert outcomes[1].detail == "ok"
    assert git.merge_calls == [tmp_path]


def test_execute_feedback_git_op_raises_for_unsupported_request(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    bad_request = cast(GitOpRequest, type("BadRequest", (), {"op": "invalid"})())
    with pytest.raises(RuntimeError, match="Unsupported git operation request: invalid"):
        orch._execute_feedback_git_op(checkout_path=tmp_path, request=bad_request)


def test_summarize_git_error_handles_empty_and_long_messages() -> None:
    assert _summarize_git_error("") == "git operation failed"
    summary = _summarize_git_error("x" * 300)
    assert summary.endswith("...")
    assert len(summary) == 240


def test_feedback_turn_returns_when_git_op_loop_blocks(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]

    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-456"),
        review_replies=(),
        general_comment=None,
        commit_message=None,
        git_ops=tuple(GitOpRequest(op="fetch_origin") for _ in range(5)),
    )

    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(state.list_pending_feedback_events(101)) == 1


def _operator_issue_comment(
    *,
    comment_id: int,
    body: str,
    user_login: str,
    updated_at: str,
    issue_number: int,
) -> PullRequestIssueComment:
    return PullRequestIssueComment(
        comment_id=comment_id,
        body=body,
        user_login=user_login,
        html_url=f"https://github.com/johnynek/mergexo/issues/{issue_number}#issuecomment-{comment_id}",
        created_at=updated_at,
        updated_at=updated_at,
    )


class OperatorGitHub:
    def __init__(
        self,
        threads: dict[int, list[PullRequestIssueComment]],
        *,
        issues: dict[int, Issue] | None = None,
    ) -> None:
        self._threads = {issue_number: list(items) for issue_number, items in threads.items()}
        self._issues = dict(issues or {})
        self.posted_comments: list[tuple[int, str]] = []
        self._next_comment_id = 5000

    def list_issue_comments(
        self, issue_number: int, *, since: str | None = None
    ) -> list[PullRequestIssueComment]:
        _ = since
        return list(self._threads.get(issue_number, []))

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        self.posted_comments.append((issue_number, body))
        self._next_comment_id += 1
        self._threads.setdefault(issue_number, []).append(
            PullRequestIssueComment(
                comment_id=self._next_comment_id,
                body=body,
                user_login="mergexo[bot]",
                html_url=f"https://example/issues/{issue_number}#issuecomment-{self._next_comment_id}",
                created_at="now",
                updated_at="now",
            )
        )

    def get_issue(self, issue_number: int) -> Issue:
        issue = self._issues.get(issue_number)
        if issue is not None:
            return issue
        return Issue(
            number=issue_number,
            title=f"Issue {issue_number}",
            body="Body",
            html_url=f"https://example/issues/{issue_number}",
            labels=(),
            author_login="issue-author",
        )


def test_scan_operator_commands_returns_when_no_issue_numbers(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_github_operations=True, operator_logins=("alice",))
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub({})
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._scan_operator_commands()
    assert github.posted_comments == []


def test_scan_operator_commands_skips_blocked_pr_threads_during_takeover(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_github_operations=True, operator_logins=("alice",))
    git = FakeGitManager(tmp_path / "checkouts")
    source_issue = _issue(number=7, labels=("agent:design", "agent:ignore"))
    github = FakeGitHub([source_issue])
    github.issue_comments = [
        _operator_issue_comment(
            comment_id=10,
            body="/mergexo help",
            user_login="alice",
            updated_at="2026-02-22T12:00:00Z",
            issue_number=101,
        )
    ]
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        7, "agent/design/7-worker", 101, "https://example/pr/101", repo_full_name=cfg.repo.full_name
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-1",
        error="blocked",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    orch._scan_operator_commands()

    assert state.get_operator_command("101:10:2026-02-22T12:00:00Z") is None
    assert github.comments == []


def test_scan_operator_commands_ingests_action_token_observations(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        operations_issue_number=77,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    token = "b" * 64
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=10,
                    body=f"status marker\n\n<!-- mergexo-action:{token} -->",
                    user_login="alice",
                    updated_at="2026-02-22T12:00:00Z",
                    issue_number=77,
                )
            ]
        }
    )
    state = StateStore(tmp_path / "state.db")
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )

    orch._scan_operator_commands()
    observed = state.get_action_token(token, repo_full_name=cfg.repo.full_name)
    assert observed is not None
    assert observed.status == "observed"
    assert github.posted_comments == []


def test_operator_scan_incremental_bootstrap_skips_history(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        enable_incremental_comment_fetch=True,
        operator_logins=("alice",),
        operations_issue_number=77,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=10,
                    body="/mergexo help",
                    user_login="alice",
                    updated_at="2026-02-22T12:00:00Z",
                    issue_number=77,
                )
            ]
        }
    )

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    orch._scan_operator_commands()
    assert state.get_operator_command("77:10:2026-02-22T12:00:00Z") is None
    assert github.posted_comments == []

    github._threads[77].append(
        _operator_issue_comment(
            comment_id=11,
            body="/mergexo help",
            user_login="alice",
            updated_at="2026-02-22T12:01:00Z",
            issue_number=77,
        )
    )
    orch._scan_operator_commands()
    assert state.get_operator_command("77:11:2026-02-22T12:01:00Z") is not None
    assert len(github.posted_comments) == 1


def test_operator_unblock_command_resets_blocked_pr_and_is_idempotent(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        7, "agent/design/7-worker", 101, "https://example/pr/101", repo_full_name=cfg.repo.full_name
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-old",
        error="blocked for test",
        repo_full_name=cfg.repo.full_name,
    )
    github = OperatorGitHub(
        {
            101: [
                _operator_issue_comment(
                    comment_id=10,
                    body="/mergexo unblock head_sha=ABC1234",
                    user_login="alice",
                    updated_at="2026-02-22T12:00:00Z",
                    issue_number=101,
                )
            ]
        }
    )

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    orch._scan_operator_commands()
    assert state.list_blocked_pull_requests() == ()
    tracked = state.list_tracked_pull_requests()
    assert len(tracked) == 1
    assert tracked[0].last_seen_head_sha == "abc1234"
    assert len(github.posted_comments) == 1
    assert github.posted_comments[0][0] == 101
    assert "status: `applied`" in github.posted_comments[0][1]
    command = state.get_operator_command("101:10:2026-02-22T12:00:00Z")
    assert command is not None
    assert command.pr_number == 101

    orch._scan_operator_commands()
    assert len(github.posted_comments) == 1


def test_operator_unblock_without_pr_fails_on_operations_issue(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        operations_issue_number=77,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        7, "agent/design/7-worker", 101, "https://example/pr/101", repo_full_name=cfg.repo.full_name
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-old",
        error="blocked for test",
        repo_full_name=cfg.repo.full_name,
    )
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=18,
                    body="/mergexo unblock",
                    user_login="alice",
                    updated_at="2026-02-22T12:08:00Z",
                    issue_number=77,
                )
            ]
        }
    )

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    orch._scan_operator_commands()

    still_blocked = state.list_blocked_pull_requests()
    assert len(still_blocked) == 1
    assert still_blocked[0].pr_number == 101
    record = state.get_operator_command("77:18:2026-02-22T12:08:00Z")
    assert record is not None
    assert record.status == "failed"
    assert record.pr_number is None
    assert "No target PR was provided" in record.result
    assert len(github.posted_comments) == 1
    assert github.posted_comments[0][0] == 77
    assert "status: `failed`" in github.posted_comments[0][1]
    assert "unblock pr=<number>" in github.posted_comments[0][1]


def test_operator_commands_reject_unauthorized_and_parse_failures(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        operations_issue_number=77,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=11,
                    body="/mergexo unblock",
                    user_login="mallory",
                    updated_at="2026-02-22T12:01:00Z",
                    issue_number=77,
                ),
                _operator_issue_comment(
                    comment_id=12,
                    body="/mergexo restart mode=rolling",
                    user_login="alice",
                    updated_at="2026-02-22T12:02:00Z",
                    issue_number=77,
                ),
            ]
        }
    )

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    orch._scan_operator_commands()

    rejected_key = "77:11:2026-02-22T12:01:00Z"
    rejected = state.get_operator_command(rejected_key)
    assert rejected is not None
    assert rejected.status == "rejected"

    failed_key = "77:12:2026-02-22T12:02:00Z"
    failed = state.get_operator_command(failed_key)
    assert failed is not None
    assert failed.status == "failed"
    assert len(github.posted_comments) == 2
    assert "status: `rejected`" in github.posted_comments[0][1]
    assert "status: `failed`" in github.posted_comments[1][1]
    assert "README.md#github-operator-commands" in github.posted_comments[1][1]


def test_operator_restart_request_drains_and_raises_signal(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice", "carol"),
        operations_issue_number=77,
        restart_supported_modes=("git_checkout", "pypi"),
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=13,
                    body="/mergexo restart mode=git_checkout",
                    user_login="alice",
                    updated_at="2026-02-22T12:03:00Z",
                    issue_number=77,
                ),
                _operator_issue_comment(
                    comment_id=14,
                    body="/mergexo restart",
                    user_login="carol",
                    updated_at="2026-02-22T12:04:00Z",
                    issue_number=77,
                ),
            ]
        }
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=True,
    )

    orch._scan_operator_commands()
    op = state.get_runtime_operation("restart")
    assert op is not None
    assert op.status == "pending"
    assert op.request_command_key == "77:13:2026-02-22T12:03:00Z"
    assert op.mode == "git_checkout"

    collapsed = state.get_operator_command("77:14:2026-02-22T12:04:00Z")
    assert collapsed is not None
    assert collapsed.status == "applied"
    assert len(github.posted_comments) == 1
    assert "collapsed" in github.posted_comments[0][1]

    with pytest.raises(RestartRequested, match="mode=git_checkout"):
        orch._drain_for_pending_restart_if_needed()
    running = state.get_runtime_operation("restart")
    assert running is not None
    assert running.status == "running"


def test_restart_drain_timeout_marks_operation_failed(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        operations_issue_number=77,
        restart_drain_timeout_seconds=1,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    command_key = "77:15:2026-02-22T12:05:00Z"
    state.record_operator_command(
        command_key=command_key,
        issue_number=77,
        pr_number=None,
        comment_id=15,
        author_login="alice",
        command="restart",
        args_json=json.dumps(
            {
                "normalized_command": "/mergexo restart",
                "args": {},
                "comment_url": "https://example/command/15",
            }
        ),
        status="applied",
        result="pending",
    )
    state.request_runtime_restart(
        requested_by="alice",
        request_command_key=command_key,
        mode="git_checkout",
    )
    github = OperatorGitHub({77: []})
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=True,
    )
    pending: Future[WorkResult] = Future()
    orch._running = {1: pending}
    orch._restart_drain_started_at_monotonic = time.monotonic() - 2.0

    assert orch._drain_for_pending_restart_if_needed() is False
    op = state.get_runtime_operation("restart")
    assert op is not None
    assert op.status == "failed"
    command = state.get_operator_command(command_key)
    assert command is not None
    assert command.status == "failed"
    assert len(github.posted_comments) == 1
    assert "timed out" in github.posted_comments[0][1]


def test_apply_unblock_command_fails_when_target_not_blocked(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_github_operations=True, operator_logins=("alice",))
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub({})
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    parsed = parse_operator_command("/mergexo unblock pr=999")
    assert isinstance(parsed, ParsedOperatorCommand)
    status, detail, pr_number = orch._apply_unblock_command(
        source_issue_number=77,
        source_pr_number=None,
        parsed=parsed,
    )
    assert status == "failed"
    assert pr_number == 999
    assert "not currently blocked" in detail


def test_operator_scan_skips_non_commands_bots_and_reconciles_existing(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        7, "agent/design/7-worker", 101, "https://example/pr/101", repo_full_name=cfg.repo.full_name
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-1",
        error="blocked",
        repo_full_name=cfg.repo.full_name,
    )
    existing_key = "101:30:2026-02-22T12:10:00Z"
    state.record_operator_command(
        command_key=existing_key,
        issue_number=101,
        pr_number=None,
        comment_id=30,
        author_login="alice",
        command="help",
        args_json=json.dumps(
            {
                "normalized_command": "/mergexo help",
                "args": {},
                "comment_url": "https://example/issues/101#issuecomment-30",
            }
        ),
        status="applied",
        result="help text",
    )
    github = OperatorGitHub(
        {
            101: [
                _operator_issue_comment(
                    comment_id=28,
                    body="regular discussion",
                    user_login="alice",
                    updated_at="2026-02-22T12:08:00Z",
                    issue_number=101,
                ),
                _operator_issue_comment(
                    comment_id=29,
                    body="/mergexo help",
                    user_login="mergexo[bot]",
                    updated_at="2026-02-22T12:09:00Z",
                    issue_number=101,
                ),
                _operator_issue_comment(
                    comment_id=30,
                    body="/mergexo help",
                    user_login="alice",
                    updated_at="2026-02-22T12:10:00Z",
                    issue_number=101,
                ),
            ]
        }
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
    )
    orch._scan_operator_commands()
    assert len(github.posted_comments) == 1
    assert "status: `help`" in github.posted_comments[0][1]
    assert state.get_operator_command("101:29:2026-02-22T12:09:00Z") is None
    orch._scan_operator_commands()
    assert len(github.posted_comments) == 1


def test_operator_help_and_restart_failure_paths(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        operations_issue_number=77,
        restart_supported_modes=("git_checkout",),
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = OperatorGitHub(
        {
            77: [
                _operator_issue_comment(
                    comment_id=31,
                    body="/mergexo help",
                    user_login="alice",
                    updated_at="2026-02-22T12:11:00Z",
                    issue_number=77,
                ),
                _operator_issue_comment(
                    comment_id=32,
                    body="/mergexo restart mode=pypi",
                    user_login="alice",
                    updated_at="2026-02-22T12:12:00Z",
                    issue_number=77,
                ),
                _operator_issue_comment(
                    comment_id=33,
                    body="/mergexo restart",
                    user_login="alice",
                    updated_at="2026-02-22T12:13:00Z",
                    issue_number=77,
                ),
            ]
        }
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=False,
    )
    orch._scan_operator_commands()

    help_record = state.get_operator_command("77:31:2026-02-22T12:11:00Z")
    assert help_record is not None
    assert help_record.command == "help"
    assert help_record.status == "applied"

    unsupported = state.get_operator_command("77:32:2026-02-22T12:12:00Z")
    assert unsupported is not None
    assert unsupported.status == "failed"
    assert "not enabled" in unsupported.result

    service_required = state.get_operator_command("77:33:2026-02-22T12:13:00Z")
    assert service_required is not None
    assert service_required.status == "failed"
    assert "mergexo service" in service_required.result
    assert len(github.posted_comments) == 3


def test_operator_reconcile_restart_skips_pending_then_posts_terminal(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_github_operations=True, operator_logins=("alice",))
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    key = "77:34:2026-02-22T12:14:00Z"
    command = state.record_operator_command(
        command_key=key,
        issue_number=77,
        pr_number=None,
        comment_id=34,
        author_login="alice",
        command="restart",
        args_json=json.dumps(
            {
                "normalized_command": "/mergexo restart",
                "args": {},
                "comment_url": "https://example/issues/77#issuecomment-34",
            }
        ),
        status="applied",
        result="pending",
    )
    state.request_runtime_restart(
        requested_by="alice", request_command_key=key, mode="git_checkout"
    )
    github = OperatorGitHub({77: []})
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=True,
    )
    orch._reconcile_operator_command(command)
    assert github.posted_comments == []

    state.set_runtime_operation_status(op_name="restart", status="failed", detail="failed")
    updated = state.update_operator_command_result(
        command_key=key, status="failed", result="failed"
    )
    assert updated is not None
    orch._reconcile_operator_command(updated)
    assert len(github.posted_comments) == 1
    assert "status: `failed`" in github.posted_comments[0][1]


def test_restart_drain_service_mode_required_and_in_progress(tmp_path: Path) -> None:
    cfg = _config(
        tmp_path,
        enable_github_operations=True,
        operator_logins=("alice",),
        restart_drain_timeout_seconds=30,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    key = "77:35:2026-02-22T12:15:00Z"
    state.record_operator_command(
        command_key=key,
        issue_number=77,
        pr_number=None,
        comment_id=35,
        author_login="alice",
        command="restart",
        args_json='{"normalized_command":"/mergexo restart","args":{}}',
        status="applied",
        result="pending",
    )
    state.request_runtime_restart(
        requested_by="alice", request_command_key=key, mode="git_checkout"
    )
    github = OperatorGitHub({77: []})
    orch_no_service = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=False,
    )
    assert orch_no_service._drain_for_pending_restart_if_needed() is False
    op = state.get_runtime_operation("restart")
    assert op is not None
    assert op.status == "failed"

    state.set_runtime_operation_status(op_name="restart", status="pending", detail=None)
    orch_in_progress = Phase1Orchestrator(
        cfg,
        state=state,
        github=cast(GitHubGateway, github),
        git_manager=git,
        agent=FakeAgent(),
        allow_runtime_restart=True,
    )
    pending: Future[WorkResult] = Future()
    orch_in_progress._running = {1: pending}
    orch_in_progress._restart_drain_started_at_monotonic = time.monotonic()
    assert orch_in_progress._is_authorized_operator("") is False
    assert orch_in_progress._is_authorized_operator("alice") is True
    assert orch_in_progress._drain_for_pending_restart_if_needed() is True


def test_operator_helper_functions_cover_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    command = OperatorCommandRecord(
        command_key="k",
        issue_number=77,
        pr_number=101,
        comment_id=9,
        author_login="alice",
        command="unblock",
        args_json="{bad-json",
        status="applied",
        result="ok",
        created_at="t1",
        updated_at="t2",
    )
    assert _operator_args_payload(command) == {}
    assert _operator_normalized_command(command) == "/mergexo unblock"
    assert (
        _operator_source_comment_url(command=command, repo_full_name="johnynek/mergexo")
        == "https://github.com/johnynek/mergexo/issues/77#issuecomment-9"
    )
    assert _operator_reply_issue_number(command) == 101

    as_list = OperatorCommandRecord(
        command_key="k2",
        issue_number=77,
        pr_number=None,
        comment_id=10,
        author_login="alice",
        command="help",
        args_json="[]",
        status="applied",
        result="ok",
        created_at="t1",
        updated_at="t2",
    )
    assert _operator_args_payload(as_list) == {}
    assert _operator_reply_issue_number(as_list) == 77

    original_json_loads = json.loads
    monkeypatch.setattr("mergexo.orchestrator.json.loads", lambda _: {1: "x"})
    assert _operator_args_payload(as_list) == {}
    monkeypatch.setattr("mergexo.orchestrator.json.loads", original_json_loads)

    payload_command = OperatorCommandRecord(
        command_key="k3",
        issue_number=77,
        pr_number=None,
        comment_id=11,
        author_login="alice",
        command="restart",
        args_json='{"normalized_command":" /mergexo restart ","comment_url":" https://x "}',
        status="rejected",
        result="no",
        created_at="t1",
        updated_at="t2",
    )
    assert _operator_normalized_command(payload_command) == "/mergexo restart"
    assert (
        _operator_source_comment_url(command=payload_command, repo_full_name="r/n") == "https://x"
    )
    assert _operator_reply_status_for_record(payload_command) == "rejected"
    help_record = OperatorCommandRecord(
        command_key="k4",
        issue_number=77,
        pr_number=None,
        comment_id=12,
        author_login="alice",
        command="help",
        args_json="{}",
        status="applied",
        result="ok",
        created_at="t1",
        updated_at="t2",
    )
    failed_record = OperatorCommandRecord(
        command_key="k5",
        issue_number=77,
        pr_number=None,
        comment_id=13,
        author_login="alice",
        command="restart",
        args_json="{}",
        status="failed",
        result="bad",
        created_at="t1",
        updated_at="t2",
    )
    applied_record = OperatorCommandRecord(
        command_key="k6",
        issue_number=77,
        pr_number=None,
        comment_id=14,
        author_login="alice",
        command="restart",
        args_json="{}",
        status="applied",
        result="good",
        created_at="t1",
        updated_at="t2",
    )
    assert _operator_reply_status_for_record(help_record) == "help"
    assert _operator_reply_status_for_record(failed_record) == "failed"
    assert _operator_reply_status_for_record(applied_record) == "applied"
    rendered = _render_operator_command_result(
        normalized_command="/mergexo restart",
        status="applied",
        detail="ok",
        source_comment_url="https://x",
    )
    assert "Source command: https://x" in rendered


def test_process_issue_roadmap_flow_creates_graph_and_pr(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    issue = _issue(7, "Roadmap Plan", labels=(cfg.repo.roadmap_label,))
    github = FakeGitHub([issue])
    git = FakeGitManager(tmp_path / "checkouts")
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    result = orch._process_issue(issue, "roadmap")

    assert result.branch == "agent/roadmap/7-roadmap-plan"
    assert len(agent.roadmap_calls) == 1
    assert len(github.created_prs) == 1
    pr_title, _head, _base, pr_body = github.created_prs[0]
    assert pr_title.startswith("Roadmap for #7:")
    assert "Refs #7" in pr_body
    graph_path = tmp_path / "checkouts" / "worker-0" / "docs/roadmap/7-roadmap-plan.graph.json"
    assert graph_path.exists()
    assert '"roadmap_issue_number": 7' in graph_path.read_text(encoding="utf-8")


def test_activate_and_advance_roadmap_nodes_creates_children_in_dependency_order(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    roadmap_issue = _issue(50, "Main Plan", labels=(cfg.repo.roadmap_label,))
    github = FakeGitHub([roadmap_issue])
    git = FakeGitManager(tmp_path / "checkouts")
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    repo = cfg.repo.full_name
    store_checkout = git.ensure_checkout(0)
    docs_dir = store_checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "50-main-plan.md").write_text("# Plan", encoding="utf-8")
    (docs_dir / "50-main-plan.graph.json").write_text(
        json.dumps(
            {
                "roadmap_issue_number": 50,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "design_doc",
                        "title": "Design",
                        "body_markdown": "Write design",
                        "depends_on": [],
                    },
                    {
                        "node_id": "n2",
                        "kind": "small_job",
                        "title": "Implement",
                        "body_markdown": "Ship code",
                        "depends_on": [{"node_id": "n1", "requires": "planned"}],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    state.mark_completed(
        50,
        "agent/roadmap/50-main-plan",
        500,
        "https://example/pr/500",
        repo_full_name=repo,
    )
    state.mark_pr_status(pr_number=500, issue_number=50, status="merged", repo_full_name=repo)

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._activate_merged_roadmaps()
    activated = state.get_roadmap_state(roadmap_issue_number=50, repo_full_name=repo)
    assert activated is not None
    assert activated.status == "active"

    orch._advance_roadmap_nodes()
    assert len(github.created_issues) == 1
    _title1, _body1, labels1 = github.created_issues[0]
    assert labels1 == (cfg.repo.trigger_label,)
    child_design = github.issues[-1]
    state.mark_completed(
        child_design.number,
        f"agent/design/{child_design.number}-design",
        501,
        "https://example/pr/501",
        repo_full_name=repo,
    )
    state.mark_pr_status(
        pr_number=501,
        issue_number=child_design.number,
        status="merged",
        repo_full_name=repo,
    )

    orch._advance_roadmap_nodes()
    assert len(github.created_issues) == 2
    _title2, _body2, labels2 = github.created_issues[1]
    assert labels2 == (cfg.repo.small_job_label,)
    child_impl = github.issues[-1]
    state.mark_completed(
        child_impl.number,
        f"agent/small/{child_impl.number}-impl",
        502,
        "https://example/pr/502",
        repo_full_name=repo,
    )
    state.mark_pr_status(
        pr_number=502,
        issue_number=child_impl.number,
        status="merged",
        repo_full_name=repo,
    )
    state.mark_completed(
        child_design.number,
        f"agent/impl/{child_design.number}-impl",
        503,
        "https://example/pr/503",
        repo_full_name=repo,
    )
    state.mark_pr_status(
        pr_number=503,
        issue_number=child_design.number,
        status="merged",
        repo_full_name=repo,
    )

    orch._advance_roadmap_nodes()
    assert 50 in github.closed_issue_numbers
    completed = state.get_roadmap_state(roadmap_issue_number=50, repo_full_name=repo)
    assert completed is not None
    assert completed.status == "completed"


def test_advance_roadmap_nodes_completion_finalizes_after_issue_close(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    graph_payload = {
        "roadmap_issue_number": 51,
        "version": 1,
        "nodes": [
            {
                "node_id": "n1",
                "kind": "small_job",
                "title": "Ship",
                "body_markdown": "Do it",
                "depends_on": [],
            }
        ],
    }
    parsed = parse_roadmap_graph_json(json.dumps(graph_payload), expected_issue_number=51)
    state.upsert_roadmap_graph(
        roadmap_issue_number=51,
        roadmap_pr_number=510,
        roadmap_doc_path="docs/roadmap/51-complete.md",
        graph_path="docs/roadmap/51-complete.graph.json",
        graph_checksum=parsed.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=51,
        node_id="n1",
        claim_token=claim.claim_token,
        child_issue_number=5101,
        child_issue_url="https://example/issues/5101",
        repo_full_name=repo,
    )
    state.record_roadmap_node_milestone(
        roadmap_issue_number=51,
        node_id="n1",
        milestone="implemented",
        repo_full_name=repo,
    )

    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    docs_dir = checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "51-complete.graph.json").write_text(parsed.canonical_json, encoding="utf-8")

    class FailingCloseGitHub(FakeGitHub):
        def close_issue(self, issue_number: int) -> None:
            _ = issue_number
            raise RuntimeError("close failed")

    failing_github = FailingCloseGitHub([_issue(51, "Complete", labels=(cfg.repo.roadmap_label,))])
    failing_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=failing_github,
        git_manager=git,
        agent=FakeAgent(),
    )
    with pytest.raises(RuntimeError, match="close failed"):
        failing_orch._advance_roadmap_nodes()

    after_failure = state.get_roadmap_state(roadmap_issue_number=51, repo_full_name=repo)
    assert after_failure is not None
    assert after_failure.status == "active"

    github = FakeGitHub([_issue(51, "Complete", labels=(cfg.repo.roadmap_label,))])
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    orch._advance_roadmap_nodes()

    finalized = state.get_roadmap_state(roadmap_issue_number=51, repo_full_name=repo)
    assert finalized is not None
    assert finalized.status == "completed"
    assert 51 in github.closed_issue_numbers


def test_publish_roadmap_status_reports_and_abandon_flow(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    roadmap_issue = _issue(
        70,
        "Status Plan",
        labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_abandon_label),
    )
    child_issue = _issue(701, "Child", labels=(cfg.repo.small_job_label,))
    github = FakeGitHub([roadmap_issue, child_issue])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=9001,
            body="/roadmap status",
            user_login="reviewer",
            html_url="https://example/issues/70#issuecomment-9001",
            created_at="2026-03-01T00:00:00.000Z",
            updated_at="2026-03-01T00:00:00.000Z",
        )
    ]
    git = FakeGitManager(tmp_path / "checkouts")
    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    repo = cfg.repo.full_name
    state.upsert_roadmap_graph(
        roadmap_issue_number=70,
        roadmap_pr_number=700,
        roadmap_doc_path="docs/roadmap/70-status-plan.md",
        graph_path="docs/roadmap/70-status-plan.graph.json",
        graph_checksum="checksum",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    node_claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=70,
        node_id="n1",
        claim_token=node_claim.claim_token,
        child_issue_number=701,
        child_issue_url="https://example/issues/701",
        repo_full_name=repo,
    )
    state.mark_roadmap_node_blocked(
        roadmap_issue_number=70,
        node_id="n1",
        repo_full_name=repo,
    )

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    orch._publish_roadmap_status_reports()
    assert any(
        issue_number == 70 and "roadmap status report" in body.lower()
        for issue_number, body in github.comments
    )

    orch._advance_roadmap_nodes()
    assert 70 in github.closed_issue_numbers
    assert 701 in github.closed_issue_numbers
    abandoned = state.get_roadmap_state(roadmap_issue_number=70, repo_full_name=repo)
    assert abandoned is not None
    assert abandoned.status == "abandoned"


def test_activate_merged_roadmaps_returns_when_no_candidates(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._activate_merged_roadmaps()


def test_advance_roadmap_nodes_returns_when_no_active_roadmaps(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._advance_roadmap_nodes()


def test_activate_merged_roadmaps_covers_missing_invalid_unreadable_parent_and_recommendation(
    tmp_path: Path,
) -> None:
    cfg0 = _config(tmp_path, enable_roadmaps=True)
    cfg = AppConfig(
        runtime=cfg0.runtime,
        repos=(replace(cfg0.repo, roadmap_recommended_node_count=1),),
        codex=cfg0.codex,
    )
    repo = cfg.repo.full_name
    issues = [
        _issue(70, "Parent Plan", labels=(cfg.repo.roadmap_label,)),
        _issue(80, "Missing Graph", labels=(cfg.repo.roadmap_label,)),
        _issue(81, "Invalid Graph", labels=(cfg.repo.roadmap_label,)),
        Issue(
            number=82,
            title="Superseding Plan",
            body="Supersedes roadmap #70",
            html_url="https://example/issues/82",
            labels=(cfg.repo.roadmap_label,),
            author_login="issue-author",
        ),
        Issue(
            number=83,
            title="Unknown Parent Plan",
            body="Supersedes roadmap #999",
            html_url="https://example/issues/83",
            labels=(cfg.repo.roadmap_label,),
            author_login="issue-author",
        ),
        _issue(84, "Unreadable Graph", labels=(cfg.repo.roadmap_label,)),
    ]
    github = FakeGitHub(issues)
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")

    state.upsert_roadmap_graph(
        roadmap_issue_number=70,
        roadmap_pr_number=700,
        roadmap_doc_path="docs/roadmap/70-parent-plan.md",
        graph_path="docs/roadmap/70-parent-plan.graph.json",
        graph_checksum="sum70",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="p1",
                kind="small_job",
                title="Parent",
                body_markdown="Parent",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )

    for issue_number in (80, 81, 82, 83, 84):
        state.mark_completed(
            issue_number,
            f"agent/roadmap/{issue_number}-plan",
            1000 + issue_number,
            f"https://example/pr/{1000 + issue_number}",
            repo_full_name=repo,
        )
        state.mark_pr_status(
            pr_number=1000 + issue_number,
            issue_number=issue_number,
            status="merged",
            repo_full_name=repo,
        )

    checkout = git.ensure_checkout(0)
    docs_dir = checkout / cfg.repo.roadmap_docs_dir
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "81-invalid-graph.graph.json").write_text("{", encoding="utf-8")
    valid_graph = {
        "roadmap_issue_number": 82,
        "version": 1,
        "nodes": [
            {
                "node_id": "a",
                "kind": "small_job",
                "title": "A",
                "body_markdown": "A",
                "depends_on": [],
            },
            {
                "node_id": "b",
                "kind": "small_job",
                "title": "B",
                "body_markdown": "B",
                "depends_on": [],
            },
        ],
    }
    (docs_dir / "82-superseding-plan.graph.json").write_text(
        json.dumps(valid_graph),
        encoding="utf-8",
    )
    unknown_parent_graph = {
        "roadmap_issue_number": 83,
        "version": 1,
        "nodes": [
            {
                "node_id": "x",
                "kind": "small_job",
                "title": "X",
                "body_markdown": "X",
                "depends_on": [],
            }
        ],
    }
    (docs_dir / "83-unknown-parent-plan.graph.json").write_text(
        json.dumps(unknown_parent_graph),
        encoding="utf-8",
    )
    (docs_dir / "84-unreadable-graph.graph.json").write_bytes(b"\xff")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    orch._activate_merged_roadmaps()

    assert any(
        issue_number == 80 and "missing canonical graph file" in body
        for issue_number, body in github.comments
    )
    assert any(
        issue_number == 81 and "invalid canonical graph" in body
        for issue_number, body in github.comments
    )
    assert any(
        issue_number == 82 and "sizing recommendation" in body.lower()
        for issue_number, body in github.comments
    )
    assert any(
        issue_number == 83 and "supersede linkage warning" in body.lower()
        for issue_number, body in github.comments
    )
    assert any(
        issue_number == 84 and "canonical graph file is unreadable" in body.lower()
        for issue_number, body in github.comments
    )
    parent = state.get_roadmap_state(roadmap_issue_number=70, repo_full_name=repo)
    assert parent is not None
    assert parent.status == "superseded"
    child = state.get_roadmap_state(roadmap_issue_number=82, repo_full_name=repo)
    assert child is not None
    assert child.parent_roadmap_issue_number == 70
    unknown_parent_child = state.get_roadmap_state(roadmap_issue_number=83, repo_full_name=repo)
    assert unknown_parent_child is not None
    assert unknown_parent_child.parent_roadmap_issue_number == 999


def test_advance_roadmap_nodes_reconciles_parent_supersede_after_activation_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    git = FakeGitManager(tmp_path / "checkouts")

    parent_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 111,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "p1",
                        "kind": "small_job",
                        "title": "Parent node",
                        "body_markdown": "Parent node",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=111,
    )
    child_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 112,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "c1",
                        "kind": "small_job",
                        "title": "Child node",
                        "body_markdown": "Child node",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=112,
    )

    state.upsert_roadmap_graph(
        roadmap_issue_number=111,
        roadmap_pr_number=1110,
        roadmap_doc_path="docs/roadmap/111-parent.md",
        graph_path="docs/roadmap/111-parent.graph.json",
        graph_checksum=parent_graph.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="p1",
                kind="small_job",
                title="Parent node",
                body_markdown="Parent node",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.upsert_roadmap_graph(
        roadmap_issue_number=112,
        roadmap_pr_number=1120,
        roadmap_doc_path="docs/roadmap/112-child.md",
        graph_path="docs/roadmap/112-child.graph.json",
        graph_checksum=child_graph.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="c1",
                kind="small_job",
                title="Child node",
                body_markdown="Child node",
                dependencies=(),
            ),
        ),
        parent_roadmap_issue_number=111,
        repo_full_name=repo,
    )

    checkout = git.ensure_checkout(0)
    docs_dir = checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "111-parent.graph.json").write_text(parent_graph.canonical_json, encoding="utf-8")
    (docs_dir / "112-child.graph.json").write_text(child_graph.canonical_json, encoding="utf-8")

    github = FakeGitHub(
        [
            _issue(111, "Parent", labels=(cfg.repo.roadmap_label,)),
            _issue(112, "Child", labels=(cfg.repo.roadmap_label,)),
        ]
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    monkeypatch.setattr(orch._state, "claim_ready_roadmap_nodes", lambda **kwargs: ())
    orch._advance_roadmap_nodes()

    parent = state.get_roadmap_state(roadmap_issue_number=111, repo_full_name=repo)
    assert parent is not None
    assert parent.status == "superseded"
    child = state.get_roadmap_state(roadmap_issue_number=112, repo_full_name=repo)
    assert child is not None
    assert child.status == "active"


def test_advance_roadmap_nodes_handles_missing_roadmap_state_and_claim_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=90,
        roadmap_pr_number=900,
        roadmap_doc_path="docs/roadmap/90.md",
        graph_path="docs/roadmap/90.graph.json",
        graph_checksum="sum90",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([_issue(90, "Plan", labels=(cfg.repo.roadmap_label,))]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    monkeypatch.setattr(orch._state, "get_roadmap_state", lambda **kwargs: None)
    orch._advance_roadmap_nodes()
    node = state.list_roadmap_nodes(roadmap_issue_number=90, repo_full_name=repo)[0]
    assert node.claim_token is None
    assert node.child_issue_number is None


def test_advance_roadmap_nodes_blocks_on_missing_graph_drift_and_releases_claim(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=91,
        roadmap_pr_number=901,
        roadmap_doc_path="docs/roadmap/91.md",
        graph_path="docs/roadmap/91.graph.json",
        graph_checksum="sum91",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    github = FakeGitHub([_issue(91, "Plan", labels=(cfg.repo.roadmap_label,))])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._advance_roadmap_nodes()
    node = state.list_roadmap_nodes(roadmap_issue_number=91, repo_full_name=repo)[0]
    assert node.claim_token is None
    assert node.child_issue_number is None
    assert any(
        issue_number == 91 and "canonical graph file is missing" in body
        for issue_number, body in github.comments
    )


def test_advance_roadmap_nodes_blocks_claims_when_adjustment_gate_returns_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    graph_payload = {
        "roadmap_issue_number": 910,
        "version": 1,
        "nodes": [
            {
                "node_id": "n1",
                "kind": "small_job",
                "title": "Ship",
                "body_markdown": "Do it",
                "depends_on": [],
            }
        ],
    }
    parsed = parse_roadmap_graph_json(json.dumps(graph_payload), expected_issue_number=910)
    state.upsert_roadmap_graph(
        roadmap_issue_number=910,
        roadmap_pr_number=9100,
        roadmap_doc_path="docs/roadmap/910.md",
        graph_path="docs/roadmap/910.graph.json",
        graph_checksum=parsed.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    docs_dir = checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "910.graph.json").write_text(parsed.canonical_json, encoding="utf-8")
    github = FakeGitHub([_issue(910, "Plan", labels=(cfg.repo.roadmap_label,))])
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    monkeypatch.setattr(orch, "_run_roadmap_adjustment_gate", lambda **kwargs: False)

    orch._advance_roadmap_nodes()

    node = state.list_roadmap_nodes(roadmap_issue_number=910, repo_full_name=repo)[0]
    assert node.claim_token is None
    assert node.child_issue_number is None
    assert not github.created_issues


def test_advance_roadmap_nodes_handles_issue_creation_exception_and_mark_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    graph_payload = {
        "roadmap_issue_number": 92,
        "version": 1,
        "nodes": [
            {
                "node_id": "n1",
                "kind": "small_job",
                "title": "Ship",
                "body_markdown": "Do it",
                "depends_on": [],
            }
        ],
    }
    parsed = parse_roadmap_graph_json(json.dumps(graph_payload), expected_issue_number=92)
    state.upsert_roadmap_graph(
        roadmap_issue_number=92,
        roadmap_pr_number=902,
        roadmap_doc_path="docs/roadmap/92.md",
        graph_path="docs/roadmap/92.graph.json",
        graph_checksum=parsed.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    checkout = FakeGitManager(tmp_path / "checkouts")
    docs_dir = checkout.ensure_checkout(0) / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "92.graph.json").write_text(parsed.canonical_json, encoding="utf-8")

    class FailingCreateIssueGitHub(FakeGitHub):
        def create_issue(
            self,
            title: str,
            body: str,
            labels: tuple[str, ...] | None = None,
        ) -> Issue:
            _ = title, body, labels
            raise RuntimeError("boom")

    failing_github = FailingCreateIssueGitHub(
        [_issue(92, "Plan", labels=(cfg.repo.roadmap_label,))]
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=failing_github,
        git_manager=checkout,
        agent=FakeAgent(),
    )
    orch._advance_roadmap_nodes()
    node = state.list_roadmap_nodes(roadmap_issue_number=92, repo_full_name=repo)[0]
    assert node.claim_token is None

    token = compute_roadmap_node_issue_token(roadmap_issue_number=92, node_id="n1")
    existing = Issue(
        number=9201,
        title="Existing child",
        body=f"existing\n<!-- mergexo-action:{token} -->",
        html_url="https://example/issues/9201",
        labels=(cfg.repo.small_job_label,),
        author_login="issue-author",
    )
    github = FakeGitHub([_issue(92, "Plan", labels=(cfg.repo.roadmap_label,)), existing])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=checkout,
        agent=FakeAgent(),
    )
    monkeypatch.setattr(orch._state, "mark_roadmap_node_issue_created", lambda **kwargs: False)
    orch._advance_roadmap_nodes()


def test_verify_roadmap_graph_checksum_invalid_and_mismatch_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    github = FakeGitHub([_issue(93, "Invalid"), _issue(94, "Mismatch")])
    checkout = git.ensure_checkout(0)
    docs_dir = checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)

    state.upsert_roadmap_graph(
        roadmap_issue_number=93,
        roadmap_pr_number=903,
        roadmap_doc_path="docs/roadmap/93.md",
        graph_path="docs/roadmap/93.graph.json",
        graph_checksum="sum93",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    (docs_dir / "93.graph.json").write_text("{", encoding="utf-8")

    valid_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 94,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "Ship",
                        "body_markdown": "Do it",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=94,
    )
    state.upsert_roadmap_graph(
        roadmap_issue_number=94,
        roadmap_pr_number=904,
        roadmap_doc_path="docs/roadmap/94.md",
        graph_path="docs/roadmap/94.graph.json",
        graph_checksum="wrong-sum",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Do it",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    (docs_dir / "94.graph.json").write_text(valid_graph.canonical_json, encoding="utf-8")

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    roadmap_invalid = state.get_roadmap_state(roadmap_issue_number=93, repo_full_name=repo)
    assert roadmap_invalid is not None
    assert (
        orch._verify_roadmap_graph_checksum(roadmap=roadmap_invalid, checkout_path=checkout)
        is False
    )
    roadmap_mismatch = state.get_roadmap_state(roadmap_issue_number=94, repo_full_name=repo)
    assert roadmap_mismatch is not None
    assert (
        orch._verify_roadmap_graph_checksum(roadmap=roadmap_mismatch, checkout_path=checkout)
        is False
    )
    assert any("canonical graph is invalid" in body for _, body in github.comments)
    assert any("checksums diverged" in body for _, body in github.comments)


def test_roadmap_node_milestones_and_sync_blocking_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=95,
        roadmap_pr_number=905,
        roadmap_doc_path="docs/roadmap/95.md",
        graph_path="docs/roadmap/95.graph.json",
        graph_checksum="sum95",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="design",
                kind="design_doc",
                title="Design",
                body_markdown="Doc",
                dependencies=(),
            ),
            RoadmapNodeGraphInput(
                node_id="road",
                kind="roadmap",
                title="Nested",
                body_markdown="Nested",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim_design, claim_road = state.claim_ready_roadmap_nodes(repo_full_name=repo)
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=95,
        node_id=claim_design.node_id,
        claim_token=claim_design.claim_token,
        child_issue_number=951,
        child_issue_url="https://example/issues/951",
        repo_full_name=repo,
    )
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=95,
        node_id=claim_road.node_id,
        claim_token=claim_road.claim_token,
        child_issue_number=952,
        child_issue_url="https://example/issues/952",
        repo_full_name=repo,
    )
    state.mark_failed(951, "waiting", repo_full_name=repo)
    state.mark_failed(952, "waiting", repo_full_name=repo)

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([_issue(95, "Plan", labels=(cfg.repo.roadmap_label,))]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    nodes = state.list_roadmap_nodes(roadmap_issue_number=95, repo_full_name=repo)
    design_node = next(node for node in nodes if node.node_id == "design")
    roadmap_node = next(node for node in nodes if node.node_id == "road")
    assert orch._roadmap_node_milestones(node=roadmap_node) == (False, False, True)

    state.upsert_roadmap_graph(
        roadmap_issue_number=952,
        roadmap_pr_number=9520,
        roadmap_doc_path="docs/roadmap/952.md",
        graph_path="docs/roadmap/952.graph.json",
        graph_checksum="sum952",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=952, repo_full_name=repo)
    assert orch._roadmap_node_milestones(node=roadmap_node) == (True, False, True)
    assert orch._roadmap_node_milestones(
        node=replace(design_node, child_issue_number=None, child_issue_url=None)
    ) == (False, False, False)

    roadmap = state.get_roadmap_state(roadmap_issue_number=95, repo_full_name=repo)
    assert roadmap is not None
    orch._sync_roadmap_node_progress(roadmap=roadmap)
    updated_design = next(
        node
        for node in state.list_roadmap_nodes(roadmap_issue_number=95, repo_full_name=repo)
        if node.node_id == "design"
    )
    assert updated_design.status == "blocked"


def test_handle_roadmap_control_labels_revision_and_abandon_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=96,
        roadmap_pr_number=906,
        roadmap_doc_path="docs/roadmap/96.md",
        graph_path="docs/roadmap/96.graph.json",
        graph_checksum="sum96",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    roadmap_issue = _issue(
        96,
        "Plan",
        labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label),
    )
    completed_child = _issue(9601, "Done child", labels=(cfg.repo.small_job_label,))
    github = FakeGitHub([roadmap_issue, completed_child])
    claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=96,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=9601,
        child_issue_url="https://example/issues/9601",
        repo_full_name=repo,
    )
    state.record_roadmap_node_milestone(
        roadmap_issue_number=96,
        node_id=claim.node_id,
        milestone="implemented",
        repo_full_name=repo,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=96, repo_full_name=repo)
    assert roadmap is not None
    orch._handle_roadmap_control_labels(roadmap=roadmap)
    revised = state.get_roadmap_state(roadmap_issue_number=96, repo_full_name=repo)
    assert revised is not None
    assert revised.status == "active"
    assert revised.adjustment_state == "awaiting_revision_merge"
    assert revised.superseding_roadmap_issue_number is None
    assert not github.created_issues
    assert any(
        "paused roadmap fan-out pending a same-roadmap revision" in body
        for _, body in github.comments
    )

    abandon_cfg = _config(tmp_path / "abandon", enable_roadmaps=True)
    abandon_repo = abandon_cfg.repo.full_name
    abandon_state = StateStore((tmp_path / "abandon") / "state.db")
    abandon_state.upsert_roadmap_graph(
        roadmap_issue_number=97,
        roadmap_pr_number=907,
        roadmap_doc_path="docs/roadmap/97.md",
        graph_path="docs/roadmap/97.graph.json",
        graph_checksum="sum97",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=abandon_repo,
    )
    claim = abandon_state.claim_ready_roadmap_nodes(repo_full_name=abandon_repo)[0]
    abandon_state.mark_roadmap_node_issue_created(
        roadmap_issue_number=97,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=9701,
        child_issue_url="https://example/issues/9701",
        repo_full_name=abandon_repo,
    )
    abandon_state.record_roadmap_node_milestone(
        roadmap_issue_number=97,
        node_id=claim.node_id,
        milestone="implemented",
        repo_full_name=abandon_repo,
    )
    abandon_github = FakeGitHub([_issue(97, "Plan"), _issue(9701, "Child")])
    abandon_orch = Phase1Orchestrator(
        abandon_cfg,
        state=abandon_state,
        github=abandon_github,
        git_manager=FakeGitManager((tmp_path / "abandon") / "checkouts"),
        agent=FakeAgent(),
    )
    abandoned = abandon_state.get_roadmap_state(
        roadmap_issue_number=97, repo_full_name=abandon_repo
    )
    assert abandoned is not None
    abandon_orch._abandon_roadmap(roadmap=abandoned, reason="manual abandon")
    assert 9701 not in abandon_github.closed_issue_numbers
    first_parent_close_count = abandon_github.closed_issue_numbers.count(97)
    abandon_orch._abandon_roadmap(roadmap=abandoned, reason="noop")
    assert abandon_github.closed_issue_numbers.count(97) == first_parent_close_count


def test_handle_roadmap_control_labels_keeps_same_roadmap_revision_request(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=990,
        roadmap_pr_number=9900,
        roadmap_doc_path="docs/roadmap/990.md",
        graph_path="docs/roadmap/990.graph.json",
        graph_checksum="sum990",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=990, repo_full_name=repo)

    github = FakeGitHub(
        [_issue(990, "Plan", labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label))]
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=990, repo_full_name=repo)
    assert roadmap is not None
    assert roadmap.superseding_roadmap_issue_number is None
    orch._handle_roadmap_control_labels(roadmap=roadmap)

    refreshed = state.get_roadmap_state(roadmap_issue_number=990, repo_full_name=repo)
    assert refreshed is not None
    assert refreshed.status == "active"
    assert refreshed.adjustment_state == "awaiting_revision_merge"
    assert refreshed.superseding_roadmap_issue_number is None
    assert not github.created_issues
    assert not github.comments


def test_maybe_apply_pending_roadmap_revision_applies_same_issue_graph_update(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=994,
        roadmap_pr_number=9940,
        roadmap_doc_path="docs/roadmap/994.md",
        graph_path="docs/roadmap/994.graph.json",
        graph_checksum="sum994",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
            RoadmapNodeGraphInput(
                node_id="n2",
                kind="small_job",
                title="Two",
                body_markdown="Two",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=994, repo_full_name=repo)

    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    docs_dir = checkout / "docs/roadmap"
    docs_dir.mkdir(parents=True, exist_ok=True)
    revised_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 994,
                "version": 2,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "One",
                        "body_markdown": "One",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=994,
    )
    (docs_dir / "994.graph.json").write_text(revised_graph.canonical_json, encoding="utf-8")

    github = FakeGitHub(
        [_issue(994, "Plan", labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label))]
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    roadmap = state.get_roadmap_state(roadmap_issue_number=994, repo_full_name=repo)
    assert roadmap is not None

    updated = orch._maybe_apply_pending_roadmap_revision(roadmap=roadmap, checkout_path=checkout)

    assert updated.status == "active"
    assert updated.graph_version == 2
    active_nodes = state.list_roadmap_nodes(roadmap_issue_number=994, repo_full_name=repo)
    assert [node.node_id for node in active_nodes] == ["n1"]
    all_nodes = state.list_roadmap_nodes(
        roadmap_issue_number=994,
        include_retired=True,
        repo_full_name=repo,
    )
    retired = next(node for node in all_nodes if node.node_id == "n2")
    assert retired.is_active is False
    assert retired.retired_in_version == 2
    assert any("applied a same-roadmap revision" in body for _, body in github.comments)


def test_maybe_apply_pending_roadmap_revision_short_circuit_and_waiting_error_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    github = FakeGitHub([])
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())

    active = state.upsert_roadmap_graph(
        roadmap_issue_number=995,
        roadmap_pr_number=9950,
        roadmap_doc_path="docs/roadmap/995.md",
        graph_path="docs/roadmap/995.graph.json",
        graph_checksum="sum995",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    assert (
        orch._maybe_apply_pending_roadmap_revision(roadmap=active, checkout_path=checkout) == active
    )
    assert orch._maybe_apply_pending_roadmap_revision(
        roadmap=replace(active, status="completed", adjustment_state="awaiting_revision_merge"),
        checkout_path=checkout,
    ) == replace(active, status="completed", adjustment_state="awaiting_revision_merge")

    state.upsert_roadmap_graph(
        roadmap_issue_number=996,
        roadmap_pr_number=9960,
        roadmap_doc_path="docs/roadmap/996.md",
        graph_path="docs/roadmap/996.graph.json",
        graph_checksum="sum996",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=996, repo_full_name=repo)
    missing = state.get_roadmap_state(roadmap_issue_number=996, repo_full_name=repo)
    assert missing is not None
    orch._maybe_apply_pending_roadmap_revision(roadmap=missing, checkout_path=checkout)
    assert any("canonical graph file is missing" in body for _, body in github.comments)

    state.upsert_roadmap_graph(
        roadmap_issue_number=997,
        roadmap_pr_number=9970,
        roadmap_doc_path="docs/roadmap/997.md",
        graph_path="docs/roadmap/997.graph.json",
        graph_checksum="sum997",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=997, repo_full_name=repo)
    unreadable_path = _write_checkout_file(git.checkout_root, "docs/roadmap/997.graph.json", slot=0)
    original_read_text = Path.read_text

    def unreadable_read_text(self: Path, *, encoding: str = "utf-8") -> str:
        if self == unreadable_path:
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "boom")
        return original_read_text(self, encoding=encoding)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_text", unreadable_read_text)
        unreadable = state.get_roadmap_state(roadmap_issue_number=997, repo_full_name=repo)
        assert unreadable is not None
        orch._maybe_apply_pending_roadmap_revision(roadmap=unreadable, checkout_path=checkout)
    assert any("canonical graph file is unreadable" in body for _, body in github.comments)

    state.upsert_roadmap_graph(
        roadmap_issue_number=998,
        roadmap_pr_number=9980,
        roadmap_doc_path="docs/roadmap/998.md",
        graph_path="docs/roadmap/998.graph.json",
        graph_checksum="sum998",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=998, repo_full_name=repo)
    _write_checkout_file(git.checkout_root, "docs/roadmap/998.graph.json", content="{", slot=0)
    invalid = state.get_roadmap_state(roadmap_issue_number=998, repo_full_name=repo)
    assert invalid is not None
    orch._maybe_apply_pending_roadmap_revision(roadmap=invalid, checkout_path=checkout)
    assert any(
        "waiting for a valid same-roadmap revision merge" in body for _, body in github.comments
    )

    same_payload = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 999,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "One",
                        "body_markdown": "One",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=999,
    )
    state.upsert_roadmap_graph(
        roadmap_issue_number=999,
        roadmap_pr_number=9990,
        roadmap_doc_path="docs/roadmap/999.md",
        graph_path="docs/roadmap/999.graph.json",
        graph_checksum=same_payload.checksum,
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=999, repo_full_name=repo)
    _write_checkout_file(
        git.checkout_root,
        "docs/roadmap/999.graph.json",
        content=same_payload.canonical_json,
        slot=0,
    )
    same = state.get_roadmap_state(roadmap_issue_number=999, repo_full_name=repo)
    assert same is not None
    comment_count = len(github.comments)
    unchanged = orch._maybe_apply_pending_roadmap_revision(roadmap=same, checkout_path=checkout)
    assert unchanged == same
    assert len(github.comments) == comment_count


def test_maybe_apply_pending_roadmap_revision_rejects_invalid_transition(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=1000,
        roadmap_pr_number=10000,
        roadmap_doc_path="docs/roadmap/1000.md",
        graph_path="docs/roadmap/1000.graph.json",
        graph_checksum="sum1000",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=1000,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=10001,
        child_issue_url="https://example/issues/10001",
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=1000, repo_full_name=repo)

    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    revised_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 1000,
                "version": 2,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "One v2",
                        "body_markdown": "One",
                        "depends_on": [],
                    }
                ],
            }
        ),
        expected_issue_number=1000,
    )
    _write_checkout_file(
        git.checkout_root,
        "docs/roadmap/1000.graph.json",
        content=revised_graph.canonical_json,
        slot=0,
    )

    github = FakeGitHub([])
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    roadmap = state.get_roadmap_state(roadmap_issue_number=1000, repo_full_name=repo)
    assert roadmap is not None

    unchanged = orch._maybe_apply_pending_roadmap_revision(roadmap=roadmap, checkout_path=checkout)

    assert unchanged.status == "active"
    assert unchanged.adjustment_state == "awaiting_revision_merge"
    refreshed = state.get_roadmap_state(roadmap_issue_number=1000, repo_full_name=repo)
    assert refreshed is not None
    assert refreshed.last_error is not None
    assert "cannot change roadmap node title" in refreshed.last_error
    assert any("rejected the merged same-roadmap revision" in body for _, body in github.comments)


def test_run_roadmap_adjustment_gate_revise_and_abandon_paths(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name

    revise_state = StateStore(tmp_path / "revise.db")
    revise_state.upsert_roadmap_graph(
        roadmap_issue_number=1003,
        roadmap_pr_number=10030,
        roadmap_doc_path="docs/roadmap/1003.md",
        graph_path="docs/roadmap/1003.graph.json",
        graph_checksum="sum1003",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    revise_git = FakeGitManager(tmp_path / "checkouts-revise")
    revise_checkout = revise_git.ensure_checkout(0)
    _write_checkout_file(
        revise_git.checkout_root,
        "docs/roadmap/1003.graph.json",
        content='{"roadmap_issue_number":1003,"version":1,"nodes":[]}',
        slot=0,
    )
    revise_agent = FakeAgent()
    revise_agent.roadmap_adjustment_result = RoadmapAdjustmentResult(
        action="revise",
        summary="Need revision",
        details="The frontier should be revised before continuing.",
        updated_roadmap_markdown="# Revised roadmap",
        updated_canonical_graph_json=(
            '{"nodes":[{"body_markdown":"Two","depends_on":[],"kind":"small_job",'
            '"node_id":"n2","title":"Two"}],"roadmap_issue_number":1003,"version":2}'
        ),
    )
    revise_github = FakeGitHub([_issue(1003, "Plan", labels=(cfg.repo.roadmap_label,))])
    revise_orch = Phase1Orchestrator(
        cfg,
        state=revise_state,
        github=revise_github,
        git_manager=revise_git,
        agent=revise_agent,
    )
    revise_roadmap = revise_state.get_roadmap_state(roadmap_issue_number=1003, repo_full_name=repo)
    assert revise_roadmap is not None

    assert (
        revise_orch._run_roadmap_adjustment_gate(
            roadmap=revise_roadmap,
            checkout_path=revise_checkout,
        )
        is False
    )

    revised = revise_state.get_roadmap_state(roadmap_issue_number=1003, repo_full_name=repo)
    assert revised is not None
    assert revised.status == "active"
    assert revised.adjustment_state == "awaiting_revision_merge"
    assert revise_agent.roadmap_adjustment_calls[0][1] == ("n1",)
    assert any(
        "adjustment gate requested a same-roadmap revision" in body
        for _, body in revise_github.comments
    )

    abandon_state = StateStore(tmp_path / "abandon.db")
    abandon_state.upsert_roadmap_graph(
        roadmap_issue_number=1004,
        roadmap_pr_number=10040,
        roadmap_doc_path="docs/roadmap/1004.md",
        graph_path="docs/roadmap/1004.graph.json",
        graph_checksum="sum1004",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    abandon_git = FakeGitManager(tmp_path / "checkouts-abandon")
    abandon_checkout = abandon_git.ensure_checkout(0)
    _write_checkout_file(
        abandon_git.checkout_root,
        "docs/roadmap/1004.graph.json",
        content='{"roadmap_issue_number":1004,"version":1,"nodes":[]}',
        slot=0,
    )
    abandon_agent = FakeAgent()
    abandon_agent.roadmap_adjustment_result = RoadmapAdjustmentResult(
        action="abandon",
        summary="Unworkable",
        details="The roadmap is no longer viable.",
    )
    abandon_github = FakeGitHub([_issue(1004, "Plan", labels=(cfg.repo.roadmap_label,))])
    abandon_orch = Phase1Orchestrator(
        cfg,
        state=abandon_state,
        github=abandon_github,
        git_manager=abandon_git,
        agent=abandon_agent,
    )
    abandon_roadmap = abandon_state.get_roadmap_state(
        roadmap_issue_number=1004, repo_full_name=repo
    )
    assert abandon_roadmap is not None

    assert (
        abandon_orch._run_roadmap_adjustment_gate(
            roadmap=abandon_roadmap,
            checkout_path=abandon_checkout,
        )
        is False
    )

    abandoned = abandon_state.get_roadmap_state(roadmap_issue_number=1004, repo_full_name=repo)
    assert abandoned is not None
    assert abandoned.status == "abandoned"
    assert 1004 in abandon_github.closed_issue_numbers


def test_evaluate_roadmap_frontier_adjustment_collects_dependency_artifacts(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=1008,
        roadmap_pr_number=10080,
        roadmap_doc_path="docs/roadmap/1008.md",
        graph_path="docs/roadmap/1008.graph.json",
        graph_checksum="sum1008",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="dep",
                kind="small_job",
                title="Dependency",
                body_markdown="Dependency body",
                dependencies=(),
            ),
            RoadmapNodeGraphInput(
                node_id="ready",
                kind="small_job",
                title="Ready",
                body_markdown="Ready body",
                dependencies=(RoadmapDependencyState(node_id="dep", requires="implemented"),),
            ),
        ),
        repo_full_name=repo,
    )
    dependency_claim = next(
        claim
        for claim in state.claim_ready_roadmap_nodes(repo_full_name=repo)
        if claim.node_id == "dep"
    )
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=1008,
        node_id="dep",
        claim_token=dependency_claim.claim_token,
        child_issue_number=10081,
        child_issue_url="https://example/issues/10081",
        repo_full_name=repo,
    )
    state.record_roadmap_node_milestone(
        roadmap_issue_number=1008,
        node_id="dep",
        milestone="implemented",
        repo_full_name=repo,
    )
    state.mark_completed(
        10081,
        "agent/impl/10081-dependency",
        20081,
        "https://example/pr/20081",
        repo_full_name=repo,
    )
    state.mark_pr_status(
        pr_number=20081,
        issue_number=10081,
        status="merged",
        repo_full_name=repo,
    )

    git = FakeGitManager(tmp_path / "checkouts")
    checkout = git.ensure_checkout(0)
    roadmap_graph = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 1008,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "dep",
                        "kind": "small_job",
                        "title": "Dependency",
                        "body_markdown": "Dependency body",
                        "depends_on": [],
                    },
                    {
                        "node_id": "ready",
                        "kind": "small_job",
                        "title": "Ready",
                        "body_markdown": "Ready body",
                        "depends_on": [{"node_id": "dep", "requires": "implemented"}],
                    },
                ],
            }
        ),
        expected_issue_number=1008,
    )
    _write_checkout_file(
        git.checkout_root,
        "docs/roadmap/1008.graph.json",
        content=roadmap_graph.canonical_json,
        slot=0,
    )
    _write_checkout_file(
        git.checkout_root,
        "docs/roadmap/1008.md",
        content="# Roadmap\n\nDependency and ready node.",
        slot=0,
    )
    github = FakeGitHub(
        [
            _issue(1008, "Plan", labels=(cfg.repo.roadmap_label,)),
            _issue(10081, "Dependency child", labels=(cfg.repo.small_job_label,)),
        ]
    )
    github.pr_snapshot = PullRequestSnapshot(
        number=20081,
        title="Dependency PR",
        body="Merged dependency work.",
        head_sha="head-20081",
        base_sha="base-20081",
        draft=False,
        state="closed",
        merged=True,
    )
    github.changed_files = ("src/dependency.py", "tests/test_dependency.py")
    github.review_summaries = [
        PullRequestIssueComment(
            comment_id=1,
            body="Looks good after the interface change.",
            user_login="reviewer",
            html_url="https://example/review-summary/1",
            created_at="t1",
            updated_at="t1",
        )
    ]
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=2,
            body="Please account for the new dependency shape.",
            user_login="engineer",
            html_url="https://example/comment/2",
            created_at="t2",
            updated_at="t2",
        ),
        PullRequestIssueComment(
            comment_id=3,
            body="bot noise",
            user_login="mergexo[bot]",
            html_url="https://example/comment/3",
            created_at="t3",
            updated_at="t3",
        ),
    ]
    agent = FakeAgent()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    roadmap = state.get_roadmap_state(roadmap_issue_number=1008, repo_full_name=repo)
    assert roadmap is not None

    result = orch._evaluate_roadmap_frontier_adjustment(
        roadmap=roadmap,
        ready_node_ids=("ready",),
        checkout_path=checkout,
    )

    assert result.action == "proceed"
    artifacts = agent.roadmap_adjustment_calls[0][2]
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact.dependency_node_id == "dep"
    assert artifact.frontier_references[0].ready_node_id == "ready"
    assert artifact.child_issue_number == 10081
    assert artifact.issue_run_status == "merged"
    assert artifact.pr_number == 20081
    assert artifact.pr_title == "Dependency PR"
    assert artifact.changed_files == ("src/dependency.py", "tests/test_dependency.py")
    assert tuple(comment.body for comment in artifact.review_summaries) == (
        "Looks good after the interface change.",
    )
    assert tuple(comment.body for comment in artifact.issue_comments) == (
        "Please account for the new dependency shape.",
    )
    assert "issue_run_status=merged" in artifact.resolution_markers


def test_collect_roadmap_dependency_artifacts_skips_missing_dependency_nodes(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=1009,
        roadmap_pr_number=10090,
        roadmap_doc_path="docs/roadmap/1009.md",
        graph_path="docs/roadmap/1009.graph.json",
        graph_checksum="sum1009",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="ready",
                kind="small_job",
                title="Ready",
                body_markdown="Ready body",
                dependencies=(RoadmapDependencyState(node_id="missing", requires="implemented"),),
            ),
        ),
        repo_full_name=repo,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([_issue(1009, "Plan", labels=(cfg.repo.roadmap_label,))]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )

    artifacts = orch._collect_roadmap_dependency_artifacts(
        roadmap_issue_number=1009,
        ready_node_ids=("ready",),
    )

    assert artifacts == ()


def test_run_roadmap_adjustment_gate_no_frontier_no_claim_and_error_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name

    no_frontier_state = StateStore(tmp_path / "nofrontier.db")
    no_frontier_state.upsert_roadmap_graph(
        roadmap_issue_number=1005,
        roadmap_pr_number=10050,
        roadmap_doc_path="docs/roadmap/1005.md",
        graph_path="docs/roadmap/1005.graph.json",
        graph_checksum="sum1005",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim = no_frontier_state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    no_frontier_state.mark_roadmap_node_issue_created(
        roadmap_issue_number=1005,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=10051,
        child_issue_url="https://example/issues/10051",
        repo_full_name=repo,
    )
    no_frontier_git = FakeGitManager(tmp_path / "checkouts-no-frontier")
    no_frontier_checkout = no_frontier_git.ensure_checkout(0)
    _write_checkout_file(
        no_frontier_git.checkout_root,
        "docs/roadmap/1005.graph.json",
        content='{"roadmap_issue_number":1005,"version":1,"nodes":[]}',
        slot=0,
    )
    no_frontier_agent = FakeAgent()
    no_frontier_github = FakeGitHub([_issue(1005, "Plan", labels=(cfg.repo.roadmap_label,))])
    no_frontier_orch = Phase1Orchestrator(
        cfg,
        state=no_frontier_state,
        github=no_frontier_github,
        git_manager=no_frontier_git,
        agent=no_frontier_agent,
    )
    no_frontier_roadmap = no_frontier_state.get_roadmap_state(
        roadmap_issue_number=1005, repo_full_name=repo
    )
    assert no_frontier_roadmap is not None
    assert (
        no_frontier_orch._run_roadmap_adjustment_gate(
            roadmap=no_frontier_roadmap,
            checkout_path=no_frontier_checkout,
        )
        is True
    )
    after_no_frontier = no_frontier_state.get_roadmap_state(
        roadmap_issue_number=1005, repo_full_name=repo
    )
    assert after_no_frontier is not None
    assert after_no_frontier.adjustment_state == "idle"
    assert not no_frontier_agent.roadmap_adjustment_calls

    no_claim_state = StateStore(tmp_path / "noclaim.db")
    no_claim_state.upsert_roadmap_graph(
        roadmap_issue_number=1006,
        roadmap_pr_number=10060,
        roadmap_doc_path="docs/roadmap/1006.md",
        graph_path="docs/roadmap/1006.graph.json",
        graph_checksum="sum1006",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim_token = no_claim_state.claim_roadmap_adjustment(
        roadmap_issue_number=1006,
        repo_full_name=repo,
    )
    assert claim_token is not None
    no_claim_git = FakeGitManager(tmp_path / "checkouts-no-claim")
    no_claim_checkout = no_claim_git.ensure_checkout(0)
    _write_checkout_file(
        no_claim_git.checkout_root,
        "docs/roadmap/1006.graph.json",
        content='{"roadmap_issue_number":1006,"version":1,"nodes":[]}',
        slot=0,
    )
    no_claim_orch = Phase1Orchestrator(
        cfg,
        state=no_claim_state,
        github=FakeGitHub([_issue(1006, "Plan", labels=(cfg.repo.roadmap_label,))]),
        git_manager=no_claim_git,
        agent=FakeAgent(),
    )
    no_claim_roadmap = no_claim_state.get_roadmap_state(
        roadmap_issue_number=1006, repo_full_name=repo
    )
    assert no_claim_roadmap is not None
    assert (
        no_claim_orch._run_roadmap_adjustment_gate(
            roadmap=no_claim_roadmap,
            checkout_path=no_claim_checkout,
        )
        is False
    )
    assert (
        no_claim_state.release_roadmap_adjustment(
            roadmap_issue_number=1006,
            claim_token=claim_token,
            repo_full_name=repo,
        )
        is True
    )

    error_state = StateStore(tmp_path / "error.db")
    error_state.upsert_roadmap_graph(
        roadmap_issue_number=1007,
        roadmap_pr_number=10070,
        roadmap_doc_path="docs/roadmap/1007.md",
        graph_path="docs/roadmap/1007.graph.json",
        graph_checksum="sum1007",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    error_git = FakeGitManager(tmp_path / "checkouts-error")
    error_checkout = error_git.ensure_checkout(0)
    _write_checkout_file(
        error_git.checkout_root,
        "docs/roadmap/1007.graph.json",
        content='{"roadmap_issue_number":1007,"version":1,"nodes":[]}',
        slot=0,
    )
    error_agent = FakeAgent()
    error_orch = Phase1Orchestrator(
        cfg,
        state=error_state,
        github=FakeGitHub([_issue(1007, "Plan", labels=(cfg.repo.roadmap_label,))]),
        git_manager=error_git,
        agent=error_agent,
    )
    monkeypatch.setattr(
        error_agent,
        "evaluate_roadmap_adjustment",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    error_roadmap = error_state.get_roadmap_state(roadmap_issue_number=1007, repo_full_name=repo)
    assert error_roadmap is not None
    assert (
        error_orch._run_roadmap_adjustment_gate(
            roadmap=error_roadmap,
            checkout_path=error_checkout,
        )
        is False
    )
    after_error = error_state.get_roadmap_state(roadmap_issue_number=1007, repo_full_name=repo)
    assert after_error is not None
    assert after_error.last_error == "roadmap adjustment evaluation failed: boom"
    assert after_error.adjustment_state == "idle"


def test_advance_roadmap_nodes_invokes_pending_revision_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=1001,
        roadmap_pr_number=10010,
        roadmap_doc_path="docs/roadmap/1001.md",
        graph_path="docs/roadmap/1001.graph.json",
        graph_checksum="sum1001",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=1001, repo_full_name=repo)
    github = FakeGitHub([_issue(1001, "Plan", labels=(cfg.repo.roadmap_label,))])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    called: list[int] = []

    def fake_apply(*, roadmap, checkout_path: Path):  # type: ignore[no-untyped-def]
        _ = checkout_path
        called.append(roadmap.roadmap_issue_number)
        return replace(roadmap, status="active", adjustment_state="idle")

    monkeypatch.setattr(orch, "_maybe_apply_pending_roadmap_revision", fake_apply)
    monkeypatch.setattr(orch, "_verify_roadmap_graph_checksum", lambda **kwargs: False)

    orch._advance_roadmap_nodes()

    assert called == [1001]


def test_handle_roadmap_control_labels_resets_and_deduplicates_requests(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=1002,
        roadmap_pr_number=10020,
        roadmap_doc_path="docs/roadmap/1002.md",
        graph_path="docs/roadmap/1002.graph.json",
        graph_checksum="sum1002",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    assert (
        state.set_roadmap_adjustment_request_version(
            roadmap_issue_number=1002,
            request_version=1,
            repo_full_name=repo,
        )
        is True
    )

    clear_github = FakeGitHub([_issue(1002, "Plan", labels=(cfg.repo.roadmap_label,))])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=clear_github,
        git_manager=FakeGitManager(tmp_path / "checkouts-clear"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=1002, repo_full_name=repo)
    assert roadmap is not None
    orch._handle_roadmap_control_labels(roadmap=roadmap)
    cleared = state.get_roadmap_state(roadmap_issue_number=1002, repo_full_name=repo)
    assert cleared is not None
    assert cleared.adjustment_request_version is None

    assert (
        state.set_roadmap_adjustment_request_version(
            roadmap_issue_number=1002,
            request_version=1,
            repo_full_name=repo,
        )
        is True
    )
    duplicate_github = FakeGitHub(
        [_issue(1002, "Plan", labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label))]
    )
    duplicate_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=duplicate_github,
        git_manager=FakeGitManager(tmp_path / "checkouts-dup"),
        agent=FakeAgent(),
    )
    duplicate_roadmap = state.get_roadmap_state(roadmap_issue_number=1002, repo_full_name=repo)
    assert duplicate_roadmap is not None
    duplicate_orch._handle_roadmap_control_labels(roadmap=duplicate_roadmap)
    still_active = state.get_roadmap_state(roadmap_issue_number=1002, repo_full_name=repo)
    assert still_active is not None
    assert still_active.status == "active"
    assert not duplicate_github.comments


def test_open_superseding_roadmap_issue_reuses_existing_issue(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=991,
        roadmap_pr_number=9910,
        roadmap_doc_path="docs/roadmap/991.md",
        graph_path="docs/roadmap/991.graph.json",
        graph_checksum="sum991",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=991, repo_full_name=repo)
    parent_issue = _issue(
        991,
        "Plan",
        labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label),
    )
    existing_superseding = Issue(
        number=9911,
        title="Existing revision",
        body="Supersedes roadmap #991",
        html_url="https://example/issues/9911",
        labels=(cfg.repo.roadmap_label,),
        author_login="issue-author",
    )
    unrelated_roadmap_issue = Issue(
        number=9912,
        title="Unrelated roadmap",
        body="No supersede lineage",
        html_url="https://example/issues/9912",
        labels=(cfg.repo.roadmap_label,),
        author_login="issue-author",
    )
    github = FakeGitHub([parent_issue, unrelated_roadmap_issue, existing_superseding])
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=991, repo_full_name=repo)
    assert roadmap is not None
    orch._open_superseding_roadmap_issue(roadmap=roadmap, issue=parent_issue)

    refreshed = state.get_roadmap_state(roadmap_issue_number=991, repo_full_name=repo)
    assert refreshed is not None
    assert refreshed.superseding_roadmap_issue_number == 9911
    assert not github.created_issues


def test_open_superseding_roadmap_issue_finalizes_after_comment(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=993,
        roadmap_pr_number=9930,
        roadmap_doc_path="docs/roadmap/993.md",
        graph_path="docs/roadmap/993.graph.json",
        graph_checksum="sum993",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    state.mark_roadmap_revision_requested(roadmap_issue_number=993, repo_full_name=repo)
    parent_issue = _issue(
        993,
        "Plan",
        labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_revision_label),
    )
    github = FakeGitHub([parent_issue])
    failing_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=993, repo_full_name=repo)
    assert roadmap is not None

    def fail_comment(**kwargs) -> None:  # type: ignore[no-untyped-def]
        _ = kwargs
        raise RuntimeError("comment failed")

    failing_orch._ensure_tokenized_issue_comment = fail_comment  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="comment failed"):
        failing_orch._open_superseding_roadmap_issue(roadmap=roadmap, issue=parent_issue)

    after_failure = state.get_roadmap_state(roadmap_issue_number=993, repo_full_name=repo)
    assert after_failure is not None
    assert after_failure.superseding_roadmap_issue_number is None
    assert len(github.created_issues) == 1

    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts-retry"),
        agent=FakeAgent(),
    )
    retried = state.get_roadmap_state(roadmap_issue_number=993, repo_full_name=repo)
    assert retried is not None
    orch._open_superseding_roadmap_issue(roadmap=retried, issue=parent_issue)

    finalized = state.get_roadmap_state(roadmap_issue_number=993, repo_full_name=repo)
    assert finalized is not None
    assert finalized.superseding_roadmap_issue_number is not None
    assert len(github.created_issues) == 1


def test_abandon_roadmap_finalizes_after_issue_close(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=992,
        roadmap_pr_number=9920,
        roadmap_doc_path="docs/roadmap/992.md",
        graph_path="docs/roadmap/992.graph.json",
        graph_checksum="sum992",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=992,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=9921,
        child_issue_url="https://example/issues/9921",
        repo_full_name=repo,
    )

    class FailingCloseGitHub(FakeGitHub):
        def close_issue(self, issue_number: int) -> None:
            _ = issue_number
            raise RuntimeError("close failed")

    failing_github = FailingCloseGitHub(
        [
            _issue(992, "Plan", labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_abandon_label)),
            _issue(9921, "Child", labels=(cfg.repo.small_job_label,)),
        ]
    )
    failing_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=failing_github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    roadmap = state.get_roadmap_state(roadmap_issue_number=992, repo_full_name=repo)
    assert roadmap is not None
    with pytest.raises(RuntimeError, match="close failed"):
        failing_orch._abandon_roadmap(roadmap=roadmap, reason="manual abandon")

    after_failure = state.get_roadmap_state(roadmap_issue_number=992, repo_full_name=repo)
    assert after_failure is not None
    assert after_failure.status == "active"

    github = FakeGitHub(
        [
            _issue(992, "Plan", labels=(cfg.repo.roadmap_label, cfg.repo.roadmap_abandon_label)),
            _issue(9921, "Child", labels=(cfg.repo.small_job_label,)),
        ]
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts-ok"),
        agent=FakeAgent(),
    )
    retried = state.get_roadmap_state(roadmap_issue_number=992, repo_full_name=repo)
    assert retried is not None
    orch._abandon_roadmap(roadmap=retried, reason="manual abandon")

    finalized = state.get_roadmap_state(roadmap_issue_number=992, repo_full_name=repo)
    assert finalized is not None
    assert finalized.status == "abandoned"
    assert 9921 in github.closed_issue_numbers
    assert 992 in github.closed_issue_numbers


def test_publish_roadmap_status_reports_skips_bot_and_non_command_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=98,
        roadmap_pr_number=908,
        roadmap_doc_path="docs/roadmap/98.md",
        graph_path="docs/roadmap/98.graph.json",
        graph_checksum="sum98",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    github = FakeGitHub([_issue(98, "Plan", labels=(cfg.repo.roadmap_label,))])
    github.issue_comments = [
        PullRequestIssueComment(
            comment_id=1,
            body="/roadmap status",
            user_login="mergexo[bot]",
            html_url="https://example/issues/98#1",
            created_at="t",
            updated_at="t",
        ),
        PullRequestIssueComment(
            comment_id=2,
            body="status please",
            user_login="reviewer",
            html_url="https://example/issues/98#2",
            created_at="t",
            updated_at="t",
        ),
    ]
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._publish_roadmap_status_reports()
    assert not github.comments

    empty = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state-empty.db"),
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts-empty"),
        agent=FakeAgent(),
    )
    empty._publish_roadmap_status_reports()


def test_handle_roadmap_revision_escalation_and_direct_feedback_call_sites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True)
    repo = cfg.repo.full_name
    state = StateStore(tmp_path / "state.db")
    state.upsert_roadmap_graph(
        roadmap_issue_number=99,
        roadmap_pr_number=909,
        roadmap_doc_path="docs/roadmap/99.md",
        graph_path="docs/roadmap/99.graph.json",
        graph_checksum="sum99",
        nodes=(
            RoadmapNodeGraphInput(
                node_id="n1",
                kind="small_job",
                title="One",
                body_markdown="One",
                dependencies=(),
            ),
        ),
        repo_full_name=repo,
    )
    claim = state.claim_ready_roadmap_nodes(repo_full_name=repo)[0]
    state.mark_roadmap_node_issue_created(
        roadmap_issue_number=99,
        node_id=claim.node_id,
        claim_token=claim.claim_token,
        child_issue_number=991,
        child_issue_url="https://example/issues/991",
        repo_full_name=repo,
    )
    state.mark_roadmap_node_blocked(
        roadmap_issue_number=99,
        node_id=claim.node_id,
        repo_full_name=repo,
    )
    github = FakeGitHub([_issue(99, "Plan"), _issue(991, "Child")])
    git = FakeGitManager(tmp_path / "checkouts")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=FakeAgent())
    escalation = RoadmapRevisionEscalation(
        kind="roadmap_revision", summary="bad base", details="details"
    )

    orch._handle_roadmap_revision_escalation(
        source_issue=_issue(123, "No link"),
        escalation=escalation,
        source_url="https://example/issues/123",
    )
    before = len(github.comments)
    orch._handle_roadmap_revision_escalation(
        source_issue=_issue(991, "Child"),
        escalation=escalation,
        source_url="https://example/issues/991",
    )
    assert len(github.comments) == before + 1
    roadmap = state.get_roadmap_state(roadmap_issue_number=99, repo_full_name=repo)
    assert roadmap is not None
    assert roadmap.status == "active"
    assert roadmap.adjustment_state == "awaiting_revision_merge"

    handled: list[str] = []
    monkeypatch.setattr(
        orch,
        "_handle_roadmap_revision_escalation",
        lambda source_issue, escalation, source_url: handled.append(source_url),
    )
    monkeypatch.setattr(orch._git, "commit_all", lambda checkout_path, message: None)
    monkeypatch.setattr(orch, "_run_required_tests_before_push", lambda *, checkout_path: None)
    direct_result = DirectStartResult(
        pr_title="t",
        pr_summary="s",
        commit_message="m",
        blocked_reason=None,
        session=None,
        escalation=escalation,
    )
    orch._run_direct_turn_with_required_tests_repair(
        issue=_issue(7, "Direct"),
        flow_label="bugfix",
        checkout_path=tmp_path,
        default_commit_message="fallback",
        regression_test_file_regex=None,
        direct_turn=lambda issue: direct_result,
    )
    assert any(url.endswith("/7") for url in handled)

    issue = _issue(7, "Feedback")
    github = FakeGitHub([issue])
    github.pr_snapshot = PullRequestSnapshot(
        number=101,
        title="Design PR",
        body="Body",
        head_sha="head-1",
        base_sha="base-1",
        draft=False,
        state="open",
        merged=False,
    )
    github.review_comments = [_review_comment()]
    github.changed_files = ("docs/design/7-add-worker-scheduler.md",)
    agent = FakeAgent()
    agent.feedback_result = FeedbackResult(
        session=AgentSession(adapter="codex", thread_id="thread-123"),
        review_replies=(),
        general_comment=None,
        commit_message=None,
        git_ops=(),
        escalation=escalation,
    )
    state = StateStore(tmp_path / "feedback.db")
    state.mark_completed(
        issue.number,
        "agent/design/7-add-worker-scheduler",
        101,
        "https://example/pr/101",
        repo_full_name=cfg.repo.full_name,
    )
    state.save_agent_session(
        issue_number=issue.number,
        adapter="codex",
        thread_id="thread-123",
        repo_full_name=cfg.repo.full_name,
    )
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=FakeGitManager(tmp_path / "feedback-checkouts"),
        agent=agent,
    )
    feedback_calls: list[str] = []
    monkeypatch.setattr(
        orch,
        "_handle_roadmap_revision_escalation",
        lambda source_issue, escalation, source_url: feedback_calls.append(source_url),
    )
    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert any("/pull/101" in url for url in feedback_calls)


def test_process_issue_roadmap_validation_and_required_tests_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_roadmaps=True, required_tests="scripts/test.sh")
    issue = _issue(120, "Roadmap", labels=(cfg.repo.roadmap_label,))
    github = FakeGitHub([issue])
    git = FakeGitManager(tmp_path / "checkouts")
    state = StateStore(tmp_path / "state.db")
    failing_agent = FakeAgent(fail=True)
    failing_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=failing_agent,
    )
    with pytest.raises(RuntimeError, match="codex failed"):
        failing_orch._process_issue(issue, "roadmap")

    mismatch_agent = FakeAgent()
    mismatch_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=mismatch_agent,
    )
    monkeypatch.setattr(
        mismatch_agent,
        "start_roadmap_from_issue",
        lambda **kwargs: RoadmapStartResult(
            roadmap=GeneratedRoadmap(
                title="Roadmap",
                summary="Summary",
                roadmap_markdown="# Plan",
                roadmap_issue_number=999,
                version=1,
                graph_nodes=(
                    RoadmapNode(
                        node_id="n1",
                        kind="small_job",
                        title="Ship",
                        body_markdown="Do it",
                    ),
                ),
                canonical_graph_json=(
                    '{"nodes":[{"body_markdown":"Do it","depends_on":[],"kind":"small_job",'
                    '"node_id":"n1","title":"Ship"}],"roadmap_issue_number":999,"version":1}'
                ),
            ),
            session=None,
        ),
    )
    with pytest.raises(DirectFlowValidationError, match="did not match"):
        mismatch_orch._process_issue(issue, "roadmap")

    required_tests_orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=github,
        git_manager=git,
        agent=FakeAgent(),
    )
    monkeypatch.setattr(
        required_tests_orch,
        "_run_required_tests_before_push",
        lambda *, checkout_path: "boom",
    )
    with pytest.raises(DirectFlowValidationError, match="required pre-push"):
        required_tests_orch._process_issue(issue, "roadmap")
    assert any(
        "required pre-push test" in body
        for issue_number, body in github.comments
        if issue_number == 120
    )


def test_process_issue_roadmap_posts_size_recommendation(tmp_path: Path) -> None:
    cfg0 = _config(tmp_path, enable_roadmaps=True)
    cfg = AppConfig(
        runtime=cfg0.runtime,
        repos=(replace(cfg0.repo, roadmap_recommended_node_count=1),),
        codex=cfg0.codex,
    )
    issue = _issue(130, "Roadmap", labels=(cfg.repo.roadmap_label,))
    github = FakeGitHub([issue])
    agent = FakeAgent()
    agent.roadmap_result = replace(
        agent.roadmap_result,
        graph_nodes=(
            RoadmapNode(node_id="n1", kind="small_job", title="One", body_markdown="One"),
            RoadmapNode(node_id="n2", kind="small_job", title="Two", body_markdown="Two"),
        ),
    )
    orch = Phase1Orchestrator(
        cfg,
        state=StateStore(tmp_path / "state.db"),
        github=github,
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=agent,
    )
    orch._process_issue(issue, "roadmap")
    assert any(
        issue_number == 130 and "sizing recommendation" in body.lower()
        for issue_number, body in github.comments
    )
