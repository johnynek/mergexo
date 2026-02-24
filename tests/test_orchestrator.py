from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
import json
import logging
import re
import sqlite3
import time
from typing import cast

from hypothesis import given, strategies as st
import pytest

from mergexo.agent_adapter import (
    AgentSession,
    DirectStartResult,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
    ReviewReply,
)
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import (
    ParsedOperatorCommand,
    compute_pre_pr_checkpoint_token,
    compute_source_issue_redirect_token,
    parse_operator_command,
)
from mergexo.github_gateway import GitHubGateway, GitHubPollingError
from mergexo.models import (
    GeneratedDesign,
    Issue,
    IssueFlow,
    OperatorCommandRecord,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    RestartMode,
    WorkResult,
)
from mergexo.orchestrator import (
    CheckpointedPrePrBlockedError,
    DirectFlowBlockedError,
    DirectFlowValidationError,
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
    _flow_label_from_branch,
    _has_regression_test_changes,
    _is_mergexo_status_comment,
    _is_merge_conflict_error,
    _normalize_feedback_terminal_status,
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
    _render_source_issue_redirect_comment,
    _render_design_doc,
    _render_operator_command_result,
    _render_regex_patterns,
    _recovery_pr_payload_for_issue,
    _resolve_issue_flow,
    _summarize_git_error,
    _slugify,
    _trigger_labels,
)
from mergexo.observability import configure_logging
from mergexo.shell import CommandError
from mergexo.state import (
    ImplementationCandidateState,
    PrePrFollowupState,
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
        self.restore_calls: list[tuple[Path, str, str]] = []
        self.fetch_calls: list[Path] = []
        self.merge_calls: list[Path] = []
        self.current_head_calls: list[Path] = []
        self.is_ancestor_calls: list[tuple[Path, str, str]] = []
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

    def push_branch(self, checkout_path: Path, branch: str) -> None:
        self.push_calls.append((checkout_path, branch))

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
        self.comments: list[tuple[int, str]] = []
        self.review_replies: list[tuple[int, int, str]] = []
        self.review_comments: list[PullRequestReviewComment] = []
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
        self.compare_calls: list[tuple[str, str]] = []
        self.compare_statuses: dict[tuple[str, str], str] = {}

    def list_open_issues_with_any_labels(self, labels: tuple[str, ...]) -> list[Issue]:
        self.requested_labels = labels
        return self.issues

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
        return PullRequest(number=101, html_url="https://example/pr/101")

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
        assert pr_number == self.pr_snapshot.number
        return self.pr_snapshot

    def list_pull_request_files(self, pr_number: int) -> tuple[str, ...]:
        assert pr_number == self.pr_snapshot.number
        return self.changed_files

    def list_pull_request_review_comments(self, pr_number: int) -> list[PullRequestReviewComment]:
        assert pr_number == self.pr_snapshot.number
        return list(self.review_comments)

    def list_pull_request_issue_comments(self, pr_number: int) -> list[PullRequestIssueComment]:
        assert pr_number == self.pr_snapshot.number
        return list(self.issue_comments)

    def list_issue_comments(self, issue_number: int) -> list[PullRequestIssueComment]:
        _ = issue_number
        return list(self.issue_comments)

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
        self.feedback_results: list[FeedbackResult] = []
        self.feedback_result = FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-123"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
            git_ops=(),
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

    def respond_to_feedback(
        self, *, session: AgentSession, turn: FeedbackTurn, cwd: Path
    ) -> FeedbackResult:
        self.feedback_calls.append((session, turn, cwd))
        if self.feedback_results:
            return self.feedback_results.pop(0)
        return self.feedback_result

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

    def can_enqueue(self, issue_number: int, *, repo_full_name: str | None = None) -> bool:
        _ = repo_full_name
        return issue_number in self.allowed

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
        self, *, issue_number: int, error: str, repo_full_name: str | None = None
    ) -> None:
        _ = repo_full_name
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

    def prune_observability_history(
        self, *, retention_days: int, repo_full_name: str | None = None
    ) -> tuple[int, int]:
        _ = retention_days, repo_full_name
        return 0, 0

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

    def ingest_feedback_events(self, events: object, *, repo_full_name: str | None = None) -> None:
        _ = events, repo_full_name

    def list_pending_feedback_events(
        self, pr_number: int, *, repo_full_name: str | None = None
    ) -> tuple[object, ...]:
        _ = pr_number, repo_full_name
        return ()

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
    enable_issue_comment_routing: bool = False,
    restart_drain_timeout_seconds: int = 900,
    restart_default_mode: str = "git_checkout",
    restart_supported_modes: tuple[str, ...] = ("git_checkout",),
    operations_issue_number: int | None = None,
    operator_logins: tuple[str, ...] = (),
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
            enable_issue_comment_routing=enable_issue_comment_routing,
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
                operations_issue_number=operations_issue_number,
                operator_logins=operator_logins,
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

    state = ReconState()
    orch = Phase1Orchestrator(
        cfg,
        state=state,
        github=FakeGitHub([]),
        git_manager=FakeGitManager(tmp_path / "checkouts"),
        agent=FakeAgent(),
    )
    orch._ensure_poll_setup()


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
    assert _trigger_labels(cfg) == ("agent:design", "agent:bugfix", "agent:small-job")

    issue = _issue(labels=("agent:design", "agent:bugfix", "agent:small-job"))
    assert (
        _resolve_issue_flow(
            issue=issue,
            design_label=cfg.trigger_label,
            bugfix_label=cfg.bugfix_label,
            small_job_label=cfg.small_job_label,
        )
        == "bugfix"
    )
    assert _issue_branch(flow="design_doc", issue_number=7, slug="x") == "agent/design/7-x"
    assert _issue_branch(flow="bugfix", issue_number=7, slug="x") == "agent/bugfix/7-x"
    assert _issue_branch(flow="small_job", issue_number=7, slug="x") == "agent/small/7-x"
    assert _default_commit_message(flow="bugfix", issue_number=7) == "fix: resolve issue #7"
    assert _default_commit_message(flow="small_job", issue_number=7) == "feat: implement issue #7"
    assert (
        _resolve_issue_flow(
            issue=_issue(labels=("triage",)),
            design_label=cfg.trigger_label,
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
    assert github.requested_labels == ("agent:design", "agent:bugfix", "agent:small-job")

    # Already-running path: skip existing issue number.
    already_running_future: Future[WorkResult] = Future()
    orch._running = {1: already_running_future}
    orch._enqueue_new_work(pool)

    # Full queue path: when running reaches max, function returns early.
    running_future: Future[WorkResult] = Future()
    orch._running = {99: running_future, 100: running_future}
    orch._enqueue_new_work(pool)


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

    orch._scan_post_pr_source_issue_comment_redirects()
    assert len(github.comments) == 1


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
    assert github.comments == []
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
    assert github.comments == []

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


def test_feedback_turn_returns_when_expected_tokens_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    monkeypatch.setattr(orch, "_fetch_remote_action_tokens", lambda pr_number: set())

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(github.review_replies) == 1
    assert len(state.list_pending_feedback_events(101)) == 1


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
    def __init__(self, threads: dict[int, list[PullRequestIssueComment]]) -> None:
        self._threads = {issue_number: list(items) for issue_number, items in threads.items()}
        self.posted_comments: list[tuple[int, str]] = []
        self._next_comment_id = 5000

    def list_issue_comments(self, issue_number: int) -> list[PullRequestIssueComment]:
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
