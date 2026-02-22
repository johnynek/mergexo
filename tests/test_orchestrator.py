from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
import json
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
from mergexo.config import AppConfig, AuthConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.feedback_loop import ParsedOperatorCommand, parse_operator_command
from mergexo.github_gateway import GitHubGateway
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
    Phase1Orchestrator,
    RestartRequested,
    SlotPool,
    _FeedbackFuture,
    _design_branch_slug,
    _design_doc_url,
    _default_commit_message,
    _has_regression_test_changes,
    _issue_branch,
    _render_design_doc,
    _render_operator_command_result,
    _resolve_issue_flow,
    _summarize_git_error,
    _operator_args_payload,
    _operator_normalized_command,
    _operator_reply_issue_number,
    _operator_reply_status_for_record,
    _operator_source_comment_url,
    _slugify,
    _trigger_labels,
)
from mergexo.observability import configure_logging
from mergexo.state import ImplementationCandidateState, StateStore, TrackedPullRequestState


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
        coding_guidelines_path: str,
        cwd: Path,
    ) -> DirectStartResult:
        _ = repo_full_name, default_branch, coding_guidelines_path
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
        coding_guidelines_path: str,
        cwd: Path,
    ) -> DirectStartResult:
        _ = repo_full_name, default_branch, coding_guidelines_path
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
        coding_guidelines_path: str,
        design_doc_path: str,
        design_doc_markdown: str,
        design_pr_number: int | None,
        design_pr_url: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = (
            repo_full_name,
            default_branch,
            coding_guidelines_path,
            design_doc_markdown,
            design_pr_number,
            design_pr_url,
        )
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

    def can_enqueue(self, issue_number: int) -> bool:
        return issue_number in self.allowed

    def mark_running(self, issue_number: int) -> None:
        self.running.append(issue_number)

    def mark_completed(
        self, *, issue_number: int, branch: str, pr_number: int, pr_url: str
    ) -> None:
        self.completed.append(
            WorkResult(issue_number=issue_number, branch=branch, pr_number=pr_number, pr_url=pr_url)
        )

    def mark_failed(self, *, issue_number: int, error: str) -> None:
        self.failed.append((issue_number, error))

    def save_agent_session(self, *, issue_number: int, adapter: str, thread_id: str | None) -> None:
        self.saved_sessions.append((issue_number, adapter, thread_id))

    def get_agent_session(self, issue_number: int) -> tuple[str, str | None] | None:
        for num, adapter, thread_id in self.saved_sessions:
            if num == issue_number:
                return adapter, thread_id
        return None

    def list_tracked_pull_requests(self) -> tuple[TrackedPullRequestState, ...]:
        return tuple(self.tracked)

    def list_implementation_candidates(self) -> tuple[ImplementationCandidateState, ...]:
        return tuple(self.implementation_candidates)

    def get_runtime_operation(self, op_name: str):  # type: ignore[no-untyped-def]
        _ = op_name
        return None

    def mark_pr_status(
        self,
        *,
        pr_number: int,
        issue_number: int,
        status: str,
        last_seen_head_sha: str | None = None,
        error: str | None = None,
    ) -> None:
        self.status_updates.append((pr_number, issue_number, status, last_seen_head_sha, error))

    def ingest_feedback_events(self, events: object) -> None:
        _ = events

    def list_pending_feedback_events(self, pr_number: int) -> tuple[object, ...]:
        _ = pr_number
        return ()

    def finalize_feedback_turn(
        self,
        *,
        pr_number: int,
        issue_number: int,
        processed_event_keys: tuple[str, ...],
        session: AgentSession,
        head_sha: str,
    ) -> None:
        _ = pr_number, issue_number, processed_event_keys, session, head_sha


def _config(
    tmp_path: Path,
    *,
    worker_count: int = 1,
    enable_feedback_loop: bool = False,
    enable_github_operations: bool = False,
    restart_drain_timeout_seconds: int = 900,
    restart_default_mode: str = "git_checkout",
    restart_supported_modes: tuple[str, ...] = ("git_checkout",),
    operations_issue_number: int | None = None,
    operator_logins: tuple[str, ...] = (),
    allowed_users: tuple[str, ...] = ("issue-author", "reviewer"),
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=worker_count,
            poll_interval_seconds=1,
            enable_feedback_loop=enable_feedback_loop,
            enable_github_operations=enable_github_operations,
            restart_drain_timeout_seconds=restart_drain_timeout_seconds,
            restart_default_mode=cast(RestartMode, restart_default_mode),
            restart_supported_modes=cast(tuple[RestartMode, ...], restart_supported_modes),
        ),
        repo=RepoConfig(
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            bugfix_label="agent:bugfix",
            small_job_label="agent:small-job",
            coding_guidelines_path="docs/python_style.md",
            design_docs_dir="docs/design",
            local_clone_source=None,
            remote_url=None,
            operations_issue_number=operations_issue_number,
            operator_logins=operator_logins,
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
        auth=AuthConfig(allowed_users=frozenset(login.lower() for login in allowed_users)),
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


def test_slot_pool_acquire_release(tmp_path: Path) -> None:
    manager = FakeGitManager(tmp_path / "checkouts")
    pool = SlotPool(manager, worker_count=1)

    lease = pool.acquire()
    assert lease.slot == 0
    assert lease.path.exists()

    pool.release(lease)
    lease2 = pool.acquire()
    assert lease2.slot == 0


@given(st.text())
def test_slugify_is_safe(text: str) -> None:
    slug = _slugify(text)
    assert slug
    assert re.fullmatch(r"[a-z0-9-]+", slug)


def test_flow_helpers() -> None:
    cfg = RepoConfig(
        owner="o",
        name="r",
        default_branch="main",
        trigger_label="agent:design",
        bugfix_label="agent:bugfix",
        small_job_label="agent:small-job",
        coding_guidelines_path="docs/python_style.md",
        design_docs_dir="docs/design",
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
    assert _has_regression_test_changes(("tests/test_a.py", "src/a.py")) is True
    assert _has_regression_test_changes(("src/a.py",)) is False
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
    assert github.comments == [(7, "Opened design PR: https://example/pr/101")]
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

    with pytest.raises(RuntimeError, match="not allowed by auth.allowed_users"):
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
    assert f"Source issue: {issue.html_url}" in github.created_prs[0][3]
    assert github.comments == [(7, "Opened bugfix PR: https://example/pr/101")]


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
    assert github.comments == [(7, "Opened small-job PR: https://example/pr/101")]


def test_process_issue_bugfix_requires_regression_tests(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
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
    assert github.comments == [(7, "Opened implementation PR: https://example/pr/101")]


def test_process_implementation_candidate_rejects_unauthorized_author(tmp_path: Path) -> None:
    cfg = _config(tmp_path, allowed_users=("reviewer",))
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[_issue(author_login="outsider")])
    agent = FakeAgent()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    with pytest.raises(RuntimeError, match="not allowed by auth.allowed_users"):
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

        def submit(self, fn, issue, flow):  # type: ignore[no-untyped-def]
            _ = fn
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
        def submit(self, fn, issue, flow):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow
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

        def submit(self, fn, candidate):  # type: ignore[no-untyped-def]
            _ = fn
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

        def submit(self, fn, issue, flow):  # type: ignore[no-untyped-def]
            _ = fn
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
        def submit(self, fn, issue, flow):  # type: ignore[no-untyped-def]
            _ = fn, issue, flow
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


def test_run_once_with_github_ops_scans_commands_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True, enable_github_operations=True)
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


def test_feedback_turn_processes_once_for_same_comment(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")

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
    cfg = _config(tmp_path, enable_feedback_loop=True, allowed_users=("reviewer",))
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")

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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.mark_pr_status(
        pr_number=101,
        issue_number=issue.number,
        status="awaiting_feedback",
        last_seen_head_sha="head-old",
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    state.mark_pr_status(
        pr_number=101,
        issue_number=issue.number,
        status="awaiting_feedback",
        last_seen_head_sha="head-old",
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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


def test_run_once_invokes_feedback_enqueue_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, worker_count=3, enable_feedback_loop=True)
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
            fut: Future[None] = Future()
            self.submitted.append(tracked.pr_number)
            return fut

    pool = FakePool()
    orch._enqueue_feedback_work(pool)
    assert pool.submitted == [101, 102]

    # Duplicate entry should be skipped when already running.
    duplicate_future: Future[None] = Future()
    orch._running_feedback = {101: _FeedbackFuture(issue_number=7, future=duplicate_future)}
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

    bad_feedback: Future[None] = Future()
    bad_feedback.set_exception(RuntimeError("feedback boom"))
    orch._running_feedback = {101: _FeedbackFuture(issue_number=7, future=bad_feedback)}

    orch._reap_finished()
    assert state.status_updates == [(101, 7, "blocked", None, "feedback boom")]


def test_reap_finished_marks_feedback_success(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    agent = FakeAgent()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    ok_feedback: Future[None] = Future()
    ok_feedback.set_result(None)
    orch._running_feedback = {101: _FeedbackFuture(issue_number=7, future=ok_feedback)}

    orch._reap_finished()
    assert orch._running_feedback == {}
    assert state.status_updates == []


def test_feedback_turn_marks_merged_pr_and_stops_tracking(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)
    assert state.list_tracked_pull_requests() == ()


def test_feedback_turn_blocks_when_session_missing(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
    git = FakeGitManager(tmp_path / "checkouts")
    issue = _issue()
    github = FakeGitHub([issue])
    github.review_comments = [_review_comment()]
    github.issue_comments = [_issue_comment()]

    agent = FakeAgent()
    state = StateStore(tmp_path / "state.db")
    state.mark_completed(
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert state.list_tracked_pull_requests() == ()
    assert any("blocked" in body.lower() for _, body in github.comments)


def test_feedback_turn_commit_no_staged_changes_blocks_and_keeps_event_pending(
    tmp_path: Path,
) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(git.fetch_calls) == 1
    assert len(state.list_pending_feedback_events(101)) == 1
    assert state.list_tracked_pull_requests() == ()
    assert len(github.comments) == 1
    assert "non-fast-forward" in github.comments[0][1].lower()


def test_feedback_turn_marks_merged_when_pr_merges_before_finalize(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    def raise_other(checkout_path: Path, message: str) -> None:
        _ = checkout_path, message
        raise RuntimeError("boom")

    git.commit_all = raise_other  # type: ignore[method-assign]

    tracked = _tracked_state_from_store(state)
    with pytest.raises(RuntimeError, match="boom"):
        orch._process_feedback_turn(tracked)


def test_feedback_turn_commit_push_marks_closed(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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


def test_feedback_turn_returns_when_expected_tokens_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)
    monkeypatch.setattr(orch, "_fetch_remote_action_tokens", lambda pr_number: set())

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(github.review_replies) == 1
    assert len(state.list_pending_feedback_events(101)) == 1


def test_normalize_filters_tokenized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True, allowed_users=("issue-author", "reviewer"))
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
    cfg = _config(tmp_path, enable_feedback_loop=True, allowed_users=("issue-author", "reviewer"))
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert len(agent.feedback_calls) == 1
    turn = agent.feedback_calls[0][1]
    assert tuple(comment.comment_id for comment in turn.review_comments) == (11,)
    assert tuple(comment.comment_id for comment in turn.issue_comments) == (21,)
    assert state.list_pending_feedback_events(101) == ()


def test_feedback_turn_ignores_all_unauthorized_comments(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True, allowed_users=("issue-author", "reviewer"))
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert state.list_pending_feedback_events(101) == ()


def test_run_feedback_agent_with_git_ops_round_trip(tmp_path: Path) -> None:
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
    cfg = _config(tmp_path, enable_feedback_loop=True)
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
        issue.number, "agent/design/7-add-worker-scheduler", 101, "https://example/pr/101"
    )
    state.save_agent_session(issue_number=issue.number, adapter="codex", thread_id="thread-123")
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
    state.mark_completed(7, "agent/design/7-worker", 101, "https://example/pr/101")
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-old",
        error="blocked for test",
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

    orch._scan_operator_commands()
    assert len(github.posted_comments) == 1


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
    state.mark_completed(7, "agent/design/7-worker", 101, "https://example/pr/101")
    state.mark_pr_status(
        pr_number=101,
        issue_number=7,
        status="blocked",
        last_seen_head_sha="head-1",
        error="blocked",
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
