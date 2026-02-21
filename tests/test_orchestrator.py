from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
import time

from hypothesis import given, strategies as st
import pytest

from mergexo.agent_adapter import (
    AgentSession,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    ReviewReply,
)
from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.models import (
    GeneratedDesign,
    Issue,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    WorkResult,
)
from mergexo.orchestrator import Phase1Orchestrator, SlotPool, _render_design_doc, _slugify
from mergexo.state import StateStore, TrackedPullRequestState


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
        self.restore_feedback_result = True

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

    def push_branch(self, checkout_path: Path, branch: str) -> None:
        self.push_calls.append((checkout_path, branch))

    def cleanup_slot(self, checkout_path: Path) -> None:
        self.cleanup_calls.append(checkout_path)

    def restore_feedback_branch(
        self, checkout_path: Path, branch: str, expected_head_sha: str
    ) -> bool:
        self.restore_calls.append((checkout_path, branch, expected_head_sha))
        return self.restore_feedback_result


class FakeGitHub:
    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
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

    def list_open_issues_with_label(self, label: str) -> list[Issue]:
        _ = label
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
        self.feedback_calls: list[tuple[AgentSession, FeedbackTurn, Path]] = []
        self.feedback_result = FeedbackResult(
            session=AgentSession(adapter="codex", thread_id="thread-123"),
            review_replies=(),
            general_comment=None,
            commit_message=None,
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
        return self.feedback_result


class FakeState:
    def __init__(self, *, allowed: set[int] | None = None) -> None:
        self.allowed = allowed or set()
        self.running: list[int] = []
        self.completed: list[WorkResult] = []
        self.failed: list[tuple[int, str]] = []
        self.saved_sessions: list[tuple[int, str, str | None]] = []
        self.tracked: list[TrackedPullRequestState] = []
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
    tmp_path: Path, *, worker_count: int = 1, enable_feedback_loop: bool = False
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            base_dir=tmp_path / "state",
            worker_count=worker_count,
            poll_interval_seconds=1,
            enable_feedback_loop=enable_feedback_loop,
        ),
        repo=RepoConfig(
            owner="johnynek",
            name="mergexo",
            default_branch="main",
            trigger_label="agent:design",
            design_docs_dir="docs/design",
            local_clone_source=None,
            remote_url=None,
        ),
        codex=CodexConfig(enabled=True, model=None, sandbox=None, profile=None, extra_args=()),
    )


def _issue(number: int = 7, title: str = "Add worker scheduler") -> Issue:
    return Issue(
        number=number,
        title=title,
        body="Body",
        html_url=f"https://example/issues/{number}",
        labels=("agent:design",),
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
    result = orch._process_issue(_issue())

    assert result.issue_number == 7
    assert result.pr_number == 101
    assert github.created_prs
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


def test_process_issue_releases_slot_on_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    agent = FakeAgent(fail=True)
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, agent=agent)

    with pytest.raises(RuntimeError, match="codex failed"):
        orch._process_issue(_issue())

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
            self.submitted: list[int] = []

        def submit(self, fn, issue):  # type: ignore[no-untyped-def]
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
            self.submitted.append(issue.number)
            return fut

    pool = FakePool()

    orch._enqueue_new_work(pool)
    assert state.running == [1]
    assert pool.submitted == [1]

    # Already-running path: skip existing issue number.
    already_running_future: Future[WorkResult] = Future()
    orch._running = {1: already_running_future}
    orch._enqueue_new_work(pool)

    # Full queue path: when running reaches max, function returns early.
    running_future: Future[WorkResult] = Future()
    orch._running = {99: running_future, 100: running_future}
    orch._enqueue_new_work(pool)


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


def test_feedback_turn_skips_agent_when_branch_head_mismatch_and_retries(tmp_path: Path) -> None:
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

    tracked = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked)

    assert agent.feedback_calls == []
    assert len(state.list_pending_feedback_events(101)) == 1

    git.restore_feedback_result = True
    tracked_retry = _tracked_state_from_store(state)
    orch._process_feedback_turn(tracked_retry)
    assert len(agent.feedback_calls) == 1
    assert state.list_pending_feedback_events(101) == ()


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
