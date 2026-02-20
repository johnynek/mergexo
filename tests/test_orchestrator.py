from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
import re
import time

from hypothesis import given, strategies as st
import pytest

from mergexo.config import AppConfig, CodexConfig, RepoConfig, RuntimeConfig
from mergexo.models import GeneratedDesign, Issue, PullRequest, WorkResult
from mergexo.orchestrator import Phase1Orchestrator, SlotPool, _render_design_doc, _slugify


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


class FakeGitHub:
    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
        self.created_prs: list[tuple[str, str, str, str]] = []
        self.comments: list[tuple[int, str]] = []

    def list_open_issues_with_label(self, label: str) -> list[Issue]:
        _ = label
        return self.issues

    def create_pull_request(self, title: str, head: str, base: str, body: str) -> PullRequest:
        self.created_prs.append((title, head, base, body))
        return PullRequest(number=101, html_url="https://example/pr/101")

    def post_issue_comment(self, issue_number: int, body: str) -> None:
        self.comments.append((issue_number, body))


class FakeCodex:
    def __init__(self, generated: GeneratedDesign | None = None, fail: bool = False) -> None:
        self.generated = generated or GeneratedDesign(
            title="Design Title",
            summary="Summary",
            touch_paths=("src/a.py",),
            design_doc_markdown="## Body\n\nDetails",
        )
        self.fail = fail
        self.calls: list[tuple[str, Path]] = []

    def generate_design_doc(self, *, prompt: str, cwd: Path) -> GeneratedDesign:
        self.calls.append((prompt, cwd))
        if self.fail:
            raise RuntimeError("codex failed")
        return self.generated


class FakeState:
    def __init__(self, *, allowed: set[int] | None = None) -> None:
        self.allowed = allowed or set()
        self.running: list[int] = []
        self.completed: list[WorkResult] = []
        self.failed: list[tuple[int, str]] = []

    def can_enqueue(self, issue_number: int) -> bool:
        return issue_number in self.allowed

    def mark_running(self, issue_number: int) -> None:
        self.running.append(issue_number)

    def mark_completed(self, *, issue_number: int, branch: str, pr_number: int, pr_url: str) -> None:
        self.completed.append(
            WorkResult(issue_number=issue_number, branch=branch, pr_number=pr_number, pr_url=pr_url)
        )

    def mark_failed(self, *, issue_number: int, error: str) -> None:
        self.failed.append((issue_number, error))


def _config(tmp_path: Path, *, worker_count: int = 1) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(base_dir=tmp_path / "state", worker_count=worker_count, poll_interval_seconds=1),
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
    return Issue(number=number, title=title, body="Body", html_url=f"https://example/issues/{number}", labels=("agent:design",))


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
    codex = FakeCodex()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)
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
    assert f"# {codex.generated.title}" in contents
    assert "touch_paths" in contents
    assert branch.startswith("agent/design/7-")


def test_process_issue_releases_slot_on_failure(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub(issues=[])
    codex = FakeCodex(fail=True)
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)

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
    codex = FakeCodex()
    state = FakeState(allowed={1})

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)

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
    codex = FakeCodex()
    state = FakeState()

    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)

    ok: Future[WorkResult] = Future()
    ok.set_result(WorkResult(issue_number=1, branch="b", pr_number=2, pr_url="u"))
    bad: Future[WorkResult] = Future()
    bad.set_exception(RuntimeError("boom"))

    orch._running = {1: ok, 2: bad}
    orch._reap_finished()

    assert [w.issue_number for w in state.completed] == [1]
    assert state.failed[0][0] == 2
    assert orch._running == {}


def test_wait_for_all_calls_sleep_when_needed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _config(tmp_path)
    git = FakeGitManager(tmp_path / "checkouts")
    github = FakeGitHub([])
    codex = FakeCodex()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)

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
    codex = FakeCodex()
    state = FakeState()
    orch = Phase1Orchestrator(cfg, state=state, github=github, git_manager=git, codex=codex)

    once_calls = {"enqueue": 0, "wait": 0}

    monkeypatch.setattr(orch, "_enqueue_new_work", lambda pool: once_calls.__setitem__("enqueue", 1))
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
