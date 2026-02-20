from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import queue
import re
import threading
import time

from mergexo.codex_adapter import CodexAdapter
from mergexo.config import AppConfig
from mergexo.git_ops import GitRepoManager
from mergexo.github_gateway import GitHubGateway
from mergexo.models import GeneratedDesign, Issue, WorkResult
from mergexo.prompts import build_design_prompt
from mergexo.state import StateStore


@dataclass(frozen=True)
class _SlotLease:
    slot: int
    path: Path


class SlotPool:
    def __init__(self, manager: GitRepoManager, worker_count: int) -> None:
        self._manager = manager
        self._slots: queue.Queue[int] = queue.Queue(maxsize=worker_count)
        for slot in range(worker_count):
            self._slots.put(slot)

    def acquire(self) -> _SlotLease:
        slot = self._slots.get(block=True)
        path = self._manager.ensure_checkout(slot)
        return _SlotLease(slot=slot, path=path)

    def release(self, lease: _SlotLease) -> None:
        self._slots.put(lease.slot)


class Phase1Orchestrator:
    def __init__(
        self,
        config: AppConfig,
        *,
        state: StateStore,
        github: GitHubGateway,
        git_manager: GitRepoManager,
        codex: CodexAdapter,
    ) -> None:
        self._config = config
        self._state = state
        self._github = github
        self._git = git_manager
        self._codex = codex
        self._slot_pool = SlotPool(git_manager, config.runtime.worker_count)
        self._running: dict[int, Future[WorkResult]] = {}
        self._running_lock = threading.Lock()

    def run(self, *, once: bool) -> None:
        self._git.ensure_layout()

        with ThreadPoolExecutor(max_workers=self._config.runtime.worker_count) as pool:
            while True:
                self._reap_finished()
                self._enqueue_new_work(pool)

                if once:
                    self._wait_for_all(pool)
                    break

                time.sleep(self._config.runtime.poll_interval_seconds)

    def _enqueue_new_work(self, pool: ThreadPoolExecutor) -> None:
        issues = self._github.list_open_issues_with_label(self._config.repo.trigger_label)
        for issue in issues:
            with self._running_lock:
                if len(self._running) >= self._config.runtime.worker_count:
                    return
                if issue.number in self._running:
                    continue

            if not self._state.can_enqueue(issue.number):
                continue

            self._state.mark_running(issue.number)
            fut = pool.submit(self._process_issue, issue)
            with self._running_lock:
                self._running[issue.number] = fut

    def _reap_finished(self) -> None:
        finished_issue_numbers: list[int] = []
        with self._running_lock:
            for issue_number, fut in self._running.items():
                if fut.done():
                    finished_issue_numbers.append(issue_number)

            for issue_number in finished_issue_numbers:
                fut = self._running.pop(issue_number)
                try:
                    result = fut.result()
                    self._state.mark_completed(
                        issue_number=result.issue_number,
                        branch=result.branch,
                        pr_number=result.pr_number,
                        pr_url=result.pr_url,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._state.mark_failed(issue_number=issue_number, error=str(exc))

    def _wait_for_all(self, pool: ThreadPoolExecutor) -> None:
        while True:
            self._reap_finished()
            with self._running_lock:
                if not self._running:
                    return
            time.sleep(1.0)

    def _process_issue(self, issue: Issue) -> WorkResult:
        lease = self._slot_pool.acquire()
        try:
            self._git.prepare_checkout(lease.path)

            slug = _slugify(issue.title)
            branch = f"agent/design/{issue.number}-{slug}"
            self._git.create_or_reset_branch(lease.path, branch)

            design_relpath = f"{self._config.repo.design_docs_dir}/{issue.number}-{slug}.md"
            prompt = build_design_prompt(
                issue=issue,
                repo_full_name=self._config.repo.full_name,
                design_doc_path=design_relpath,
                default_branch=self._config.repo.default_branch,
            )
            generated = self._codex.generate_design_doc(prompt=prompt, cwd=lease.path)

            design_abs_path = lease.path / design_relpath
            design_abs_path.parent.mkdir(parents=True, exist_ok=True)
            design_abs_path.write_text(
                _render_design_doc(issue=issue, design=generated),
                encoding="utf-8",
            )

            self._git.commit_all(lease.path, f"docs: add design for issue #{issue.number}")
            self._git.push_branch(lease.path, branch)

            pr = self._github.create_pull_request(
                title=f"Design doc for #{issue.number}: {generated.title}",
                head=branch,
                base=self._config.repo.default_branch,
                body=(
                    f"Design doc for issue #{issue.number}.\n\n"
                    f"Refs #{issue.number}\n\n"
                    f"Source issue: {issue.html_url}"
                ),
            )

            self._github.post_issue_comment(
                issue_number=issue.number,
                body=f"Opened design PR: {pr.html_url}",
            )

            return WorkResult(
                issue_number=issue.number,
                branch=branch,
                pr_number=pr.number,
                pr_url=pr.html_url,
            )
        finally:
            try:
                self._git.cleanup_slot(lease.path)
            finally:
                self._slot_pool.release(lease)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "issue"


def _render_design_doc(*, issue: Issue, design: GeneratedDesign) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    touch_paths = "\n".join(f"  - {path}" for path in design.touch_paths)
    return (
        "---\n"
        f"issue: {issue.number}\n"
        "priority: 3\n"
        "touch_paths:\n"
        f"{touch_paths}\n"
        "depends_on: []\n"
        "estimated_size: M\n"
        f"generated_at: {now}\n"
        "---\n\n"
        f"# {design.title}\n\n"
        f"_Issue: #{issue.number} ({issue.html_url})_\n\n"
        f"## Summary\n\n{design.summary}\n\n"
        f"{design.design_doc_markdown.strip()}\n"
    )
