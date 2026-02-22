from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging

from mergexo.config import RepoConfig, RuntimeConfig
from mergexo.observability import log_event
from mergexo.shell import CommandError, run


LOGGER = logging.getLogger("mergexo.git_ops")


@dataclass(frozen=True)
class RepoLayout:
    mirror_path: Path
    checkouts_root: Path


class GitRepoManager:
    def __init__(self, runtime: RuntimeConfig, repo: RepoConfig) -> None:
        self.runtime = runtime
        self.repo = repo
        self.layout = RepoLayout(
            mirror_path=runtime.base_dir / "repos" / repo.owner / f"{repo.name}.git",
            checkouts_root=runtime.base_dir / "checkouts" / repo.owner / repo.name,
        )

    def ensure_layout(self) -> None:
        self.layout.mirror_path.parent.mkdir(parents=True, exist_ok=True)
        self.layout.checkouts_root.mkdir(parents=True, exist_ok=True)
        self._ensure_mirror()

    def ensure_checkout(self, slot: int) -> Path:
        checkout_path = self.slot_path(slot)
        remote_url = self.repo.effective_remote_url
        if not checkout_path.exists():
            log_event(
                LOGGER,
                "git_checkout_cloned",
                slot=slot,
                checkout_path=str(checkout_path),
            )
            run(
                [
                    "git",
                    "clone",
                    "--reference-if-able",
                    str(self.layout.mirror_path),
                    remote_url,
                    str(checkout_path),
                ]
            )
        else:
            log_event(
                LOGGER,
                "git_checkout_remote_set",
                slot=slot,
                checkout_path=str(checkout_path),
            )
            run(["git", "-C", str(checkout_path), "remote", "set-url", "origin", remote_url])
        return checkout_path

    def slot_path(self, slot: int) -> Path:
        return self.layout.checkouts_root / f"worker-{slot:02d}"

    def prepare_checkout(self, checkout_path: Path) -> None:
        log_event(
            LOGGER,
            "git_prepare_checkout",
            checkout_path=str(checkout_path),
            default_branch=self.repo.default_branch,
        )
        run(["git", "-C", str(checkout_path), "fetch", "origin", "--prune", "--tags"])
        run(
            [
                "git",
                "-C",
                str(checkout_path),
                "checkout",
                "-B",
                self.repo.default_branch,
                f"origin/{self.repo.default_branch}",
            ]
        )
        run(
            [
                "git",
                "-C",
                str(checkout_path),
                "reset",
                "--hard",
                f"origin/{self.repo.default_branch}",
            ]
        )
        run(["git", "-C", str(checkout_path), "clean", "-ffdx"])

    def create_or_reset_branch(self, checkout_path: Path, branch: str) -> None:
        log_event(
            LOGGER,
            "git_branch_reset",
            checkout_path=str(checkout_path),
            branch=branch,
        )
        run(["git", "-C", str(checkout_path), "checkout", "-B", branch])

    def list_staged_files(self, checkout_path: Path) -> tuple[str, ...]:
        run(["git", "-C", str(checkout_path), "add", "-A"])
        diff = run(["git", "-C", str(checkout_path), "diff", "--cached", "--name-only"]).strip()
        if not diff:
            return ()
        return tuple(line for line in diff.splitlines() if line.strip())

    def commit_all(self, checkout_path: Path, message: str) -> None:
        if not self.list_staged_files(checkout_path):
            raise RuntimeError("No staged changes to commit")
        log_event(
            LOGGER,
            "git_commit",
            checkout_path=str(checkout_path),
            has_message=bool(message.strip()),
        )
        run(["git", "-C", str(checkout_path), "commit", "-m", message])

    def push_branch(self, checkout_path: Path, branch: str) -> None:
        log_event(
            LOGGER,
            "git_push",
            checkout_path=str(checkout_path),
            branch=branch,
        )
        try:
            run(["git", "-C", str(checkout_path), "push", "-u", "origin", branch])
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "git_push_failed",
                checkout_path=str(checkout_path),
                branch=branch,
                error_type=type(exc).__name__,
            )
            raise

    def fetch_origin(self, checkout_path: Path) -> None:
        log_event(
            LOGGER,
            "git_fetch_origin",
            checkout_path=str(checkout_path),
        )
        run(["git", "-C", str(checkout_path), "fetch", "origin", "--prune", "--tags"])

    def merge_origin_default_branch(self, checkout_path: Path) -> None:
        log_event(
            LOGGER,
            "git_merge_origin_default_branch",
            checkout_path=str(checkout_path),
            default_branch=self.repo.default_branch,
        )
        run(
            [
                "git",
                "-C",
                str(checkout_path),
                "merge",
                "--no-edit",
                f"origin/{self.repo.default_branch}",
            ]
        )

    def cleanup_slot(self, checkout_path: Path) -> None:
        self.prepare_checkout(checkout_path)

    def restore_feedback_branch(
        self, checkout_path: Path, branch: str, expected_head_sha: str
    ) -> bool:
        log_event(
            LOGGER,
            "git_restore_feedback_branch",
            checkout_path=str(checkout_path),
            branch=branch,
        )
        self.prepare_checkout(checkout_path)
        self._checkout_remote_branch(checkout_path, branch)
        if self.current_head_sha(checkout_path) == expected_head_sha:
            return True

        # Retry once after a fresh fetch to handle race windows on remote updates.
        run(["git", "-C", str(checkout_path), "fetch", "origin", "--prune", "--tags"])
        self._checkout_remote_branch(checkout_path, branch)
        return self.current_head_sha(checkout_path) == expected_head_sha

    def current_head_sha(self, checkout_path: Path) -> str:
        return run(["git", "-C", str(checkout_path), "rev-parse", "HEAD"]).strip()

    def is_ancestor(self, checkout_path: Path, older_sha: str, newer_sha: str) -> bool:
        if older_sha == newer_sha:
            return True
        try:
            merge_base = run(
                ["git", "-C", str(checkout_path), "merge-base", older_sha, newer_sha]
            ).strip()
        except CommandError:
            # Missing or unrelated commits should be treated as non-ancestor in guard checks.
            return False
        return merge_base == older_sha

    def _checkout_remote_branch(self, checkout_path: Path, branch: str) -> None:
        run(["git", "-C", str(checkout_path), "checkout", "-B", branch, f"origin/{branch}"])
        run(["git", "-C", str(checkout_path), "reset", "--hard", f"origin/{branch}"])

    def _ensure_mirror(self) -> None:
        remote_url = self.repo.effective_remote_url
        source = self.repo.local_clone_source or remote_url
        if not self.layout.mirror_path.exists():
            log_event(
                LOGGER,
                "git_mirror_cloned",
                mirror_path=str(self.layout.mirror_path),
            )
            run(["git", "clone", "--mirror", source, str(self.layout.mirror_path)])

        log_event(
            LOGGER,
            "git_mirror_synced",
            mirror_path=str(self.layout.mirror_path),
        )
        run(
            [
                "git",
                f"--git-dir={self.layout.mirror_path}",
                "remote",
                "set-url",
                "origin",
                remote_url,
            ]
        )
        run(
            [
                "git",
                f"--git-dir={self.layout.mirror_path}",
                "fetch",
                "origin",
                "--prune",
                "--tags",
            ]
        )
