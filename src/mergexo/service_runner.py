from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import sys
from threading import Event, Lock, Thread
import time
import traceback
from typing import Callable, Literal, cast
from urllib.parse import urlparse

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, RepoConfig
from mergexo.feedback_loop import (
    append_action_token,
    compute_operator_command_token,
    extract_action_tokens,
)
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.models import (
    ContinuousDeployState,
    OperatorCommandRecord,
    OperatorReplyStatus,
    RestartMode,
)
from mergexo.observability import log_event, logging_repo_context
from mergexo.orchestrator import (
    GlobalWorkLimiter,
    Phase1Orchestrator,
    RestartRequested,
    _operator_normalized_command,
    _operator_reply_issue_number,
    _operator_source_comment_url,
    _render_operator_command_result,
)
from mergexo.shell import run
from mergexo.state import StateStore


LOGGER = logging.getLogger("mergexo.service_runner")
_RESTART_OPERATION_NAME = "restart"
_RESTART_PENDING_STATUSES = {"pending", "running"}
_CONTINUOUS_DEPLOY_REQUESTED_BY = "continuous_deploy"
_CONTINUOUS_COMMAND_KEY_PREFIX = "continuous:"
_DEFAULT_GITHUB_AUTH_SHUTDOWN_DETAIL = (
    "GitHub CLI is not authenticated. Run `gh auth login` and restart MergeXO."
)
_HEALTH_REASON_STARTING = "starting"
_HEALTH_REASON_AUTH_SHUTDOWN_PENDING = "auth_shutdown_pending"
_HEALTH_REASON_ROLLBACK_IN_PROGRESS = "rollback_in_progress"
_HEALTH_REASON_FATAL_ERROR = "fatal_error"


ServiceSignalKind = Literal[
    "poll_completed",
    "work_reaped",
    "restart_drain_state_changed",
    "fatal_error",
]


@dataclass(frozen=True)
class ServiceSignal:
    kind: ServiceSignalKind
    repo_full_name: str | None = None
    detail: str | None = None


@dataclass
class _ServiceHealthTracker:
    started_at_monotonic: float = field(default_factory=time.monotonic)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _poll_cycle_completed: bool = False
    _auth_shutdown_pending: bool = False
    _rollback_in_progress: bool = False
    _fatal_error: str | None = None
    _active_sha: str | None = None

    def set_poll_cycle_completed(self) -> None:
        with self._lock:
            self._poll_cycle_completed = True

    def set_auth_shutdown_pending(self, pending: bool) -> None:
        with self._lock:
            self._auth_shutdown_pending = pending

    def set_rollback_in_progress(self, in_progress: bool) -> None:
        with self._lock:
            self._rollback_in_progress = in_progress

    def set_fatal_error(self, detail: str) -> None:
        with self._lock:
            self._fatal_error = detail

    def set_active_sha(self, sha: str | None) -> None:
        normalized = sha.strip() if isinstance(sha, str) else ""
        with self._lock:
            self._active_sha = normalized or None

    def response_payload(self) -> tuple[int, dict[str, object]]:
        with self._lock:
            uptime_seconds = int(max(0.0, time.monotonic() - self.started_at_monotonic))
            if self._fatal_error is not None:
                return (
                    503,
                    {
                        "status": "unhealthy",
                        "reason": _HEALTH_REASON_FATAL_ERROR,
                        "uptime_seconds": uptime_seconds,
                    },
                )
            if self._rollback_in_progress:
                return (
                    503,
                    {
                        "status": "unhealthy",
                        "reason": _HEALTH_REASON_ROLLBACK_IN_PROGRESS,
                        "uptime_seconds": uptime_seconds,
                    },
                )
            if self._auth_shutdown_pending:
                return (
                    503,
                    {
                        "status": "unhealthy",
                        "reason": _HEALTH_REASON_AUTH_SHUTDOWN_PENDING,
                        "uptime_seconds": uptime_seconds,
                    },
                )
            if not self._poll_cycle_completed:
                return (
                    503,
                    {
                        "status": "unhealthy",
                        "reason": _HEALTH_REASON_STARTING,
                        "uptime_seconds": uptime_seconds,
                    },
                )
            return (
                200,
                {
                    "status": "healthy",
                    "active_sha": self._active_sha,
                    "uptime_seconds": uptime_seconds,
                },
            )


@dataclass(frozen=True)
class _HealthServerHandle:
    host: str
    port: int
    server: ThreadingHTTPServer
    thread: Thread


@dataclass(frozen=True)
class _ContinuousDeployCandidate:
    from_sha: str
    to_sha: str


@dataclass(frozen=True)
class ServiceRunner:
    config: AppConfig
    state: StateStore
    agent: AgentAdapter
    startup_argv: tuple[str, ...]
    agent_by_repo_full_name: dict[str, AgentAdapter] = field(default_factory=dict)
    github: GitHubGateway | None = None
    git_manager: GitRepoManager | None = None
    repo_runtimes: tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...] = ()
    stop_event: Event | None = None
    signal_sink: Callable[[ServiceSignal], None] | None = None

    def run(self, *, once: bool) -> None:
        health_tracker = _ServiceHealthTracker()
        health_server: _HealthServerHandle | None = None
        try:
            runtimes = self._effective_repo_runtimes()
            repo_count = len(runtimes)
            if repo_count < 1:
                raise RuntimeError("No repositories configured for service runner")
            health_tracker.set_active_sha(self._current_checkout_sha())
            health_server = self._start_health_server(health_tracker=health_tracker)
            if self._reconcile_continuous_deploy_startup(health_tracker=health_tracker):
                return
            github_by_repo_full_name = {repo.full_name: github for repo, github, _ in runtimes}
            work_limiter = GlobalWorkLimiter(self.config.runtime.worker_count)
            orchestrators = tuple(
                Phase1Orchestrator(
                    self.config,
                    state=self.state,
                    github=github,
                    git_manager=git_manager,
                    repo=repo,
                    github_by_repo_full_name=github_by_repo_full_name,
                    agent=self._agent_for_repo(repo),
                    allow_runtime_restart=True,
                    work_limiter=work_limiter,
                )
                for repo, github, git_manager in runtimes
            )

            with ThreadPoolExecutor(max_workers=self.config.runtime.worker_count) as pool:
                auth_shutdown_detail: str | None = None
                if once:
                    polls_completed_in_cycle = 0
                    for index, (repo, _, _) in enumerate(runtimes):
                        if self._should_stop():
                            return
                        self._poll_repo_once(
                            repo=repo,
                            orchestrator=orchestrators[index],
                            pool=pool,
                            work_limiter=work_limiter,
                            allow_enqueue=auth_shutdown_detail is None,
                        )
                        auth_shutdown_detail = self._update_auth_shutdown_detail(
                            current_detail=auth_shutdown_detail,
                            orchestrator=orchestrators[index],
                            repo=repo,
                        )
                        health_tracker.set_auth_shutdown_pending(auth_shutdown_detail is not None)
                        polls_completed_in_cycle += 1
                        if polls_completed_in_cycle >= repo_count:
                            polls_completed_in_cycle = 0
                            self._process_completed_poll_cycle(
                                health_tracker=health_tracker,
                                auth_shutdown_detail=auth_shutdown_detail,
                            )
                    if (
                        self._total_pending_futures(orchestrators) == 0
                        and not self._restart_operation_is_pending()
                        and auth_shutdown_detail is None
                    ):
                        return

                    restart_drain_started_at_monotonic: float | None = None
                    while True:
                        if self._should_stop():
                            return
                        for index, (repo, _, _) in enumerate(runtimes):
                            if self._should_stop():
                                return
                            self._poll_repo_once(
                                repo=repo,
                                orchestrator=orchestrators[index],
                                pool=pool,
                                work_limiter=work_limiter,
                                allow_enqueue=False,
                            )
                            auth_shutdown_detail = self._update_auth_shutdown_detail(
                                current_detail=auth_shutdown_detail,
                                orchestrator=orchestrators[index],
                                repo=repo,
                            )
                            health_tracker.set_auth_shutdown_pending(
                                auth_shutdown_detail is not None
                            )
                        self._process_completed_poll_cycle(
                            health_tracker=health_tracker,
                            auth_shutdown_detail=auth_shutdown_detail,
                        )
                        restart_drain_started_at_monotonic, should_exit = (
                            self._process_global_restart_drain(
                                orchestrators=orchestrators,
                                restart_drain_started_at_monotonic=restart_drain_started_at_monotonic,
                                exit_after_terminal=True,
                            )
                        )
                        if should_exit:
                            return
                        if (
                            auth_shutdown_detail is not None
                            and self._total_pending_futures(orchestrators) == 0
                        ):
                            raise RuntimeError(auth_shutdown_detail)
                        if self._total_pending_futures(orchestrators) == 0:
                            return
                        if self._wait_for_stop(0.1):
                            return

                poll_index = 0
                polls_completed_in_cycle = 0
                restart_drain_started_at_monotonic: float | None = None
                next_continuous_deploy_check_monotonic = time.monotonic()
                while True:
                    if self._should_stop():
                        return
                    repo, _, _ = runtimes[poll_index]
                    # When restart is pending we disable enqueue for every repo. We still call
                    # poll_once so each orchestrator can reap finished futures and checkpoint
                    # worker terminal state. That gives us a drain loop with no new GitHub work
                    # ingestion while preserving state consistency before supervisor re-exec.
                    self._poll_repo_once(
                        repo=repo,
                        orchestrator=orchestrators[poll_index],
                        pool=pool,
                        work_limiter=work_limiter,
                        allow_enqueue=(
                            auth_shutdown_detail is None
                            and not self._restart_operation_is_pending()
                        ),
                    )
                    auth_shutdown_detail = self._update_auth_shutdown_detail(
                        current_detail=auth_shutdown_detail,
                        orchestrator=orchestrators[poll_index],
                        repo=repo,
                    )
                    health_tracker.set_auth_shutdown_pending(auth_shutdown_detail is not None)
                    restart_drain_started_at_monotonic, should_exit = (
                        self._process_global_restart_drain(
                            orchestrators=orchestrators,
                            restart_drain_started_at_monotonic=restart_drain_started_at_monotonic,
                            exit_after_terminal=False,
                        )
                    )
                    if should_exit:
                        return
                    polls_completed_in_cycle += 1
                    if polls_completed_in_cycle >= repo_count:
                        polls_completed_in_cycle = 0
                        self._process_completed_poll_cycle(
                            health_tracker=health_tracker,
                            auth_shutdown_detail=auth_shutdown_detail,
                        )
                    next_continuous_deploy_check_monotonic = self._maybe_schedule_continuous_deploy(
                        enabled=not once,
                        orchestrators=orchestrators,
                        auth_shutdown_detail=auth_shutdown_detail,
                        next_check_deadline_monotonic=next_continuous_deploy_check_monotonic,
                    )
                    poll_index = (poll_index + 1) % repo_count
                    if auth_shutdown_detail is not None:
                        if self._total_pending_futures(orchestrators) == 0:
                            raise RuntimeError(auth_shutdown_detail)
                        if self._wait_for_stop(0.1):
                            return
                        continue
                    if self._wait_for_stop(self.config.runtime.poll_interval_seconds):
                        return
        except Exception as exc:
            self._record_continuous_deploy_boot_error(detail=traceback.format_exc())
            health_tracker.set_fatal_error(str(exc))
            self._emit_signal(ServiceSignal(kind="fatal_error", detail=str(exc)))
            raise
        finally:
            self._stop_health_server(health_server)

    def _continuous_deploy_enabled(self) -> bool:
        return self.config.runtime.continuous_deploy_enabled

    def _continuous_deploy_checkout_root(self) -> Path:
        return self.config.runtime.git_checkout_root or Path.cwd()

    def _continuous_deploy_branch(self) -> str:
        return self.config.runtime.continuous_deploy_branch

    def _continuous_deploy_command_key(self, *, target_sha: str) -> str:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return f"{_CONTINUOUS_COMMAND_KEY_PREFIX}{target_sha}:{timestamp}"

    def _is_continuous_deploy_command_key(self, command_key: str) -> bool:
        return command_key.startswith(_CONTINUOUS_COMMAND_KEY_PREFIX)

    def _current_checkout_sha(self) -> str | None:
        checkout_root = self._continuous_deploy_checkout_root()
        try:
            revision = run(["git", "-C", str(checkout_root), "rev-parse", "HEAD"]).strip()
        except Exception:
            return None
        return revision or None

    def _record_continuous_deploy_boot_error(self, *, detail: str) -> None:
        if not self._continuous_deploy_enabled():
            return
        normalized_detail = detail.strip()
        if not normalized_detail:
            return
        try:
            deploy_state = self.state.get_continuous_deploy_state()
            if deploy_state.status != "awaiting_health":
                return
            self.state.set_continuous_deploy_last_error(detail=normalized_detail)
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "continuous_deploy_error_persist_failed",
                error_type=type(exc).__name__,
            )

    def _process_completed_poll_cycle(
        self,
        *,
        health_tracker: _ServiceHealthTracker,
        auth_shutdown_detail: str | None,
    ) -> None:
        if auth_shutdown_detail is not None:
            return
        if self._restart_operation_is_pending():
            return
        active_sha = self._mark_continuous_deploy_healthy_if_ready()
        if active_sha is None:
            active_sha = self._current_checkout_sha()
        health_tracker.set_active_sha(active_sha)
        health_tracker.set_poll_cycle_completed()

    def _mark_continuous_deploy_healthy_if_ready(self) -> str | None:
        try:
            deploy_state = self.state.get_continuous_deploy_state()
        except Exception:
            return None
        if deploy_state.status != "awaiting_health":
            return deploy_state.active_sha
        active_sha = (
            self._current_checkout_sha() or deploy_state.target_sha or deploy_state.active_sha
        )
        updated = self.state.mark_continuous_deploy_healthy(active_sha=active_sha)
        log_event(
            LOGGER,
            "continuous_deploy_marked_healthy",
            target_sha=updated.target_sha,
            active_sha=updated.active_sha,
        )
        return updated.active_sha

    def _reconcile_continuous_deploy_startup(
        self, *, health_tracker: _ServiceHealthTracker
    ) -> bool:
        if not self._continuous_deploy_enabled():
            return False

        deploy_state = self.state.get_continuous_deploy_state()
        health_tracker.set_active_sha(deploy_state.active_sha or self._current_checkout_sha())
        if deploy_state.status != "awaiting_health":
            return False

        attempt = self.state.increment_continuous_deploy_boot_attempt()
        log_event(
            LOGGER,
            "continuous_deploy_boot_attempt_observed",
            boot_attempt_count=attempt.boot_attempt_count,
            target_sha=attempt.target_sha,
            previous_sha=attempt.previous_sha,
        )
        if attempt.boot_attempt_count <= self.config.runtime.continuous_deploy_max_boot_failures:
            return False

        reason_detail = (
            "continuous deploy health checks failed repeatedly; "
            f"boot_attempt_count={attempt.boot_attempt_count}, "
            f"max_boot_failures={self.config.runtime.continuous_deploy_max_boot_failures}"
        )
        return self._perform_continuous_deploy_rollback(
            attempt=attempt,
            error_detail=reason_detail,
            health_tracker=health_tracker,
        )

    def _maybe_schedule_continuous_deploy(
        self,
        *,
        enabled: bool,
        orchestrators: tuple[Phase1Orchestrator, ...],
        auth_shutdown_detail: str | None,
        next_check_deadline_monotonic: float,
    ) -> float:
        if not enabled or not self._continuous_deploy_enabled():
            return next_check_deadline_monotonic

        now = time.monotonic()
        if now < next_check_deadline_monotonic:
            return next_check_deadline_monotonic

        interval_seconds = self.config.runtime.continuous_deploy_check_interval_seconds
        if auth_shutdown_detail is not None:
            return next_check_deadline_monotonic
        if self._restart_operation_is_pending():
            return next_check_deadline_monotonic
        if self._total_pending_futures(orchestrators) != 0:
            return next_check_deadline_monotonic

        try:
            deploy_state = self.state.get_continuous_deploy_state()
            if deploy_state.status == "awaiting_health":
                return now + interval_seconds

            candidate = self._detect_continuous_deploy_candidate(
                blocked_target_sha=deploy_state.blocked_target_sha
            )
            if candidate is None:
                return now + interval_seconds
            self._request_continuous_deploy_restart(candidate=candidate)
            return now + interval_seconds
        except Exception as exc:  # noqa: BLE001
            log_event(
                LOGGER,
                "continuous_deploy_check_failed",
                error_type=type(exc).__name__,
                detail=str(exc),
            )
            return now + interval_seconds

    def _detect_continuous_deploy_candidate(
        self, *, blocked_target_sha: str | None
    ) -> _ContinuousDeployCandidate | None:
        checkout_root = self._continuous_deploy_checkout_root()
        branch = self._continuous_deploy_branch()
        run(
            [
                "git",
                "-C",
                str(checkout_root),
                "fetch",
                "origin",
                branch,
                "--prune",
                "--tags",
            ]
        )
        local_sha = run(["git", "-C", str(checkout_root), "rev-parse", "HEAD"]).strip()
        remote_sha = run(["git", "-C", str(checkout_root), "rev-parse", f"origin/{branch}"]).strip()
        if not local_sha:
            raise RuntimeError("git rev-parse HEAD returned empty revision")
        if not remote_sha:
            raise RuntimeError(f"git rev-parse origin/{branch} returned empty revision")
        if local_sha == remote_sha:
            return None
        if blocked_target_sha == remote_sha:
            log_event(
                LOGGER,
                "continuous_deploy_target_blocked",
                blocked_target_sha=blocked_target_sha,
            )
            return None

        merge_base = run(
            ["git", "-C", str(checkout_root), "merge-base", local_sha, remote_sha]
        ).strip()
        if merge_base != local_sha:
            log_event(
                LOGGER,
                "continuous_deploy_diverged_skip",
                local_sha=local_sha,
                remote_sha=remote_sha,
                merge_base=merge_base,
            )
            return None

        return _ContinuousDeployCandidate(from_sha=local_sha, to_sha=remote_sha)

    def _request_continuous_deploy_restart(self, *, candidate: _ContinuousDeployCandidate) -> None:
        command_key = self._continuous_deploy_command_key(target_sha=candidate.to_sha)
        operation, created = self.state.request_runtime_restart(
            requested_by=_CONTINUOUS_DEPLOY_REQUESTED_BY,
            request_command_key=command_key,
            mode="git_checkout",
            request_repo_full_name=self.config.repo.full_name,
        )
        if not created:
            log_event(
                LOGGER,
                "continuous_deploy_restart_not_created",
                command_key=command_key,
                existing_command_key=operation.request_command_key,
            )
            return

        deploy_state = self.state.start_continuous_deploy_attempt(
            previous_sha=candidate.from_sha,
            target_sha=candidate.to_sha,
        )
        log_event(
            LOGGER,
            "continuous_deploy_restart_requested",
            command_key=command_key,
            from_sha=candidate.from_sha,
            to_sha=candidate.to_sha,
            status=deploy_state.status,
        )

    def _perform_continuous_deploy_rollback(
        self,
        *,
        attempt: ContinuousDeployState,
        error_detail: str,
        health_tracker: _ServiceHealthTracker,
    ) -> bool:
        target_sha = (attempt.target_sha or "").strip()
        previous_sha = (attempt.previous_sha or "").strip()
        if not previous_sha:
            detail = (
                "continuous deploy rollback failed: missing previous_sha "
                f"(target_sha={target_sha or '<unknown>'})"
            )
            self.state.mark_continuous_deploy_failed(error=detail)
            raise RuntimeError(detail)

        health_tracker.set_rollback_in_progress(True)
        checkout_root = self._continuous_deploy_checkout_root()
        branch = self._continuous_deploy_branch()
        try:
            run(
                [
                    "git",
                    "-C",
                    str(checkout_root),
                    "fetch",
                    "origin",
                    branch,
                    "--prune",
                    "--tags",
                ]
            )
            run(
                [
                    "git",
                    "-C",
                    str(checkout_root),
                    "checkout",
                    "-B",
                    branch,
                    previous_sha,
                ]
            )
            run(["uv", "sync"], cwd=checkout_root)
        except Exception as exc:  # noqa: BLE001
            detail = f"continuous deploy rollback failed while restoring {previous_sha}: {exc}"
            self.state.mark_continuous_deploy_failed(error=detail)
            health_tracker.set_rollback_in_progress(False)
            log_event(
                LOGGER,
                "continuous_deploy_rollback_failed",
                target_sha=target_sha,
                previous_sha=previous_sha,
                error_type=type(exc).__name__,
            )
            raise RuntimeError(detail) from exc

        rollback_issue_error = self._report_continuous_deploy_rollback_issue(
            attempt=attempt,
            error_detail=error_detail,
        )
        final_error_detail = error_detail
        if rollback_issue_error is not None:
            final_error_detail = f"{error_detail}\n\nrollback_issue_error: {rollback_issue_error}"
        rolled_back = self.state.mark_continuous_deploy_rolled_back(error=final_error_detail)
        health_tracker.set_active_sha(rolled_back.active_sha)
        log_event(
            LOGGER,
            "continuous_deploy_rolled_back",
            target_sha=rolled_back.target_sha,
            previous_sha=rolled_back.previous_sha,
            blocked_target_sha=rolled_back.blocked_target_sha,
        )
        self._reexec()
        return True

    def _report_continuous_deploy_rollback_issue(
        self, *, attempt: ContinuousDeployState, error_detail: str
    ) -> str | None:
        github = self._github_for_repo(self.config.repo.full_name)
        target_sha = (attempt.target_sha or "<unknown>").strip() or "<unknown>"
        title = f"MergeXO auto-rollback for {target_sha[:12]}"
        body = self._render_continuous_deploy_rollback_issue(
            attempt=attempt,
            error_detail=error_detail,
        )
        try:
            created = github.create_issue(title=title, body=body, labels=None)
            log_event(
                LOGGER,
                "continuous_deploy_rollback_issue_created",
                issue_number=created.number,
                issue_url=created.html_url,
                target_sha=attempt.target_sha,
            )
            return None
        except Exception as exc:  # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
            log_event(
                LOGGER,
                "continuous_deploy_rollback_issue_failed",
                target_sha=attempt.target_sha,
                detail=detail,
            )
            return detail

    def _render_continuous_deploy_rollback_issue(
        self, *, attempt: ContinuousDeployState, error_detail: str
    ) -> str:
        failure_detail = (attempt.last_error or "").strip() or "<none captured>"
        return "\n".join(
            (
                "MergeXO rolled back an automatic continuous deploy attempt.",
                "",
                f"- target_sha: {attempt.target_sha or '<unknown>'}",
                f"- previous_sha: {attempt.previous_sha or '<unknown>'}",
                f"- boot_attempt_count: {attempt.boot_attempt_count}",
                "",
                "Rollback trigger detail:",
                "```",
                error_detail.strip() or "<none>",
                "```",
                "",
                "Captured failure detail:",
                "```",
                failure_detail,
                "```",
            )
        )

    def _start_health_server(
        self, *, health_tracker: _ServiceHealthTracker
    ) -> _HealthServerHandle | None:
        port = self.config.runtime.continuous_deploy_healthcheck_port
        if port == 0:
            return None
        host = self.config.runtime.continuous_deploy_healthcheck_host
        tracker = health_tracker

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if urlparse(self.path).path != "/healthz":
                    self.send_error(404)
                    return
                status_code, payload = tracker.response_payload()
                body = json.dumps(payload, sort_keys=True).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = format, args
                return

        server = ThreadingHTTPServer((host, port), HealthHandler)
        thread = Thread(target=server.serve_forever, name="mergexo-healthz", daemon=True)
        thread.start()
        log_event(
            LOGGER,
            "service_healthcheck_started",
            host=host,
            port=port,
        )
        return _HealthServerHandle(host=host, port=port, server=server, thread=thread)

    def _stop_health_server(self, health_server: _HealthServerHandle | None) -> None:
        if health_server is None:
            return
        health_server.server.shutdown()
        health_server.server.server_close()
        health_server.thread.join(timeout=2.0)

    def _wait_for_stop(self, timeout_seconds: float) -> bool:
        if self.stop_event is None:
            time.sleep(timeout_seconds)
            return False
        return self.stop_event.wait(timeout_seconds)

    def _should_stop(self) -> bool:
        return self.stop_event.is_set() if self.stop_event is not None else False

    def _emit_signal(self, signal: ServiceSignal) -> None:
        if self.signal_sink is None:
            return
        try:
            self.signal_sink(signal)
        except Exception:
            LOGGER.exception("service_signal_sink_failed")

    def _poll_repo_once(
        self,
        *,
        repo: RepoConfig,
        orchestrator: Phase1Orchestrator,
        pool: ThreadPoolExecutor,
        work_limiter: GlobalWorkLimiter,
        allow_enqueue: bool,
    ) -> None:
        with logging_repo_context(repo.full_name):
            pending_before = orchestrator.pending_work_count()
            orchestrator.poll_once(pool, allow_enqueue=allow_enqueue)
            pending_after = orchestrator.pending_work_count()
            running_issue_count, running_feedback_count = orchestrator.queue_counts()
            log_event(
                LOGGER,
                "service_repo_polled",
                repo_full_name=repo.full_name,
                allow_enqueue=allow_enqueue,
                running_issue_count=running_issue_count,
                running_feedback_count=running_feedback_count,
            )
            log_event(
                LOGGER,
                "service_global_capacity",
                in_flight=work_limiter.in_flight(),
                repo_pending_future_count=pending_after,
            )
            self._emit_signal(ServiceSignal(kind="poll_completed", repo_full_name=repo.full_name))
            if pending_after < pending_before:
                self._emit_signal(ServiceSignal(kind="work_reaped", repo_full_name=repo.full_name))

    def _update_auth_shutdown_detail(
        self,
        *,
        current_detail: str | None,
        orchestrator: object,
        repo: RepoConfig,
    ) -> str | None:
        if current_detail is not None:
            return current_detail
        if not self._orchestrator_github_auth_shutdown_pending(orchestrator):
            return None
        detail = self._orchestrator_github_auth_shutdown_reason(orchestrator)
        log_event(
            LOGGER,
            "service_github_auth_shutdown_started",
            repo_full_name=repo.full_name,
            detail=detail,
        )
        return detail

    def _orchestrator_github_auth_shutdown_pending(self, orchestrator: object) -> bool:
        pending_getter = getattr(orchestrator, "github_auth_shutdown_pending", None)
        if not callable(pending_getter):
            return False
        try:
            return bool(pending_getter())
        except Exception:
            return False

    def _orchestrator_github_auth_shutdown_reason(self, orchestrator: object) -> str:
        reason_getter = getattr(orchestrator, "github_auth_shutdown_reason", None)
        if callable(reason_getter):
            try:
                reason = str(reason_getter()).strip()
            except Exception:
                reason = ""
            if reason:
                return reason
        return _DEFAULT_GITHUB_AUTH_SHUTDOWN_DETAIL

    def _total_pending_futures(self, orchestrators: tuple[Phase1Orchestrator, ...]) -> int:
        return sum(orchestrator.pending_work_count() for orchestrator in orchestrators)

    def _restart_operation_is_pending(self) -> bool:
        operation = self.state.get_runtime_operation(_RESTART_OPERATION_NAME)
        return operation is not None and operation.status in _RESTART_PENDING_STATUSES

    def _process_global_restart_drain(
        self,
        *,
        orchestrators: tuple[Phase1Orchestrator, ...],
        restart_drain_started_at_monotonic: float | None,
        exit_after_terminal: bool,
    ) -> tuple[float | None, bool]:
        operation = self.state.get_runtime_operation(_RESTART_OPERATION_NAME)
        if operation is None or operation.status not in _RESTART_PENDING_STATUSES:
            return None, False

        with logging_repo_context(operation.request_repo_full_name):
            pending_futures = self._total_pending_futures(orchestrators)
            # Each orchestrator poll reaps finished futures and writes terminal worker state
            # into sqlite. During this drain phase, operator command scans and work enqueue are
            # already paused by restart-pending gates in the orchestrators, so we are not
            # consuming new messages. Waiting for this global count to reach zero gives us a
            # safe restart boundary with no in-flight worker state left uncheckpointed.
            now = time.monotonic()
            if restart_drain_started_at_monotonic is None:
                restart_drain_started_at_monotonic = now
                self._emit_signal(
                    ServiceSignal(
                        kind="restart_drain_state_changed",
                        repo_full_name=operation.request_repo_full_name,
                        detail="started",
                    )
                )
                log_event(
                    LOGGER,
                    "service_restart_drain_started",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                )

            if pending_futures == 0:
                self._emit_signal(
                    ServiceSignal(
                        kind="restart_drain_state_changed",
                        repo_full_name=operation.request_repo_full_name,
                        detail="completed",
                    )
                )
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="running",
                    detail="workers drained; supervisor update running",
                )
                log_event(
                    LOGGER,
                    "service_restart_drain_completed",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                )
                restarted = self._handle_restart_requested(
                    requested=RestartRequested(
                        mode=operation.mode,
                        command_key=operation.request_command_key,
                        repo_full_name=operation.request_repo_full_name,
                    )
                )
                if restarted:
                    return None, True
                return None, exit_after_terminal

            elapsed = now - restart_drain_started_at_monotonic
            drain_timeout = self.config.runtime.restart_drain_timeout_seconds
            if elapsed >= drain_timeout:
                self._emit_signal(
                    ServiceSignal(
                        kind="restart_drain_state_changed",
                        repo_full_name=operation.request_repo_full_name,
                        detail="failed",
                    )
                )
                detail = (
                    f"Restart drain timed out after {drain_timeout} seconds with "
                    f"{pending_futures} pending future(s)."
                )
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="failed",
                    detail=detail,
                )
                command = self.state.update_operator_command_result(
                    command_key=operation.request_command_key,
                    status="failed",
                    result=detail,
                    repo_full_name=operation.request_repo_full_name,
                )
                if command is not None:
                    self._post_operator_command_result(
                        command=command,
                        reply_status="failed",
                        detail=detail,
                    )
                log_event(
                    LOGGER,
                    "operator_command_failed",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    command="restart",
                    reason="drain_timeout",
                    pending_futures=pending_futures,
                    timeout_seconds=drain_timeout,
                )
                log_event(
                    LOGGER,
                    "service_restart_drain_timeout",
                    command_key=operation.request_command_key,
                    actor=operation.requested_by,
                    mode=operation.mode,
                    pending_futures=pending_futures,
                    timeout_seconds=drain_timeout,
                )
                return None, exit_after_terminal

            log_event(
                LOGGER,
                "service_restart_drain_progress",
                command_key=operation.request_command_key,
                actor=operation.requested_by,
                mode=operation.mode,
                pending_futures=pending_futures,
                elapsed_seconds=round(elapsed, 3),
            )
            return restart_drain_started_at_monotonic, False

    def _handle_restart_requested(self, *, requested: RestartRequested) -> bool:
        command_key = requested.command_key
        mode = requested.mode
        request_repo_full_name = requested.repo_full_name
        git_branch: str | None = None
        if mode == "git_checkout" and self._is_continuous_deploy_command_key(command_key):
            git_branch = self._continuous_deploy_branch()
        with logging_repo_context(request_repo_full_name):
            log_event(
                LOGGER,
                "restart_update_started",
                command_key=command_key,
                mode=mode,
            )
            try:
                self._run_update(mode=mode, git_branch=git_branch)
            except Exception as exc:  # noqa: BLE001
                detail = f"Restart update failed in mode={mode}: {exc}"
                self.state.set_runtime_operation_status(
                    op_name=_RESTART_OPERATION_NAME,
                    status="failed",
                    detail=detail,
                )
                record = self.state.update_operator_command_result(
                    command_key=command_key,
                    status="failed",
                    result=detail,
                    repo_full_name=request_repo_full_name or None,
                )
                if record is not None:
                    self._post_operator_command_result(
                        command=record,
                        reply_status="failed",
                        detail=detail,
                    )
                log_event(
                    LOGGER,
                    "operator_command_failed",
                    command_key=command_key,
                    actor=record.author_login if record is not None else "<unknown>",
                    command="restart",
                )
                log_event(
                    LOGGER,
                    "restart_update_failed",
                    command_key=command_key,
                    mode=mode,
                )
                return False

            detail = f"Restart update completed in mode={mode}; re-executing service command."
            self.state.set_runtime_operation_status(
                op_name=_RESTART_OPERATION_NAME,
                status="completed",
                detail=detail,
            )
            record = self.state.update_operator_command_result(
                command_key=command_key,
                status="applied",
                result=detail,
                repo_full_name=request_repo_full_name or None,
            )
            if record is not None:
                self._post_operator_command_result(
                    command=record, reply_status="applied", detail=detail
                )
            log_event(
                LOGGER,
                "operator_command_applied",
                command_key=command_key,
                actor=record.author_login if record is not None else "<unknown>",
                command="restart",
                mode=mode,
            )
            log_event(
                LOGGER,
                "restart_completed",
                command_key=command_key,
                mode=mode,
            )
            self._reexec()
            return True

    def _run_update(self, *, mode: RestartMode, git_branch: str | None = None) -> None:
        if mode not in self.config.runtime.restart_supported_modes:
            supported = ", ".join(self.config.runtime.restart_supported_modes)
            raise RuntimeError(f"Mode {mode} is not supported. Enabled modes: {supported}")

        if mode == "git_checkout":
            checkout_root = self.config.runtime.git_checkout_root or Path.cwd()
            branch = git_branch or self.config.repo.default_branch
            run(
                [
                    "git",
                    "-C",
                    str(checkout_root),
                    "pull",
                    "--ff-only",
                    "origin",
                    branch,
                ]
            )
            run(["uv", "sync"], cwd=checkout_root)
            return

        service_python = self.config.runtime.service_python
        if service_python is None:
            raise RuntimeError("runtime.service_python is required for mode=pypi")
        run(["uv", "pip", "install", "--python", service_python, "--upgrade", "mergexo"])

    def _post_operator_command_result(
        self,
        *,
        command: OperatorCommandRecord,
        reply_status: OperatorReplyStatus,
        detail: str,
    ) -> None:
        default_repo_full_name = command.repo_full_name or self.config.repo.full_name
        with logging_repo_context(default_repo_full_name):
            github = self._github_for_repo(command.repo_full_name)
            issue_number = _operator_reply_issue_number(command)
            token = compute_operator_command_token(command_key=command.command_key)
            if self._issue_has_action_token(github=github, issue_number=issue_number, token=token):
                return
            body = _render_operator_command_result(
                normalized_command=_operator_normalized_command(command),
                status=reply_status,
                detail=detail,
                source_comment_url=_operator_source_comment_url(
                    command=command,
                    repo_full_name=default_repo_full_name,
                ),
            )
            github.post_issue_comment(
                issue_number=issue_number,
                body=append_action_token(body=body, token=token),
            )

    def _issue_has_action_token(
        self, *, github: GitHubGateway, issue_number: int, token: str
    ) -> bool:
        comments = github.list_issue_comments(issue_number)
        return any(token in extract_action_tokens(comment.body) for comment in comments)

    def _effective_repo_runtimes(
        self,
    ) -> tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...]:
        if self.repo_runtimes:
            return self.repo_runtimes
        if self.github is not None and self.git_manager is not None:
            return ((self.config.repo, self.github, self.git_manager),)
        runtimes: list[tuple[RepoConfig, GitHubGateway, GitRepoManager]] = []
        for repo in self.config.repos:
            runtimes.append(
                (
                    repo,
                    GitHubGateway(repo.owner, repo.name),
                    GitRepoManager(self.config.runtime, repo),
                )
            )
        return tuple(runtimes)

    def _agent_for_repo(self, repo: RepoConfig) -> AgentAdapter:
        return self.agent_by_repo_full_name.get(repo.full_name, self.agent)

    def _github_for_repo(self, repo_full_name: str) -> GitHubGateway:
        if repo_full_name:
            for repo, github, _ in self._effective_repo_runtimes():
                if repo.full_name == repo_full_name:
                    return github
        if self.github is not None:
            return self.github
        # Fallback to first configured runtime for defensive paths.
        runtimes = self._effective_repo_runtimes()
        return runtimes[0][1]

    def _reexec(self) -> None:
        if not self.startup_argv:
            raise RuntimeError("Cannot restart service without startup argv")
        argv = list(self.startup_argv)
        os.execvpe(argv[0], argv, os.environ.copy())


def run_service(
    *,
    config: AppConfig,
    state: StateStore,
    agent: AgentAdapter,
    agent_by_repo_full_name: dict[str, AgentAdapter] | None = None,
    once: bool,
    github: GitHubGateway | None = None,
    git_manager: GitRepoManager | None = None,
    repo_runtimes: tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...] | None = None,
    startup_argv: tuple[str, ...] | None = None,
    stop_event: Event | None = None,
    signal_sink: Callable[[ServiceSignal], None] | None = None,
) -> None:
    argv = startup_argv or tuple(sys.argv)
    runner = ServiceRunner(
        config=config,
        state=state,
        agent=agent,
        agent_by_repo_full_name=cast(dict[str, AgentAdapter], dict(agent_by_repo_full_name or {})),
        startup_argv=argv,
        github=github,
        git_manager=git_manager,
        repo_runtimes=repo_runtimes or (),
        stop_event=stop_event,
        signal_sink=signal_sink,
    )
    runner.run(once=once)
