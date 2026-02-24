from __future__ import annotations

from queue import SimpleQueue
import sys
from threading import Event, Thread

from mergexo.agent_adapter import AgentAdapter
from mergexo.config import AppConfig, RepoConfig
from mergexo.github_gateway import GitHubGateway
from mergexo.git_ops import GitRepoManager
from mergexo.observability_tui import run_observability_tui
from mergexo.service_runner import ServiceSignal, run_service
from mergexo.state import StateStore


def run_default_mode(
    *,
    config: AppConfig,
    state: StateStore,
    agent: AgentAdapter,
    agent_by_repo_full_name: dict[str, AgentAdapter],
    repo_runtimes: tuple[tuple[RepoConfig, GitHubGateway, GitRepoManager], ...],
    startup_argv: tuple[str, ...] | None = None,
) -> None:
    if not _is_interactive_terminal():
        raise RuntimeError(
            "Console mode requires an interactive terminal. Use `mergexo service` instead."
        )

    stop_event = Event()
    service_signals: SimpleQueue[ServiceSignal] = SimpleQueue()
    service_errors: list[BaseException] = []

    def service_signal_sink(signal: ServiceSignal) -> None:
        service_signals.put(signal)

    def run_service_thread() -> None:
        try:
            run_service(
                config=config,
                state=state,
                repo_runtimes=repo_runtimes,
                agent=agent,
                agent_by_repo_full_name=agent_by_repo_full_name,
                once=False,
                startup_argv=startup_argv,
                stop_event=stop_event,
                signal_sink=service_signal_sink,
            )
        except BaseException as exc:  # noqa: BLE001
            service_errors.append(exc)
            stop_event.set()

    service_thread = Thread(target=run_service_thread, name="mergexo-service", daemon=True)
    service_thread.start()

    tui_error: BaseException | None = None
    try:
        run_observability_tui(
            db_path=config.runtime.base_dir / "state.db",
            refresh_seconds=config.runtime.observability_refresh_seconds,
            default_window=config.runtime.observability_default_window,
            row_limit=config.runtime.observability_row_limit,
            service_signal_queue=service_signals,
            on_shutdown=stop_event.set,
        )
    except BaseException as exc:  # noqa: BLE001
        tui_error = exc
    finally:
        stop_event.set()
        service_thread.join(timeout=_service_join_timeout_seconds(config))

    if service_thread.is_alive():
        raise RuntimeError("Service thread did not stop after observability UI shutdown.")
    if service_errors:
        error = service_errors[0]
        if isinstance(error, Exception):
            raise error
        raise RuntimeError("Service thread failed with a non-Exception error.") from error
    if tui_error is not None:
        raise tui_error


def _service_join_timeout_seconds(config: AppConfig) -> float:
    return max(1.0, float(config.runtime.poll_interval_seconds) + 5.0)


def _is_interactive_terminal() -> bool:
    return bool(sys.stdin.isatty() and sys.stdout.isatty())
