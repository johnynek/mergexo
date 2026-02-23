from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
from typing import Final, Literal, TextIO, cast


_LOGGER_NAME: Final[str] = "mergexo"
_MAX_VALUE_LEN: Final[int] = 120
_VERBOSE_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s"
_LOW_VERBOSITY_EVENTS: Final[frozenset[str]] = frozenset(
    {
        "issue_enqueued",
        "github_pr_created",
        "monitored_comment_detected",
        "codex_invocation_started",
        "codex_invocation_finished",
        "git_push_failed",
        "github_pr_create_failed",
        "github_issue_comment_failed",
        "github_review_reply_failed",
    }
)


VerboseMode = Literal["low", "high"]


def configure_logging(
    verbose: bool | str | None,
    *,
    state_dir: Path | None = None,
) -> None:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.propagate = False
    logger.handlers.clear()
    mode = _normalize_verbose_mode(verbose)

    if mode is None:
        logger.addHandler(logging.NullHandler())
        logger.setLevel(logging.CRITICAL + 1)
        return

    stream_handler = logging.StreamHandler(sys.stderr)
    _configure_handler(stream_handler, mode)
    logger.addHandler(stream_handler)

    if state_dir is not None:
        file_handler = _UtcDailyFileHandler(base_dir=state_dir)
        _configure_handler(file_handler, mode)
        logger.addHandler(file_handler)

    logger.setLevel(logging.INFO)


def log_event(logger: logging.Logger, event: str, **fields: object) -> None:
    logger.info(_build_event_message(event=event, fields=fields))


def _build_event_message(*, event: str, fields: dict[str, object]) -> str:
    parts = [f"event={_normalize_field_value(event)}"]
    for key in sorted(fields.keys()):
        parts.append(f"{key}={_normalize_field_value(fields[key])}")
    return " ".join(parts)


def _normalize_field_value(value: object) -> str:
    if value is None:
        normalized = "null"
    elif isinstance(value, bool):
        normalized = "true" if value else "false"
    elif isinstance(value, int | float):
        normalized = str(value)
    elif isinstance(value, str):
        collapsed = " ".join(value.split())
        if len(collapsed) > _MAX_VALUE_LEN:
            collapsed = f"{collapsed[:_MAX_VALUE_LEN]}..."
        normalized = collapsed if collapsed else "<empty>"
    else:
        normalized = f"<{type(value).__name__}>"

    if any(ch.isspace() for ch in normalized) or "=" in normalized:
        return json.dumps(normalized)
    return normalized


def _normalize_verbose_mode(verbose: bool | str | None) -> VerboseMode | None:
    if verbose is None:
        return None
    if isinstance(verbose, bool):
        return "high" if verbose else None
    normalized = verbose.strip().lower()
    if normalized in {"low", "high"}:
        return cast(VerboseMode, normalized)
    raise ValueError(f"Unsupported verbose mode: {verbose!r}")


def _configure_handler(handler: logging.Handler, mode: VerboseMode) -> None:
    handler.setFormatter(logging.Formatter(_VERBOSE_FORMAT))
    if mode == "low":
        handler.addFilter(_LowVerbosityFilter())


def _extract_event_name(message: str) -> str | None:
    if not message.startswith("event="):
        return None
    first_field = message.split(" ", 1)[0]
    if first_field == "event=":
        return None
    return first_field[len("event=") :]


class _LowVerbosityFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return True
        event_name = _extract_event_name(record.getMessage())
        return event_name in _LOW_VERBOSITY_EVENTS


class _UtcDailyFileHandler(logging.Handler):
    def __init__(self, *, base_dir: Path) -> None:
        super().__init__()
        self._logs_dir = base_dir / "logs"
        self._stream: TextIO | None = None
        self._active_date = ""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            stream = self._stream_for_current_date()
            stream.write(f"{self.format(record)}\n")
            stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        self.acquire()
        try:
            self._close_stream()
            super().close()
        finally:
            self.release()

    def _stream_for_current_date(self) -> TextIO:
        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._stream is None or self._active_date != date_key:
            self._close_stream()
            self._logs_dir.mkdir(parents=True, exist_ok=True)
            path = self._logs_dir / f"{date_key}.log"
            self._stream = path.open("a", encoding="utf-8")
            self._active_date = date_key
        return self._stream

    def _close_stream(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None
