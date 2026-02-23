from __future__ import annotations

from datetime import datetime, timezone
import io
import logging
from pathlib import Path
import sys

import pytest

from mergexo import observability
from mergexo.observability import configure_logging, log_event


@pytest.fixture(autouse=True)
def restore_mergexo_logger_state() -> None:
    logger = logging.getLogger("mergexo")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    try:
        yield
    finally:
        for handler in logger.handlers:
            if handler not in original_handlers:
                handler.close()
        logger.handlers.clear()
        for handler in original_handlers:
            logger.addHandler(handler)
        logger.setLevel(original_level)
        logger.propagate = original_propagate


def test_configure_logging_quiet_mode_is_idempotent() -> None:
    configure_logging(verbose=False)
    logger = logging.getLogger("mergexo")
    assert logger.propagate is False
    assert logger.level > logging.CRITICAL
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.NullHandler)

    configure_logging(verbose=False)
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.NullHandler)


def test_configure_logging_none_mode_is_quiet() -> None:
    configure_logging(verbose=None)
    logger = logging.getLogger("mergexo")
    assert logger.propagate is False
    assert logger.level > logging.CRITICAL
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.NullHandler)


def test_configure_logging_verbose_mode_is_idempotent() -> None:
    configure_logging(verbose=True)
    logger = logging.getLogger("mergexo")
    assert logger.propagate is False
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr
    assert handler.formatter is not None
    assert "%(threadName)s" in handler.formatter._fmt

    configure_logging(verbose=True)
    assert len(logger.handlers) == 1


def test_configure_logging_low_mode_filters_to_high_signal_events(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(verbose="low")
    logger = logging.getLogger("mergexo.tests.low")

    logger.info("event=poll_started once=true")
    logger.info("event=issue_enqueued issue_number=1")
    logger.info("plain_message=ignored")
    logger.info("event=")
    logger.error("event=command_failed command=git push")

    stderr = capsys.readouterr().err
    assert "event=poll_started" not in stderr
    assert "event=issue_enqueued issue_number=1" in stderr
    assert "plain_message=ignored" not in stderr
    assert all(not line.endswith("event=") for line in stderr.splitlines())
    assert "event=command_failed command=git push" in stderr


def test_configure_logging_writes_utc_daily_file(
    tmp_path: Path,
) -> None:
    configure_logging(verbose="high", state_dir=tmp_path)
    logger = logging.getLogger("mergexo.tests.file")
    logger.info("event=issue_enqueued issue_number=2")

    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_path = tmp_path / "logs" / f"{date_key}.log"
    assert log_path.exists()
    assert "event=issue_enqueued issue_number=2" in log_path.read_text(encoding="utf-8")


def test_configure_logging_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported verbose mode"):
        configure_logging(verbose="noisy")


def test_utc_daily_file_handler_handles_emit_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = observability._UtcDailyFileHandler(base_dir=Path("/tmp"))
    called: dict[str, object] = {}

    monkeypatch.setattr(
        handler,
        "_stream_for_current_date",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(handler, "handleError", lambda record: called.setdefault("record", record))

    record = logging.LogRecord(
        name="mergexo.tests.observability",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="event=issue_enqueued issue_number=1",
        args=(),
        exc_info=None,
    )
    handler.emit(record)
    assert "record" in called


def test_log_event_formats_and_normalizes_fields() -> None:
    logger = logging.getLogger("mergexo.tests.observability")
    logger.handlers.clear()
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_event(
        logger,
        "test_event",
        b=2,
        a="multi\nline value",
        none_value=None,
        bool_value=True,
        empty="   ",
        long_text="x" * 121,
        complex_value={"k": "v"},
    )

    message = stream.getvalue().strip()
    assert message.startswith("event=test_event ")
    # sorted field order
    assert message.index("a=") < message.index("b=")
    assert 'a="multi line value"' in message
    assert "b=2" in message
    assert "none_value=null" in message
    assert "bool_value=true" in message
    assert "empty=<empty>" in message
    assert "complex_value=<dict>" in message
    assert "long_text=" in message
    assert "..." in message
    logger.handlers.clear()
