from __future__ import annotations

import io
import logging
import sys

import pytest

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
