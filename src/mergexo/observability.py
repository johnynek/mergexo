from __future__ import annotations

import json
import logging
import sys
from typing import Final


_LOGGER_NAME: Final[str] = "mergexo"
_MAX_VALUE_LEN: Final[int] = 120
_VERBOSE_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s"


def configure_logging(verbose: bool) -> None:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.propagate = False
    logger.handlers.clear()

    if verbose:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(_VERBOSE_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return

    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.CRITICAL + 1)


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
