"""
SAQ Structured Logging

Provides JSON-formatted logging for integration with ELK/Loki/Datadog etc.

Usage:
    from saq.logging import setup

    # Enable JSON logging
    setup(format="json", level="INFO")

    # Or via environment variable
    # SAQ_LOG_FORMAT=json SAQ_LOG_LEVEL=DEBUG python -m saq settings
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Fields that belong to the standard LogRecord and should not be forwarded
_LOG_RECORD_FIELDS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | frozenset(
    ("message", "asctime", "args", "exc_info", "exc_text", "stack_info", "taskName")
)


class JSONFormatter(logging.Formatter):
    """Format LogRecord as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _LOG_RECORD_FIELDS:
                continue
            try:
                json.dumps({key: value})
                log_entry[key] = value
            except (TypeError, ValueError):
                log_entry[key] = str(value)

        if record.exc_info and isinstance(record.exc_info, tuple) and record.exc_info[1]:
            log_entry["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


def setup(
    format: str = "text",
    level: str = "INFO",
    logger_name: str = "saq",
) -> None:
    """
    Configure SAQ logging.

    Args:
        format: 'text' (default plain text) or 'json' (structured JSON).
        level: Log level name (DEBUG, INFO, WARNING, ERROR).
        logger_name: Logger to configure.
    """
    saq_logger = logging.getLogger(logger_name)
    saq_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)

    if format == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )

    saq_logger.addHandler(handler)
    saq_logger.setLevel(getattr(logging, level.upper()))


def auto_setup() -> None:
    """Auto-configure logging from environment variables."""
    fmt = os.environ.get("SAQ_LOG_FORMAT", "text")
    level = os.environ.get("SAQ_LOG_LEVEL", "INFO")
    setup(format=fmt, level=level)
