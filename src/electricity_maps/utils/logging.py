"""Structured logging configuration for the ETL pipeline.

Uses ``structlog`` for rich, structured log output in development
and standard ``logging`` for library compatibility.
"""

from __future__ import annotations

import logging
import sys

import structlog


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured logging for the pipeline.

    Call once at application startup (e.g. in a DAG or notebook).
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging so httpx/tenacity logs are visible
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=level,
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)
