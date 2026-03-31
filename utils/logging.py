"""
Structured logging setup using structlog.

Inputs: Optional log level override.
Outputs: Configured structlog logger ready for use across all modules.
Assumptions: Called once at startup before any logging occurs.
"""

import logging
import os
import sys

import structlog


def setup_logging(level: str | None = None) -> None:
    """
    Configure structlog with JSON output for production, pretty console for dev.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR). Falls back to
               LOG_LEVEL env var, then INFO.
    """
    log_level = getattr(logging, (level or os.getenv("LOG_LEVEL", "INFO")).upper())

    # Shared processors for both structlog and stdlib
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Use pretty console output if attached to a terminal, JSON otherwise
    if sys.stderr.isatty():
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging to use structlog formatting
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)
