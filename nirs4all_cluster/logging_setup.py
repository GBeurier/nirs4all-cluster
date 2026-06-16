"""Structured logging setup for the server and worker daemons.

Library code uses module loggers (``logging.getLogger("nirs4all_cluster.…")``)
and never configures handlers; the ``server``/``worker`` CLI entry points call
``configure_logging`` exactly once. CLI command output (``status``/``logs``/``jobs``…)
stays on ``print`` — that is user-facing UX, not operational logging.
"""

from __future__ import annotations

import logging

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: str = "info", log_file: str | None = None) -> None:
    """Configure root logging once. Streams to stderr and, optionally, to a file."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=_FORMAT,
        datefmt=_DATEFMT,
        handlers=handlers,
        force=True,
    )
