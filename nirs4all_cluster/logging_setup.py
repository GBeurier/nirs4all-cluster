"""Structured logging setup for the server and worker daemons.

Library code uses module loggers (``logging.getLogger("nirs4all_cluster.…")``)
and never configures handlers; the ``server``/``worker`` CLI entry points call
``configure_logging`` exactly once. CLI command output (``status``/``logs``/``jobs``…)
stays on ``print`` — that is user-facing UX, not operational logging.
"""

from __future__ import annotations

import logging
from typing import Literal, TypedDict

LOG_LEVELS = ("critical", "error", "warning", "info", "debug")


class _QuietUvicornProfile(TypedDict):
    log_config: None
    log_level: Literal["warning"]
    access_log: Literal[False]


QUIET_UVICORN_PROFILE: _QuietUvicornProfile = {"log_config": None, "log_level": "warning", "access_log": False}

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


class _OwnedDiagnostics(logging.Filter):
    """Enforce the namespace allow-list at every owned output handler."""

    def __init__(self, level: int):
        super().__init__()
        self.application_level = level
        self.third_party_level = max(logging.WARNING, level)

    def filter(self, record: logging.LogRecord) -> bool:
        application = record.name == "nirs4all_cluster" or record.name.startswith("nirs4all_cluster.")
        return record.levelno >= (self.application_level if application else self.third_party_level)


def configure_logging(level: str = "info", log_file: str | None = None) -> None:
    """Configure owned daemon diagnostics; raw transport DEBUG/INFO stays disabled."""
    if level not in LOG_LEVELS:
        raise ValueError("invalid application log level")
    numeric_level = getattr(logging, level.upper())
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    for handler in handlers:
        handler.addFilter(_OwnedDiagnostics(numeric_level))
    logging.basicConfig(
        level=max(logging.WARNING, numeric_level),
        format=_FORMAT,
        datefmt=_DATEFMT,
        handlers=handlers,
        force=True,
    )
    logging.getLogger("nirs4all_cluster").setLevel(numeric_level)
