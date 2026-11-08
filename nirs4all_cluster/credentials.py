"""Cluster raw bearer files and nonsecret diagnostics; shared policy stays in the leaf."""

import os
import re
import stat
from pathlib import Path

from nirs4all_access import hash_token

_MAX_TOKEN_FILE_BYTES = 1024


def read_bearer_file(path: str | Path) -> str:
    """Read one bounded private regular file without blocking on FIFOs/devices."""
    fd = None
    try:
        fd = os.open(
            path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0)
        )
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or (os.name == "posix" and info.st_mode & 0o077):
            raise ValueError("token file must be a private regular file (0600)")
        with os.fdopen(fd, "rb") as stream:
            fd = None  # ownership transferred, including exceptions during read
            raw = stream.read(_MAX_TOKEN_FILE_BYTES + 1)
        if len(raw) > _MAX_TOKEN_FILE_BYTES:
            raise ValueError("invalid token file")
        token = raw.decode("utf-8", errors="strict").strip()
        hash_token(token)
        return token
    except (OSError, ValueError, UnicodeError):
        raise ValueError(
            "cannot read token file: require bounded, valid credential in a private regular file (0600)"
        ) from None
    finally:
        if fd is not None:
            os.close(fd)


def safe_diagnostic(message: str) -> str:
    """Bound diagnostics and redact known secrets and leaf-recognized credentials."""
    for name, value in os.environ.items():
        if value and (
            name == "NIRS4ALL_PROJECT_TOKEN"
            or name.startswith("N4CLUSTER_")
            and any(x in name for x in ("TOKEN", "SECRET", "CREDENTIAL"))
        ):
            message = message.replace(value, "[REDACTED]")
    pieces = re.split(r"([\s'\"=,;:()]+)", message)
    for index, piece in enumerate(pieces):
        try:
            hash_token(piece)
        except ValueError:
            continue
        pieces[index] = "[REDACTED]"
    return "".join(pieces)[:300]
