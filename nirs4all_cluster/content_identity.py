"""Atomic transport integrity; no archive or scientific interpretation."""

import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any


def verify_download(path: Path, expected: str | None, size: str | None = None) -> Path:
    """Check an already-materialized input against the submission-pinned identity."""
    if not expected or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise ValueError("missing or invalid artifact content identity")
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected or (size is not None and path.stat().st_size != int(size)):
        raise ValueError("downloaded artifact content identity mismatch")
    return path


def receive_verified_download(response: Any, dest: str | Path) -> Path:
    """Publish bytes only after header, stream length and digest verification.

    Callers check the HTTP status before entering. Failed retrieval never truncates
    an existing destination, and never leaves a new plausible but invalid archive.
    """
    expected = response.headers.get("X-N4C-SHA256")
    raw_size = response.headers.get("X-N4C-Size")
    if not expected or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise ValueError("missing or invalid artifact content identity")
    if raw_size is None or re.fullmatch(r"[0-9]{1,20}", raw_size) is None:
        raise ValueError("missing or invalid artifact size identity")
    size = int(raw_size)
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".n4cluster-download-", dir=dest.parent)
    temporary = Path(name)
    try:
        digest = hashlib.sha256()
        received = 0
        with os.fdopen(fd, "wb") as fh:
            os.fchmod(fh.fileno(), 0o600) if hasattr(os, "fchmod") else None
            for chunk in response.iter_bytes():
                received += len(chunk)
                if received > size:
                    raise ValueError("downloaded artifact size mismatch")
                fh.write(chunk)
                digest.update(chunk)
            if received != size or digest.hexdigest() != expected:
                raise ValueError("downloaded artifact content identity mismatch")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, dest)
        return dest
    finally:
        temporary.unlink(missing_ok=True)
