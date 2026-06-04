"""Content-addressed blob store on local disk.

Artifacts (uploaded pipelines/datasets, exported ``.n4a`` models, log bundles,
task workspaces) are stored by SHA-256 under ``<state>/objects/aa/bb/<sha>`` so
identical bytes are stored once. The DB row (``artifacts`` table) maps an opaque
``artifact_id`` to the on-disk path; this class owns the bytes only.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import BinaryIO

_CHUNK = 1024 * 1024


class ArtifactTooLarge(Exception):
    """Raised when an upload exceeds the configured size limit mid-stream."""


class ArtifactStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, sha256: str) -> Path:
        return self.root / sha256[:2] / sha256[2:4] / sha256

    def put_bytes(self, data: bytes) -> tuple[str, str, int]:
        sha = hashlib.sha256(data).hexdigest()
        dest = self._path_for(sha)
        size = len(data)
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.parent / f".tmp_{os.getpid()}_{sha[:8]}"
            tmp.write_bytes(data)
            os.replace(tmp, dest)
        return sha, str(dest), size

    def put_stream(self, stream: BinaryIO, max_bytes: int | None = None) -> tuple[str, str, int]:
        """Stream to a temp file while hashing, then move into place by hash.

        If ``max_bytes`` is set, the temp file is deleted and ``ArtifactTooLarge``
        is raised as soon as the size is exceeded — so an over-limit upload never
        commits a (leaked) blob into the content-addressed store.
        """
        hasher = hashlib.sha256()
        size = 0
        fd, tmp_name = tempfile.mkstemp(dir=self.root, prefix=".tmp_")
        try:
            with os.fdopen(fd, "wb") as tmp:
                while True:
                    chunk = stream.read(_CHUNK)
                    if not chunk:
                        break
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        raise ArtifactTooLarge(f"artifact exceeds {max_bytes} bytes")
                    hasher.update(chunk)
                    tmp.write(chunk)
            sha = hasher.hexdigest()
            dest = self._path_for(sha)
            if dest.exists():
                os.unlink(tmp_name)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                os.replace(tmp_name, dest)
            return sha, str(dest), size
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def open(self, sha256: str) -> BinaryIO:
        return open(self._path_for(sha256), "rb")

    def exists(self, sha256: str) -> bool:
        return self._path_for(sha256).exists()
