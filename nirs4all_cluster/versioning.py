"""Protocol / version compatibility — shared by client, server, worker, materialize.

This module never imports nirs4all. It centralises the bits all four sides need
to agree on a wire and to trace version drift:

- ``API_VERSION`` — the protocol major. Two builds with the same ``API_VERSION``
  speak the same wire contract regardless of their package version.
- ``CLUSTER_VERSION`` — the ``nirs4all-cluster`` package version each side advertises.
- the HTTP headers used to exchange both on every ``/v1`` call and response,
- the compatibility rule (same protocol major ⇒ compatible; different ⇒ not),
- a canonical pipeline content fingerprint so the client and the worker agree on
  the identity of an inline pipeline end-to-end.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from . import __version__

# Protocol major. Bump only on a breaking wire-contract change; it is deliberately
# independent of the package version (a patch/minor package bump keeps the wire).
API_VERSION = 1

CLUSTER_VERSION = __version__

# Headers carried on every /v1 request and response so each side can record the
# other's identity and warn on drift.
H_VERSION = "X-N4C-Version"  # peer's nirs4all-cluster package version
H_API = "X-N4C-Api"  # peer's protocol major (int rendered as string)
H_ROLE = "X-N4C-Role"  # "client" | "worker" | "server"


class ClusterVersionError(RuntimeError):
    """Raised when a peer reports an incompatible protocol major (HTTP 426)."""


def request_headers(role: str) -> dict[str, str]:
    """Default headers a client/worker attaches to every request."""
    return {H_VERSION: CLUSTER_VERSION, H_API: str(API_VERSION), H_ROLE: role}


def parse_api(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_incompatible(peer_api: int | None) -> bool:
    """True iff the peer's protocol major differs from ours.

    An absent/garbage API header is treated as compatible (a legacy peer that
    predates the handshake): only an explicit, differing major is rejected.
    """
    return peer_api is not None and peer_api != API_VERSION


def is_divergent(peer_version: str | None) -> bool:
    """Same protocol major but a different package version — compatible, worth noting."""
    return bool(peer_version) and peer_version != CLUSTER_VERSION


def _canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def fingerprint_obj(obj: Any) -> str:
    """sha256 of a canonical JSON form — stable across client and worker for an inline pipeline."""
    return "sha256:" + hashlib.sha256(_canonical(obj)).hexdigest()


def fingerprint_file(path: str | Path) -> str:
    """sha256 of a file's raw bytes — used for path/artifact pipelines the worker reads."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()
