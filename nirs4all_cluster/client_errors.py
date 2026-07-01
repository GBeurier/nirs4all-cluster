"""Typed, rights-respecting errors for the cluster clients.

This module maps the server's HTTP responses to a small exception hierarchy so
callers (core / Studio / CLI) can react to *why* a call failed — above all, to the
RBAC verdicts the credential-bound server returns after the RBAC slice:

- **401** — the credential is missing or invalid → :class:`ClusterAuthError`.
- **403** — the credential is valid but lacks a required right →
  :class:`ClusterPermissionError`, which parses the offending ``principal`` and the
  ``missing_rights`` out of the server's detail string.

Everything else maps to a status-specific subclass (404/409/413/422/other-4xx/5xx),
and transport failures become :class:`ClusterConnectionError`.

Design red-lines it honours:

- **never imports nirs4all** (same as the rest of the control plane);
- **never imports the server** — the granted/needed rights are parsed from the wire
  *detail string* as plain names, so the client stays decoupled from
  ``server/auth.py`` while still speaking the same ``{submit,read,cancel,execute,
  admin}`` vocabulary. Protocol-major incompatibility (HTTP 426) keeps its existing
  identity, :class:`nirs4all_cluster.versioning.ClusterVersionError`, which is
  re-exported here for discoverability.
"""

from __future__ import annotations

import json
import re
from typing import Any

from .versioning import ClusterVersionError

__all__ = [
    "ClusterError",
    "ClusterConnectionError",
    "ClusterAuthError",
    "ClusterPermissionError",
    "ClusterNotFoundError",
    "ClusterConflictError",
    "ClusterPayloadTooLargeError",
    "ClusterValidationError",
    "ClusterRequestError",
    "ClusterServerError",
    "ClusterVersionError",
    "parse_missing_rights",
    "raise_for_response",
]


class ClusterError(Exception):
    """Base class for every error the typed clients raise.

    ``status`` is the HTTP status that produced it (``None`` for a transport
    failure); ``detail`` is the server's ``detail`` message when present.
    """

    # Default status for the fixed-status subclasses; instances override via __init__.
    status: int | None = None

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        detail: str | None = None,
        method: str | None = None,
        url: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if status is not None:
            self.status = status
        self.detail = detail
        self.method = method
        self.url = url

    def __str__(self) -> str:
        return f"[{self.status}] {self.message}" if self.status else self.message


class ClusterConnectionError(ClusterError):
    """The server could not be reached (DNS / connect / read / timeout)."""


class ClusterAuthError(ClusterError):
    """401 — the bearer credential is missing or invalid."""

    status = 401


class ClusterPermissionError(ClusterError):
    """403 — the credential is valid but lacks a required right.

    ``missing_rights`` holds the right names the server said were missing (a subset
    of ``{submit, read, cancel, execute, admin}``); ``principal`` is the identity the
    server matched the token to, when it disclosed one.
    """

    status = 403

    def __init__(
        self,
        message: str,
        *,
        missing_rights: frozenset[str] = frozenset(),
        principal: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        self.missing_rights = frozenset(missing_rights)
        self.principal = principal


class ClusterNotFoundError(ClusterError):
    """404 — the job / task / artifact does not exist."""

    status = 404


class ClusterConflictError(ClusterError):
    """409 — the request conflicts with the resource's state (e.g. task already taken)."""

    status = 409


class ClusterPayloadTooLargeError(ClusterError):
    """413 — the request body (or streamed upload) exceeded the server's limit."""

    status = 413


class ClusterValidationError(ClusterError):
    """422 — the request failed the server's boundary validation."""

    status = 422


class ClusterRequestError(ClusterError):
    """A 4xx not covered by a more specific subclass."""


class ClusterServerError(ClusterError):
    """5xx — the server failed to handle an otherwise valid request."""


_PRINCIPAL_RE = re.compile(r"principal '([^']*)'")
_MISSING_RIGHTS_RE = re.compile(r"lacks required right\(s\):\s*(.+?)\s*$")


def parse_missing_rights(detail: str | None) -> tuple[str | None, frozenset[str]]:
    """Extract ``(principal, missing_rights)`` from a 403 detail string.

    The server emits ``principal 'alice' lacks required right(s): submit, cancel``.
    Returns ``(None, frozenset())`` for any string that does not match, so callers
    can rely on the shape without guarding.
    """
    if not detail:
        return None, frozenset()
    principal_match = _PRINCIPAL_RE.search(detail)
    principal = principal_match.group(1) if principal_match else None
    rights_match = _MISSING_RIGHTS_RE.search(detail)
    rights = (
        frozenset(name.strip() for name in rights_match.group(1).split(",") if name.strip())
        if rights_match
        else frozenset()
    )
    return principal, rights


def _detail_of(response: Any) -> str:
    """Best-effort human detail for a failed response (FastAPI ``{"detail": ...}``)."""
    try:
        data = response.json()
    except Exception:
        try:
            text = response.text
        except Exception:
            text = ""
        return (text or "").strip() or f"HTTP {response.status_code}"
    if isinstance(data, str):
        return data
    if isinstance(data, dict) and "detail" in data:
        detail = data["detail"]
        return detail if isinstance(detail, str) else json.dumps(detail, separators=(",", ":"))
    return json.dumps(data, separators=(",", ":"))


_STATUS_MAP: dict[int, type[ClusterError]] = {
    401: ClusterAuthError,
    404: ClusterNotFoundError,
    409: ClusterConflictError,
    413: ClusterPayloadTooLargeError,
    422: ClusterValidationError,
}


def raise_for_response(response: Any) -> None:
    """Raise the typed error matching ``response``; return ``None`` on a 2xx/3xx.

    Works on both normal and streamed responses (an unread streamed body is read
    first so the ``detail`` can be recovered).
    """
    status = int(response.status_code)
    if status < 400:
        return
    try:
        response.read()  # no-op if already read; materialises a streamed error body
    except Exception:
        pass
    detail = _detail_of(response)
    request = getattr(response, "request", None)
    method = getattr(request, "method", None)
    url = str(getattr(request, "url", "")) or None

    if status == 426:
        # Protocol-major mismatch keeps its established identity.
        raise ClusterVersionError(detail or "incompatible protocol major")
    if status == 403:
        principal, missing = parse_missing_rights(detail)
        raise ClusterPermissionError(
            detail or "forbidden",
            status=status,
            detail=detail,
            method=method,
            url=url,
            missing_rights=missing,
            principal=principal,
        )
    error_cls = _STATUS_MAP.get(status)
    if error_cls is None:
        error_cls = ClusterServerError if status >= 500 else ClusterRequestError
    raise error_cls(detail or f"HTTP {status}", status=status, detail=detail, method=method, url=url)
