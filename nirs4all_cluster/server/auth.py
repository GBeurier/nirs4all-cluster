"""Credential-bound RBAC for the cluster control plane.

This module never imports nirs4all (same red-line as the rest of the server). It
replaces the prototype's single advisory model — one shared bearer token plus an
``X-N4C-Role`` header used only for logging — with **rights derived from the
credential the caller presents**.

Trusted-LAN V1 (the smallest step that yields real RBAC):

- A **principal** is a named identity bound to one static bearer token and a set
  of **rights** drawn from ``{submit, read, cancel, execute, admin}``.
- Rights are composed into named **roles** (``submitter``/``executor``/
  ``viewer``/``admin``) for convenience; the wire/credential carries rights.
- The server derives a caller's rights from its token, **never** from the
  advisory ``X-N4C-Role`` header (which stays informational, for version drift
  logging only).

mTLS/OIDC, per-identity certificates and token rotation are post-V1; this rights
vocabulary is the stable seam that survives that credential-mechanism swap.
"""

from __future__ import annotations

import hmac
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import Enum


class Right(str, Enum):
    """A single granted capability. ``admin`` is a wildcard (see ``Principal.has``)."""

    SUBMIT = "submit"
    READ = "read"
    CANCEL = "cancel"
    EXECUTE = "execute"
    ADMIN = "admin"


ALL_RIGHTS: frozenset[Right] = frozenset(Right)

# Named roles = bundles of rights (SW7 CLUSTER spec §3b / DEC-CLU-001 CL2).
#   submitter : a CLI/SDK/Studio user or the benchmarks queue.
#   executor  : the worker agent — "rx" = read + execute.
#   viewer    : a dashboard / monitoring cockpit (read-only).
#   admin     : the server operator (every right).
ROLES: dict[str, frozenset[Right]] = {
    "admin": ALL_RIGHTS,
    "submitter": frozenset({Right.SUBMIT, Right.READ, Right.CANCEL}),
    "executor": frozenset({Right.READ, Right.EXECUTE}),
    "viewer": frozenset({Right.READ}),
}


class AuthError(Exception):
    """An authentication/authorization failure. ``status`` is the HTTP code to map to."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def rights_from_roles(roles: Iterable[str]) -> frozenset[Right]:
    """Resolve role names to the union of their rights. Raises on an unknown role."""
    granted: set[Right] = set()
    for role in roles:
        try:
            granted |= ROLES[role]
        except KeyError:
            raise ValueError(f"unknown role {role!r}; known roles: {sorted(ROLES)}") from None
    return frozenset(granted)


@dataclass(frozen=True)
class Principal:
    """A named identity, its static bearer token, and the rights it was granted."""

    name: str
    token: str
    rights: frozenset[Right] = field(default_factory=frozenset)

    def has(self, right: Right) -> bool:
        """True if this principal holds ``right`` (``admin`` grants everything)."""
        return Right.ADMIN in self.rights or right in self.rights

    @classmethod
    def from_roles(cls, name: str, token: str, roles: Iterable[str]) -> Principal:
        return cls(name=name, token=token, rights=rights_from_roles(roles))


def bearer_token(authorization: str | None) -> str | None:
    """Extract the token from an ``Authorization: Bearer <token>`` header."""
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token


class Authorizer:
    """Resolves a bearer credential to a :class:`Principal` and checks its rights.

    Two modes, chosen by whether any principal is configured:

    - **open (dev)** — no principals: every request is granted all rights. This
      preserves the prototype's "no ``--token`` ⇒ no auth" trusted-LAN default.
    - **enforced** — one or more principals: a request must present a valid bearer
      token, and the matched principal's rights gate each route.
    """

    # The synthetic principal returned in open/dev mode — holds every right.
    _DEV = Principal(name="dev", token="", rights=ALL_RIGHTS)

    def __init__(self, principals: Sequence[Principal] = ()) -> None:
        self._principals: list[Principal] = list(principals)

    @property
    def enforced(self) -> bool:
        return bool(self._principals)

    def principal_for_token(self, token: str | None) -> Principal | None:
        """Return the principal owning ``token``, or ``None`` if it matches none.

        In open mode the dev principal is returned regardless of the token. The
        comparison loop never early-exits, so a valid token cannot be
        distinguished from an invalid one by timing.
        """
        if not self.enforced:
            return self._DEV
        if not token:
            return None
        matched: Principal | None = None
        for principal in self._principals:
            if hmac.compare_digest(principal.token, token):
                matched = principal
        return matched

    def check(self, token: str | None, rights: Sequence[Right]) -> Principal:
        """Authenticate ``token`` and assert it holds every right in ``rights``.

        Raises :class:`AuthError` (401) on an unknown/missing credential and (403)
        when the principal lacks a required right.
        """
        principal = self.principal_for_token(token)
        if principal is None:
            raise AuthError(401, "invalid or missing token")
        missing = [r for r in rights if not principal.has(r)]
        if missing:
            names = ", ".join(r.value for r in missing)
            raise AuthError(403, f"principal {principal.name!r} lacks required right(s): {names}")
        return principal
