"""Cluster-owned roles. Shared credential verification belongs to nirs4all-access."""

from collections.abc import Iterable
from enum import Enum

from nirs4all_access import ProjectPrincipal

Principal = ProjectPrincipal


class Right(str, Enum):
    READ = "cluster.read"
    SUBMIT = "cluster.submit"
    CANCEL = "cluster.cancel"
    EXECUTE = "cluster.execute"
    ADMIN = "cluster.admin"


ALL_RIGHTS = frozenset(Right)
ROLES = {
    "admin": ALL_RIGHTS,
    "submitter": frozenset({Right.READ, Right.SUBMIT, Right.CANCEL}),
    "executor": frozenset({Right.READ, Right.EXECUTE}),
    "viewer": frozenset({Right.READ}),
}


def rights_from_roles(roles: Iterable[str]) -> frozenset[Right]:
    granted: set[Right] = set()
    for role in roles:
        if role not in ROLES:
            raise ValueError("unknown Cluster role")
        granted.update(ROLES[role])
    if not granted:
        raise ValueError("empty permissions")
    return frozenset(granted)
