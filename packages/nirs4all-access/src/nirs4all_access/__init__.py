"""Immutable project-token v1 verification; standard library only."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import secrets
import stat
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

__all__ = [
    "CONTRACT_VERSION",
    "STORE_SCHEMA",
    "AccessError",
    "CredentialRecord",
    "CredentialStoreError",
    "ProjectPrincipal",
    "ProjectTokenVerifier",
    "__version__",
    "dump_credential_store",
    "generate_token",
    "hash_token",
    "issue_credential",
    "load_credential_store",
    "parse_bearer",
    "parse_credential_store",
    "require_secure_transport",
    "write_credential_store",
]

__version__ = "0.1.0"
CONTRACT_VERSION = 1
STORE_SCHEMA = "nirs4all-access/project-token-store/v1"
_TOKEN = re.compile(r"n4p1\.([0-9a-f]{16})\.[0-9a-f]{64}")
_SERVICE = r"[a-z][a-z0-9-]{0,31}"
_ACTION = r"[a-z][a-z0-9_]{0,31}"
_DEVICES = {"con", "prn", "aux", "nul", *[f"{p}{i}" for p in ("com", "lpt") for i in range(1, 10)]}


class CredentialStoreError(ValueError):
    """Invalid hash-only credential configuration (never includes input secrets)."""


def _matches(pattern: str, value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(pattern, value, flags=re.ASCII) is not None


def _expiry(value: object) -> None:
    try:
        valid = value is None or ((type(value) is float or type(value) is int) and math.isfinite(value))
    except (OverflowError, TypeError):
        valid = False
    if not valid:
        raise CredentialStoreError("invalid expires_at")


@dataclass(frozen=True)
class ProjectPrincipal:
    project_id: str
    principal_id: str
    services: frozenset[str]
    rights: frozenset[str]
    group_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _matches(r"[a-z0-9][a-z0-9_-]{0,62}", self.project_id) or self.project_id in _DEVICES:
            raise CredentialStoreError("invalid project_id")
        if not _matches(r"[a-z0-9][a-z0-9._-]{0,127}", self.principal_id):
            raise CredentialStoreError("invalid principal_id")
        if (
            not isinstance(self.services, frozenset)
            or not self.services
            or any(not _matches(_SERVICE, s) for s in self.services)
        ):
            raise CredentialStoreError("invalid services")
        if (
            not isinstance(self.rights, frozenset)
            or not self.rights
            or any(not _matches(_SERVICE + r"\." + _ACTION, r) for r in self.rights)
        ):
            raise CredentialStoreError("invalid rights")
        if {r.split(".", 1)[0] for r in self.rights} != self.services:
            raise CredentialStoreError("service/right mismatch")
        if type(self.group_ids) is not tuple or self.group_ids:
            raise CredentialStoreError("groups are reserved until schema v2")


@dataclass(frozen=True)
class CredentialRecord:
    key_id: str
    token_hash: str = field(repr=False)
    principal: ProjectPrincipal
    expires_at: float | None = None
    revoked: bool = False

    def __post_init__(self) -> None:
        if not _matches(r"[0-9a-f]{16}", self.key_id):
            raise CredentialStoreError("invalid key_id")
        if not _matches(r"sha256:[0-9a-f]{64}", self.token_hash):
            raise CredentialStoreError("invalid token_hash")
        if not isinstance(self.principal, ProjectPrincipal):
            raise CredentialStoreError("invalid principal")
        _expiry(self.expires_at)
        if type(self.revoked) is not bool:
            raise CredentialStoreError("invalid revoked")


class AccessError(Exception):
    def __init__(self, status: int, detail: str, *, code: str, missing_rights: frozenset[str] = frozenset()):
        super().__init__(detail)
        self.status, self.detail, self.code, self.missing_rights = status, detail, code, missing_rights


def hash_token(token: str) -> str:
    if not isinstance(token, str) or _TOKEN.fullmatch(token) is None:
        raise ValueError("malformed project credential")
    return "sha256:" + hashlib.sha256(b"nirs4all-access/project-token/v1\x00" + token.encode("ascii")).hexdigest()


def generate_token() -> str:
    return f"n4p1.{secrets.token_hex(8)}.{secrets.token_hex(32)}"


def issue_credential(principal: ProjectPrincipal, *, expires_at: float | None = None) -> tuple[str, CredentialRecord]:
    token = generate_token()
    return token, CredentialRecord(token.split(".")[1], hash_token(token), principal, expires_at)


def parse_bearer(authorization: str | None) -> str | None:
    if not isinstance(authorization, str):
        return None
    match = re.fullmatch(r"(?i:Bearer) +([^\s]+)", authorization)
    return match[1] if match else None


class ProjectTokenVerifier:
    def __init__(self, records: Iterable[CredentialRecord]):
        records = tuple(records)
        if any(not isinstance(r, CredentialRecord) for r in records):
            raise CredentialStoreError("invalid credential record")
        if len({r.key_id for r in records}) != len(records) or len({r.token_hash for r in records}) != len(records):
            raise CredentialStoreError("duplicate key_id or token_hash")
        self._records = {r.key_id: r for r in records}

    def verify(
        self, token: str | None, *, service: str, required_rights: Iterable[str] = (), now: float | None = None
    ) -> ProjectPrincipal:
        if not _matches(_SERVICE, service) or isinstance(required_rights, str):
            raise ValueError("invalid service or required_rights")
        required = frozenset(required_rights)
        if any(not _matches(re.escape(service) + r"\." + _ACTION, r) for r in required):
            raise ValueError("required right must belong to service")
        timestamp = time.time() if now is None else now
        try:
            if type(timestamp) not in (int, float) or not math.isfinite(timestamp):
                raise ValueError("invalid now")
        except OverflowError:
            raise ValueError("invalid now") from None
        try:
            digest = hash_token(token)  # type: ignore[arg-type]
        except ValueError:
            raise AccessError(401, "invalid or missing credential", code="invalid_credential") from None
        record = self._records.get(token.split(".")[1])  # type: ignore[union-attr]
        equal = hmac.compare_digest(
            digest.encode("ascii"), (record.token_hash if record else "sha256:" + "0" * 64).encode("ascii")
        )
        if (
            not equal
            or record is None
            or record.revoked
            or (record.expires_at is not None and timestamp >= record.expires_at)
        ):
            raise AccessError(401, "invalid or missing credential", code="invalid_credential")
        principal = record.principal
        if service not in principal.services:
            raise AccessError(
                403,
                f"principal '{principal.principal_id}' is not granted service '{service}'",
                code="service_not_granted",
            )
        missing = required - principal.rights
        if missing:
            raise AccessError(
                403,
                f"principal '{principal.principal_id}' lacks required right(s): {', '.join(sorted(missing))}",
                code="missing_rights",
                missing_rights=missing,
            )
        return principal


_FIELDS = frozenset(
    {"key_id", "token_hash", "project_id", "principal_id", "services", "rights", "group_ids", "expires_at", "revoked"}
)


def parse_credential_store(data: bytes) -> tuple[CredentialRecord, ...]:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise CredentialStoreError("duplicate JSON key")
            value[key] = item
        return value

    def invalid_constant(value):
        raise CredentialStoreError("non-finite JSON constant")

    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"), object_pairs_hook=pairs, parse_constant=invalid_constant
        )
        if (
            not isinstance(value, dict)
            or set(value) != {"schema", "credentials"}
            or value["schema"] != STORE_SCHEMA
            or not isinstance(value["credentials"], list)
        ):
            raise CredentialStoreError("invalid store schema")
        records = []
        for item in value["credentials"]:
            if not isinstance(item, dict) or set(item) != _FIELDS:
                raise CredentialStoreError("invalid credential fields")
            for name in ("services", "rights", "group_ids"):
                array = item[name]
                if (
                    not isinstance(array, list)
                    or any(not isinstance(x, str) for x in array)
                    or len(array) != len(set(array))
                ):
                    raise CredentialStoreError("invalid or duplicate array entries")
            p = ProjectPrincipal(
                item["project_id"],
                item["principal_id"],
                frozenset(item["services"]),
                frozenset(item["rights"]),
                tuple(item["group_ids"]),
            )
            records.append(CredentialRecord(item["key_id"], item["token_hash"], p, item["expires_at"], item["revoked"]))
        ProjectTokenVerifier(records)
        return tuple(records)
    except CredentialStoreError:
        raise
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise CredentialStoreError("invalid credential store") from None


def dump_credential_store(records: Iterable[CredentialRecord]) -> bytes:
    records = tuple(records)
    ProjectTokenVerifier(records)
    values = []
    for r in sorted(records, key=lambda r: r.key_id):
        p = r.principal
        values.append(
            {
                "key_id": r.key_id,
                "token_hash": r.token_hash,
                "project_id": p.project_id,
                "principal_id": p.principal_id,
                "services": sorted(p.services),
                "rights": sorted(p.rights),
                "group_ids": list(p.group_ids),
                "expires_at": r.expires_at,
                "revoked": r.revoked,
            }
        )
    return (
        json.dumps(
            {"schema": STORE_SCHEMA, "credentials": values}, sort_keys=True, allow_nan=False, separators=(",", ":")
        )
        + "\n"
    ).encode("utf-8")


def load_credential_store(path: str | os.PathLike[str]) -> tuple[CredentialRecord, ...]:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or (os.name == "posix" and info.st_mode & 0o077):
            raise CredentialStoreError("credential store must be a private regular file (0600)")
        fh = os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise
    with fh:
        return parse_credential_store(fh.read())


def write_credential_store(path: str | os.PathLike[str], records: Iterable[CredentialRecord]) -> None:
    data = dump_credential_store(records)
    path = Path(path)
    temp = path.with_name(f".{path.name}.{secrets.token_hex(12)}.tmp")
    try:
        fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "wb") as fh:
            if hasattr(os, "fchmod"):
                os.fchmod(fh.fileno(), 0o600)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def require_secure_transport(host: str, *, tls: bool, trusted_proxy: bool = False) -> None:
    if type(tls) is not bool or type(trusted_proxy) is not bool:
        raise ValueError("tls and trusted_proxy must be bool")
    loopback = host.lower() == "localhost"
    try:
        loopback |= ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    if not loopback and not tls and not trusted_proxy:
        raise ValueError("non-loopback bearer transport requires TLS or an explicitly trusted TLS proxy")
