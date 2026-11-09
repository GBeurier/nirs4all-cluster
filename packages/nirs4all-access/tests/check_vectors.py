"""Standalone installed-wheel acceptance. Run with -I and vector file argument."""

import hashlib
import importlib.abc
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path


class BlockServices(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"fastapi", "starlette", "pydantic", "httpx", "nirs4all", "nirs4all_cluster"}:
            raise AssertionError("forbidden service import")


sys.meta_path.insert(0, BlockServices())
import nirs4all_access as a  # noqa: E402

v = json.loads(Path(sys.argv[1]).read_text())
assert a.__version__ == "0.1.0" and a.CONTRACT_VERSION == 1
assert a.hash_token(v["token"]) == v["hash"]
assert v["hash"] == "sha256:" + hashlib.sha256(b"nirs4all-access/project-token/v1\0" + v["token"].encode()).hexdigest()
assert v["hash"][7:] != v["negative_hash"]


def store(record):
    return json.dumps({"schema": a.STORE_SCHEMA, "credentials": [record]}).encode()


def principal_fields(principal):
    return {"project_id": principal.project_id, "principal_id": principal.principal_id,
            "services": sorted(principal.services), "rights": sorted(principal.rights),
            "group_ids": list(principal.group_ids)}


def expected_fields(record):
    return {key: sorted(record[key]) if key in {"services", "rights"} else record[key]
            for key in ("project_id", "principal_id", "services", "rights", "group_ids")}


assert a.hash_token(v["token_b"]) == v["hash_b"]
assert v["hash_b"] == "sha256:" + hashlib.sha256(
    b"nirs4all-access/project-token/v1\0" + v["token_b"].encode()
).hexdigest()
assert v["record_b"]["token_hash"] == v["hash_b"]
secret_values = {v["token"], v["hash"], v["negative_hash"], v["token_b"], v["hash_b"], *v["splice_tokens"]}
secret_values.update(case["token"] for case in v["verify"] if isinstance(case["token"], str))
secret_values.update(record["token_hash"] for case in v["verify"] for record in case.get("records", []))


def no_secrets(text):
    assert all(secret not in text for secret in secret_values), "fixture secret leaked"


case_keys = {"name", "token", "service", "required_rights", "now", "status", "code", "missing_rights"}
for case in v["verify"]:
    keys = set(case)
    if "records" in case:
        assert keys == case_keys | {"records", "expected_principal"}, "unexpected vector keys"
        records = a.parse_credential_store(json.dumps({"schema": a.STORE_SCHEMA, "credentials": case["records"]}).encode())
        expected_principal = (expected_fields(case["expected_principal"])
                              if case["status"] == 200 else None)
    else:
        assert keys in (case_keys, case_keys | {"patch"}, case_keys | {"empty"}), "unexpected vector keys"
        record = dict(v["record"], **case.get("patch", {}))
        records = () if case.get("empty") else a.parse_credential_store(store(record))
        expected_principal = expected_fields(record) if case["status"] == 200 else None
    expected = {"status": case["status"], "code": case["code"],
                "missing_rights": case["missing_rights"], "principal": expected_principal}
    try:
        principal = a.ProjectTokenVerifier(records).verify(
            case["token"], service=case["service"], required_rights=case["required_rights"], now=case["now"]
        )
        actual = {"status": 200, "code": None, "missing_rights": [], "principal": principal_fields(principal)}
        no_secrets(repr(principal))
    except a.AccessError as exc:
        actual = {"status": exc.status, "code": exc.code,
                  "missing_rights": sorted(exc.missing_rights), "principal": None}
        no_secrets(str(exc) + repr(exc))
    if actual != expected:
        raise AssertionError("ACCESS_VECTOR_FAILURE " + json.dumps(
            {"case": case["name"], "expected": expected, "actual": actual}, sort_keys=True))
for case in v["invalid_records"]:
    try:
        a.parse_credential_store(store(dict(v["record"], **case["patch"])))
    except a.CredentialStoreError:
        pass
    else:
        raise AssertionError(case["name"])
base = store(v["record"])
invalid = [
    base.replace(b"null", b"NaN"),
    base.replace(b"null", b"1e999"),
    base.replace(b"null", b"9" * 500),
    base.replace(b'"revoked": false', b'"revoked": false, "revoked": true'),
    base.replace(a.STORE_SCHEMA.encode(), b"wrong"),
    b"\xff",
    json.dumps({"schema": a.STORE_SCHEMA, "credentials": [v["record"], v["record"]]}).encode(),
]
for data in invalid:
    try:
        a.parse_credential_store(data)
    except a.CredentialStoreError:
        pass
    else:
        raise AssertionError("invalid store accepted")
r = a.parse_credential_store(base)[0]
no_secrets(repr(r))
for rights in ["", "cluster.read", ["datasets.read"]]:
    try:
        a.ProjectTokenVerifier([r]).verify(v["token"], service="cluster", required_rights=rights)
    except ValueError:
        pass
    else:
        raise AssertionError("API misuse accepted")
for now in [float("inf"), float("nan"), 10**500]:
    try:
        a.ProjectTokenVerifier([r]).verify(v["token"], service="cluster", now=now)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid now accepted")
refusal_children = 0
with tempfile.TemporaryDirectory() as tmp:
    p = Path(tmp) / "credentials.json"
    a.write_credential_store(p, [r])
    assert p.stat().st_mode & 0o777 == 0o600
    assert a.dump_credential_store(a.load_credential_store(p)) == p.read_bytes()
    assert list(Path(tmp).iterdir()) == [p]
    p.chmod(0o644)
    try:
        a.load_credential_store(p)
    except a.CredentialStoreError:
        pass
    else:
        raise AssertionError("public permissions accepted")
    p.chmod(0o600)
    if os.name == "posix":
        link = Path(tmp) / "private-link.json"
        link.symlink_to(p)
        assert a.load_credential_store(link) == (r,)
        link.unlink()
        for mask in (0o277, 0o377, 0o777):
            q = Path(tmp) / f"umask-{mask:o}.json"
            old_mask = os.umask(mask)
            try:
                a.write_credential_store(q, [r])
            finally:
                os.umask(old_mask)
            assert q.stat().st_mode & 0o777 == 0o600, oct(mask)
            assert a.load_credential_store(q) == (r,)
            q.unlink()
        fifo = Path(tmp) / "fifo"
        os.mkfifo(fifo, 0o600)
        fifo_link = Path(tmp) / "fifo-link"
        fifo_link.symlink_to(fifo)
        directory = Path(tmp) / "directory"
        directory.mkdir()
        targets = [fifo, fifo_link, directory]
        if Path("/dev/zero").exists():
            targets.append(Path("/dev/zero"))
        refuse = """import os, sys
import nirs4all_access as a
fds = '/proc/self/fd'
before = len(os.listdir(fds)) if os.path.isdir(fds) else None
for _ in range(32):
    try:
        a.load_credential_store(sys.argv[1])
    except a.CredentialStoreError:
        pass
    else:
        raise AssertionError('non-regular file accepted')
if before is not None:
    assert len(os.listdir(fds)) == before, 'descriptor leaked on refusal'
print('REFUSED')
"""
        for target in targets:
            command = [sys.executable, "-B"]
            if sys.flags.isolated:
                command.append("-I")
            result = subprocess.run(
                [*command, "-c", refuse, str(target)],
                capture_output=True,
                text=True,
                timeout=3,
                check=True,
            )
            assert result.stdout.strip() == "REFUSED", target.name
            refusal_children += 1
for host in ["0.0.0.0", "::", "localhost.example", "example.com"]:
    try:
        a.require_secure_transport(host, tls=False)
    except ValueError:
        pass
    else:
        raise AssertionError(host)
for host in ["localhost", "127.0.0.1", "::1"]:
    a.require_secure_transport(host, tls=False)
for host in ["localhost", "example.com"]:
    a.require_secure_transport(host, tls=True)
    a.require_secure_transport(host, tls=False, trusted_proxy=True)
    for flag in ["false", "0", "no", "", 0, 1, 0.0, 1.0, None, [], {}, v["token"]]:
        for kwargs in [{"tls": flag}, {"tls": False, "trusted_proxy": flag}]:
            try:
                a.require_secure_transport(host, **kwargs)
            except ValueError as exc:
                no_secrets(str(exc) + repr(exc))
            else:
                raise AssertionError("non-bool transport flag accepted")
expected_exports = {
    "__version__",
    "CONTRACT_VERSION",
    "STORE_SCHEMA",
    "ProjectPrincipal",
    "CredentialRecord",
    "AccessError",
    "CredentialStoreError",
    "ProjectTokenVerifier",
    "parse_bearer",
    "generate_token",
    "hash_token",
    "issue_credential",
    "parse_credential_store",
    "dump_credential_store",
    "load_credential_store",
    "write_credential_store",
    "require_secure_transport",
}
assert set(a.__all__) == expected_exports and len(a.__all__) == len(expected_exports)
namespace: dict[str, object] = {}
exec("from nirs4all_access import *", namespace)
assert set(namespace) - {"__builtins__"} == expected_exports
for value in [*a.parse_credential_store(store(v["record_b"])),
              a.parse_credential_store(store(v["record_b"]))[0].principal,
              a.ProjectTokenVerifier(a.parse_credential_store(store(v["record_b"]))),
              r, r.principal, a.ProjectTokenVerifier([r])]:
    no_secrets(repr(value))
try:
    a.ProjectTokenVerifier([r]).verify(v["token"] + "\n", service="cluster")
except a.AccessError:
    text = traceback.format_exc()
    no_secrets(text)
else:
    raise AssertionError("malformed token accepted")
assert a.parse_bearer("bEaReR  " + v["token"]) == v["token"]
assert a.parse_bearer("Bearer " + v["token"] + "\n") is None
assert (Path(a.__file__).parent / "py.typed").is_file()
if "-I" in sys.orig_argv:
    assert [d.metadata["Name"] for d in importlib.metadata.distributions()] == ["nirs4all-access"]
print(
    json.dumps(
        {
            "status": "passed",
            "verify_vectors": len(v["verify"]),
            "invalid_records": len(v["invalid_records"]),
            "module": a.__file__,
            "refusal_children": refusal_children,
            "regressions": [
                "bounded_nonregular_refusal",
                "descriptor_cleanup",
                "private_regular_symlink",
                "strict_transport_bool",
                "umask_0600",
                "__all__",
                "secret_redaction",
            ],
        }
    )
)
