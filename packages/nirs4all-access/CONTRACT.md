# Project-token HTTP v1

Package `nirs4all-access` 0.1.0 (Python >=3.11) is an independent standard-library
leaf. Import `nirs4all_access`; no Cluster, scheduler, science or HTTP dependency.
`CONTRACT_VERSION=1`, `STORE_SCHEMA=nirs4all-access/project-token-store/v1`.
Its API is specified in `API.md`. All services use this verifier, never copied
parsers or a mandatory compute-service dependency. Optional service extra:
`nirs4all-access>=0.1,<0.2`, imported lazily by the service adapter.

Tokens match **fullmatch** `n4p1\.[0-9a-f]{16}\.[0-9a-f]{64}`. The middle
component is the key ID; the last is 256 random bits. Hash is `sha256:` plus
lowercase SHA256 of `b"nirs4all-access/project-token/v1\x00" + ascii(token)`;
the separator is one NUL byte. Credentials are never persisted in clear text.
Provisioning intentionally returns the generated secret once.

`ProjectPrincipal` is frozen and contains project_id, principal_id, services,
rights, group_ids. Project IDs match `[a-z0-9][a-z0-9_-]{0,62}` excluding Windows
reserved devices con/prn/aux/nul/com1-9/lpt1-9. Principal IDs match
`[a-z0-9][a-z0-9._-]{0,127}`. Services match `[a-z][a-z0-9-]{0,31}`. Assigned IDs:
cluster, arena, datasets, repository. Rights match `<service>.<action>` where
action matches `[a-z][a-z0-9_]{0,31}`. Right prefixes must equal services. Rights
are exact membership: `cluster.admin` alone does not grant `cluster.submit`.
Roles are owner constants expanded to explicit rights; Cluster admin expands to
read/submit/cancel/execute/admin. Content services own their action vocabularies.
No implicit groups or wildcard grants: group_ids must be empty until schema v2.
One credential grants exactly one immutable project; names are not authority.

Store JSON has exactly schema and credentials. Records have exactly nine required
fields: key_id, token_hash, project_id, principal_id, services, rights, group_ids,
expires_at, revoked. Expiry is finite numeric epoch seconds or null, never bool;
revoked is bool only. Unknown fields (including plaintext token), duplicate keys,
array duplicates, duplicate digests/key IDs, nonfinite numbers and malformed UTF-8
are rejected. Canonical dump sorts keys, records by key_id and arrays, compact JSON
with a trailing newline. Writes use a same-directory O_EXCL temp file, enforce
0600 with fchmod where available regardless of umask, fsync, and replace. Loading
checks the raw descriptor before reading and refuses non-regular files without
blocking, including FIFOs, symlinks to FIFOs, directories and devices. Symlinks to
private regular files are accepted. POSIX loading refuses group/other permissions;
Windows permission-bit enforcement is unavailable. Stores are trusted operator
configuration. Verifiers are immutable restart snapshots; each service must
refuse startup with zero currently active credentials for that service.

Authenticate HTTP and WS from `Authorization: Bearer <token>` before body reads.
Scheme is case insensitive; one or more spaces allowed; no whitespace inside the
token. Refuse query credentials and all project selector headers/paths. Owner
adapters sanitize rejected query strings before logging. This is necessary but
insufficient: transport DEBUG logging may expose request targets and headers
before ASGI runs. Owned adapters must also use a fixed quiet transport profile;
for Uvicorn this is `log_config=None, log_level="warning", access_log=False`,
with root/owned-handler allow-list filtering that keeps third-party records at
WARNING or stricter. Application diagnostic verbosity must never enable wire or
access logs. Direct hosts, reverse proxies, packet captures, core dumps,
debuggers and shell history remain operator-owned. Query strings are not an
authentication channel; even opaque embedded values must not reach owned logs.
This documentation successor changes no leaf Python/API/schema/vector behavior.
Client credential source
is NIRS4ALL_PROJECT_TOKEN or a private --token-file, never argv.

Validation order: programmer errors (invalid service, string required_rights,
cross-service rights, nonfinite now) raise ValueError. Missing/malformed/unknown/
wrong/revoked/expired credentials return identical 401, code invalid_credential,
detail `invalid or missing credential`, with `WWW-Authenticate: Bearer`. Expired
means now >= expires_at. Known/unknown keys use constant-time hash comparison,
including a dummy hash for unknown keys. Valid credential lacking service gives
403 service_not_granted; lacking rights gives 403 missing_rights with sorted names.
Only after authorization does the owner return 404 for unknown/foreign resources.
No open mode. An empty verifier denies all requests.

Only localhost and IP loopback literals permit cleartext. Every other host needs
TLS or an explicit trusted TLS-proxy acknowledgement. Both transport flags must
be bool; strings and numeric flags raise ValueError. Worker session secrets,
attempt fences and partitioned store policies remain Cluster-owned. A project
consumer grants code execution on trusted project providers, not a sandbox.

Golden vectors: `tests/vectors/project-token-v1.json`. Run `tests/check_vectors.py`
against an installed wheel with `python -I`; it blocks nonstdlib service imports.
