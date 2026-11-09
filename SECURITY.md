# Security policy

## Supported scope

The API v2 beta provides trusted private project services: immutable credential
principals, explicit rights, project-separated DB/CAS/event stores, worker session
secrets and attempt fencing. It is not a sandbox for hostile science code.
See [security and scope](docs/security-and-scope.md) for the exact guarantees and
trusted-provider attestation boundary, and [the shared contract](packages/nirs4all-access/CONTRACT.md).

No open mode or single shared-token startup exists. Credentials are hash-only,
issued through token-create, loaded from private config, and changed at restart.
HTTP/WS use Authorization Bearer headers only. TLS is required outside loopback
(or actual trusted proxy termination); project admin remains project-scoped.
No mTLS/OIDC, container sandbox, quotas or encryption-at-rest is claimed.

A consumer grants code execution on its project's providers: owner declarative
YAML can construct arbitrary Python classes. The entrypoint dual opt-in only
gates python_entrypoint. Use one OS identity per project provider and trusted
consumer access. Path allowlists protect entry paths but cannot sandbox science.

## Exposed credentials

Revoke or rotate an exposed credential, restart the server, and remove it from
sources and downstream logs/artifacts. Do not put secrets in argv, URLs, docs or
fixtures. Only the explicitly synthetic shared-contract vector directory is
excluded from entropy scanning; other sources use generated test credentials.
Run scripts/secret_shape_guard.py and the configured secret scanner before a
future publication. Do not weaken authentication to support the deferred dashboard.

## Reporting a vulnerability

Report privately to **nirs4all-admin@cirad.fr**, including affected version and a
bounded reproduction. Do not include real tokens or private science data.

## Owned transport logging

`server` and `worker` accept only critical/error/warning/info/debug for
`--log-level`; it controls application diagnostics only. They deliberately have
no access log: raw HTTP/WS targets and request headers are never logged by these
owned entrypoints. Third-party output stays WARNING or stricter. Error/critical
intentionally filter INFO registration/finalization diagnostics. There is no
transport-level flag or environment bypass.

Direct ASGI hosts must use `configure_logging` plus the fixed Uvicorn profile
`log_config=None, log_level="warning", access_log=False`. Sanitizing queries in
ASGI cannot prevent pre-ASGI wire DEBUG logs. Direct hosting, reverse proxies,
packet captures, core dumps, debuggers and shell history are operator-owned.
Query strings are not a credential channel; opaque embedded tokens can be served
as ordinary input but are not logged by owned entrypoints. SDK/WorkerClient do
not configure host logging: current HTTPX/HTTPCore DEBUG measurements found no
credential/header leakage, without extending the CLI guarantee to host logging.
See the operations logging policy for exact scope and diagnostic levels.
