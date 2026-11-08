# Configuration

The API v2 server always requires project credentials. It reads only the hash-only
`<state>/auth/credentials.json` store at startup. It never reads client token
environment variables and has no open mode, shared-token argument or plaintext
principal configuration. Credential changes apply at restart.

Initialize an empty directory with `n4cluster init-state --state ./cluster-state`,
then provision at least one credential with `token-create` (see cli-reference.md).
Nonempty unmarked state is refused **before any mkdir, SQLite open, WAL or schema
change**. Preserve/export old state separately and choose a new empty directory;
do not manually add a marker to a legacy store.

```
cluster-state/
  cluster-state-v2.json
  auth/credentials.json             # hashes only, POSIX 0600
  projects/<credential-project-id>/
    store.sqlite                    # independent WAL queue and metadata
    objects/aa/bb/<sha256>           # independent project CAS
```

Every configured active project opens at lifespan startup and gets an independent
reaper, broker and database; shutdown cancels reapers and closes all databases.
Identical bytes in different projects remain separate. A credential selects the
project; no project header, path, role header or payload overrides it.

Client/worker credential precedence: private `--token-file`, then
`NIRS4ALL_PROJECT_TOKEN`, then the deprecated `N4CLUSTER_TOKEN` client alias.
`N4CLUSTER_SERVER` supplies the client/worker URL (default http://127.0.0.1:8765).
Never put tokens in argv or URLs. A token file must be private (0600 on POSIX).

Server settings retain lease_ttl_s=60, heartbeat_interval_s=10,
worker_dead_after_s=45, reaper_interval_s=5, max_artifact_mb=2048,
max_request_mb=16, allow_python_jobs=False and cors_origins=[]. JSON bytes are
counted in ASGI receive, including chunked requests; multipart has a bounded
transport allowance for framing and a separate exact blob limit. Unauthorized
requests are rejected before consuming the body. CORS is opt-in and outermost.

Loopback permits local HTTP. Other binds require --ssl-keyfile/--ssl-certfile or
--trusted-tls-proxy acknowledging actual TLS termination. Clients reject bearer
transport over HTTP to non-loopback hosts; use the HTTPS proxy address.
Workers deny shared input paths by default: repeat --allow-path-root for each
provisioned directory. --python-exe selects an explicitly provisioned interpreter.

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

## Client URL admission and failure diagnostics

The common transport used by ClusterClient, WorkerClient and the worker accepts
absolute HTTP(S) base URLs only. Userinfo (including empty or encoded credentials),
query and fragment components (including empty `?` or `#`) are refused with a
fixed non-echoing error, even without a bearer token. Valid loopback IPv4/IPv6,
localhost and HTTPS service base paths remain supported. Authentication belongs
in the bearer header, never in the server URL.

Owned CLI and agent HTTP failure diagnostics contain status/class only:
`HTTP 404`, for example, or `ConnectError`. Request URLs, headers, exception
messages and exception chains are not rendered. The same safe agent diagnostic
is persisted as TaskFailure.error and returned to consumers. Failures remain
visible at their configured severity; task retry/attempt/cancellation policy is
unchanged. Raw HTTPX failures at the CLI return 3 for 401/403 and 4 otherwise;
wrapped SDK connection failures return 4 without their raw message. SDK clients
continue to leave host logging alone. There is no routing-URL exception to the
owned entrypoint privacy guarantee.
