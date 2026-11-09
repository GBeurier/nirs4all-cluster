# Operations

## Running the system

Follow quickstart.md to initialize **new** state and provision separate consumer
and provider credentials. Start one loopback server process, or bind behind a real
TLS proxy with --trusted-tls-proxy. Workers use --token-file and an explicit
--allow-path-root list. Consumers use --token-file or NIRS4ALL_PROJECT_TOKEN.
Server credentials are hash-only config loaded at restart. The server never reads
client token environment variables. The unchanged dashboard live feed is deferred.

## Recovery model

Leasing is the backbone of correctness:

- A lease has a TTL; heartbeats renew only declared active (task, attempt) pairs, so a task that runs
  longer than the TTL is not reaped while its worker is healthy.
- If a worker goes silent past `worker_dead_after_s`, it is marked dead; the reaper
  requeues its in-flight tasks (or fails them once attempts are exhausted).
- A **cancelled** job's reaped lease is moved to `cancelled`, never relaunched.
- `idempotency_key` deduplicates submissions (a unique index + race handling).

## Task state machine

```{mermaid}
stateDiagram-v2
    [*] --> queued
    queued --> leased
    queued --> cancelled
    leased --> running
    leased --> queued: lease expired / retry
    leased --> cancelled
    running --> succeeded
    running --> failed
    running --> queued: lease expired / retry
    running --> cancelled
    failed --> queued: retry
    succeeded --> [*]
    failed --> [*]
    cancelled --> [*]
```

State changes always go through the scheduler's transitions, which the database enforces —
there is no `UPDATE … status` around them.

## Routing

The scheduler matches a task's `requirements` against each worker: labels, a soft memory
floor, a fail-closed `min_gpu_count`, and PEP 440 `packages` specifiers checked against the
versions the worker declared. An `nirs4all.run` job implicitly requires `nirs4all` present,
so it never routes to a worker that can't prove the library.

## Logging and observability

`--log-level` controls application diagnostics only: critical, error, warning,
info (default), or debug. `--log-file` adds the same filtered diagnostics to a file.
Only the `nirs4all_cluster` namespace follows this level. Other libraries emit at
WARNING or stricter; handler filters enforce this even for explicitly DEBUG or
previously unknown third-party loggers. At error/critical, lower-severity
application diagnostics are intentionally absent. Job finalization and worker
registration are INFO; provisioning/runtime failures are ERROR.

At accepted log levels, `n4cluster server` and `n4cluster worker` never log raw
HTTP/WS targets or request headers. There is deliberately no access log and no
transport verbosity, access-log flag or environment override. The fixed Uvicorn
profile is defined once in `logging_setup.py`:

```python
uvicorn.run(app, log_config=None, log_level="warning", access_log=False)
```

Direct `create_app` ASGI hosting must configure the same namespace allow-list
and handler filters (using `configure_logging`) and pass this exact profile.
ASGI query sanitization alone cannot prevent wire DEBUG logging before ASGI runs.
Reverse proxies, packet captures, core dumps, debuggers and shell history are
operator-owned. The CLI guarantee does not cover arbitrary external hosting.

Query strings are not a credential channel. Exact query credentials are refused;
an opaque embedded token may be served as ordinary input, but owned entrypoints
never log it. Always send `Authorization: Bearer` and keep secrets out of URLs.
SDK and WorkerClient under a host's DEBUG logging were measured with the current
HTTPX/HTTPCore versions without credential/header leaks; library clients do not
configure host logging, and this measurement is not a host-logging guarantee.

Lifecycle, reaper failures, finalization and version diagnostics remain structured
application records. Job events remain persisted and available over authenticated
WebSocket streams; frontend live-feed migration remains deferred.

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
