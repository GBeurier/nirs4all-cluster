# Security and scope

This is a trusted private project service, not a sandbox for hostile science code.
A consumer credential grants **code execution on that project's providers**:
owner YAML class construction can instantiate arbitrary Python classes. The dual
--allow-python-jobs / --allow-python opt-in gates only python_entrypoint, not that
owner loader behavior. Provision one OS identity per project provider, isolate
filesystem/network permissions at the OS boundary, and grant consumer rights only
to trusted collaborators. A path-root allowlist protects materialization entry
paths; it cannot sandbox arbitrary code or paths embedded in scientific configs.

Only the child runner imports nirs4all. Before launching it or its runtime-version
probe, the worker removes known control-plane credential environment keys,
including N4CLUSTER_TOKEN, NIRS4ALL_PROJECT_TOKEN and worker-secret keys. Scientific
thread settings and explicitly provisioned PYTHONPATH/Python remain available.
No credentials are persisted in auth files, provenance or generated archives.
Caller-supplied science params remain trusted caller content.

Credentials authenticate one immutable project/principal and exact rights through
the independent stdlib nirs4all-access leaf. Cluster roles expand to cluster.read,
cluster.submit, cluster.cancel, cluster.execute, cluster.admin; admin expands to all
five and is still project-scoped. Header roles, project selectors and group claims
grant nothing. Groups are reserved and rejected until schema v2. Rotation/revocation
is a restart snapshot. The normative spec and golden vectors live in
`packages/nirs4all-access/CONTRACT.md` and `tests/vectors/project-token-v1.json`.
Other service owners consume this same optional leaf, never a copied verifier or
a compute-service dependency.

HTTP and WebSocket use Authorization Bearer headers only, before body reads.
401 invalid credential precedes 403 missing service/right and resource 404.
WWW-Authenticate: Bearer accompanies 401. Query credentials are rejected and their
query string is cleared on denial; the owned quiet transport profile also prevents
wire logs before ASGI runs. Public endpoints
are exactly /, /healthz, /version, /ui and /ui/; API docs/discovery are disabled.
TLS is required outside loopback, with an explicit trusted proxy option for binds.

Each project has its own DB, in-memory event broker and CAS. Resource IDs, events,
replay/live streams, pagination, stats and idempotency are project-local. Worker
registration additionally returns a random per-worker session secret (hash-only
on the server), required for lifecycle calls. Every task report includes worker ID
and attempt; the DB lock checks assignment, in-flight status and lease expiry
before mutations. Heartbeats renew only declared live (task, attempt) pairs.
Terminal event names and best_model links are server-owned. Result refs must be
uploaded under that worker's current attempt and role. Earlier attempts cannot
become the final model; model links are exposed only after accepted success.

The runner calls the real run_result.export and the owner's load_bundle API for
archive metadata/version and supported source-capture checks. It records
nirs4all-python-n4a-1.0, bundle_format_version, owner version, SHA256 and byte size.
Requested export refusal/missing/invalid archive is non-retriable failure.
export_best_model=false reports not_requested. Default jobs always report
canonical_core=false.

The only path that reports canonical_core=true is an explicit SDK job with
engine="native" and native_profile="nirs4all.n4a.container.v1". Core's single
native fit seals the container; the runner exports those exact bytes, requires the
file SHA-256 and size to equal Core's sealed receipt, and reports the container
id, physical profile and the model/run/result/score identities read from the owner
manifest (no bundle_format_version exists for this profile). canonical_core=true
means only "sealed by Core's native linear single-source producer, as reported by
a trusted provider": the server does not verify it, parse the archive or infer
scientific validity, and it implies no host-model, Python-estimator or general
feature parity. This profile writes no task workspace, so the default isolated
task-workspace semantics are not honored: the runner refuses keep_task_workspace
and a robustness evidence handoff for it, and the SDK refuses workspace_path,
random_state, tuning, calibration and sessions rather than ignoring them.
The route also needs a Core, Methods and IO cohort that provides the canonical
producer; until then it fails closed with no bundle or interim V2 fallback.

The **server verifies transport/reference identity**, not scientific truth. It
checks content hashes/sizes and current provider/task/attempt links. A malicious
trusted provider can lie about its exporter or science; the server does not import
science or parse archives. SDK and workers verify downloaded bytes against the
server identity; the worker also verifies submit-pinned input hashes.

The unchanged /ui frontend still uses query-based WebSocket auth and its live feed
is unavailable under v2. REST panels may use the project token; dashboard migration
requires a later joint frontend batch. Do not weaken auth to restore that feed.

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
