# CLI reference

`n4cluster init-state --state NEW_EMPTY_DIR` explicitly bootstraps state. It
refuses any nonempty directory. The server never bootstraps or upgrades old state.

`n4cluster token-create --state DIR --project PROJECT_ID --principal PRINCIPAL_ID
--role ROLE` issues a random project secret and prints it once to stdout. Only its
hash is persisted. Roles are submitter, executor, viewer, admin; repeat --role or
comma-separate roles. Optional --expires-at is finite epoch seconds. Credential
changes require a server restart. Redirect the intentional one-time output into a
private file using a restrictive umask; never paste it into logs or command args.

```bash
n4cluster init-state --state ./cluster-state
umask 077
n4cluster token-create --state ./cluster-state --project lab --principal consumer --role submitter > consumer.token
n4cluster token-create --state ./cluster-state --project lab --principal provider --role executor > provider.token
n4cluster server --state ./cluster-state --host 127.0.0.1 --port 8765
n4cluster worker --server http://127.0.0.1:8765 --token-file provider.token --state ./provider-state --allow-path-root /shared
n4cluster submit examples/job.shared-path.yaml --token-file consumer.token --wait --out ./results
```

Client commands (`submit`, `run`, `status`, `jobs`, `logs`, `cancel`, `artifacts`,
`workers`) and `worker` accept --server and --token-file. A secret manager may
supply NIRS4ALL_PROJECT_TOKEN instead. There is no --token credential argument.

`server`: --state, --host, --port, --lease-ttl, --allow-python-jobs,
--cors-origin (repeat), --log-level, --log-file, --ssl-keyfile, --ssl-certfile,
--trusted-tls-proxy. Non-loopback binds need TLS or explicit proxy acknowledgement.

`worker`: --state, --labels k=v, --slots, --memory-gb, --gpus, --allow-python,
--allow-path-root (repeat; deny by default), --python-exe, --name,
--poll-interval, --log-level, --log-file. Use one OS identity per project provider.

`run`: --pipeline and --dataset (repeat for matrix), --param KEY=YAML_VALUE,
--n-jobs/--inner-n-jobs, --require-label KEY=VALUE, --name, --priority,
--rank-metric, --rank-mode min|max, --idempotency-key, --no-export-best-model,
--keep-task-workspace, --wait, --timeout, --out. `submit` accepts a YAML/JSON job.
Both --wait commands exit 1 for failed/cancelled or incomplete jobs, even if some
matrix tasks succeeded. No-export success is explicit and exits zero.

Auth/permission errors exit 3, connection errors 4, invalid input/protocol 2;
other server failures exit 1. Token-create stdout is intentionally secret and is
not a normal diagnostic channel.

`n4cluster token-revoke --state DIR --key-id EXACT_KEY_ID` revokes one configured
credential using its public, exact 16-character key selector. No token is accepted
in argv and no secret is printed. Hash-only configuration changes atomically;
**restart the server** to replace its immutable verifier snapshot. Other project
credentials and stored jobs are preserved. Repeating revocation is safe. Legacy
--token/--principal server options and --auth-file are explicitly refused with
new-state/project-token guidance; option abbreviations are disabled everywhere.
Token files are bounded private regular files, checked on a nonblocking raw
file descriptor before reading; FIFOs, devices and public files are refused.

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
