# Python SDK

`ClusterClient` is the public SDK. It speaks the REST API and nothing more — it never
imports `nirs4all`.

```{code-block} python
import os
from nirs4all_cluster import ClusterClient, build_nirs4all_run_request

with ClusterClient("https://cluster.example", token=os.environ["NIRS4ALL_PROJECT_TOKEN"]) as client:
    job = client.submit_nirs4all_run(
        pipelines=["/shared/pls.yaml", "/shared/rf.yaml"],
        datasets=["/shared/corn", "/shared/wheat"],
        n_jobs=1,
        rank_metric="best_rmse", rank_mode="min",
    )
    job = client.wait(job.id)
    for row in job.aggregate.ranking:
        print(row["pipeline"], row["dataset"], row["metrics"])
    client.download_best_model(job.id, "best.n4a")

    # ops helpers
    print(client.stats())
    for j in client.list_jobs(status="running"):
        print(j.id, j.status)

    # Same request builder, useful for core / CLI callers that need to inspect or
    # persist the exact contract before submission.
    req = build_nirs4all_run_request(
        pipeline="/shared/pls.yaml",
        dataset="/shared/corn",
        params={"random_state": 42, "refit": True},
    )
    assert req.parity.scope == "atomic"
```

`submit_run()` remains as a compatibility alias. The core-facing adapter accepts local
`nirs4all.run` vocabulary where it is meaningful: `n_jobs` is translated to the
worker-local `inner_n_jobs`, while `workspace_path` is omitted because every distributed
task gets an isolated workspace. The stored `DistributedRunParity` contract says the beta
expects metric parity for whole-run tasks (atomic or explicit `pipelines x datasets`) and
does **not** claim fine-grained DAG, variant, fold, subtree, or workspace parity.

If the server runs an incompatible protocol major the client raises
`nirs4all_cluster.versioning.ClusterVersionError`; a compatible-but-different package
version is logged once (see {doc}`versioning`).

## API

```{eval-rst}
.. autoclass:: nirs4all_cluster.ClusterClient
   :members:
   :special-members: __init__, __enter__, __exit__
```

The credential binds one project; see rest-api.md for protocol v2. WorkerClient
registers a provider, keeps its session header and sends the leased attempt on
every report. Complete tasks with explicit delivery metadata; failed uploads and
completions raise typed errors. SDK downloads verify content hashes and sizes.

API v2 provider lifecycle calls require the immutable leased attempt explicitly:
`worker.start_task(task.task_id, attempt=task.attempt)`, and likewise
`report_event`, `upload_artifact`, `complete_task` and `fail_task`. Retain the
original task/attempt with its work; never look up the newest lease by task ID.
Overlapping retries on the same provider have separate heartbeat/terminal entries.
All three download clients stream into private temporary files and atomically
replace the destination only after full hash/size validation; a failed retrieval
preserves an existing qualified model.

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
