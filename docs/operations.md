# Operations

## Running the system

```{code-block} bash
# coordinator (single process — the design mandates one server)
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state --log-file server.log

# workers (inside a provisioned nirs4all environment), repeated per machine
N4CLUSTER_TOKEN=$N4CLUSTER_TOKEN n4cluster worker --server http://HOST:8765 --labels site=lab --slots 2
```

Watch live state at `http://HOST:8765/ui` ({doc}`dashboard`) or with `n4cluster jobs` /
`n4cluster workers` / `n4cluster logs`.

## Recovery model

Leasing is the backbone of correctness:

- A lease has a TTL; **every heartbeat renews** the active leases, so a task that runs
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

## Logging & observability

Server and worker emit structured logs (`--log-level`, `--log-file`): lifecycle, reaper
errors, dead workers, job finalization, version divergence, and per-task progress. Every
event is also persisted and fanned out over the WebSocket streams.
