# Architecture

```{mermaid}
flowchart LR
    subgraph Client
      CLI[n4cluster CLI]
      SDK[ClusterClient SDK]
    end
    subgraph Server["FastAPI server · single process"]
      API[REST + WebSocket]
      SCHED[scheduler · state machines + matching]
      DB[(SQLite · WAL)]
      OBJ[(object store · sha256)]
      EV[event broker]
    end
    subgraph Worker
      AGENT[agent · lease + heartbeat]
      RUN[subprocess runner]
    end
    CLI & SDK -- REST/WS --> API
    AGENT -- long-poll lease + heartbeat --> API
    AGENT --> RUN
    RUN -- imports --> N4A[nirs4all.run]
    API --- SCHED --- DB
    API --- OBJ
    API --- EV
```

A **single server process** owns the queue (SQLite in WAL mode), a content-addressed
object store, the scheduler (state machines + worker matching), and an in-process event
broker. Workers **poll** the server (long-poll lease + heartbeat) rather than receiving
pushes — simpler for a LAN and for machines behind NAT.

## The load-bearing invariant

The **only** module that imports `nirs4all` is the runner subprocess
(`nirs4all_cluster/runners/nirs4all_run.py`). The server, client, worker agent,
materializer and executor stay `nirs4all`-free. This buys three things: the control plane
runs without the library installed; a native-backend segfault can't kill the worker; and
the parent can `terminate()` the child for real cancellation.

## A task's life

```{mermaid}
sequenceDiagram
    participant C as Client
    participant S as Server
    participant W as Worker
    C->>S: POST /v1/jobs
    W->>S: POST /v1/workers/{id}/lease (long-poll)
    S-->>W: TaskPayload
    W->>S: POST /v1/tasks/{id}/start
    W->>S: POST /v1/tasks/{id}/events (progress)
    W->>S: POST /v1/tasks/{id}/artifacts (upload by sha256)
    W->>S: POST /v1/tasks/{id}/complete (TaskResult + fingerprint)
    C->>S: GET /v1/jobs/{id} (aggregate + ranking)
```

Recovery rests on leases: a lease has a TTL renewed on every heartbeat, so a long task is
not wrongly reaped while its worker is healthy; if the worker goes silent the lease lapses
and the task is requeued (or failed after its attempts). A cancelled job's reaped lease is
never relaunched. See {doc}`../operations`.
