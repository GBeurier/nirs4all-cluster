# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`nirs4all-cluster` is a **trusted-LAN beta** (graduated from a validation prototype): a small
distributed job queue (client / server / polling workers) for `nirs4all.run()`. It is a sibling
repo of the nirs4all ecosystem (checked out under `/home/delete/nirs4all/`, see the parent
`CLAUDE.md`), but is **not** listed in that ecosystem index and has no production (GA) commitment.

It originally existed to **measure** whether a distributed queue for `nirs4all.run()` is justified
(see the go/no-go criteria in `README.md` and `PROTOTYPE_TO_PRODUCTION.md`); it is now packaged and
published as a usable trusted-LAN beta. The broader product decision — a native cluster vs. a Dask
opt-in backend in `nirs4all` — remains open, and the documented non-goals still bound the scope.

**`PROTOTYPE_DESIGN.md` is the source of truth** for the data/security/recovery model and the
non-goals — it was written before the code, deliberately. Read it before changing the wire
contract, state machines, or matching policy. `WORKLOG.md` records measured results.

## The one load-bearing invariant

**Only `nirs4all_cluster/runners/nirs4all_run.py` may import `nirs4all`.** It runs as a child
process (`python -m nirs4all_cluster.runners.nirs4all_run`). The server, client, worker agent,
materializer, and executor must stay `nirs4all`-free so the server/client run without the
library installed (the same guard pattern `nirs4all-studio` uses). The subprocess boundary also
buys crash isolation (a native-backend segfault doesn't kill the worker) and real
cancellation (the parent can `terminate()` the child). Don't move `nirs4all` logic up the stack;
if the runner needs more from a run result, extend the summary it emits, not the agent.

## Commands

Use the sibling nirs4all venv for anything that touches `nirs4all` (integration tests,
`scripts/validation.py`); the package's own deps are enough for the unit/API suite.

```bash
# Install (server + client + worker transport only; nirs4all is NOT a dependency)
uv pip install -e ".[dev]"

# Green gate
ruff check .
mypy nirs4all_cluster
pytest -q                         # 45 unit/API tests — run WITHOUT nirs4all installed

# A single test / file / pattern
pytest tests/test_scheduler.py -q
pytest tests/test_server_api.py::<name> -q
pytest -k "lease and retry" -q

# Integration + end-to-end (need the nirs4all venv AND nirs4all-data present;
# tests skip themselves otherwise — see tests/test_integration_nirs4all.py)
/home/delete/nirs4all/nirs4all/.venv/bin/python -m pytest tests/test_integration_nirs4all.py -q
/home/delete/nirs4all/nirs4all/.venv/bin/python scripts/validation.py   # real OS processes; SIGKILLs a worker to prove recovery
```

Running the system (entry point `n4cluster`, also `$N4CLUSTER_SERVER` / `$N4CLUSTER_TOKEN`):

```bash
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state [--token ${N4CLUSTER_TOKEN} [--allow-python-jobs]
n4cluster worker --server http://HOST:8765 --labels site=lab --slots 1 [--gpus N] [--allow-python]
n4cluster submit examples/job.shared-path.yaml --wait --out ./results
n4cluster status|logs|cancel|artifacts|workers <job_id>
```

Ruff: line-length 120, py311, ignores `E501`/`UP042` (keep `str, Enum`, not `StrEnum`).

## Architecture (the parts that span files)

```
client (SDK/CLI) ──REST + WS──► server (FastAPI + SQLite + object store + scheduler + events)
                                     ▲
                  long-polling HTTP lease + heartbeat
                                     │
                                 worker agent ──► subprocess runner ──► nirs4all.run(workspace=task_ws)
```

**`schemas.py` is the only boundary-validation point** (ecosystem convention: validate at
edges, trust internal code). Every Pydantic model — the wire contract — lives here and is
imported by server, worker, and client so the format stays in one place. A job provides one of
`pipeline`/`pipelines` and one of `dataset`/`datasets`; lists decompose into the cartesian
product (Level 1).

Two small `nirs4all`-free shared modules sit beside it: **`versioning.py`** (the protocol
`API_VERSION`, the `X-N4C-*` handshake headers, compatibility helpers, and the canonical
pipeline `fingerprint_*` functions — imported by client, server, worker and materializer) and
**`logging_setup.py`** (`configure_logging` for the server/worker daemons). The server also
serves a single-file ops dashboard at **`/ui`** (`server/static/index.html`) backed by a global
WebSocket feed `GET /v1/events/stream`, plus `/healthz`, `/version`, `/v1/stats`, and
filterable `GET /v1/jobs`. A one-line `@app.middleware("http")` does the version handshake
(reject incompatible protocol major → 426; log/emit `version_divergence` on compatible drift)
and the JSON request-size guard; both are config-driven and respect the non-goals.

**Server = policy + mechanism, split on purpose:**
- `server/scheduler.py` (policy) — the `JOB_TRANSITIONS` / `TASK_TRANSITIONS` state machines
  (mirroring the design doc), `requirements_match` (labels → memory floor → GPU fail-closed →
  PEP 440 package versions), and ranking comparison. Pure and unit-tested in isolation
  (`tests/test_state_machine.py`).
- `server/db.py` (mechanism) — one `sqlite3` connection (WAL, `check_same_thread=False`) behind
  a reentrant lock. The design mandates a **single server process**, which is what makes leasing
  atomic without an external broker. The DB *enforces* the scheduler's transitions on every
  status change. Key subtlety: **worker slot usage is derived live from the task table**
  (`_in_flight_count`), never a mutable counter — this survives reaping/revival/races without
  drift. Don't reintroduce a counter.
- `server/app.py` — FastAPI routes (separate client API and worker API), plus a background
  **reaper** loop that requeues expired leases and marks silent workers dead. `_finalize_job` /
  `_build_aggregate` recompute the job aggregate (ranking + best-model artifact) and flip the
  job to a terminal state atomically/idempotently (`try_set_job_status`) so two workers
  finishing the last tasks can't both flip it.
- `server/artifacts.py` — content-addressed blob store (`objects/aa/bb/<sha256>`); identical
  bytes stored once; over-limit streams are rejected mid-upload so no leaked blob lands.
- `server/events.py` — every event is persisted (history pagination) and fanned out to live
  WebSocket subscribers via an in-process asyncio broker with bounded, drop-oldest queues.

**Worker** (`worker/`), never imports `nirs4all`:
- `agent.py` — registers, runs a heartbeat thread + a lease loop (one thread per leased task up
  to `slots`). Auto-detects GPUs via `nvidia-smi` (no torch/tf import) and advertises declared
  package versions so the server can route on them. Cancellation is **cooperative**: the server
  returns `cancel_task_ids` on heartbeat; the agent terminates the matching subprocess.
- `materialize.py` — resolves every pipeline/dataset ref to a concrete local path (downloads
  uploaded artifacts, safely extracts zips — rejects zip-slip / absolute / symlink members) and
  emits a plain-dict *runner spec*. `dataset kind="catalog"` is intentionally `NotImplemented`
  in the prototype.
- `executor.py` — launches the runner subprocess, captures the log, polls for
  cancellation/timeout, and reads back `result.json`.

**Recovery & correctness model** (all in the design doc): lease + TTL + retry on worker crash,
`idempotency_key` dedupe (unique index + `IntegrityError` race handling), cooperative
cancellation that a cancelled job's reaped lease never relaunches, and exact metric parity vs a
local `nirs4all.run()` (the go/no-go criterion).

## Conventions specific to this repo

- **Respect the non-goals** (`PROTOTYPE_DESIGN.md` §Non-goals): no modifying other libs; no open
  multi-tenancy; no secure sandbox for arbitrary Python (hence `python_entrypoint` pipelines are
  gated behind both the server's `--allow-python-jobs` and the worker's `--allow-python`); no
  K8s/Ray/Dask-class scheduler; no concurrent writes to a shared `nirs4all` workspace; no fold
  distribution. PRs that cross these are out of scope.
- **State changes go through the state machine.** Add a transition to `scheduler.py` first; the
  DB validates against it. Don't `UPDATE ... status` around `_set_task_status` /
  `validate_*_transition`.
- An `nirs4all.run` job auto-gets a presence requirement for `nirs4all` (`packages["nirs4all"]=""`)
  unless the client pins a range — a job never routes to a worker that can't prove the library.
- `worker_local` dataset kind currently behaves like `shared_path` (true locality routing is a
  future feature); pin placement yourself via `requirements.labels`.
