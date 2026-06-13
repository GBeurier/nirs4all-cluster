# nirs4all-cluster

> **Status: public alpha / validation prototype.** This repository is public so that
> architecture, tests, and measurements are inspectable. It is still a prototype:
> its goal is to measure whether a distributed job queue for`nirs4all.run()`is
> justified, not to promise a ready-to-use cluster platform. See
> [`PROTOTYPE_DESIGN.md`](PROTOTYPE_DESIGN.md) for the design and
> [`PROTOTYPE_TO_PRODUCTION.md`](PROTOTYPE_TO_PRODUCTION.md) for conditions
> possible passage into product.

**Distributed** execution of`nirs4all`pipelines (client / server / workers): a coordinator
receives jobs and dispatches the work to workers that poll the server. The prototype
**does not modify any other library** in the ecosystem:`nirs4all`is only imported by the
runner subprocess, and the server/client works without it.

## What the prototype does

- Submission of a`nirs4all.run()`job via Python SDK or CLI. - FastAPI server + SQLite file + local object store addressed by SHA-256. - Workers polling (long-polling HTTP + heartbeat), with a task sandbox per folder. - Atomic job (Level 0) and`pipelines × datasets`decomposition (Level 1) with aggregation/ranking. - Download artifacts: JSON summary, logs, best`.n4a`model. - Recovery after worker crash (lease + retry), cooperative cancellation, idempotence. - Routing by capabilities: labels, memory, **package versions (PEP 440)**, **GPU/CUDA** (auto-detected,`requirements.min_gpu_count`or`cuda=true`label). A`nirs4all.run`job requires`nirs4all`by default.

## Installation

```bash
# Worker environment = an existing nirs4all environment + this package:
uv pip install -e .            # serveur + client + transport worker
# (workers provide nirs4all themselves; it is not a hard dependency)
```

Python ≥ 3.11. The server and client only need FastAPI/uvicorn/httpx/pydantic; only the
worker needs a provisioned`nirs4all`environment.

## Quickstart (LAN de confiance)

```bash
# 1) serveur
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state

# 2) one or more workers (on machines that can see nirs4all and the dataset)
#    The worker auto-detects GPUs (nvidia-smi) and declares the cuda + gpu_count labels;
#    force with --gpus N (0 to hide GPUs).
n4cluster worker --server http://HOST:8765 --labels site=lab --slots 1

# 3) submit a job and wait for the result
n4cluster submit examples/job.shared-path.yaml --wait --out ./results
n4cluster status   <job_id>
n4cluster logs     <job_id>
n4cluster cancel   <job_id>
n4cluster artifacts <job_id> --out ./results
```

SDK Python :

```python
from nirs4all_cluster import ClusterClient

client = ClusterClient("http://host:8765", token=None)
job = client.submit_run(
    pipeline="/shared/pipelines/pls.yaml",                 # kind=path
    dataset="/shared/datasets/corn",                       # kind=shared_path
    params={"random_state": 42, "refit": True},
)
job = client.wait(job.id)
print(job.aggregate.best_metric, job.aggregate.ranking)
client.download_best_model(job.id, "best_model.n4a")
```

## Architecture

```
submitter (SDK/CLI/Studio) ──REST + WS──► serveur (FastAPI + SQLite + object store + scheduler + events)
                                              ▲
                          long-polling HTTP + heartbeat
                                              │
                                          workers ──► subprocess runner ──► nirs4all.run(workspace=task_ws)
```

- **`nirs4all_cluster/server/`** —`app.py`(API),`db.py`(SQLite file, atomic leasing, reaper),`scheduler.py`(state machines + matching),`artifacts.py`(SHA-256 store),`events.py`(broker). - **`nirs4all_cluster/worker/`** —`agent.py`(polling loop),`materialize.py`(resolution of
  references → local paths),`executor.py`(subprocess + capture + undo). - **`nirs4all_cluster/runners/nirs4all_run.py`** — **only** module that imports`nirs4all`. - **`client.py`** (SDK), **`cli.py`** (`n4cluster`), **`schemas.py`** (Pydantic contract).

## Tests and validation

```bash
pytest -q                                   # 45 unit/API tests without nirs4all + 3 integration tests
python scripts/validation.py                # end-to-end harness on nirs4all-data (8/8)
```

Results measured on`nirs4all-data`(see [`WORKLOG.md`](WORKLOG.md)): atomic job →`.n4a`,
2 workers in parallel, **kill worker → retry**, cancellation not restarted,`pipeline ×
dataset`aggregation, and **exact metric parity vs local`nirs4all.run()`(diff = 0.0)** — beyond the criterion
go/no-go ≤ 1e-10.

## Go/no-go criteria to upgrade to product

The go remains conditional on **all** of these conditions:

1. ≥ 2 labs/partners explicitly request distributed execution. *(not measurable here)*
2. Speedup ≥ 3× on a real workload (grid search AOM / HPO on ≥ 32 datasets). *(to be measured)*
3. Metric-identical results (≤ 1e-10) with single-machine. → **reached: diff = 0.0** on the atomic job. 4. Data + security + recovery model written **before** the code. → done in`PROTOTYPE_DESIGN.md`. 5. Framing topics covered from the start (mTLS, secrets, third-party sandboxing, IP/GDPR datasets,
   heavy TF/Torch/JAX environments, transfer costs, idempotence/resumption, quotas/fairness,
   heterogeneous scheduling). → listed in`PROTOTYPE_TO_PRODUCTION.md`.

Without these conditions: **no-go product** — and the default option remains a Dask opt-in backend in`nirs4all`, not an in-house platform. The deposit remains public as a measuring bench and reference for
design, not as a product roadmap commitment.

## Non-objectifs (rappel)

No modification of other libs, no open multi-tenant, no sandbox for Python code
arbitrary, no K8s/Ray/Dask type scheduler, no concurrent writing in a workspace`nirs4all`shared, no distribution of folds. See`PROTOTYPE_DESIGN.md`§ Non-objectives.

## References

`PROTOTYPE_DESIGN.md`,`PROTOTYPE_TO_PRODUCTION.md`,`WORKLOG.md`, and`nirs4all-ecosystem/NIRS4ALL-ECOSYSTEM_VISION.md`(annex *Perspective: distributed execution*, risk R13).
