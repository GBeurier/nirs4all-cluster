# Prototype design - nirs4all-cluster

## Objective

Build an isolated Python prototype in this repository that allows users of
`nirs4all` to submit jobs to one server, then execute them on several
workers. The prototype must validate the need for a “distributed job queue”
without modifying `nirs4all`, `nirs4all-studio`, `nirs4all-io`,
`nirs4all-methods`, or any other ecosystem library.

The prototype is not a definitive platform. It is used to measure:

- whether the distributed work unit is well chosen;
- whether the results remain compatible with local execution;
- whether transfer of data and artifacts is acceptable;
- which network, recovery, and security guarantees become essential.

## Context observed in the ecosystem

- `nirs4all.run()` is the stable public entry point for launching a pipeline on
  a dataset. It accepts a pipeline, a list of pipelines, a dataset, or a list
  of datasets, then executes the Cartesian product.
- `PipelineRunner` already exposes `n_jobs` to parallelize variants locally
  with `joblib/loky`. In parallel mode, local workers do not write directly
  into the `WorkspaceStore`; the parent then rebuilds the state. This is an
  important signal: in a cluster, each worker must produce an isolated result,
  then the server must aggregate it.
- The `nirs4all` workspace is a folder containing `store.sqlite`, arrays, and
  artifacts. You should not write from several machines into the same SQLite
  workspace.
- `nirs4all-studio` already has an in-memory `JobManager`, FastAPI routes, and
  progress WebSockets. It is useful for UX, but it is not a durable
  multi-machine queue.
- `nirs4all-datasets` and `nirs4all-io` point the way for datasets: versioned
  references, checksums, local cache, late materialization.

## Use cases to cover

### MVP

1. Submit a `nirs4all.run()` job from a CLI or a small Python SDK.
2. Start a local or LAN server.
3. Connect several preconfigured Python workers.
4. Assign jobs to workers based on availability and simple capabilities.
5. Track status, logs, approximate progress and results.
6. Download the output artifacts: JSON summary, logs, `.n4a` model, optional
   task workspace.

### Cases to anticipate

- Launching from Studio: the Studio backend would submit to the cluster instead
  of using the local `JobManager`.
- Batch `pipelines x datasets`: decomposition into several independent tasks.
- Grid search / HPO: decomposition into explicit variants, then aggregation.
- Heterogeneous workers: CPU, GPU, RAM, `torch/tensorflow/jax` backend, versions.
- Internal arena: nightly batch on datasets and edge-case scenarios.
- Federated calculation: dataset remains on a given worker/site; only the
  result comes back.
- Recovery after worker or server crash.

## Non-goals of the prototype

- No modification of other libraries.
- No multi-tenancy open to third parties.
- No secure sandbox for arbitrary Python code.
- No advance scheduler such as Kubernetes, Ray or Dask.
- No concurrent writing in a shared `nirs4all` workspace.
- No guarantee of perfect parity on decomposed jobs until the non-regression
  measures are written.

## Proposed architecture

```
submitter Python/CLI/Studio
        |
        | REST + WebSocket/SSE
        v
cluster server
  - API FastAPI
  - queue SQLite
  - scheduler simple
  - object store local
  - events/logs
        ^
        | long-polling HTTP + heartbeat
        |
workers nirs4all
  - preinstalled Python environment
  - task sandbox per folder
  - `nirs4all.run(..., workspace_path=task_workspace)`
  - upload results
```

Network choice: workers poll the server instead of receiving pushes.
It's simpler for a LAN, machines behind NAT, and a prototype. THE
server maintains a stable public API for clients and a separate worker API.

## Components

### Server

Responsibilities:

- receive submissions;
- validate and persist jobs;
- materialize the input artifacts;
- decompose a logical job into executable tasks;
- record workers and their capabilities;
- assign task leases;
- track heartbeats, retries, timeouts, and cancellations;
- store events, logs, and results;
- expose REST + WebSocket/SSE to clients.

Implementation MVP :

- FastAPI + Uvicorn ;
- SQLite via `sqlite3` standard library ;
- storage of artifacts on disk by SHA-256;
- FIFO scheduler with optional priority;
- a single server process.

### Worker

Responsibilities:

- check in with your abilities;
- request an available task;
- download or resolve inputs;
- create an isolated task workspace;
- run `nirs4all.run()` with a dedicated `workspace_path`;
- capture stdout/stderr/logs;
- export the best model if requested;
- upload results and artifacts;
- send heartbeat and progress events.

The worker is not given dependencies to install dynamically. Its Python
environment is provisioned before startup. Declared capabilities are used for
routing.

### Client Python

Surface cible :

```python
from nirs4all_cluster import ClusterClient

client = ClusterClient("http://server:8765")
job = client.submit_run(
    pipeline={"kind": "path", "path": "/shared/pipelines/pls.yaml"},
    dataset={"kind": "shared_path", "path": "/shared/data/corn"},
    params={"verbose": 1, "random_state": 42, "refit": True},
)
client.wait(job.id)
result = client.get_result(job.id)
```

The client is deliberately thin: it talks to the server, but does not reimplement
`nirs4all`.

### CLI

Proposed commands:

```bash
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state
n4cluster worker --server http://host:8765 --labels site=lab,cuda=false
n4cluster submit job.yaml
n4cluster status <job_id>
n4cluster logs <job_id>
n4cluster cancel <job_id>
n4cluster artifacts <job_id> --out ./results
```

## Data model

### Entities

- `Job`: logical request submitted by a client.
- `Task`: executable unit leased to a worker.
- `Worker`: connected agent, with heartbeats and abilities.
- `Lease`: temporary assignment of a task to a worker.
- `Artifact`: blob address by hash, input or output.
- `Event`: change of state, log structure, progress.

### Job states

```
queued -> running -> succeeded
queued -> cancelled
running -> cancelling -> cancelled
running -> failed
failed -> queued    # optional manual retry
```

### Task states

```
queued -> leased -> running -> succeeded
queued -> leased -> lost -> queued
running -> lost -> queued|failed
running -> failed -> queued|failed
running -> cancelled
```

A lease expires if the worker stops heartbeating. The task becomes `queued` again
when `attempt < max_attempts`.

### Tables SQLite

- `jobs(id, type, status, priority, created_at, updated_at, owner, request_json,
  result_json, error, idempotency_key)`
- `tasks(id, job_id, status, attempt, max_attempts, worker_id, lease_expires_at,
  requirements_json, payload_json, result_json, error)`
- `workers(id, status, last_seen_at, labels_json, capabilities_json, slots_total,
  slots_used, version_json)`
- `artifacts(id, sha256, kind, path, size_bytes, created_at, metadata_json)`
- `events(id, job_id, task_id, worker_id, ts, level, type, message, data_json)`

## Execution granularity

### Level 0 - atomic job

The server creates a single task that calls `nirs4all.run()` on a worker.
It is the fastest to implement and useful for distributing several jobs
independent.

Limit: a large grid search remains monolithic on a worker.

### Level 1 - pipeline x dataset matrix

If the submission contains multiple explicit pipelines or datasets, the server
creates one task per combination. Each task executes a simple `nirs4all.run()`,
with its own workspace. The server then aggregates the metrics.

This decomposition is natural because `nirs4all.run()` already does the
performs the Cartesian product locally.

### Level 2 - explicit variants

For large sweeps, the client or server provides a list of pipelines
already concretized. Each variant becomes a task, typically with
`refit=False`. The server selects the best results and launches a task
final refit/export task for the best pipeline.

This level must be tested against monolithic local execution. It is not necessary
not promise parity as long as the aggregation does not reproduce the semantics
of `nirs4all`.

### Level 3 - distributed folds

To be postponed. Distributing folds affects anti-leakage guarantees,
reconstruction of the blind and the selection/refit. To be considered only after
coupling with a more formal orchestration layer or after a dedicated spike.

## Job specification

Format YAML/JSON cible :

```yaml
type: nirs4all.run
name: pls-corn
pipeline:
  kind: path
  path: /shared/pipelines/pls.yaml
dataset:
  kind: shared_path
  path: /shared/datasets/corn
params:
  verbose: 1
  random_state: 42
  refit: true
  save_artifacts: true
requirements:
  labels:
    cuda: "false"
  min_memory_gb: 8
  packages:
    nirs4all: ">=0.9,<0.10"
outputs:
  export_best_model: true
  keep_task_workspace: false
retry:
  max_attempts: 2
```

## Input References

### Pipeline

Kinds supported in order of preference:

1. `path`: file YAML/JSON accessible by the worker.
2. `artifact`: file uploaded to the server then downloaded by the worker.
3. `inline_json` : pipeline serialisable JSON.
4. `python_entrypoint`: Python module in a bundle with
   `build_pipeline()`. Reserved for trusted environments.

Point 4 is useful for a proto because many Python pipelines contain
sklearn objects that cannot be serialized into their own JSON. It is also dangerous:
no multi-tenant with this mode without sandbox.

### Dataset

Supported kinds:

1. `shared_path`: path available on all workers.
2. `artifact`: zip uploads, decompresses in the task sandbox.
3. `catalog`: versioned `nirs4all-datasets` / DOI identifier, resolved by the
   worker with a local cache.
4. `worker_local`: dataset present only on worker labels, useful for a future
   federated mode.

For the MVP, `shared_path` is the simplest and most realistic cluster of
cluster setup. `artifact` is for small datasets and demos.

## Execution worker

Pseudo-code :

```python
task = lease_task()
workdir = state / "tasks" / task.id
workspace = workdir / "workspace"
inputs = materialize_inputs(task, workdir / "inputs")

pipeline = load_pipeline(inputs.pipeline)
dataset = load_dataset_spec(inputs.dataset)
run_params = dict(task.params)
inner_n_jobs = run_params.pop("inner_n_jobs", 1)

result = nirs4all.run(
    pipeline=pipeline,
    dataset=dataset,
    workspace_path=workspace,
    n_jobs=inner_n_jobs,
    **run_params,
)

summary = summarize_run_result(result)
if task.outputs.export_best_model:
    result.export(workdir / "outputs" / "best_model.n4a")

upload_outputs(summary, logs, optional_workspace, model)
complete_task()
```

By default, `inner_n_jobs=1` to avoid overconsuming a machine in
combining local parallelism and cluster parallelism. A worker can announce
several slots if the machine allows it.

## API REST

### Client API

- `POST /v1/jobs` : submit a job.
- `GET /v1/jobs` : list jobs.
- `GET /v1/jobs/{job_id}`: status and summary.
- `POST /v1/jobs/{job_id}/cancel` : request cancellation.
- `GET /v1/jobs/{job_id}/tasks` : task details.
- `GET /v1/jobs/{job_id}/events` : paginated events.
- `GET /v1/jobs/{job_id}/artifacts` : available outputs.
- `GET /v1/artifacts/{artifact_id}` : download an artifact.
- `WS /v1/jobs/{job_id}/events/stream` : real-time progress.

### Worker API

- `POST /v1/workers/register`
- `POST /v1/workers/{worker_id}/heartbeat`
- `POST /v1/workers/{worker_id}/lease`
- `POST /v1/tasks/{task_id}/start`
- `POST /v1/tasks/{task_id}/events`
- `POST /v1/tasks/{task_id}/complete`
- `POST /v1/tasks/{task_id}/fail`
- `POST /v1/tasks/{task_id}/artifacts`

## Scheduling

MVP :

- FIFO by priority;
- filtrage par labels (`cuda=true`, `site=lab-a`, `python=3.11`) ;
- slots par worker ;
- lease timeout ;
- bounded retries;
- cancellation cooperative.

Plus tard :

- duration/RAM estimation;
- data locality;
- user/project quotas;
- fairness;
- GPU routing;
- preemption.

## Results and aggregation

Chaque task retourne au minimum :

```json
{
  "status": "succeeded",
  "nirs4all_version": "0.9.1",
  "duration_seconds": 123.4,
  "metrics": {
    "best_score": 0.91,
    "best_rmse": 0.12,
    "best_r2": 0.91,
    "best_accuracy": null
  },
  "counts": {
    "num_predictions": 12
  },
  "artifacts": {
    "model": "artifact_id",
    "logs": "artifact_id",
    "workspace": null
  }
}
```

For a job composed of several tasks, the server calculates:

- number of succeeded/failed tasks;
- best result according to the requested metric;
- ranking table;
- artifact of the best model;
- errors per task.

The aggregation of the complete `nirs4all` workspace is not part of the MVP. It may be possible later via controlled import/export of `WorkspaceStore`, but you should avoid cross-machine SQLite tinkering.

## Security

MVP acceptable only for a trusted LAN:

- static server/worker/client token;
- closed-by-default CORS;
- no execution of anonymous jobs;
- logs without secrets;
- cleaning workdir after retention;
- default refusal of `python_entrypoint ` mode if `--allow-python-jobs` is not
  active.

Avant tout usage multi-utilisateur :

- TLS or mTLS;
- client/worker identities;
- token rotation;
- container sandbox per task;
- CPU/RAM/disk quotas;
- optional no-network policy;
- allowlist of shared paths;
- encryption or strict retention of sensitive artifacts.

## Proposed code layout

```text
nirs4all-cluster/
  pyproject.toml
  PROTOTYPE_DESIGN.md
  nirs4all_cluster/
    __init__.py
    cli.py
    schemas.py
    client.py
    server/
      app.py
      db.py
      scheduler.py
      artifacts.py
      events.py
    worker/
      agent.py
      executor.py
      materialize.py
    runners/
      nirs4all_run.py
  tests/
    test_scheduler.py
    test_state_machine.py
    test_artifacts.py
    test_worker_smoke.py
  examples/
    job.shared-path.yaml
    job.uploaded-bundle.yaml
```

## Prototype implementation plan

### Phase 0 - skeleton

- `pyproject.toml` minimal.
- CLI `server`, `worker`, `submit`, `status`.
- Schemas Pydantic.
- simple SQLite migrations.
- Unit testing of state transitions.

### Phase 1 - minimal distributed queue

- FastAPI server.
- Register/heartbeat workers.
- Lease FIFO.
- Execution of a dummy task `echo`.
- Events/logs.
- Retry on expired lease.

### Phase 2 - atomic nirs4all runner

- Materialisation `shared_path` et `artifact`.
- Execution of `nirs4all.run()` in the task workspace.
- JSON summary.
- Export `.n4a`.
- Upload/download artifacts.
- Smoke test with a mini dataset.

### Phase 3 - simple decomposition

- Job `matrix`: explicit pipelines x explicit datasets.
- Ranking aggregation.
- Best-artifact selection.
- Comparison with local execution on a small workload.

### Phase 4 - future Studio/API integration

- REST adapter that reproduces the `JobManager` concepts of Studio.
- WebSocket compatible progression job.
- Documentation to replace local Studio execution with an opt-in cluster.

## Validation tests

Mandatory tests before considering the prototype useful:

- a worker executes an atomic job and returns a `.n4a` model;
- two workers execute two jobs in parallel;
- a worker killed during a task causes a retry;
- a canceled job is not restarted;
- a job `pipeline x dataset` aggregates the results;
- the same atomic job gives metrics equivalent to `nirs4all.run()`
  local ;
- no file outside of `state_dir` serveur/worker is created except paths
  explicitement declares.

Mesures a collecter :

- queue waiting time;
- temps de transfert inputs/outputs ;
- worker execution time;
- overhead serveur ;
- artifact size;
- taux de retry ;
- metric difference vs local.

## Decisions pragmatiques

- Start with HTTP polling and SQLite, not Redis/RabbitMQ.
- Do not share `nirs4all` workspaces between workers.
- Utiliser `nirs4all.run()` comme boite noire au depart.
- Do not distribute the folds in the first prototype.
- Accepter `python_entrypoint` seulement en mode confiance explicite.
- Measure parity before automatically decomposing variants.

## Questions ouvertes

- What canonical pipeline representation should become the network contract:
YAML `nirs4all`, JSON Studio, Python bundle, or several formats supported?
- Should we keep the complete worker workspaces or only the summaries and
  `.n4a` ?
- How to properly import several results into a Studio workspace without
  toucher `nirs4all` ?
- Quelle granularite donne le meilleur compromis : run complet, variant, fold ?
- What minimum security is required for real first-time users?
- What dataset cache policy should be adopted on workers?

## Recommandation

For a quick prototype in this folder, implement first:

1. serveur FastAPI + SQLite + object store local ;
2. worker polling + sandbox de task ;
3. job atomique `nirs4all.run()` ;
4. `.n4a` artifacts + JSON summary;
5. explicit decomposition `pipelines x datasets`.

This trajectory validates the client/serveur/workers model without forcing
changes in other libraries. If the measurements show a real gain
and an acceptable parity, the logical next step is to decide if the backend must
remain a native queue or if the effort should migrate to a more Dask/Ray backend
standard.
