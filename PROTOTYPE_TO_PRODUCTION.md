# From prototype to production — nirs4all-cluster

This document summarizes what the prototype has **validated**, then honestly describes what is
still missing for real use. It completes `PROTOTYPE_DESIGN.md` (the design) and `WORKLOG.md` (the log). It addresses the decision: *should we turn this prototype into a product, and if so,
how?*

> **Starting position (unchanged).** The default option recommended by the ecosystem remains an
> **opt-in execution backend in`nirs4all`** (e.g.`nirs4all[dask]`), **not** a default-operated home cluster. The repository is public as an auditable prototype: it produces
> measurements and de-risks the decision, without preempting it. See the`README.md`go/no-go criteria.

## 1. What the prototype demonstrated

Measured on`nirs4all-data`via`scripts/validation.py`(8/8) and the integration suite (`pytest`):

| Design question | Measured response |
|---|---|
| Is the distributed work unit well chosen? | Yes for Level 0 (atomic job) and Level 1 (`pipelines × datasets`). Each task = an isolated`nirs4all.run()`with its own workspace. |
| Do the results remain compatible with local execution? | **Yes, metric-identical**:`best_rmse`cluster == local, **diff = 0.0** (≪ criterion ≤ 1e-10). |
| Is data/artifact transfer acceptable? | Yes for`shared_path`(no transfer) and small`.n4a`(~50 KB). The transfer of large datasets via`artifact`remains to be measured. |
| What network/recovery/security guarantees become essential? | Resumption (lease+retry) and cancellation: validated. Security: only a static token — insufficient outside a trusted LAN (see §4). |

**Validated architecture**: single FastAPI server + SQLite (WAL) + SHA-256 object store; workers in
polling (long-poll + heartbeat, leases renewed at heartbeat); runner subprocess isolation
(crash containment + real cancellation);`nirs4all`imported **only** by the runner.

**Assumed limits of the prototype** (compliant with non-objectives): a single server process; no
multi-tenant; no sandbox for arbitrary Python code; no distribution of folds (Level 3); no concurrent writing in a shared`nirs4all`workspace.

## 2. Fork: native stack vs Dask/Ray backend

Before industrializing **anything**, decide:

- **`nirs4all[dask]`opt-in backend (recommended by default).** Reuses a proven scheduler
  (Dask/Ray), zero new services to operate, direct integration into`nirs4all.run(..., backend=...)`. The prototype shows that the`pipelines × datasets`decomposition is trivial and metric-identical: this is exactly what a Dask backend would do, without an in-house server/queue. - **Native stack (this repository).** Justified **only** if Dask/Ray does not cover emerging needs well: heterogeneous workers behind NAT with long-polling, federated calculation (dataset that does not move), multi-day durable queues, or multi-tenant Studio integration with quotas. As long as these needs are not **funded and requested**, the cost of operating an in-house distributed service is not justified.

The prototype is designed so that this switch is possible in both directions:`nirs4all.run()`is
used in black box, so the same contract (`pipeline`,`dataset`,`params`) would feed a backend
Dask without client-side rewriting.

> **Beyond the "whole run" grain.** The real value (and difficulty) is the **fine-grained** distribution: distributed subtrees, sweep points, and folds (including refit-with-folds), not entire`run()`s. Inspection of the`nirs4all`engine (typed step unit,`DataSelector`= data view, scopes
>`(variant, fold, phase)`, content-addressed artifacts + calculation cache key, trace/replay,
>deterministic selection/refit) shows that the engine already has about 80% of the necessary abstractions, but they are wired in-process/single-host. Complete mapping of **extraction points** and what is missing
> (distributed control plane): **[`docs/DISTRIBUTED_EXECUTION_DESIGN.md`](docs/DISTRIBUTED_EXECUTION_DESIGN.md)**.

## 3. Measurements still to be produced before a trip

Go remains conditioned (README §go/no-go). What remains to be measured:

1. **Speedup ≥ 3×** on a real workload (grid search AOM / HPO on ≥ 32 datasets) — the prototype
   allows (`job.matrix.yaml`), you just have to launch a real sweep and time vs single-machine. 2. **Cost of transfers** for large datasets in`artifact`(zip upload/extract) and large`.n4a`(deep models). The prototype already measures`submit_latency`,`exec`,`overhead`,`n4a_size`; extend. 3. **Parity Level 2 (explicit variants)**: the design prohibits promising parity as long as
   aggregation does not reproduce the selection/refit semantics of`nirs4all`. To write and measure
   before any automatic decomposition of the sweeps. 4. **≥ 2 requesting partners** (non-technical).

## 4. Production gaps by area

Format : *prototype actuel → exigence production*.

### Reliability & scalability
- A server process + SQLite (a single writer) → **Postgres** (or dedicated Redis/broker) to authorize
  several server processes and real throughput; keep SQLite only for the single lab node. - Object store on local disk → **S3/MinIO** (or network storage) with lifecycle/retention. - In-process Reaper → resilient supervisor + lease lost/retry metrics. - *Already done*: renewal of leases at heartbeat, bounded retry, idempotence, NaN→null sanitization.

### Safety (biggest gap)
- Shared static token → **mTLS or OIDC**, distinct client/worker identities, **token rotation**. - No isolation between submitters → **container sandbox per task**, CPU/RAM/disk quotas, optional no-network policy, **allowlist** of shared paths (`shared_path`/`path` are accepted as-is). - `python_entrypoint` (arbitrary code) → **never** in multi-tenant; today it is correctly behind `--allow-python-jobs`, but it must be banned as soon as a third party can submit. - Artifacts → encryption/strict retention for sensitive data (IP/GDPR datasets). - Artifact size limit: present, but should be tightened (refusal in streaming, not only after the fact).

### Scheduling
- FIFO + priority + labels + slots → duration/RAM estimation, **data locality**, **GPU** routing,
  **quotas/fairness** per user/project, preemption.

### Data
-`shared_path`/`artifact`→ wire the **kind`catalog`** (DOI`nirs4all-datasets`:`nirs4all_datasets.load()`+`resolve_config()`+ cache verified by checksum). Today`catalog`raises an explicit`NotImplementedError`(deferred, honest). - Dataset cache policy on workers (reuse between tasks). -`worker_local`→ **federated** mode (dataset which remains on the site, only the result comes back).

### Results & aggregation
- Summary by task + ranking + best`.n4a`→ clean import of several results into a workspace
  Studio **without touching`nirs4all`** (via controlled export/import of`WorkspaceStore`, no SQLite
  DIY cross-machine). - Level 3 (folds distributed): **delayed** — affects anti-leakage, reconstruction of the store and
  at the refit; only consider after a dedicated spike or coupling with`dag-ml`.

### Studio Integration (Phase 4)
- The server already exposes typed events and a WS stream. For an **opt-in** cluster backend in Studio,
  provide an adapter that maps job states (`queued/running/succeeded/failed/cancelling/cancelled`)
  and renames the events to the Studio WS vocabulary (`job_started/job_progress/job_completed/
  job_failed/job_log`, channel`job:{id}`). Studio would replace its local`JobManager`with the cluster in
  opt-in, without reimplementing NIRS/ML logic.

### Versions & availability of libraries (implemented)
- The worker **declares** to the registry its interpreter and the installed versions of a set of packages
  relevant (`nirs4all`,`numpy`,`scipy`,`scikit-learn`,`pandas`,`polars`,`torch`,`tensorflow`,`jax`). The scheduler **applies**`requirements.packages`(PEP 440 specifiers via`packaging`,
  e.g.`{"nirs4all": ">=0.9,<0.10"}`,`{"python": ">=3.11"}`, or`""`= presence required). A package
  undeclared never satisfies an explicit requirement (availability unknown = unavailable). - **Availability by default**: a`nirs4all.run`job implicitly requires the presence of`nirs4all`,
  so it's never routed to a worker that doesn't have it (the client can overload with a range). Each task result also records the`nirs4all_version`that produced it (traceability). - **GPU/CUDA (implemented)**: the worker auto-detects GPUs via`nvidia-smi`and declares`capabilities.gpu_count`/`gpu_names`/`cuda_version`+ a`cuda=true|false`label (auto). The scheduler
  route on the`cuda`label **and** on`requirements.min_gpu_count`(fail-closed: GPU not declared = 0). Override CLI:`--gpus N`. Validated on 2 real GPUs (RTX 4090 + 5090, CUDA 13.1) via WSL. - Remains to be produced: container images versioned by capacity (CPU/GPU, TF/Torch/JAX) side
  provisioning worker, and GPU memory declaration per device.

### Observability & exploitation
-`events`+ WS table → Prometheus metrics, structured logs, tracing, tail dashboards. - Packaging worker: today the worker inherits a provisioned`nirs4all`env; in production,
  container images versioned by capacity (CPU/GPU, TF/Torch/JAX).

## 5. Recommended trajectory

1. **Now**: keep the prototype as a measuring bench. Run the actual AOM/HPO sweep for the
   speedup criterion ≥ 3×; measure the transfer cost on large datasets. 2. **If Level 2 parity + speedup confirmed and ≥ 2 requesters**: prototype the **Dask opt-in backend**
   in`nirs4all`and compare it to this native queue on the same workload. Decide on the backend by
   numbers, not by architecture. 3. **Only if a need not covered by Dask is funded** (federated, NAT/long-poll, Studio
   multi-tenant): harden this repository according to §4 (security first: mTLS + identities + sandbox), migrate
   SQLite→Postgres and disk→network object, then the Studio adapter.

## 6. Open-ended questions — prototype-informed answers

- *Pipeline network contract?* → The valid prototype **YAML/JSON`nirs4all`** (`path`/`artifact`/`inline_json`) as main contract, JSON-serializable;`python_entrypoint`reserved for trust. - *Keep the worker workspaces complete?* → No by default: summary +`.n4a`are sufficient (option`keep_task_workspace`for debugging). The aggregation of complete workspaces remains outside of MVP. - *Optimal granularity?* → Atomic Job and`pipeline × dataset`already give parity + parallelism; the variant (Level 2) requires a dedicated parity measure; the fold (Level 3) is delayed. - *Minimum security for real users?* → At least mTLS + identities + sandbox per task
  **before** any use outside a trusted LAN.

## 7. Revue

The code and this roadmap have been reviewed by`codex`(read-only review) and by a review
adversarial multi-agents (4 dimensions: competition/states, security, API contract, adherence to
design). The conclusions and corrections applied are recorded in`WORKLOG.md`(§ Review).
