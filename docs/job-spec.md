# Job specification & wire contract

A job is a YAML/JSON document (the wire contract is the Pydantic models in
`nirs4all_cluster.schemas`). Provide **exactly one** of `pipeline`/`pipelines` and **one**
of `dataset`/`datasets`; lists decompose into the cartesian product
({doc}`concepts/job-decomposition`).

The Python/CLI adapter adds an optional `parity` field (`DistributedRunParity`) to jobs
it creates. This is traceability metadata, not a scheduler input: it records that the
distributed beta runs one whole `nirs4all.run` per task, expects metric parity for atomic
and explicit matrix jobs, and defers fine-grained DAG/variant/fold/subtree parity until
core/dag-ml execution-unit and data-provider contracts exist.

The server also persists additive scheduler/rights metadata:

- `scheduler` (`DagSchedulerContract`) records whether the request is atomic, an
  explicit `pipeline × dataset` matrix, or a DAG-shaped whole-run job. V1 still leases
  whole `nirs4all.run` tasks; it does not claim fine-grained graph execution.
- `submission` (`JobSubmissionMetadata`) is overwritten by the server from the
  authenticated submitter credential (`submit` right).
- leased `TaskPayload.assignment` records the server-authoritative executor assignment
  returned to a worker/client holding `execute`.
- stored `TaskResult.provenance` records the authenticated executor principal, worker id,
  job id, task id, attempt, and execute rights used to report the result.

Studio/native submitters may also attach `nativePayload` (`NativeExperimentLaunchPayload`).
The cluster beta treats it as a preserved launch manifest: it is validated at the boundary,
stored with `request_json`, copied into every task payload, and returned to workers on lease.
Scheduling still depends only on `pipeline`/`dataset`/`params`/`requirements`, and workers still
run whole `nirs4all.run` tasks.

When Studio requests spectral/OOD replay evidence publication, the native manifest carries
`manifest.robustnessEvidencePublicationHandoff`. This block records the destination
(`result_metadata.robustness_evidence`), fail-closed behavior, fields to publish
(`prediction_arrays.X`, `result_metadata.robustness_evidence.X`,
`result_metadata.robustness_evidence.predictor_bundle`), and accepted
row-alignment strategies (`sample_indices`, full-dataset length, unique metadata identity, or
explicit relation materialization identity). This is a materialization contract for native
runners, not proof that replay evidence already exists or that a robustness report has been
computed.

The worker materializer passes this native payload into the subprocess runner spec. If the
handoff is present, the `nirs4all.run` subprocess attempts a local workspace publication after
model export: it reloads path-backed datasets through `DatasetConfigs`, opens the isolated task
`WorkspaceStore`, selects row-aligned `X` by stored `sample_indices`, full-dataset row count, or
explicit identity columns in `sample_metadata` /
`result_metadata.relation_replay_manifest.materialization_manifest` /
`result_metadata.relation_materialization_manifest`, then upserts `prediction_arrays.X` and
`result_metadata.robustness_evidence.{X,predictor_bundle,publisher}`. After
uploading artifacts, the worker enriches the same trace with
`published_artifacts.predictor_bundle`. The trace remains fail-closed: non-path datasets,
unloadable datasets, duplicate/missing identity values, missing model bundles, or unprovable row
alignment keep the required fields under `missing` instead of declaring spectral/OOD replay ready.

## Pipeline references (`kind`)

- `path` — a pipeline YAML readable on the worker (shared/worker-local filesystem).
- `inline_json` — the pipeline embedded in the job. The client pins a content fingerprint
  so the server can confirm the worker ran exactly what was submitted ({doc}`versioning`).
- `artifact` — an uploaded artifact id (see `ClusterClient.upload_artifact`).
- `python_entrypoint` — a `module:builder` callable; **gated** behind `--allow-python-jobs`
  (server) and `--allow-python` (worker).

## Dataset references (`kind`)

`shared_path`, `artifact`, `worker_local` (today behaves like `shared_path`), and
`catalog` (a `nirs4all-datasets` id/DOI — **not implemented** in the beta worker).

## Runtime requirement & mixed fleets

Every task runs a whole `nirs4all.run()` in a worker subprocess, so a task may only
land on a worker that can *prove* the runtime. On submission the server attests a
package-availability requirement for `nirs4all.run` jobs:

- If you pin nothing, the server injects a **presence-only** requirement
  (`requirements.packages.nirs4all == ""`): any declared version qualifies, and a
  worker that never declared `nirs4all` is **never** leased the task (fail-closed).
- Pin a range in `requirements.packages.nirs4all` (e.g. `">=0.9,<0.10"`) to constrain
  routing to a compatible library — the server preserves your pin, it does not
  overwrite it.
- Pinning *other* packages (extra fleet capabilities such as `torch`) **composes with**
  the mandatory `nirs4all` presence rather than replacing it; a worker must declare
  every pinned package to be eligible.

Because eligibility is decided from what a worker declares at registration, a fleet can
mix plain `nirs4all` workers with richer runtimes: routing stays correct, and a worker
that cannot prove the required packages is simply passed over.

## Examples

```{literalinclude} ../examples/job.shared-path.yaml
:language: yaml
:caption: examples/job.shared-path.yaml — atomic job on a shared filesystem
```

```{literalinclude} ../examples/job.matrix.yaml
:language: yaml
:caption: examples/job.matrix.yaml — one pipeline × three datasets
```

```{literalinclude} ../examples/job.uploaded-bundle.yaml
:language: yaml
:caption: examples/job.uploaded-bundle.yaml — inline pipeline + uploaded dataset
```

## Schema reference

```{eval-rst}
.. autoclass:: nirs4all_cluster.schemas.JobRequest
.. autoclass:: nirs4all_cluster.schemas.PipelineRef
.. autoclass:: nirs4all_cluster.schemas.DatasetRef
.. autoclass:: nirs4all_cluster.schemas.Requirements
.. autoclass:: nirs4all_cluster.schemas.Outputs
.. autoclass:: nirs4all_cluster.schemas.RetryPolicy
.. autoclass:: nirs4all_cluster.schemas.DistributedRunParity
.. autoclass:: nirs4all_cluster.schemas.DagSchedulerContract
.. autoclass:: nirs4all_cluster.schemas.JobSubmissionMetadata
.. autoclass:: nirs4all_cluster.schemas.NativeExperimentLaunchPayload
.. autoclass:: nirs4all_cluster.schemas.NativeExperimentLaunchPayloadManifest
.. autoclass:: nirs4all_cluster.schemas.NativeRobustnessEvidencePublicationHandoff
.. autoclass:: nirs4all_cluster.schemas.TaskAssignmentMetadata
.. autoclass:: nirs4all_cluster.schemas.ResultProvenance
.. autoclass:: nirs4all_cluster.schemas.JobView
.. autoclass:: nirs4all_cluster.schemas.JobAggregate
.. autoclass:: nirs4all_cluster.schemas.TaskView
.. autoclass:: nirs4all_cluster.schemas.TaskResult
.. autoclass:: nirs4all_cluster.schemas.RunMetrics
.. autoclass:: nirs4all_cluster.schemas.ClusterStats
```
