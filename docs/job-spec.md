# Job specification & wire contract

A job is a YAML/JSON document (the wire contract is the Pydantic models in
`nirs4all_cluster.schemas`). Provide **exactly one** of `pipeline`/`pipelines` and **one**
of `dataset`/`datasets`; lists decompose into the cartesian product
({doc}`concepts/job-decomposition`).

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
.. autoclass:: nirs4all_cluster.schemas.JobView
.. autoclass:: nirs4all_cluster.schemas.JobAggregate
.. autoclass:: nirs4all_cluster.schemas.TaskView
.. autoclass:: nirs4all_cluster.schemas.TaskResult
.. autoclass:: nirs4all_cluster.schemas.RunMetrics
.. autoclass:: nirs4all_cluster.schemas.ClusterStats
```
