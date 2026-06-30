"""Pydantic models — the network contract between client, server and worker.

These models are the *only* boundary validation in the system (per the
ecosystem convention: validate at system boundaries, trust internal code). The
server, worker and client all import from here so the wire format stays in one
place.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

# --------------------------------------------------------------------------- #
# Enums / state machines
# --------------------------------------------------------------------------- #


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    CANCELLED = "cancelled"


class WorkerStatus(str, Enum):
    ALIVE = "alive"
    DEAD = "dead"


class EventLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# --------------------------------------------------------------------------- #
# Input references (pipeline / dataset)
# --------------------------------------------------------------------------- #


class PipelineRef(BaseModel):
    """How a worker should obtain the pipeline to run.

    ``kind`` ordering mirrors the design's preference list. ``python_entrypoint``
    is only honoured when the server is started with ``--allow-python-jobs``.
    """

    kind: Literal["path", "artifact", "inline_json", "python_entrypoint"]
    path: str | None = None
    artifact_id: str | None = None
    inline: Any | None = None
    # python_entrypoint: a bundle artifact exposing ``build_pipeline()`` in module.
    bundle_artifact_id: str | None = None
    entrypoint: str | None = None  # e.g. "my_pipelines.pls:build_pipeline"
    # Optional content fingerprint the client computed for this pipeline (inline
    # only — the client cannot read a worker-side ``path``). The server compares it
    # against the fingerprint the worker reports and emits a divergence event on
    # mismatch (traceability; never fatal).
    expected_fingerprint: str | None = None

    @model_validator(mode="after")
    def _check_kind_fields(self) -> PipelineRef:
        required = {
            "path": "path",
            "artifact": "artifact_id",
            "inline_json": "inline",
            "python_entrypoint": "entrypoint",
        }[self.kind]
        if getattr(self, required) is None:
            raise ValueError(f"pipeline kind={self.kind!r} requires field {required!r}")
        return self


class DatasetRef(BaseModel):
    """How a worker should obtain the dataset to run on."""

    kind: Literal["shared_path", "artifact", "catalog", "worker_local"]
    path: str | None = None
    artifact_id: str | None = None
    catalog_id: str | None = None  # nirs4all-datasets id / DOI
    name: str | None = None  # human label for ranking tables

    @model_validator(mode="after")
    def _check_kind_fields(self) -> DatasetRef:
        required = {
            "shared_path": "path",
            "artifact": "artifact_id",
            "catalog": "catalog_id",
            "worker_local": "path",
        }[self.kind]
        if getattr(self, required) is None:
            raise ValueError(f"dataset kind={self.kind!r} requires field {required!r}")
        return self

    def label(self) -> str:
        return self.name or self.path or self.artifact_id or self.catalog_id or self.kind


class Requirements(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict)
    min_memory_gb: float | None = None
    # Minimum GPU count. Fail-closed: a worker that did not declare GPUs is
    # treated as having 0 (unlike the soft memory floor), so a GPU requirement
    # never routes to a CPU-only worker.
    min_gpu_count: int | None = None
    # package -> PEP 440 specifier, e.g. {"nirs4all": ">=0.9,<0.10"}. An empty
    # string means "must be present, any version". Validated at the boundary.
    packages: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_specifiers(self) -> Requirements:
        from packaging.specifiers import InvalidSpecifier, SpecifierSet

        for package, spec in self.packages.items():
            if spec:
                try:
                    SpecifierSet(spec)
                except InvalidSpecifier as exc:
                    raise ValueError(f"invalid version specifier for {package!r}: {spec!r}") from exc
        return self


class Outputs(BaseModel):
    export_best_model: bool = True
    keep_task_workspace: bool = False


class RetryPolicy(BaseModel):
    max_attempts: int = Field(default=2, ge=1, le=10)


# --------------------------------------------------------------------------- #
# Job submission (client -> server)
# --------------------------------------------------------------------------- #


class JobRequest(BaseModel):
    """A logical job submitted by a client.

    Provide exactly one of ``pipeline``/``pipelines`` and one of
    ``dataset``/``datasets``. When a list is given the server decomposes the job
    into one task per (pipeline, dataset) combination (design Level 1).
    """

    type: Literal["nirs4all.run"] = "nirs4all.run"
    name: str | None = None
    priority: int = 0

    pipeline: PipelineRef | None = None
    pipelines: list[PipelineRef] | None = None
    dataset: DatasetRef | None = None
    datasets: list[DatasetRef] | None = None

    params: dict[str, Any] = Field(default_factory=dict)
    requirements: Requirements = Field(default_factory=Requirements)
    outputs: Outputs = Field(default_factory=Outputs)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)

    # Metric used to rank tasks of a composite job (key inside TaskResult.metrics).
    rank_metric: str = "best_rmse"
    rank_mode: Literal["min", "max"] = "min"

    idempotency_key: str | None = None

    @model_validator(mode="after")
    def _check_one_of(self) -> JobRequest:
        if (self.pipeline is None) == (self.pipelines is None):
            raise ValueError("provide exactly one of 'pipeline' or 'pipelines'")
        if (self.dataset is None) == (self.datasets is None):
            raise ValueError("provide exactly one of 'dataset' or 'datasets'")
        if self.pipelines is not None and not self.pipelines:
            raise ValueError("'pipelines' must not be empty")
        if self.datasets is not None and not self.datasets:
            raise ValueError("'datasets' must not be empty")
        return self

    def pipeline_list(self) -> list[PipelineRef]:
        return self.pipelines if self.pipelines is not None else [self.pipeline]  # type: ignore[list-item]

    def dataset_list(self) -> list[DatasetRef]:
        return self.datasets if self.datasets is not None else [self.dataset]  # type: ignore[list-item]

    def pipeline_list_has_python(self) -> bool:
        return any(p.kind == "python_entrypoint" for p in self.pipeline_list())


# --------------------------------------------------------------------------- #
# Worker registration / leasing
# --------------------------------------------------------------------------- #


class WorkerRegister(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict)
    capabilities: dict[str, Any] = Field(default_factory=dict)
    slots_total: int = Field(default=1, ge=1)
    version: dict[str, Any] = Field(default_factory=dict)
    name: str | None = None


class WorkerRegistered(BaseModel):
    worker_id: str
    heartbeat_interval_s: float = 10.0
    lease_ttl_s: float = 60.0
    # Rights the server granted the registering credential (diagnostics only;
    # additive, non-breaking). Empty in open/dev mode is also valid.
    rights: list[str] = Field(default_factory=list)


class HeartbeatAck(BaseModel):
    ok: bool = True
    # Tasks the server wants the worker to stop (cooperative cancellation).
    cancel_task_ids: list[str] = Field(default_factory=list)


class TaskPayload(BaseModel):
    """Everything a worker needs to execute a task. Returned by /lease."""

    task_id: str
    job_id: str
    type: str
    attempt: int
    pipeline: PipelineRef
    dataset: DatasetRef
    params: dict[str, Any] = Field(default_factory=dict)
    outputs: Outputs = Field(default_factory=Outputs)
    lease_expires_at: float


class LeaseResponse(BaseModel):
    task: TaskPayload | None = None


# --------------------------------------------------------------------------- #
# Task lifecycle reports (worker -> server)
# --------------------------------------------------------------------------- #


class TaskEvent(BaseModel):
    level: EventLevel = EventLevel.INFO
    type: str = "log"
    message: str = ""
    progress: float | None = None  # 0..1 approximate
    data: dict[str, Any] = Field(default_factory=dict)


class RunMetrics(BaseModel):
    best_score: float | None = None
    best_rmse: float | None = None
    best_r2: float | None = None
    best_mae: float | None = None
    best_accuracy: float | None = None


class TaskResult(BaseModel):
    """Summary a worker reports on task completion. Mirrors design's JSON."""

    status: Literal["succeeded"] = "succeeded"
    nirs4all_version: str | None = None
    # sha256 of the pipeline content the worker actually ran (traceability).
    pipeline_fingerprint: str | None = None
    duration_seconds: float = 0.0
    metrics: RunMetrics = Field(default_factory=RunMetrics)
    counts: dict[str, int] = Field(default_factory=dict)
    # artifact ids by role (model/logs/workspace) — filled after uploads.
    artifacts: dict[str, str | None] = Field(default_factory=dict)
    extra: dict[str, Any] = Field(default_factory=dict)


class TaskFailure(BaseModel):
    error: str
    traceback: str | None = None
    retriable: bool = True


# --------------------------------------------------------------------------- #
# Server -> client views
# --------------------------------------------------------------------------- #


class TaskView(BaseModel):
    id: str
    job_id: str
    status: TaskStatus
    attempt: int
    max_attempts: int
    worker_id: str | None = None
    dataset_label: str | None = None
    pipeline_label: str | None = None
    result: TaskResult | None = None
    error: str | None = None


class JobAggregate(BaseModel):
    num_tasks: int = 0
    num_succeeded: int = 0
    num_failed: int = 0
    num_running: int = 0
    num_queued: int = 0
    best_metric: float | None = None
    best_task_id: str | None = None
    best_model_artifact_id: str | None = None
    ranking: list[dict[str, Any]] = Field(default_factory=list)
    errors: dict[str, str] = Field(default_factory=dict)


class JobView(BaseModel):
    id: str
    type: str
    name: str | None = None
    status: JobStatus
    priority: int = 0
    created_at: float
    updated_at: float
    aggregate: JobAggregate = Field(default_factory=JobAggregate)
    error: str | None = None


class EventView(BaseModel):
    id: int
    job_id: str | None = None
    task_id: str | None = None
    worker_id: str | None = None
    ts: float
    level: EventLevel
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class ArtifactView(BaseModel):
    id: str
    sha256: str
    kind: str
    size_bytes: int
    created_at: float
    filename: str | None = None


class ClusterStats(BaseModel):
    """Server-wide counters for the dashboard header and ``n4cluster`` tooling."""

    server_version: str
    api_version: int
    jobs_by_status: dict[str, int] = Field(default_factory=dict)
    workers_alive: int = 0
    workers_dead: int = 0
    tasks_in_flight: int = 0
