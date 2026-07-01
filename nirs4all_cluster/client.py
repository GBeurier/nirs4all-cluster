"""``ClusterClient`` — the thin submitter / inspection Python SDK.

It speaks the server's REST client API and nothing more; it never imports nirs4all
and never reimplements pipeline/dataset logic. Friendly helpers turn plain strings
and dicts into the wire schema.

Every call is **rights-respecting**: a request the credential is not allowed to make
raises a typed error from :mod:`nirs4all_cluster.client_errors` (``401`` →
:class:`ClusterAuthError`, ``403`` → :class:`ClusterPermissionError` carrying the
missing rights) instead of an opaque HTTP error, so core / Studio / CLI can react to
the RBAC verdict. The executor half (worker registration + task lifecycle) lives in
:class:`nirs4all_cluster.client_worker.WorkerClient`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .client_errors import ClusterConnectionError, raise_for_response
from .client_transport import make_http_client, request
from .schemas import (
    ClusterStats,
    DatasetRef,
    DistributedRunParity,
    EventView,
    JobRequest,
    JobView,
    PipelineRef,
    TaskView,
)
from .versioning import fingerprint_obj, is_incompatible

PipelineInput = PipelineRef | dict | str
DatasetInput = DatasetRef | dict | str

_TERMINAL = {"succeeded", "failed", "cancelled"}
_FINE_GRAINED_DAG_DEFERRED = [
    "variant-level DAG distribution requires a core/dag-ml execution-unit contract",
    "fold-level distribution requires core-owned OOF/selection/refit parity contracts",
    "subtree/cache distribution requires a shared data/artifact provider contract",
]


@dataclass(frozen=True)
class ServerInfo:
    """What :meth:`ClusterClient.server_info` learned from the ``/version`` handshake.

    ``compatible`` is the client's verdict on the server's protocol major: ``True``
    means the two speak the same wire contract (``api_version == API_VERSION``).
    """

    service: str
    version: str
    api_version: int
    compatible: bool


def _as_pipeline(value: PipelineInput) -> PipelineRef:
    if isinstance(value, PipelineRef):
        ref = value
    elif isinstance(value, str):
        ref = PipelineRef(kind="path", path=value)
    else:
        ref = PipelineRef.model_validate(value)
    # Pin a content fingerprint for inline pipelines so the server can trace
    # whether the worker ran exactly what was submitted (the client cannot read a
    # worker-side ``path``, so only inline pipelines get one).
    if ref.kind == "inline_json" and ref.expected_fingerprint is None and ref.inline is not None:
        ref = ref.model_copy(update={"expected_fingerprint": fingerprint_obj(ref.inline)})
    return ref


def _as_dataset(value: DatasetInput) -> DatasetRef:
    if isinstance(value, DatasetRef):
        return value
    if isinstance(value, str):
        return DatasetRef(kind="shared_path", path=value)
    return DatasetRef.model_validate(value)


def _normalize_run_params(
    params: dict[str, Any] | None,
    *,
    n_jobs: int | None,
    inner_n_jobs: int | None,
    workspace_path: str | Path | None,
) -> tuple[dict[str, Any], dict[str, str], list[str]]:
    run_params = dict(params or {})
    translated: dict[str, str] = {}
    omitted: list[str] = []

    params_workspace = run_params.pop("workspace_path", None)
    if workspace_path is not None or params_workspace is not None:
        omitted.append("workspace_path")
        if workspace_path is not None and params_workspace is not None and str(workspace_path) != str(params_workspace):
            raise ValueError("workspace_path was provided both as an argument and in params with different values")

    params_n_jobs = run_params.pop("n_jobs", None)
    params_inner_n_jobs = run_params.pop("inner_n_jobs", None)
    requested_inner = inner_n_jobs

    if n_jobs is not None:
        requested_inner = _choose_inner_n_jobs(requested_inner, int(n_jobs), source="n_jobs")
        translated["n_jobs"] = "inner_n_jobs"
    if params_n_jobs is not None:
        requested_inner = _choose_inner_n_jobs(requested_inner, int(params_n_jobs), source="params['n_jobs']")
        translated["n_jobs"] = "inner_n_jobs"
    if params_inner_n_jobs is not None:
        requested_inner = _choose_inner_n_jobs(
            requested_inner, int(params_inner_n_jobs), source="params['inner_n_jobs']"
        )
    if requested_inner is not None:
        if requested_inner < 1:
            raise ValueError("inner_n_jobs must be >= 1")
        run_params["inner_n_jobs"] = requested_inner
    return run_params, translated, sorted(set(omitted))


def _choose_inner_n_jobs(current: int | None, candidate: int, *, source: str) -> int:
    if candidate < 1:
        raise ValueError(f"{source} must be >= 1")
    if current is not None and current != candidate:
        raise ValueError(f"conflicting nirs4all.run parallelism values: {current} vs {candidate} from {source}")
    return candidate


def build_nirs4all_run_request(
    *,
    pipeline: PipelineInput | None = None,
    dataset: DatasetInput | None = None,
    pipelines: list[PipelineInput] | None = None,
    datasets: list[DatasetInput] | None = None,
    params: dict[str, Any] | None = None,
    n_jobs: int | None = None,
    inner_n_jobs: int | None = None,
    workspace_path: str | Path | None = None,
    name: str | None = None,
    priority: int = 0,
    requirements: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
    retry: dict[str, Any] | None = None,
    rank_metric: str = "best_rmse",
    rank_mode: str = "min",
    idempotency_key: str | None = None,
    metric_tolerance_abs: float = 1e-6,
) -> JobRequest:
    """Build the core/CLI-facing ``nirs4all.run`` cluster job contract.

    This accepts the local ``nirs4all.run`` vocabulary where it matters:
    ``workspace_path`` is intentionally omitted because every worker task gets an
    isolated workspace, while ``n_jobs`` is translated to the runner's
    ``inner_n_jobs`` parameter.
    """
    run_params, translated, omitted = _normalize_run_params(
        params, n_jobs=n_jobs, inner_n_jobs=inner_n_jobs, workspace_path=workspace_path
    )
    payload: dict[str, Any] = {
        "type": "nirs4all.run",
        "name": name,
        "priority": priority,
        "params": run_params,
        "rank_metric": rank_metric,
        "rank_mode": rank_mode,
        "idempotency_key": idempotency_key,
    }
    plural = pipelines is not None or datasets is not None
    if pipelines is not None:
        payload["pipelines"] = [_as_pipeline(p).model_dump() for p in pipelines]
    elif pipeline is not None:
        payload["pipeline"] = _as_pipeline(pipeline).model_dump()
    if datasets is not None:
        payload["datasets"] = [_as_dataset(d).model_dump() for d in datasets]
    elif dataset is not None:
        payload["dataset"] = _as_dataset(dataset).model_dump()
    if requirements is not None:
        payload["requirements"] = requirements
    if outputs is not None:
        payload["outputs"] = outputs
    if retry is not None:
        payload["retry"] = retry
    payload["parity"] = DistributedRunParity(
        scope="pipeline_dataset_matrix" if plural else "atomic",
        metric_tolerance_abs=metric_tolerance_abs,
        preserved_params=sorted(k for k in run_params if k != "inner_n_jobs"),
        translated_params=translated,
        omitted_local_kwargs=omitted,
        deferred=list(_FINE_GRAINED_DAG_DEFERRED),
    ).model_dump()
    req = JobRequest.model_validate(payload)
    if req.scheduler is None:
        req = req.model_copy(update={"scheduler": req.inferred_scheduler_contract()})
    return req


class ClusterClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._http = make_http_client(
            self.base_url, token=token, role="client", timeout=timeout, transport=transport
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ClusterClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Handshake
    # ------------------------------------------------------------------ #
    def server_info(self) -> ServerInfo:
        """Probe ``GET /version`` for reachability + protocol compatibility.

        Call it once at startup to fail fast on an unreachable server
        (:class:`ClusterConnectionError`) or an incompatible protocol major
        (``compatible=False``). ``/version`` is unauthenticated, so it does not
        validate the credential — the first authenticated call does that, raising
        :class:`ClusterAuthError` / :class:`ClusterPermissionError` as appropriate.
        """
        resp = request(self._http, "GET", "/version")
        data = resp.json()
        api_version = int(data.get("api_version", 0))
        return ServerInfo(
            service=data.get("service", "nirs4all-cluster"),
            version=data.get("version", "?"),
            api_version=api_version,
            compatible=not is_incompatible(api_version),
        )

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def submit(self, job: JobRequest | dict[str, Any]) -> JobView:
        req = job if isinstance(job, JobRequest) else JobRequest.model_validate(job)
        resp = request(self._http, "POST", "/v1/jobs", json=req.model_dump())
        return JobView.model_validate(resp.json())

    def submit_run(
        self,
        *,
        pipeline: PipelineInput | None = None,
        dataset: DatasetInput | None = None,
        pipelines: list[PipelineInput] | None = None,
        datasets: list[DatasetInput] | None = None,
        params: dict[str, Any] | None = None,
        name: str | None = None,
        priority: int = 0,
        requirements: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
        retry: dict[str, Any] | None = None,
        rank_metric: str = "best_rmse",
        rank_mode: str = "min",
        idempotency_key: str | None = None,
    ) -> JobView:
        return self.submit_nirs4all_run(
            pipeline=pipeline,
            dataset=dataset,
            pipelines=pipelines,
            datasets=datasets,
            params=params,
            name=name,
            priority=priority,
            requirements=requirements,
            outputs=outputs,
            retry=retry,
            rank_metric=rank_metric,
            rank_mode=rank_mode,
            idempotency_key=idempotency_key,
        )

    def submit_nirs4all_run(
        self,
        *,
        pipeline: PipelineInput | None = None,
        dataset: DatasetInput | None = None,
        pipelines: list[PipelineInput] | None = None,
        datasets: list[DatasetInput] | None = None,
        params: dict[str, Any] | None = None,
        n_jobs: int | None = None,
        inner_n_jobs: int | None = None,
        workspace_path: str | Path | None = None,
        name: str | None = None,
        priority: int = 0,
        requirements: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
        retry: dict[str, Any] | None = None,
        rank_metric: str = "best_rmse",
        rank_mode: str = "min",
        idempotency_key: str | None = None,
        metric_tolerance_abs: float = 1e-6,
    ) -> JobView:
        """Submit a local ``nirs4all.run`` shaped job through the cluster adapter."""
        return self.submit(
            build_nirs4all_run_request(
                pipeline=pipeline,
                dataset=dataset,
                pipelines=pipelines,
                datasets=datasets,
                params=params,
                n_jobs=n_jobs,
                inner_n_jobs=inner_n_jobs,
                workspace_path=workspace_path,
                name=name,
                priority=priority,
                requirements=requirements,
                outputs=outputs,
                retry=retry,
                rank_metric=rank_metric,
                rank_mode=rank_mode,
                idempotency_key=idempotency_key,
                metric_tolerance_abs=metric_tolerance_abs,
            )
        )

    def upload_artifact(self, path: str | Path, *, kind: str = "input") -> str:
        """Upload an input file (pipeline YAML / dataset zip); returns artifact_id."""
        path = Path(path)
        with open(path, "rb") as fh:
            resp = request(
                self._http,
                "POST",
                "/v1/artifacts",
                params={"kind": kind},
                files={"file": (path.name, fh, "application/octet-stream")},
            )
        return resp.json()["artifact_id"]

    # ------------------------------------------------------------------ #
    # Inspection
    # ------------------------------------------------------------------ #
    def get_job(self, job_id: str) -> JobView:
        resp = request(self._http, "GET", f"/v1/jobs/{job_id}")
        return JobView.model_validate(resp.json())

    def list_jobs(
        self,
        limit: int = 100,
        *,
        status: str | None = None,
        name: str | None = None,
        created_before: float | None = None,
    ) -> list[JobView]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        if name:
            params["name"] = name
        if created_before is not None:
            params["created_before"] = created_before
        resp = request(self._http, "GET", "/v1/jobs", params=params)
        return [JobView.model_validate(j) for j in resp.json()]

    def stats(self) -> ClusterStats:
        resp = request(self._http, "GET", "/v1/stats")
        return ClusterStats.model_validate(resp.json())

    def get_tasks(self, job_id: str) -> list[TaskView]:
        resp = request(self._http, "GET", f"/v1/jobs/{job_id}/tasks")
        return [TaskView.model_validate(t) for t in resp.json()]

    def get_events(self, job_id: str, after_id: int = 0, limit: int = 500) -> list[EventView]:
        resp = request(self._http, "GET", f"/v1/jobs/{job_id}/events", params={"after_id": after_id, "limit": limit})
        return [EventView.model_validate(e) for e in resp.json()]

    def list_workers(self) -> list[dict[str, Any]]:
        resp = request(self._http, "GET", "/v1/workers")
        return resp.json()

    # ------------------------------------------------------------------ #
    # Control
    # ------------------------------------------------------------------ #
    def cancel(self, job_id: str) -> JobView:
        resp = request(self._http, "POST", f"/v1/jobs/{job_id}/cancel")
        return JobView.model_validate(resp.json())

    def wait(self, job_id: str, *, poll: float = 2.0, timeout: float | None = None) -> JobView:
        start = time.time()
        while True:
            job = self.get_job(job_id)
            if job.status.value in _TERMINAL:
                return job
            if timeout is not None and (time.time() - start) > timeout:
                raise TimeoutError(f"job {job_id} did not finish within {timeout}s (status={job.status.value})")
            time.sleep(poll)

    def get_result(self, job_id: str) -> JobView:
        """Alias for get_job — the aggregate (ranking, best model) lives on the view."""
        return self.get_job(job_id)

    # ------------------------------------------------------------------ #
    # Artifacts
    # ------------------------------------------------------------------ #
    def list_artifacts(self, job_id: str) -> list[dict[str, Any]]:
        resp = request(self._http, "GET", f"/v1/jobs/{job_id}/artifacts")
        return resp.json()

    def download_artifact(self, artifact_id: str, dest: str | Path) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"/v1/artifacts/{artifact_id}"
        try:
            with self._http.stream("GET", url) as resp:
                if resp.status_code >= 400:
                    raise_for_response(resp)  # reads + maps to the typed error (e.g. 404/403)
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
        except httpx.TransportError as exc:
            raise ClusterConnectionError(str(exc), method="GET", url=url) from exc
        return dest

    def download_best_model(self, job_id: str, dest: str | Path) -> Path | None:
        # Use the aggregate's resolved id (single source of truth) rather than
        # scanning artifact rows, which can contain stale best_model links.
        artifact_id = self.get_job(job_id).aggregate.best_model_artifact_id
        if artifact_id is None:
            return None
        return self.download_artifact(artifact_id, dest)

    def download_all_artifacts(self, job_id: str, out_dir: str | Path) -> list[Path]:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        seen: set[str] = set()
        for art in self.list_artifacts(job_id):
            if art["id"] in seen:  # best_model + model can point to the same blob
                continue
            seen.add(art["id"])
            # Filenames come from the server; never let them escape out_dir.
            raw = art.get("filename") or f"{art['id']}.bin"
            name = Path(raw).name or f"{art['id']}.bin"
            dest = out_dir / f"{art['role']}_{art.get('task_id') or 'job'}_{name}"
            written.append(self.download_artifact(art["id"], dest))
        return written
