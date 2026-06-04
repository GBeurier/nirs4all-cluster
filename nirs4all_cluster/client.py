"""``ClusterClient`` — the thin Python SDK.

It speaks the server's REST API and nothing more; it never imports nirs4all and
never reimplements pipeline/dataset logic. Friendly helpers turn plain strings
and dicts into the wire schema.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import httpx

from .schemas import (
    DatasetRef,
    EventView,
    JobRequest,
    JobView,
    PipelineRef,
    TaskView,
)

PipelineInput = PipelineRef | dict | str
DatasetInput = DatasetRef | dict | str

_TERMINAL = {"succeeded", "failed", "cancelled"}


def _as_pipeline(value: PipelineInput) -> PipelineRef:
    if isinstance(value, PipelineRef):
        return value
    if isinstance(value, str):
        return PipelineRef(kind="path", path=value)
    return PipelineRef.model_validate(value)


def _as_dataset(value: DatasetInput) -> DatasetRef:
    if isinstance(value, DatasetRef):
        return value
    if isinstance(value, str):
        return DatasetRef(kind="shared_path", path=value)
    return DatasetRef.model_validate(value)


class ClusterClient:
    def __init__(self, base_url: str, *, token: str | None = None, timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._http = httpx.Client(base_url=self.base_url, headers=headers, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ClusterClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def submit(self, request: JobRequest | dict[str, Any]) -> JobView:
        req = request if isinstance(request, JobRequest) else JobRequest.model_validate(request)
        resp = self._http.post("/v1/jobs", json=req.model_dump())
        resp.raise_for_status()
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
        payload: dict[str, Any] = {
            "type": "nirs4all.run",
            "name": name,
            "priority": priority,
            "params": params or {},
            "rank_metric": rank_metric,
            "rank_mode": rank_mode,
            "idempotency_key": idempotency_key,
        }
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
        return self.submit(payload)

    def upload_artifact(self, path: str | Path, *, kind: str = "input") -> str:
        """Upload an input file (pipeline YAML / dataset zip); returns artifact_id."""
        path = Path(path)
        with open(path, "rb") as fh:
            resp = self._http.post(
                "/v1/artifacts",
                params={"kind": kind},
                files={"file": (path.name, fh, "application/octet-stream")},
            )
        resp.raise_for_status()
        return resp.json()["artifact_id"]

    # ------------------------------------------------------------------ #
    # Inspection
    # ------------------------------------------------------------------ #
    def get_job(self, job_id: str) -> JobView:
        resp = self._http.get(f"/v1/jobs/{job_id}")
        resp.raise_for_status()
        return JobView.model_validate(resp.json())

    def list_jobs(self, limit: int = 100) -> list[JobView]:
        resp = self._http.get("/v1/jobs", params={"limit": limit})
        resp.raise_for_status()
        return [JobView.model_validate(j) for j in resp.json()]

    def get_tasks(self, job_id: str) -> list[TaskView]:
        resp = self._http.get(f"/v1/jobs/{job_id}/tasks")
        resp.raise_for_status()
        return [TaskView.model_validate(t) for t in resp.json()]

    def get_events(self, job_id: str, after_id: int = 0, limit: int = 500) -> list[EventView]:
        resp = self._http.get(f"/v1/jobs/{job_id}/events", params={"after_id": after_id, "limit": limit})
        resp.raise_for_status()
        return [EventView.model_validate(e) for e in resp.json()]

    def list_workers(self) -> list[dict[str, Any]]:
        resp = self._http.get("/v1/workers")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Control
    # ------------------------------------------------------------------ #
    def cancel(self, job_id: str) -> JobView:
        resp = self._http.post(f"/v1/jobs/{job_id}/cancel")
        resp.raise_for_status()
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
        resp = self._http.get(f"/v1/jobs/{job_id}/artifacts")
        resp.raise_for_status()
        return resp.json()

    def download_artifact(self, artifact_id: str, dest: str | Path) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self._http.stream("GET", f"/v1/artifacts/{artifact_id}") as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_bytes():
                    fh.write(chunk)
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
