"""FastAPI application: client API + worker API + live event stream.

A single-process server (per design). State lives in ``<state_dir>``:
``store.sqlite`` (queue + metadata) and ``objects/`` (content-addressed blobs).
A background reaper requeues expired leases and marks silent workers dead.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from .. import __version__
from ..schemas import (
    ArtifactView,
    ClusterStats,
    EventView,
    HeartbeatAck,
    JobAggregate,
    JobRequest,
    JobStatus,
    JobView,
    LeaseResponse,
    TaskEvent,
    TaskFailure,
    TaskResult,
    TaskStatus,
    TaskView,
    WorkerRegister,
    WorkerRegistered,
)
from ..versioning import (
    API_VERSION,
    CLUSTER_VERSION,
    H_API,
    H_ROLE,
    H_VERSION,
    is_divergent,
    is_incompatible,
    parse_api,
)
from .artifacts import ArtifactStore, ArtifactTooLarge
from .db import Database
from .events import EventBroker
from .scheduler import IllegalTransition, aggregate_metric_better

logger = logging.getLogger("nirs4all_cluster.server")

# Static assets for the built-in /ui dashboard.
_STATIC_DIR = Path(__file__).parent / "static"

# Task states considered terminal for job finalization.
_TERMINAL = {TaskStatus.SUCCEEDED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value}


@dataclass
class ServerConfig:
    state_dir: str
    token: str | None = None
    allow_python_jobs: bool = False
    lease_ttl_s: float = 60.0
    heartbeat_interval_s: float = 10.0
    reaper_interval_s: float = 5.0
    worker_dead_after_s: float = 45.0
    max_artifact_mb: int = 2048
    # Max body size (MB) for non-upload (JSON) requests. Multipart artifact
    # uploads are exempt — they keep their own streaming ``max_artifact_mb`` limit.
    max_request_mb: int = 16
    # Allowed CORS origins (opt-in; off by default — trusted-LAN posture). When set,
    # a browser on another origin (e.g. a separate dashboard) may call the API.
    cors_origins: list[str] = field(default_factory=list)


def _sanitize(value: Any) -> Any:
    """Replace NaN/inf floats with None so the payload is valid JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def create_app(config: ServerConfig) -> FastAPI:
    state = Path(config.state_dir)
    state.mkdir(parents=True, exist_ok=True)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(state / "store.sqlite")
        store = ArtifactStore(state / "objects")
        broker = EventBroker(db)
        broker.set_loop(asyncio.get_running_loop())
        app.state.db = db
        app.state.store = store
        app.state.broker = broker
        app.state.config = config
        # (role, version) pairs already reported as divergent — throttles the
        # version_divergence event/log so a chatty peer is noted only once.
        app.state.seen_versions = set()
        logger.info("server ready: nirs4all-cluster %s (api v%s) state=%s", CLUSTER_VERSION, API_VERSION, state)
        reaper = asyncio.create_task(_reaper_loop(app))
        try:
            yield
        finally:
            logger.info("server shutting down")
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper
            db.close()

    app = FastAPI(title="nirs4all-cluster", version=__version__, lifespan=lifespan)

    if config.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Version headers every /v1 response (including early rejections) advertises.
    version_headers = {H_VERSION: CLUSTER_VERSION, H_API: str(API_VERSION)}

    @app.middleware("http")
    async def _guard(request: Request, call_next: Any) -> Any:
        path = request.url.path
        # Request-size guard for JSON endpoints. Multipart artifact uploads (paths
        # ending in /artifacts) are exempt — they stream under max_artifact_mb.
        if not path.endswith("/artifacts"):
            length = request.headers.get("content-length")
            if length is not None:
                try:
                    if int(length) > config.max_request_mb * 1024 * 1024:
                        return JSONResponse(
                            status_code=413,
                            content={"detail": "request body too large"},
                            headers=version_headers,
                        )
                except ValueError:
                    pass
        # Protocol/version handshake on the API surface.
        if path.startswith("/v1/"):
            peer_api = parse_api(request.headers.get(H_API))
            if is_incompatible(peer_api):
                return JSONResponse(
                    status_code=426,
                    content={"detail": f"incompatible protocol: server api={API_VERSION}, peer api={peer_api}"},
                    headers=version_headers,
                )
            peer_version = request.headers.get(H_VERSION)
            if is_divergent(peer_version):
                _note_divergence(request.app, request.headers.get(H_ROLE, "client"), peer_version)
        response = await call_next(request)
        response.headers[H_VERSION] = CLUSTER_VERSION
        response.headers[H_API] = str(API_VERSION)
        return response

    # ------------------------------------------------------------------ #
    # Auth
    # ------------------------------------------------------------------ #
    def auth(authorization: str | None = Header(default=None)) -> None:
        token = config.token
        if not token:
            return  # dev mode: no auth
        expected = f"Bearer {token}"
        if authorization is None or not hmac.compare_digest(authorization, expected):
            raise HTTPException(status_code=401, detail="invalid or missing token")

    def db_of(request: Request) -> Database:
        return request.app.state.db

    def store_of(request: Request) -> ArtifactStore:
        return request.app.state.store

    def broker_of(request: Request) -> EventBroker:
        return request.app.state.broker

    # ------------------------------------------------------------------ #
    # Health / dashboard
    # ------------------------------------------------------------------ #
    @app.get("/")
    def health() -> dict[str, Any]:
        return {"service": "nirs4all-cluster", "version": CLUSTER_VERSION, "api_version": API_VERSION, "ok": True}

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/version")
    def version() -> dict[str, Any]:
        return {"service": "nirs4all-cluster", "version": CLUSTER_VERSION, "api_version": API_VERSION}

    @app.get("/ui", include_in_schema=False)
    @app.get("/ui/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")

    # ------------------------------------------------------------------ #
    # Client API
    # ------------------------------------------------------------------ #
    @app.post("/v1/jobs", dependencies=[Depends(auth)])
    def submit_job(req: JobRequest, request: Request) -> JobView:
        db = db_of(request)
        broker = broker_of(request)
        if req.pipeline_list_has_python() and not config.allow_python_jobs:
            raise HTTPException(
                status_code=400,
                detail="python_entrypoint pipelines require the server flag --allow-python-jobs",
            )
        # Availability by default: an nirs4all.run job only goes to workers that
        # declared nirs4all. The client can override with an explicit version range.
        if req.type == "nirs4all.run" and "nirs4all" not in req.requirements.packages:
            req.requirements.packages["nirs4all"] = ""
        if req.idempotency_key:
            existing = db.find_job_by_idempotency(req.idempotency_key)
            if existing is not None:
                return _job_view(db, existing["id"])
        try:
            job_id, task_ids = db.create_job_with_tasks(req)
        except sqlite3.IntegrityError:
            # Concurrent duplicate idempotency_key won the insert race — return it.
            if req.idempotency_key:
                existing = db.find_job_by_idempotency(req.idempotency_key)
                if existing is not None:
                    return _job_view(db, existing["id"])
            raise
        broker.emit(
            level="info",
            type="job_submitted",
            message=f"job {job_id} queued with {len(task_ids)} task(s)",
            job_id=job_id,
            data={"num_tasks": len(task_ids)},
        )
        return _job_view(db, job_id)

    @app.get("/v1/jobs", dependencies=[Depends(auth)])
    def list_jobs(
        request: Request,
        limit: int = 100,
        status: str | None = None,
        name: str | None = None,
        created_before: float | None = None,
    ) -> list[JobView]:
        db = db_of(request)
        rows = db.list_jobs(limit=limit, status=status, name=name, created_before=created_before)
        return [_job_view(db, row["id"]) for row in rows]

    @app.get("/v1/stats", dependencies=[Depends(auth)])
    def stats(request: Request) -> ClusterStats:
        db = db_of(request)
        workers = db.count_workers_by_status()
        return ClusterStats(
            server_version=CLUSTER_VERSION,
            api_version=API_VERSION,
            jobs_by_status=db.count_jobs_by_status(),
            workers_alive=workers.get("alive", 0),
            workers_dead=workers.get("dead", 0),
            tasks_in_flight=db.count_tasks_in_flight(),
        )

    @app.get("/v1/jobs/{job_id}", dependencies=[Depends(auth)])
    def get_job(job_id: str, request: Request) -> JobView:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_view(db, job_id)

    @app.get("/v1/jobs/{job_id}/tasks", dependencies=[Depends(auth)])
    def get_job_tasks(job_id: str, request: Request) -> list[TaskView]:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return [_task_view(row) for row in db.list_tasks_for_job(job_id)]

    @app.post("/v1/jobs/{job_id}/cancel", dependencies=[Depends(auth)])
    def cancel_job(job_id: str, request: Request) -> JobView:
        db = db_of(request)
        broker = broker_of(request)
        row = db.get_job(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        status = JobStatus(row["status"])
        if status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED):
            return _job_view(db, job_id)
        still_running = db.cancel_job_tasks(job_id)
        if still_running:
            db.set_job_status(job_id, JobStatus.CANCELLING)
        else:
            db.set_job_status(job_id, JobStatus.CANCELLED)
        broker.emit(
            level="warning",
            type="job_cancel_requested",
            message=f"cancellation requested for job {job_id}",
            job_id=job_id,
            data={"running_tasks": still_running},
        )
        _finalize_job(db, broker, job_id)
        return _job_view(db, job_id)

    @app.get("/v1/jobs/{job_id}/events", dependencies=[Depends(auth)])
    def get_events(job_id: str, request: Request, after_id: int = 0, limit: int = 500) -> list[EventView]:
        db = db_of(request)
        rows = db.list_events(job_id, after_id=after_id, limit=limit)
        return [_event_view(r) for r in rows]

    @app.get("/v1/jobs/{job_id}/artifacts", dependencies=[Depends(auth)])
    def get_job_artifacts(job_id: str, request: Request) -> list[dict[str, Any]]:
        db = db_of(request)
        out = []
        for row in db.list_job_artifacts(job_id):
            view = ArtifactView(
                id=row["id"],
                sha256=row["sha256"],
                kind=row["kind"],
                size_bytes=row["size_bytes"],
                created_at=row["created_at"],
                filename=row["filename"],
            )
            out.append({"role": row["role"], "task_id": row["task_id"] or None, **view.model_dump()})
        return out

    @app.post("/v1/artifacts", dependencies=[Depends(auth)])
    async def upload_input_artifact(
        request: Request, file: UploadFile, kind: str = "input"
    ) -> dict[str, Any]:
        """Client-side upload of an input artifact (pipeline file / dataset zip).

        Returns an ``artifact_id`` to reference in a job's ``artifact`` ref. The
        worker fetches it via ``GET /v1/artifacts/{id}``.
        """
        store = store_of(request)
        db = db_of(request)
        max_bytes = config.max_artifact_mb * 1024 * 1024
        try:
            sha, path, size = await asyncio.to_thread(store.put_stream, file.file, max_bytes)
        except ArtifactTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        artifact_id = db.add_artifact(
            sha256=sha, kind=kind, path=path, size_bytes=size, filename=file.filename
        )
        return {"artifact_id": artifact_id, "sha256": sha, "size_bytes": size}

    @app.get("/v1/artifacts/{artifact_id}", dependencies=[Depends(auth)])
    def download_artifact(artifact_id: str, request: Request) -> FileResponse:
        db = db_of(request)
        row = db.get_artifact(artifact_id)
        if row is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        filename = row["filename"] or f"{artifact_id}.bin"
        return FileResponse(row["path"], filename=filename, media_type="application/octet-stream")

    @app.websocket("/v1/jobs/{job_id}/events/stream")
    async def stream_events(websocket: WebSocket, job_id: str) -> None:
        # Auth via ?token= query param for WS (headers are awkward in browsers).
        provided = websocket.query_params.get("token") or ""
        if config.token and not hmac.compare_digest(provided, config.token):
            await websocket.close(code=4401)
            return
        db = websocket.app.state.db
        broker = websocket.app.state.broker
        await websocket.accept()
        # Replay history first, then live.
        last_id = 0
        for row in db.list_events(job_id):
            await websocket.send_json(_event_view(row).model_dump(mode="json"))
            last_id = row["id"]
        queue = await broker.subscribe(job_id)
        try:
            while True:
                payload = await queue.get()
                if payload["id"] > last_id:
                    await websocket.send_json(_jsonable(payload))
                    last_id = payload["id"]
        except WebSocketDisconnect:
            pass
        finally:
            await broker.unsubscribe(job_id, queue)

    @app.websocket("/v1/events/stream")
    async def stream_all_events(websocket: WebSocket) -> None:
        """Global live feed across all jobs + workers (powers the /ui dashboard)."""
        provided = websocket.query_params.get("token") or ""
        if config.token and not hmac.compare_digest(provided, config.token):
            await websocket.close(code=4401)
            return
        db = websocket.app.state.db
        broker = websocket.app.state.broker
        await websocket.accept()
        last_id = 0
        for row in db.list_recent_events():
            await websocket.send_json(_event_view(row).model_dump(mode="json"))
            last_id = row["id"]
        queue = await broker.subscribe_global()
        try:
            while True:
                payload = await queue.get()
                if payload["id"] > last_id:
                    await websocket.send_json(_jsonable(payload))
                    last_id = payload["id"]
        except WebSocketDisconnect:
            pass
        finally:
            await broker.unsubscribe_global(queue)

    # ------------------------------------------------------------------ #
    # Worker API
    # ------------------------------------------------------------------ #
    @app.post("/v1/workers/register", dependencies=[Depends(auth)])
    def register_worker(reg: WorkerRegister, request: Request) -> WorkerRegistered:
        db = db_of(request)
        broker = broker_of(request)
        worker_id = db.register_worker(reg)
        broker.emit(
            level="info",
            type="worker_registered",
            message=f"worker {worker_id} registered",
            worker_id=worker_id,
            data={"labels": reg.labels, "slots": reg.slots_total},
        )
        return WorkerRegistered(
            worker_id=worker_id,
            heartbeat_interval_s=config.heartbeat_interval_s,
            lease_ttl_s=config.lease_ttl_s,
        )

    @app.post("/v1/workers/{worker_id}/heartbeat", dependencies=[Depends(auth)])
    def heartbeat(worker_id: str, request: Request) -> HeartbeatAck:
        db = db_of(request)
        if not db.heartbeat_worker(worker_id, config.lease_ttl_s):
            raise HTTPException(status_code=404, detail="unknown worker")
        # Tell the worker which of its in-flight tasks belong to cancelling jobs.
        cancel_ids = []
        for task_id in db.tasks_for_worker(worker_id):
            task = db.get_task(task_id)
            if task is None:
                continue
            job = db.get_job(task["job_id"])
            if job and job["status"] in (JobStatus.CANCELLING.value, JobStatus.CANCELLED.value):
                cancel_ids.append(task_id)
        return HeartbeatAck(ok=True, cancel_task_ids=cancel_ids)

    @app.post("/v1/workers/{worker_id}/lease", dependencies=[Depends(auth)])
    def lease(worker_id: str, request: Request) -> LeaseResponse:
        db = db_of(request)
        broker = broker_of(request)
        if db.get_worker(worker_id) is None:
            raise HTTPException(status_code=404, detail="unknown worker")
        payload = db.lease_next_task(worker_id, config.lease_ttl_s)
        if payload is not None:
            broker.emit(
                level="info",
                type="task_leased",
                message=f"task {payload.task_id} leased to {worker_id}",
                job_id=payload.job_id,
                task_id=payload.task_id,
                worker_id=worker_id,
                data={"attempt": payload.attempt},
            )
        return LeaseResponse(task=payload)

    @app.post("/v1/tasks/{task_id}/start", dependencies=[Depends(auth)])
    def start_task(task_id: str, request: Request, worker_id: str) -> dict[str, Any]:
        db = db_of(request)
        broker = broker_of(request)
        try:
            row = db.start_task(task_id, worker_id)
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except IllegalTransition as exc:
            # e.g. the task was cancelled between lease and start.
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        broker.emit(
            level="info",
            type="task_started",
            message=f"task {task_id} started",
            job_id=row["job_id"],
            task_id=task_id,
            worker_id=worker_id,
        )
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/events", dependencies=[Depends(auth)])
    def task_event(task_id: str, ev: TaskEvent, request: Request) -> dict[str, Any]:
        db = db_of(request)
        broker = broker_of(request)
        row = db.get_task(task_id)
        if row is None:
            raise HTTPException(status_code=404, detail="task not found")
        broker.emit(
            level=ev.level.value,
            type=ev.type,
            message=ev.message,
            job_id=row["job_id"],
            task_id=task_id,
            worker_id=row["worker_id"],
            data={"progress": ev.progress, **ev.data},
        )
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/artifacts", dependencies=[Depends(auth)])
    async def upload_artifact(
        task_id: str,
        request: Request,
        file: UploadFile,
        role: str = "output",
        kind: str = "blob",
    ) -> dict[str, Any]:
        db = db_of(request)
        store = store_of(request)
        row = db.get_task(task_id)
        if row is None:
            raise HTTPException(status_code=404, detail="task not found")
        max_bytes = config.max_artifact_mb * 1024 * 1024
        try:
            sha, path, size = await asyncio.to_thread(store.put_stream, file.file, max_bytes)
        except ArtifactTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        artifact_id = db.add_artifact(
            sha256=sha, kind=kind, path=path, size_bytes=size, filename=file.filename
        )
        db.link_job_artifact(row["job_id"], task_id, role, artifact_id)
        return {"artifact_id": artifact_id, "sha256": sha, "size_bytes": size}

    @app.post("/v1/tasks/{task_id}/complete", dependencies=[Depends(auth)])
    def complete_task(task_id: str, result: TaskResult, request: Request, worker_id: str) -> dict[str, Any]:
        db = db_of(request)
        broker = broker_of(request)
        row = db.get_task(task_id)
        if row is None:
            raise HTTPException(status_code=404, detail="task not found")
        job_row = db.get_job(row["job_id"])
        cancelling = job_row is not None and job_row["status"] in (
            JobStatus.CANCELLING.value,
            JobStatus.CANCELLED.value,
        )
        if cancelling:
            db.cancel_running_task(task_id, worker_id)
        else:
            try:
                db.complete_task(task_id, worker_id, result.model_dump())
            except PermissionError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            # Pipeline version trace: if the client pinned a fingerprint for an
            # inline pipeline and the worker ran something different, note it
            # (non-fatal — the result still stands; this is traceability).
            expected = None
            with contextlib.suppress(ValueError, TypeError, AttributeError):
                expected = (json.loads(row["payload_json"]).get("pipeline") or {}).get("expected_fingerprint")
            if expected and result.pipeline_fingerprint and expected != result.pipeline_fingerprint:
                logger.warning(
                    "pipeline fingerprint mismatch on task %s: expected=%s actual=%s",
                    task_id, expected, result.pipeline_fingerprint,
                )
                broker.emit(
                    level="warning",
                    type="pipeline_fingerprint_mismatch",
                    message=f"task {task_id}: pipeline content differs from what was submitted",
                    job_id=row["job_id"],
                    task_id=task_id,
                    worker_id=worker_id,
                    data={"expected": expected, "actual": result.pipeline_fingerprint},
                )
        broker.emit(
            level="info",
            type="task_completed",
            message=f"task {task_id} completed",
            job_id=row["job_id"],
            task_id=task_id,
            worker_id=worker_id,
            data={"metrics": _jsonable(result.metrics.model_dump())},
        )
        _finalize_job(db, broker, row["job_id"])
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/fail", dependencies=[Depends(auth)])
    def fail_task(task_id: str, failure: TaskFailure, request: Request, worker_id: str) -> dict[str, Any]:
        db = db_of(request)
        broker = broker_of(request)
        row = db.get_task(task_id)
        if row is None:
            raise HTTPException(status_code=404, detail="task not found")
        job_row = db.get_job(row["job_id"])
        cancelling = job_row is not None and job_row["status"] in (
            JobStatus.CANCELLING.value,
            JobStatus.CANCELLED.value,
        )
        if cancelling:
            db.cancel_running_task(task_id, worker_id)
            requeued = False
        else:
            try:
                updated = db.fail_task(task_id, worker_id, failure.error, retriable=failure.retriable)
            except PermissionError:
                return {"ok": True, "ignored": True, "requeued": False}  # stale report
            requeued = updated["status"] == TaskStatus.QUEUED.value
        broker.emit(
            level="error",
            type="task_failed",
            message=f"task {task_id} failed: {failure.error}",
            job_id=row["job_id"],
            task_id=task_id,
            worker_id=worker_id,
            data={"retriable": failure.retriable, "requeued": requeued},
        )
        _finalize_job(db, broker, row["job_id"])
        return {"ok": True, "requeued": requeued}

    @app.get("/v1/workers", dependencies=[Depends(auth)])
    def list_workers(request: Request) -> list[dict[str, Any]]:
        db = db_of(request)
        out = []
        for w in db.list_workers():
            version = json.loads(w["version_json"]) if w["version_json"] else {}
            cluster_version = version.get("nirs4all_cluster")
            out.append(
                {
                    "id": w["id"],
                    "name": w["name"],
                    "status": w["status"],
                    "slots_total": w["slots_total"],
                    "slots_used": w["slots_used"],
                    "last_seen_at": w["last_seen_at"],
                    "labels": json.loads(w["labels_json"]),
                    "capabilities": json.loads(w["capabilities_json"]) if w["capabilities_json"] else {},
                    "version": version,
                    "cluster_version": cluster_version,
                    "version_divergent": is_divergent(cluster_version),
                }
            )
        return out

    return app


def _note_divergence(app: FastAPI, role: str, peer_version: str | None) -> None:
    """Log + emit a one-shot event when a compatible peer runs a different version."""
    seen: set[tuple[str, str | None]] = app.state.seen_versions
    key = (role, peer_version)
    if key in seen:
        return
    seen.add(key)
    logger.warning(
        "version divergence: %s runs nirs4all-cluster %s; server runs %s (compatible, api v%s)",
        role, peer_version, CLUSTER_VERSION, API_VERSION,
    )
    broker: EventBroker = app.state.broker
    broker.emit(
        level="warning",
        type="version_divergence",
        message=f"{role} runs nirs4all-cluster {peer_version}; server runs {CLUSTER_VERSION} (compatible)",
        data={
            "role": role,
            "peer_version": peer_version,
            "server_version": CLUSTER_VERSION,
            "api_version": API_VERSION,
        },
    )


# --------------------------------------------------------------------------- #
# Background reaper
# --------------------------------------------------------------------------- #


async def _reaper_loop(app: FastAPI) -> None:
    config: ServerConfig = app.state.config
    db: Database = app.state.db
    broker: EventBroker = app.state.broker
    while True:
        await asyncio.sleep(config.reaper_interval_s)
        try:
            dead = db.mark_dead_workers(config.worker_dead_after_s)
            for worker_id in dead:
                logger.warning("worker %s marked dead (no heartbeat)", worker_id)
                broker.emit(
                    level="warning",
                    type="worker_dead",
                    message=f"worker {worker_id} marked dead (no heartbeat)",
                    worker_id=worker_id,
                )
            affected = db.reap_expired_leases()
            jobs = set()
            for task_id, job_id in affected:
                jobs.add(job_id)
                broker.emit(
                    level="warning",
                    type="task_lost",
                    message=f"task {task_id} lease expired",
                    job_id=job_id,
                    task_id=task_id,
                )
            for job_id in jobs:
                _finalize_job(db, broker, job_id)
        except Exception as exc:  # reaper must never die
            logger.exception("reaper iteration failed")
            broker.emit(level="error", type="reaper_error", message=str(exc))


# --------------------------------------------------------------------------- #
# Aggregation / finalization
# --------------------------------------------------------------------------- #


def _build_aggregate(db: Database, job_id: str) -> tuple[JobAggregate, str | None, str | None]:
    job_row = db.get_job(job_id)
    if job_row is None:
        raise KeyError(job_id)
    req = JobRequest.model_validate_json(job_row["request_json"])
    tasks = db.list_tasks_for_job(job_id)
    agg = JobAggregate(num_tasks=len(tasks))
    best_value: float | None = None
    best_task_id: str | None = None
    ranking: list[dict[str, Any]] = []
    for row in tasks:
        status = row["status"]
        if status == TaskStatus.SUCCEEDED.value:
            agg.num_succeeded += 1
        elif status == TaskStatus.FAILED.value:
            agg.num_failed += 1
            if row["error"]:
                agg.errors[row["id"]] = row["error"]
        elif status == TaskStatus.RUNNING.value:
            agg.num_running += 1
        elif status in (TaskStatus.QUEUED.value, TaskStatus.LEASED.value):
            agg.num_queued += 1
        if status == TaskStatus.SUCCEEDED.value and row["result_json"]:
            result = TaskResult.model_validate_json(row["result_json"])
            metric_value = getattr(result.metrics, req.rank_metric, None)
            if isinstance(metric_value, float) and not math.isfinite(metric_value):
                metric_value = None
            ranking.append(
                {
                    "task_id": row["id"],
                    "dataset": row["dataset_label"],
                    "pipeline": row["pipeline_label"],
                    req.rank_metric: metric_value,
                    "metrics": {k: _sanitize(v) for k, v in result.metrics.model_dump().items()},
                }
            )
            if aggregate_metric_better(metric_value, best_value, req.rank_mode):
                best_value = metric_value
                best_task_id = row["id"]
    def _rank_key(entry: dict[str, Any]) -> tuple[int, float]:
        value = entry[req.rank_metric]
        if value is None:
            return (1, 0.0)  # None always sorts last, regardless of direction
        return (0, -value if req.rank_mode == "max" else value)

    ranking.sort(key=_rank_key)
    agg.ranking = ranking
    agg.best_metric = best_value
    agg.best_task_id = best_task_id
    # Resolve the best model artifact (if exported).
    best_model_artifact_id = None
    if best_task_id is not None:
        for art in db.list_job_artifacts(job_id):
            if art["task_id"] == best_task_id and art["role"] in ("model", "best_model"):
                best_model_artifact_id = art["id"]
                break
    agg.best_model_artifact_id = best_model_artifact_id
    return agg, best_task_id, best_model_artifact_id


def _finalize_job(db: Database, broker: EventBroker, job_id: str) -> None:
    job_row = db.get_job(job_id)
    if job_row is None:
        return
    agg, best_task_id, best_model_artifact_id = _build_aggregate(db, job_id)
    db.set_job_result(job_id, _jsonable(agg.model_dump()))
    # Keep exactly one job-level best_model link (a later task can beat an earlier one).
    db.clear_job_artifact_role(job_id, "best_model")
    if best_model_artifact_id:
        db.link_job_artifact(job_id, best_task_id, "best_model", best_model_artifact_id)

    tasks = db.list_tasks_for_job(job_id)
    all_terminal = all(t["status"] in _TERMINAL for t in tasks)
    if not all_terminal:
        return
    # Re-read the freshest status to pick the right terminal state.
    fresh = db.get_job(job_id)
    current = JobStatus(fresh["status"]) if fresh else JobStatus(job_row["status"])
    if current == JobStatus.CANCELLING:
        final = JobStatus.CANCELLED
    elif agg.num_succeeded > 0:
        final = JobStatus.SUCCEEDED
    else:
        final = JobStatus.FAILED
    # Atomic + idempotent: if a concurrent finalize already flipped the job, this
    # returns False and we skip the duplicate event (no IllegalTransition raised).
    if not db.try_set_job_status(job_id, final):
        return
    logger.info("job %s finalized: %s", job_id, final.value)
    broker.emit(
        level="info" if final == JobStatus.SUCCEEDED else "warning",
        type=f"job_{final.value}",
        message=f"job {job_id} {final.value}",
        job_id=job_id,
        data={"aggregate": _jsonable(agg.model_dump())},
    )


# --------------------------------------------------------------------------- #
# View helpers
# --------------------------------------------------------------------------- #


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return _sanitize(value)


def _job_view(db: Database, job_id: str) -> JobView:
    row = db.get_job(job_id)
    if row is None:
        raise KeyError(job_id)
    agg, _, _ = _build_aggregate(db, job_id)
    return JobView(
        id=row["id"],
        type=row["type"],
        name=row["name"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        aggregate=agg,
        error=row["error"],
    )


def _task_view(row: Any) -> TaskView:
    result = TaskResult.model_validate_json(row["result_json"]) if row["result_json"] else None
    return TaskView(
        id=row["id"],
        job_id=row["job_id"],
        status=TaskStatus(row["status"]),
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        worker_id=row["worker_id"],
        dataset_label=row["dataset_label"],
        pipeline_label=row["pipeline_label"],
        result=result,
        error=row["error"],
    )


def _event_view(row: Any) -> EventView:
    return EventView(
        id=row["id"],
        job_id=row["job_id"],
        task_id=row["task_id"],
        worker_id=row["worker_id"],
        ts=row["ts"],
        level=row["level"],
        type=row["type"],
        message=row["message"],
        data=json.loads(row["data_json"]) if row["data_json"] else {},
    )
