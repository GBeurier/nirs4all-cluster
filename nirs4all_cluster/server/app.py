"""FastAPI application: client API + worker API + live event stream.

A single-process server (per design). Authenticated credentials select independent
``projects/<safe-id>/store.sqlite`` and ``objects/`` stores under explicit v2 state.
A background reaper requeues expired leases and marks silent workers dead.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import secrets
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from nirs4all_access import ProjectTokenVerifier

from .. import __version__
from ..schemas import (
    ActiveAttempt,
    ArtifactView,
    ClusterStats,
    DagSchedulerContract,
    EventView,
    HeartbeatAck,
    JobAggregate,
    JobRequest,
    JobStatus,
    JobSubmissionMetadata,
    JobView,
    LeaseResponse,
    ResultProvenance,
    TaskAssignmentMetadata,
    TaskEvent,
    TaskFailure,
    TaskResult,
    TaskStatus,
    TaskView,
    WorkerHeartbeat,
    WorkerRegister,
    WorkerRegistered,
)
from ..versioning import (
    API_VERSION,
    CLUSTER_VERSION,
    is_divergent,
)
from .artifacts import ArtifactStore, ArtifactTooLarge
from .auth import Principal, Right
from .db import Database
from .events import EventBroker
from .frontdoor import CredentialSafeCORSMiddleware, FrontDoor
from .scheduler import IllegalTransition, aggregate_metric_better
from .state import state_credentials, validate_state

logger = logging.getLogger("nirs4all_cluster.server")

# Static assets for the built-in /ui dashboard.
_STATIC_DIR = Path(__file__).parent / "static"

# Task states considered terminal for job finalization.
_TERMINAL = {TaskStatus.SUCCEEDED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value}


@dataclass
class ServerConfig:
    state_dir: str
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


def _right_values(principal: Principal) -> list[str]:
    return sorted(principal.rights)


def _server_attested_job_request(req: JobRequest, principal: Principal) -> JobRequest:
    """Attach credential-derived submit metadata before storing a client job."""
    inferred = req.inferred_scheduler_contract()
    # The whole scheduler contract is server-attested. A client may send the field
    # for traceability, but its shape must not override what the server infers
    # from the validated payload.
    scheduler = DagSchedulerContract(shape=inferred.shape)
    submission = JobSubmissionMetadata(
        principal=principal.principal_id, project_id=principal.project_id, granted_rights=_right_values(principal)
    )
    return req.model_copy(update={"scheduler": scheduler, "submission": submission})


@dataclass
class ProjectService:
    db: Database
    store: ArtifactStore
    broker: EventBroker


def create_app(config: ServerConfig) -> FastAPI:
    state = validate_state(config.state_dir)
    records = state_credentials(state)
    import time

    active = [
        r
        for r in records
        if not r.revoked and (r.expires_at is None or r.expires_at > time.time()) and "cluster" in r.principal.services
    ]
    if not active:
        raise ValueError("no active Cluster credentials; provision a project token and restart")
    verifier = ProjectTokenVerifier(records)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        services: dict[str, ProjectService] = {}
        reapers = []
        app.state.services = services
        try:
            for project in sorted({r.principal.project_id for r in active}):
                directory = state / "projects" / project
                if directory.is_symlink():
                    raise ValueError("project directory must not be a symlink")
                directory.mkdir(mode=0o700, exist_ok=True)
                for child in ("store.sqlite", "objects"):
                    if (directory / child).is_symlink():
                        raise ValueError("project stores must not be symlinks")
                db = Database(directory / "store.sqlite")
                try:
                    store = ArtifactStore(directory / "objects")
                except BaseException:
                    db.close()
                    raise
                broker = EventBroker(db)
                broker.set_loop(asyncio.get_running_loop())
                services[project] = ProjectService(db, store, broker)
            reapers = [asyncio.create_task(_reaper_loop(service, config)) for service in services.values()]
            yield
        finally:
            for reaper in reapers:
                reaper.cancel()
            for reaper in reapers:
                with contextlib.suppress(asyncio.CancelledError):
                    await reaper
            for service in services.values():
                service.db.close()

    app = FastAPI(
        title="nirs4all-cluster",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(FrontDoor, owner=app, verifier=verifier, config=config)
    if config.cors_origins:
        app.add_middleware(
            CredentialSafeCORSMiddleware, allow_origins=config.cors_origins, allow_methods=["*"], allow_headers=["*"]
        )

    def requires(*rights: Right) -> Callable[..., Principal]:
        def dependency(request: Request) -> Principal:
            principal = request.state.principal
            if not set(rights) <= principal.rights:
                raise HTTPException(403, "credential lacks required rights")
            return principal

        return dependency

    def db_of(request: Request) -> Database:
        return request.state.service.db

    def store_of(request: Request) -> ArtifactStore:
        return request.state.service.store

    def broker_of(request: Request) -> EventBroker:
        return request.state.service.broker

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
    @app.post("/v1/jobs", dependencies=[Depends(requires(Right.SUBMIT))])
    def submit_job(req: JobRequest, request: Request) -> JobView:
        db = db_of(request)
        broker = broker_of(request)
        principal: Principal = request.state.principal
        req = _server_attested_job_request(req, principal)
        # Reject every referenced input before decomposition, including list/bundle forms.
        for ref in [*req.pipeline_list(), *req.dataset_list()]:
            for name in ("artifact_id", "bundle_artifact_id"):
                aid = getattr(ref, name, None)
                if aid and db.get_artifact(aid) is None:
                    raise HTTPException(404, "input artifact not found")

        def replay(existing):
            def canonical(value):
                return value.model_dump(exclude={"submission", "scheduler"})

            if canonical(JobRequest.model_validate_json(existing["request_json"])) != canonical(req):
                raise HTTPException(409, "idempotency key already used for a different request")
            return _job_view(db, existing["id"])

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
                return replay(existing)
        try:
            job_id, task_ids = db.create_job_with_tasks(req, owner=principal.principal_id)
        except sqlite3.IntegrityError:
            # Concurrent duplicate idempotency_key won the insert race — return it.
            if req.idempotency_key:
                existing = db.find_job_by_idempotency(req.idempotency_key)
                if existing is not None:
                    return replay(existing)
            raise
        broker.emit(
            level="info",
            type="job_submitted",
            message=f"job {job_id} queued with {len(task_ids)} task(s)",
            job_id=job_id,
            data={"num_tasks": len(task_ids)},
        )
        return _job_view(db, job_id)

    @app.get("/v1/jobs", dependencies=[Depends(requires(Right.READ))])
    def list_jobs(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        status: str | None = None,
        name: str | None = None,
        created_before: float | None = None,
    ) -> list[JobView]:
        db = db_of(request)
        rows = db.list_jobs(limit=limit, status=status, name=name, created_before=created_before)
        return [_job_view(db, row["id"]) for row in rows]

    @app.get("/v1/stats", dependencies=[Depends(requires(Right.READ))])
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

    @app.get("/v1/jobs/{job_id}", dependencies=[Depends(requires(Right.READ))])
    def get_job(job_id: str, request: Request) -> JobView:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_view(db, job_id)

    @app.get("/v1/jobs/{job_id}/tasks", dependencies=[Depends(requires(Right.READ))])
    def get_job_tasks(job_id: str, request: Request) -> list[TaskView]:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return [_task_view(row) for row in db.list_tasks_for_job(job_id)]

    @app.post("/v1/jobs/{job_id}/cancel", dependencies=[Depends(requires(Right.CANCEL))])
    def cancel_job(job_id: str, request: Request) -> JobView:
        db = db_of(request)
        broker = broker_of(request)
        with db.project_guard():
            if db.get_job(job_id) is None:
                raise HTTPException(404, "job not found")
            running = db.request_job_cancel(job_id)
            if running is not None:
                broker.emit(
                    level="warning", type="job_cancel_requested", job_id=job_id, data={"running_tasks": running}
                )
                _finalize_job(db, broker, job_id)
            return _job_view(db, job_id)

    @app.get("/v1/jobs/{job_id}/events", dependencies=[Depends(requires(Right.READ))])
    def get_events(
        job_id: str,
        request: Request,
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=500, ge=1, le=500),
    ) -> list[EventView]:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(404, "job not found")
        rows = db.list_events(job_id, after_id=after_id, limit=limit)
        return [_event_view(r) for r in rows]

    @app.get("/v1/jobs/{job_id}/artifacts", dependencies=[Depends(requires(Right.READ))])
    def get_job_artifacts(job_id: str, request: Request) -> list[dict[str, Any]]:
        db = db_of(request)
        if db.get_job(job_id) is None:
            raise HTTPException(404, "job not found")
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

    @app.post("/v1/artifacts", dependencies=[Depends(requires(Right.SUBMIT))])
    async def upload_input_artifact(request: Request, file: UploadFile, kind: str = "input") -> dict[str, Any]:
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
        artifact_id = db.add_artifact(sha256=sha, kind=kind, path=path, size_bytes=size, filename=file.filename)
        return {"artifact_id": artifact_id, "sha256": sha, "size_bytes": size}

    @app.get("/v1/artifacts/{artifact_id}", dependencies=[Depends(requires(Right.READ))])
    def download_artifact(artifact_id: str, request: Request) -> FileResponse:
        db = db_of(request)
        row = db.get_artifact(artifact_id)
        if row is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        filename = row["filename"] or f"{artifact_id}.bin"
        return FileResponse(
            store_of(request)._path_for(row["sha256"]),
            filename=filename,
            media_type="application/octet-stream",
            headers={"X-N4C-SHA256": row["sha256"], "X-N4C-Size": str(row["size_bytes"])},
        )

    async def event_stream(websocket: WebSocket, job_id: str | None = None) -> None:
        service = websocket.state.service
        db, broker = service.db, service.broker
        if job_id is not None and db.get_job(job_id) is None:
            await websocket.send_denial_response(JSONResponse({"detail": "job not found"}, status_code=404))
            return
        queue = await broker.subscribe(job_id) if job_id else await broker.subscribe_global()
        try:
            await websocket.accept()
            async with anyio.create_task_group() as stream_tasks:

                async def publish():
                    last_id = 0
                    try:
                        while True:
                            rows = (
                                db.list_events(job_id, after_id=last_id)
                                if job_id
                                else db.list_all_events(after_id=last_id)
                            )
                            if not rows:
                                break
                            for row in rows:
                                await websocket.send_json(_event_view(row).model_dump(mode="json"))
                                last_id = row["id"]
                        while True:
                            payload = await queue.get()
                            if payload["id"] > last_id:
                                await websocket.send_json(_jsonable(payload))
                                last_id = payload["id"]
                    except WebSocketDisconnect:
                        stream_tasks.cancel_scope.cancel()

                stream_tasks.start_soon(publish)
                while (await websocket.receive())["type"] != "websocket.disconnect":
                    pass
                stream_tasks.cancel_scope.cancel()
        finally:
            # Cleanup must finish even when the connection's enclosing cancel scope
            # is already cancelled; do not swallow unrelated cancellation/errors.
            with anyio.CancelScope(shield=True):
                if job_id:
                    await broker.unsubscribe(job_id, queue)
                else:
                    await broker.unsubscribe_global(queue)

    @app.websocket("/v1/jobs/{job_id}/events/stream")
    async def stream_events(websocket: WebSocket, job_id: str) -> None:
        await event_stream(websocket, job_id)

    @app.websocket("/v1/events/stream")
    async def stream_all_events(websocket: WebSocket) -> None:
        await event_stream(websocket)

    @app.get("/v1/project", dependencies=[Depends(requires(Right.READ))])
    def project_binding(request: Request) -> dict[str, Any]:
        p = request.state.principal
        return {
            "project_id": p.project_id,
            "principal_id": p.principal_id,
            "services": sorted(p.services),
            "rights": sorted(p.rights),
            "group_ids": [],
        }

    @app.post("/v1/consumers/register", dependencies=[Depends(requires(Right.SUBMIT))])
    def register_consumer(request: Request) -> dict[str, Any]:
        p = request.state.principal
        return {"kind": "consumer", "project_id": p.project_id, "principal_id": p.principal_id}

    # ------------------------------------------------------------------ #
    # Worker API
    # ------------------------------------------------------------------ #
    @app.exception_handler(PermissionError)
    async def invalid_report(request: Request, exc: PermissionError):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(IllegalTransition)
    async def illegal_report(request: Request, exc: IllegalTransition):
        return JSONResponse({"detail": "illegal lifecycle report"}, status_code=409)

    @app.post("/v1/workers/register", dependencies=[Depends(requires(Right.EXECUTE))])
    def register_worker(reg: WorkerRegister, request: Request) -> WorkerRegistered:
        db, broker = db_of(request), broker_of(request)
        p = request.state.principal
        secret = secrets.token_urlsafe(32)
        worker_id = db.register_worker(reg, principal=p.principal_id, secret=secret)
        broker.emit(
            level="info", type="worker_registered", message=f"worker {worker_id} registered", worker_id=worker_id
        )
        return WorkerRegistered(
            worker_id=worker_id,
            worker_secret=secret,
            project_id=p.project_id,
            principal_id=p.principal_id,
            heartbeat_interval_s=config.heartbeat_interval_s,
            lease_ttl_s=config.lease_ttl_s,
            rights=sorted(p.rights),
        )

    @app.post("/v1/workers/{worker_id}/heartbeat", dependencies=[Depends(requires(Right.EXECUTE))])
    def heartbeat(worker_id: str, heartbeat: WorkerHeartbeat, request: Request) -> HeartbeatAck:
        db = db_of(request)
        db.heartbeat_worker(worker_id, config.lease_ttl_s, [(a.task_id, a.attempt) for a in heartbeat.active_attempts])
        cancel_ids = []
        cancel_attempts = []
        for task_id in db.tasks_for_worker(worker_id):
            task = db.get_task(task_id)
            if task is None:
                continue
            job = db.get_job(task["job_id"])
            if job and job["status"] in ("cancelling", "cancelled"):
                cancel_ids.append(task_id)
                cancel_attempts.append(ActiveAttempt(task_id=task_id, attempt=task["attempt"]))
        return HeartbeatAck(cancel_task_ids=cancel_ids, cancel_attempts=cancel_attempts)

    @app.post("/v1/workers/{worker_id}/lease", dependencies=[Depends(requires(Right.EXECUTE))])
    def lease(worker_id: str, request: Request) -> LeaseResponse:
        db, broker = db_of(request), broker_of(request)
        p = request.state.principal
        payload = db.lease_next_task(worker_id, config.lease_ttl_s)
        if payload is not None:
            payload = payload.model_copy(
                update={
                    "assignment": TaskAssignmentMetadata(
                        executor_principal=p.principal_id, worker_id=worker_id, granted_rights=sorted(p.rights)
                    )
                }
            )
            broker.emit(
                level="info",
                type="task_leased",
                job_id=payload.job_id,
                task_id=payload.task_id,
                worker_id=worker_id,
                data={"attempt": payload.attempt},
            )
        return LeaseResponse(task=payload)

    def guard(db, task_id, worker_id, attempt, statuses):
        if db.get_task(task_id) is None:
            raise HTTPException(404, "task not found")
        return db.task_guard(task_id, worker_id, attempt, statuses)

    @app.post("/v1/tasks/{task_id}/start", dependencies=[Depends(requires(Right.EXECUTE))])
    def start_task(task_id: str, request: Request, worker_id: str, attempt: int) -> dict[str, Any]:
        db, broker = db_of(request), broker_of(request)
        with guard(db, task_id, worker_id, attempt, {"leased", "running"}) as row:
            if row["status"] == "running":
                return {"ok": True}
            db.start_task(task_id, worker_id, attempt)
            broker.emit(level="info", type="task_started", job_id=row["job_id"], task_id=task_id, worker_id=worker_id)
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/events", dependencies=[Depends(requires(Right.EXECUTE))])
    def task_event(task_id: str, ev: TaskEvent, request: Request, worker_id: str, attempt: int) -> dict[str, Any]:
        db, broker = db_of(request), broker_of(request)
        if ev.type not in {"log", "progress"}:
            raise HTTPException(400, "event type is reserved for the server")
        with guard(db, task_id, worker_id, attempt, {"running"}) as row:
            broker.emit(
                level=ev.level.value,
                type=ev.type,
                message=ev.message,
                job_id=row["job_id"],
                task_id=task_id,
                worker_id=worker_id,
                data={**ev.data, "progress": ev.progress, "attempt": attempt},
            )
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/artifacts", dependencies=[Depends(requires(Right.EXECUTE))])
    async def upload_artifact(
        task_id: str,
        request: Request,
        file: UploadFile,
        worker_id: str,
        attempt: int,
        role: str = "output",
        kind: str = "blob",
    ) -> dict[str, Any]:
        db, store = db_of(request), store_of(request)
        if role not in {"model", "logs", "workspace", "output"}:
            raise HTTPException(400, "invalid or reserved artifact role")
        with guard(db, task_id, worker_id, attempt, {"running"}):
            pass
        try:
            sha, path, size = await asyncio.to_thread(store.put_stream, file.file, config.max_artifact_mb * 1024 * 1024)
        except ArtifactTooLarge as exc:
            raise HTTPException(413, str(exc)) from exc
        with guard(db, task_id, worker_id, attempt, {"running"}) as row:
            art = db.get_task_artifact(task_id, attempt, role)
            if art:
                if art["sha256"] != sha:
                    raise HTTPException(409, "artifact role already bound to different content")
                artifact_id = art["id"]
            else:
                artifact_id = db.add_artifact(sha256=sha, kind=kind, path=path, size_bytes=size, filename=file.filename)
                db.link_job_artifact(row["job_id"], task_id, role, artifact_id, attempt)
        return {"artifact_id": artifact_id, "sha256": sha, "size_bytes": size}

    @app.post("/v1/tasks/{task_id}/complete", dependencies=[Depends(requires(Right.EXECUTE))])
    def complete_task(
        task_id: str, result: TaskResult, request: Request, worker_id: str, attempt: int
    ) -> dict[str, Any]:
        db, broker = db_of(request), broker_of(request)
        p = request.state.principal
        with guard(db, task_id, worker_id, attempt, {"running", "succeeded"}) as row:
            result = result.model_copy(
                update={
                    "provenance": ResultProvenance(
                        reported_by_principal=p.principal_id,
                        project_id=p.project_id,
                        worker_id=worker_id,
                        job_id=row["job_id"],
                        task_id=task_id,
                        attempt=attempt,
                        granted_rights=sorted(p.rights),
                    )
                }
            )
            if row["status"] == "succeeded":
                if json.loads(row["result_json"]) != result.model_dump():
                    raise HTTPException(409, "terminal report differs from accepted result")
                return {"ok": True}
            job = db.get_job(row["job_id"])
            if job is None:
                raise HTTPException(404, "job not found")
            if job["status"] in {"cancelling", "cancelled"}:
                db.cancel_running_task(task_id, worker_id)
                broker.emit(
                    level="warning",
                    type="task_cancelled",
                    job_id=row["job_id"],
                    task_id=task_id,
                    worker_id=worker_id,
                    data={"reported_status": "succeeded"},
                )
            else:
                db.complete_task(task_id, worker_id, result.model_dump(), attempt)
                expected = (json.loads(row["payload_json"]).get("pipeline") or {}).get("expected_fingerprint")
                if expected and result.pipeline_fingerprint and expected != result.pipeline_fingerprint:
                    broker.emit(
                        level="warning",
                        type="pipeline_fingerprint_mismatch",
                        job_id=row["job_id"],
                        task_id=task_id,
                        worker_id=worker_id,
                        data={"expected": expected, "actual": result.pipeline_fingerprint},
                    )
                broker.emit(
                    level="info",
                    type="task_completed",
                    job_id=row["job_id"],
                    task_id=task_id,
                    worker_id=worker_id,
                    data={"attempt": attempt, "metrics": _jsonable(result.metrics.model_dump())},
                )
            _finalize_job(db, broker, row["job_id"])
        return {"ok": True}

    @app.post("/v1/tasks/{task_id}/fail", dependencies=[Depends(requires(Right.EXECUTE))])
    def fail_task(task_id: str, failure: TaskFailure, request: Request, worker_id: str, attempt: int) -> dict[str, Any]:
        db, broker = db_of(request), broker_of(request)
        with guard(db, task_id, worker_id, attempt, {"leased", "running", "failed"}) as row:
            if row["status"] == "failed":
                if failure.error != row["error"]:
                    raise HTTPException(409, "terminal failure differs")
                return {"ok": True, "requeued": False}
            job = db.get_job(row["job_id"])
            if job is None:
                raise HTTPException(404, "job not found")
            if job["status"] in {"cancelling", "cancelled"}:
                db.cancel_running_task(task_id, worker_id)
                requeued = False
            else:
                updated = db.fail_task(task_id, worker_id, failure.error, retriable=failure.retriable, attempt=attempt)
                requeued = updated["status"] == "queued"
            broker.emit(
                level="error",
                type="task_failed",
                message=failure.error,
                job_id=row["job_id"],
                task_id=task_id,
                worker_id=worker_id,
                data={"attempt": attempt, "requeued": requeued},
            )
            _finalize_job(db, broker, row["job_id"])
        return {"ok": True, "requeued": requeued}

    @app.get("/v1/workers", dependencies=[Depends(requires(Right.READ))])
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


async def _reaper_loop(service: ProjectService, config: ServerConfig) -> None:
    db = service.db
    broker = service.broker
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
            lost = db.reap_tasks_for_workers(dead)
            for task_id, job_id in lost:
                broker.emit(
                    level="warning",
                    type="task_lost",
                    message=f"task {task_id} lost with dead worker",
                    job_id=job_id,
                    task_id=task_id,
                )
            affected = db.reap_expired_leases()
            jobs = set()
            for _task_id, job_id in lost:
                jobs.add(job_id)
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
    if best_task_id is None and ranking:
        best_task_id = ranking[0]["task_id"]
        agg.best_task_id = best_task_id
    best_model_artifact_id = None
    if best_task_id is not None:
        accepted = db.get_task(best_task_id)
        if accepted is None:
            raise KeyError(best_task_id)
        accepted_result = TaskResult.model_validate_json(accepted["result_json"])
        best_model_artifact_id = accepted_result.artifacts.get("model")
    all_terminal = all(t["status"] in _TERMINAL for t in tasks)
    if all_terminal:
        agg.completeness = (
            "incomplete"
            if agg.num_succeeded != len(tasks)
            else "complete"
            if req.outputs.export_best_model
            else "not_requested"
        )
    agg.best_model_artifact_id = best_model_artifact_id
    return agg, best_task_id, best_model_artifact_id


def _finalize_job(db: Database, broker: EventBroker, job_id: str) -> None:
    with db.project_guard():
        job_row = db.get_job(job_id)
        if job_row is None:
            return
        agg, best_task_id, best_model_artifact_id = _build_aggregate(db, job_id)
        db.set_job_result(job_id, _jsonable(agg.model_dump()))
        # Keep exactly one job-level best_model link (a later task can beat an earlier one).
        db.clear_job_artifact_role(job_id, "best_model")
        if best_model_artifact_id and best_task_id:
            best_task = db.get_task(best_task_id)
            if best_task is not None:
                db.link_job_artifact(job_id, best_task_id, "best_model", best_model_artifact_id, best_task["attempt"])

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
    req = JobRequest.model_validate_json(row["request_json"])
    submission = req.submission
    if submission is None and row["owner"]:
        submission = JobSubmissionMetadata(principal=row["owner"])
    return JobView(
        id=row["id"],
        type=row["type"],
        name=row["name"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        aggregate=agg,
        scheduler=req.scheduler or req.inferred_scheduler_contract(),
        submission=submission,
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
