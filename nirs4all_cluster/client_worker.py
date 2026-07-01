"""``WorkerClient`` — the typed executor-side control-plane adapter.

This is the *server-registration* half of the client layer. A worker host uses it
to **register with the server** (learning its ``worker_id`` and the rights the
credential was granted) and to drive a task's lifecycle: lease → start →
events/artifacts → complete/fail.

Like :class:`~nirs4all_cluster.client.ClusterClient`, it returns the wire models
from :mod:`nirs4all_cluster.schemas` and raises the rights-respecting errors from
:mod:`nirs4all_cluster.client_errors` — so a credential lacking the ``execute``
right fails with :class:`ClusterPermissionError` instead of an opaque HTTP error.

It is deliberately transport-only: **no polling loop, no subprocess, no nirs4all**.
The polling daemon (:mod:`nirs4all_cluster.worker.agent`) owns those concerns; this
adapter is the reusable seam that daemon — or a future core / Studio-managed worker —
can build on.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from .client_errors import ClusterConnectionError, raise_for_response
from .client_transport import make_http_client, request
from .schemas import (
    HeartbeatAck,
    LeaseResponse,
    TaskEvent,
    TaskFailure,
    TaskPayload,
    TaskResult,
    WorkerRegister,
    WorkerRegistered,
)


class WorkerClient:
    """Typed HTTP client for the server's worker API (requires the ``execute`` right)."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 120.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.worker_id: str | None = None
        self._http = make_http_client(
            self.base_url, token=token, role="worker", timeout=timeout, transport=transport
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> WorkerClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _require_worker_id(self) -> str:
        if self.worker_id is None:
            raise RuntimeError("call register() before using the worker lifecycle methods")
        return self.worker_id

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #
    def register(
        self,
        registration: WorkerRegister | None = None,
        *,
        labels: dict[str, str] | None = None,
        capabilities: dict[str, Any] | None = None,
        slots_total: int = 1,
        version: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> WorkerRegistered:
        """Register with the server and record the assigned ``worker_id``.

        Pass a prebuilt :class:`WorkerRegister`, or the convenience fields. The
        returned :class:`WorkerRegistered` echoes the rights the credential was
        granted (``rights``) for executor self-diagnosis. Raises
        :class:`ClusterPermissionError` when the credential lacks ``execute``.
        """
        reg = registration or WorkerRegister(
            labels=labels or {},
            capabilities=capabilities or {},
            slots_total=slots_total,
            version=version or {},
            name=name,
        )
        resp = request(self._http, "POST", "/v1/workers/register", json=reg.model_dump())
        registered = WorkerRegistered.model_validate(resp.json())
        self.worker_id = registered.worker_id
        return registered

    # ------------------------------------------------------------------ #
    # Lease loop primitives
    # ------------------------------------------------------------------ #
    def heartbeat(self) -> HeartbeatAck:
        """Keep-alive; returns the tasks the server wants stopped (cooperative cancel)."""
        worker_id = self._require_worker_id()
        resp = request(self._http, "POST", f"/v1/workers/{worker_id}/heartbeat")
        return HeartbeatAck.model_validate(resp.json())

    def lease(self) -> TaskPayload | None:
        """Atomically claim the next eligible task, or ``None`` when the queue is empty."""
        worker_id = self._require_worker_id()
        resp = request(self._http, "POST", f"/v1/workers/{worker_id}/lease")
        return LeaseResponse.model_validate(resp.json()).task

    # ------------------------------------------------------------------ #
    # Task lifecycle reports
    # ------------------------------------------------------------------ #
    def start_task(self, task_id: str) -> None:
        """Mark a leased task running. Raises :class:`ClusterConflictError` (409) if the
        task was cancelled / reassigned between lease and start."""
        worker_id = self._require_worker_id()
        request(self._http, "POST", f"/v1/tasks/{task_id}/start", params={"worker_id": worker_id})

    def report_event(self, task_id: str, event: TaskEvent) -> None:
        """Report progress / a log line for a running task."""
        request(self._http, "POST", f"/v1/tasks/{task_id}/events", json=event.model_dump())

    def complete_task(self, task_id: str, result: TaskResult) -> None:
        """Report a task succeeded with its summary metrics."""
        worker_id = self._require_worker_id()
        request(
            self._http,
            "POST",
            f"/v1/tasks/{task_id}/complete",
            params={"worker_id": worker_id},
            json=result.model_dump(),
        )

    def fail_task(self, task_id: str, failure: TaskFailure) -> None:
        """Report a task failed (the server requeues it if attempts remain and it is retriable)."""
        worker_id = self._require_worker_id()
        request(
            self._http,
            "POST",
            f"/v1/tasks/{task_id}/fail",
            params={"worker_id": worker_id},
            json=failure.model_dump(),
        )

    # ------------------------------------------------------------------ #
    # Artifact transfer
    # ------------------------------------------------------------------ #
    def upload_artifact(self, task_id: str, path: str | Path, *, role: str = "output", kind: str = "blob") -> str:
        """Upload a task output (model / logs / workspace); returns the artifact_id."""
        path = Path(path)
        with open(path, "rb") as fh:
            resp = request(
                self._http,
                "POST",
                f"/v1/tasks/{task_id}/artifacts",
                params={"role": role, "kind": kind},
                files={"file": (path.name, fh, "application/octet-stream")},
            )
        return resp.json()["artifact_id"]

    def download_artifact(self, artifact_id: str, dest: str | Path) -> Path:
        """Download an input artifact (pipeline file / dataset zip) to ``dest``."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"/v1/artifacts/{artifact_id}"
        try:
            with self._http.stream("GET", url) as resp:
                if resp.status_code >= 400:
                    raise_for_response(resp)
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
        except httpx.TransportError as exc:
            raise ClusterConnectionError(str(exc), method="GET", url=url) from exc
        return dest
