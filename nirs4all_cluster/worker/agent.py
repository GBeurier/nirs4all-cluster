"""Polling worker agent.

Registers with the server, heartbeats on a background thread, leases tasks up to
its slot count, materializes inputs, runs each task in a subprocess, uploads
results, and reports completion/failure. Cancellation is cooperative: the server
returns ``cancel_task_ids`` on heartbeat; the agent terminates the matching
subprocess and reports it.

The agent never imports nirs4all — only the runner subprocess does.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import threading
import zipfile
from pathlib import Path
from typing import Any

import httpx

from ..client_transport import make_http_client
from ..content_identity import receive_verified_download
from ..schemas import RunMetrics, TaskFailure, TaskPayload, TaskResult, WorkerRegister
from ..versioning import (
    API_VERSION,
    CLUSTER_VERSION,
    H_API,
    H_VERSION,
    ClusterVersionError,
    is_divergent,
)
from .executor import execute_task, runner_environment
from .materialize import build_runner_spec

logger = logging.getLogger("nirs4all_cluster.worker")


class WorkerAgent:
    def __init__(
        self,
        server: str,
        *,
        token: str | None = None,
        state_dir: str = "./worker-state",
        labels: dict[str, str] | None = None,
        capabilities: dict[str, Any] | None = None,
        slots: int = 1,
        allow_python: bool = False,
        name: str | None = None,
        poll_interval: float = 2.0,
        python_exe: str | None = None,
        gpu_count: int | None = None,
        allowed_roots: tuple[Path, ...] = (),
    ):
        self._warned_servers: set[str | None] = set()
        self._http = make_http_client(server, token=token, role="worker", timeout=120.0)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.labels = labels or {}
        self.capabilities = capabilities or {}
        self.slots = max(1, slots)
        self.allow_python = allow_python
        self.name = name
        self.poll_interval = poll_interval
        self.python_exe = python_exe
        self.allowed_roots = allowed_roots
        self._attempts: set[tuple[str, int]] = set()
        self._fatal_auth = False
        # gpu_count: None -> auto-detect (nvidia-smi); an int forces the declared
        # count (e.g. 0 to hide GPUs, or N when nvidia-smi is unavailable).
        self.gpu_count_override = gpu_count
        self._declare_gpu()

        self.worker_id: str | None = None
        self._heartbeat_interval = 10.0
        self._cancel_requested: set[tuple[str, int]] = set()
        self._cancel_lock = threading.Lock()
        self._stop = threading.Event()
        self._active = 0
        self._active_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def _post(self, path: str, **kwargs) -> httpx.Response:
        """Bounded transient retries; auth failure stops the daemon visibly."""
        for attempt in range(3):
            try:
                response = self._http.post(path, **kwargs)
                if response.status_code == 401:
                    self._fatal_auth = True
                    self._stop.set()
                    logger.error("worker credential rejected; restart with a valid credential")
                response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                transient = not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code >= 500
                if not transient or attempt == 2:
                    raise
                self._stop.wait(0.1 * (attempt + 1))
        raise RuntimeError("unreachable retry state")

    def _declare_gpu(self) -> None:
        """Detect (or force) GPUs and fold them into labels + capabilities.

        Adds capability fields ``gpu_count``/``gpu_names``/``cuda``/``cuda_version``
        and a ``cuda=true|false`` label (so the design's label-based GPU routing
        works without manual ``--labels``). User-provided values are not
        overwritten.
        """
        if self.gpu_count_override is not None:
            gpu = {
                "cuda": self.gpu_count_override > 0,
                "gpu_count": self.gpu_count_override,
                "gpu_names": [],
                "cuda_version": None,
                "driver_version": None,
            }
        else:
            gpu = _detect_gpu()
        self.capabilities.setdefault("cuda", gpu["cuda"])
        self.capabilities.setdefault("gpu_count", gpu["gpu_count"])
        if gpu["gpu_names"]:
            self.capabilities.setdefault("gpu_names", gpu["gpu_names"])
        if gpu["cuda_version"]:
            self.capabilities.setdefault("cuda_version", gpu["cuda_version"])
        self.labels.setdefault("cuda", "true" if gpu["cuda"] else "false")

    def _on_response(self, response: httpx.Response) -> None:
        """httpx hook: note server version drift; raise on protocol incompatibility."""
        server_version = response.headers.get(H_VERSION)
        if is_divergent(server_version) and server_version not in self._warned_servers:
            self._warned_servers.add(server_version)
            logger.warning(
                "server runs nirs4all-cluster %s; worker runs %s (compatible)", server_version, CLUSTER_VERSION
            )
        if response.status_code == 426:
            raise ClusterVersionError(
                f"server rejected worker as protocol-incompatible "
                f"(server api={response.headers.get(H_API)}, worker api={API_VERSION})"
            )

    # ------------------------------------------------------------------ #
    # Public lifecycle
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Register, then serve until interrupted."""
        self.register()
        self.serve()

    def serve(self) -> None:
        """Run the heartbeat + lease loops until stopped (assumes registered)."""
        if self.worker_id is None:
            raise RuntimeError("call register() before serve()")
        hb = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        try:
            self._lease_loop()
        except KeyboardInterrupt:
            pass
        finally:
            self._stop.set()
            for t in list(self._threads):
                t.join(timeout=30)
            self._http.close()
        if self._fatal_auth:
            raise RuntimeError("worker stopped: invalid credential")

    def stop(self) -> None:
        self._stop.set()

    def register(self) -> str:
        reg = WorkerRegister(
            labels=self.labels,
            capabilities=self.capabilities,
            slots_total=self.slots,
            version=_environment_version(self.python_exe),
            name=self.name,
        )
        resp = self._post("/v1/workers/register", json=reg.model_dump())
        resp.raise_for_status()
        data = resp.json()
        self.worker_id = data["worker_id"]
        self._http.headers["X-N4C-Worker-Secret"] = data["worker_secret"]
        self._heartbeat_interval = data.get("heartbeat_interval_s", 10.0)
        logger.info("registered as %s (slots=%s, labels=%s)", self.worker_id, self.slots, self.labels)
        granted = data.get("rights") or []
        if granted:
            logger.info("server granted rights: %s", ", ".join(granted))
        return self.worker_id

    # ------------------------------------------------------------------ #
    # Loops
    # ------------------------------------------------------------------ #
    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                with self._active_lock:
                    active = [{"task_id": t, "attempt": a} for t, a in self._attempts]
                resp = self._post(f"/v1/workers/{self.worker_id}/heartbeat", json={"active_attempts": active})
                if resp.status_code == 200:
                    ids = [(x["task_id"], x["attempt"]) for x in resp.json().get("cancel_attempts", [])]
                    with self._cancel_lock:
                        self._cancel_requested.update(ids)
            except (httpx.HTTPError, ClusterVersionError):
                pass
            self._stop.wait(self._heartbeat_interval)

    def _lease_loop(self) -> None:
        while not self._stop.is_set():
            if self._active_count() >= self.slots:
                self._stop.wait(self.poll_interval)
                continue
            task = self._lease()
            if task is None:
                self._stop.wait(self.poll_interval)
                continue
            self._inc_active()
            with self._active_lock:
                self._attempts.add((task.task_id, task.attempt))
            t = threading.Thread(target=self._handle_task, args=(task,), name=f"task-{task.task_id}")
            t.start()
            self._threads = [x for x in self._threads if x.is_alive()] + [t]

    def _lease(self) -> TaskPayload | None:
        try:
            resp = self._post(f"/v1/workers/{self.worker_id}/lease")
            resp.raise_for_status()
            payload = resp.json().get("task")
            return TaskPayload.model_validate(payload) if payload else None
        except (httpx.HTTPError, ClusterVersionError):
            return None

    # ------------------------------------------------------------------ #
    # Task handling
    # ------------------------------------------------------------------ #
    def _handle_task(self, task: TaskPayload) -> None:
        workdir = (self.state_dir / "tasks" / task.task_id / str(task.attempt)).resolve()
        try:
            start = self._post(
                f"/v1/tasks/{task.task_id}/start", params={"worker_id": self.worker_id, "attempt": task.attempt}
            )
            if start.status_code != 200:
                # Task was cancelled/reassigned before we started — abandon it
                # without doing (or reporting) any work.
                return
            try:
                spec = build_runner_spec(task, workdir, self._download_artifact, allowed_roots=self.allowed_roots)
            except (NotImplementedError, FileNotFoundError, ValueError) as exc:
                # Deterministic input errors can never succeed on retry.
                self._report_fail(task, f"materialize error: {type(exc).__name__}: {exc}", retriable=False)
                return
            exec_result = execute_task(
                spec,
                workdir,
                allow_python=self.allow_python,
                python_exe=self.python_exe,
                cancel_check=lambda: self._is_cancelled(task.task_id, task.attempt),
                on_tick=lambda elapsed: self._emit_progress(task, elapsed),
            )
            status = exec_result.result.get("status")
            if exec_result.cancelled or status == "cancelled":
                self._report_fail(task, "task cancelled", retriable=False)
            elif status == "succeeded":
                self._report_success(task, exec_result, spec.get("pipeline_fingerprint"))
            else:
                error = exec_result.result.get("error", "unknown runner failure")
                self._upload_log(task, exec_result.log_path)
                self._report_fail(task, error, retriable=exec_result.result.get("retriable", True))
        except Exception as exc:  # noqa: BLE001 - agent must keep running
            from ..transport_diagnostics import http_failure_diagnostic

            detail = http_failure_diagnostic(exc) if isinstance(exc, httpx.HTTPError) else f"{type(exc).__name__}: {exc}"
            self._report_fail(task, f"agent error: {detail}", retriable=True)
        finally:
            with self._cancel_lock:
                self._cancel_requested.discard((task.task_id, task.attempt))
            self._cleanup(task, workdir)
            with self._active_lock:
                self._attempts.discard((task.task_id, task.attempt))
            self._dec_active()

    def _report_success(self, task: TaskPayload, exec_result: Any, pipeline_fingerprint: str | None) -> None:
        summary = exec_result.result
        artifacts: dict[str, str | None] = {"model": None, "logs": None, "workspace": None}

        model_path = (summary.get("produced") or {}).get("model")
        if task.outputs.export_best_model and (not model_path or not Path(model_path).is_file()):
            raise ValueError("requested model export is incomplete")
        if model_path and Path(model_path).exists():
            artifacts["model"] = self._upload(
                task.task_id, Path(model_path), attempt=task.attempt, role="model", kind="model"
            )
        artifacts["logs"] = self._upload_log(task, exec_result.log_path)
        trace = (summary.get("extra") or {}).get("robustness_evidence_publication_trace")
        evidence_workspace = isinstance(trace, dict) and bool(
            (trace.get("publication_summary") or {}).get("workspace_required")
        )
        if (task.outputs.keep_task_workspace or evidence_workspace) and exec_result.workspace_path.exists():
            zipped = self._zip_dir(exec_result.workspace_path, exec_result.workspace_path.parent / "workspace.zip")
            artifacts["workspace"] = self._upload(
                task.task_id, zipped, attempt=task.attempt, role="workspace", kind="workspace"
            )
        if evidence_workspace and not artifacts["workspace"]:
            raise RuntimeError("Robustness evidence workspace upload failed; evidence is not delivered")

        extra = summary.get("extra", {})
        if isinstance(extra, dict):
            self._attach_robustness_artifact_refs(extra, artifacts)
        else:
            extra = {}

        result = TaskResult(
            status="succeeded",
            nirs4all_version=summary.get("nirs4all_version"),
            pipeline_fingerprint=pipeline_fingerprint,
            duration_seconds=float(summary.get("duration_seconds", 0.0) or 0.0),
            metrics=RunMetrics(**(summary.get("metrics") or {})),
            counts=summary.get("counts", {}),
            artifacts=artifacts,
            extra=extra,
            delivery=summary.get("delivery", {}),
        )
        self._post(
            f"/v1/tasks/{task.task_id}/complete",
            params={"worker_id": self.worker_id, "attempt": task.attempt},
            json=result.model_dump(),
        )
        logger.info("task %s accepted by server (%.1fs)", task.task_id, result.duration_seconds)

    def _report_fail(self, task: TaskPayload, error: str, *, retriable: bool) -> None:
        logger.warning(
            "task %s failed (retriable=%s): %s", task.task_id, retriable, error.splitlines()[0] if error else ""
        )
        failure = TaskFailure(error=error[:4000], retriable=retriable)
        try:
            self._post(
                f"/v1/tasks/{task.task_id}/fail",
                params={"worker_id": self.worker_id, "attempt": task.attempt},
                json=failure.model_dump(),
            )
        except httpx.HTTPError as exc:
            logger.error("failure report was not accepted (%s)", type(exc).__name__)

    @staticmethod
    def _attach_robustness_artifact_refs(extra: dict[str, Any], artifacts: dict[str, str | None]) -> None:
        trace = extra.get("robustness_evidence_publication_trace")
        if not isinstance(trace, dict):
            return
        published_artifacts = trace.setdefault("published_artifacts", {})
        if not isinstance(published_artifacts, dict):
            published_artifacts = {}
            trace["published_artifacts"] = published_artifacts
        if artifacts.get("model"):
            published_artifacts["predictor_bundle"] = artifacts["model"]
        if artifacts.get("workspace"):
            published_artifacts["workspace"] = artifacts["workspace"]

    def _emit_progress(self, task: TaskPayload, elapsed: float) -> None:
        try:
            self._post(
                f"/v1/tasks/{task.task_id}/events",
                params={"worker_id": self.worker_id, "attempt": task.attempt},
                json={"type": "progress", "message": f"running ({elapsed:.0f}s elapsed)", "level": "info"},
            )
        except httpx.HTTPError:
            pass

    # ------------------------------------------------------------------ #
    # Artifact transfer
    # ------------------------------------------------------------------ #
    def _download_artifact(self, artifact_id: str, dest: Path) -> Path:
        with self._http.stream("GET", f"/v1/artifacts/{artifact_id}") as resp:
            resp.raise_for_status()
            return receive_verified_download(resp, dest)

    def _upload(self, task_id: str, path: Path, *, attempt: int, role: str, kind: str) -> str | None:
        try:
            with open(path, "rb") as fh:
                resp = self._post(
                    f"/v1/tasks/{task_id}/artifacts",
                    params={
                        "role": role,
                        "kind": kind,
                        "worker_id": self.worker_id,
                        "attempt": attempt,
                    },
                    files={"file": (path.name, fh, "application/octet-stream")},
                )
            resp.raise_for_status()
            return resp.json()["artifact_id"]
        except httpx.HTTPError:
            raise

    def _upload_log(self, task: TaskPayload, log_path: Path) -> str | None:
        if log_path.exists():
            return self._upload(task.task_id, log_path, attempt=task.attempt, role="logs", kind="log")
        return None

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _is_cancelled(self, task_id: str, attempt: int) -> bool:
        with self._cancel_lock:
            return (task_id, attempt) in self._cancel_requested

    def _active_count(self) -> int:
        with self._active_lock:
            return self._active

    def _inc_active(self) -> None:
        with self._active_lock:
            self._active += 1

    def _dec_active(self) -> None:
        with self._active_lock:
            self._active = max(0, self._active - 1)

    def _cleanup(self, task: TaskPayload, workdir: Path) -> None:
        if not task.outputs.keep_task_workspace and workdir.exists():
            shutil.rmtree(workdir, ignore_errors=True)
            if workdir.parent.parent == (self.state_dir / "tasks").resolve():
                try:
                    workdir.parent.rmdir()
                except OSError:
                    pass

    @staticmethod
    def _zip_dir(src: Path, dest: Path) -> Path:
        with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in src.rglob("*"):
                if path.is_file():
                    zf.write(path, path.relative_to(src))
        return dest


def _detect_gpu() -> dict[str, Any]:
    """Best-effort NVIDIA GPU detection via ``nvidia-smi`` (no torch/tf import).

    Returns a stable shape even when no GPU / no driver is present. Kept
    framework-agnostic and light so the agent stays nirs4all-free and fast.
    """
    info: dict[str, Any] = {
        "cuda": False,
        "gpu_count": 0,
        "gpu_names": [],
        "cuda_version": None,
        "driver_version": None,
    }
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return info
    if out.returncode != 0:
        return info
    names: list[str] = []
    driver: str | None = None
    for line in out.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        names.append(parts[0])
        if len(parts) >= 2:
            driver = parts[1]
    info["gpu_count"] = len(names)
    info["gpu_names"] = names
    info["driver_version"] = driver
    info["cuda"] = len(names) > 0
    if names:
        try:
            header = subprocess.run(["nvidia-smi"], capture_output=True, text=True, timeout=10)
            match = re.search(r"CUDA Version:\s*([0-9.]+)", header.stdout)
            if match:
                info["cuda_version"] = match.group(1)
        except (FileNotFoundError, OSError, subprocess.SubprocessError):
            pass
    return info


def _environment_version(python_exe: str | None = None) -> dict[str, Any]:
    import json
    import sys
    import tempfile

    # Probe the actual provisioned child, including dependency and Python routing.
    # Never substitute the transport agent's environment for a distinct runner.
    runtime: dict[str, Any] = {}
    failure = None
    try:
        with tempfile.TemporaryDirectory(prefix="n4cluster-runtime-probe-") as neutral:
            probe = subprocess.run(
                [python_exe or sys.executable, "-m", "nirs4all_cluster.runners.nirs4all_run", "--environment-version"],
                env=runner_environment(),
                cwd=neutral,
                capture_output=True,
                text=True,
                timeout=30,
            )
        if probe.returncode == 0:
            runtime = json.loads(probe.stdout.strip().splitlines()[-1])
        else:
            # Keep exception class/message from the final traceback line, with any
            # token-bearing text redacted by the Cluster diagnostic helper.
            from ..credentials import safe_diagnostic

            failure = safe_diagnostic(
                probe.stderr.strip().splitlines()[-1] if probe.stderr.strip() else "child exited without diagnostics"
            )
    except (OSError, subprocess.SubprocessError, ValueError, IndexError) as exc:
        failure = type(exc).__name__
    if failure:
        logger.error("runner environment unavailable; provider cannot execute science jobs: %s", failure)
    return {
        "python": runtime.get("python"),
        "platform": runtime.get("platform"),
        "executable": runtime.get("executable", python_exe or sys.executable),
        "nirs4all_cluster": CLUSTER_VERSION,
        "api_version": API_VERSION,
        "packages": runtime.get("packages", {}),
    }
