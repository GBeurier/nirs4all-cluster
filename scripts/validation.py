#!/usr/bin/env python
"""End-to-end validation harness for the nirs4all-cluster prototype.

Runs the design's "Tests de validation" against a *real* server + worker
subprocesses on real nirs4all-data, and collects the "Mesures a collecter".
Unlike the pytest integration tests (in-process threads), this uses real OS
processes so it can SIGKILL a worker mid-task and prove crash recovery.

Run with the nirs4all venv:
    /home/delete/nirs4all/nirs4all/.venv/bin/python scripts/validation.py
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
DATA = Path("/home/delete/nirs4all/nirs4all-data/regression")
GRAPEVINE = DATA / "GRAPEVINE_LeafTraits" / "PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra"
PIPELINE = ROOT / "examples" / "pipelines" / "pls.yaml"

sys.path.insert(0, str(ROOT))
from nirs4all_cluster.client import ClusterClient  # noqa: E402

RESULTS: dict = {"tests": {}, "measurements": {}}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _n4(*args: str) -> list[str]:
    return [PY, "-m", "nirs4all_cluster.cli", *args]


@contextlib.contextmanager
def server(state: Path, port: int, lease_ttl: float = 6.0):
    proc = subprocess.Popen(
        _n4("server", "--host", "127.0.0.1", "--port", str(port), "--state", str(state),
            "--lease-ttl", str(lease_ttl), "--log-level", "warning"),
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    base = f"http://127.0.0.1:{port}"
    try:
        _await_health(base)
        yield base
    finally:
        proc.send_signal(signal.SIGINT)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)
        if proc.poll() is None:
            proc.kill()


def _await_health(base: str, timeout: float = 30.0) -> None:
    import httpx

    deadline = time.time() + timeout
    while time.time() < deadline:
        with contextlib.suppress(httpx.HTTPError):
            if httpx.get(f"{base}/", timeout=1).status_code == 200:
                return
        time.sleep(0.2)
    raise RuntimeError("server did not become healthy")


def start_worker(base: str, state: Path, name: str, heartbeat_env: float = 1.0) -> subprocess.Popen:
    return subprocess.Popen(
        _n4("worker", "--server", base, "--state", str(state), "--name", name, "--poll-interval", "0.5"),
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )


def _wait_task_running(client: ClusterClient, job_id: str, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        tasks = client.get_tasks(job_id)
        if any(t.status.value == "running" for t in tasks):
            return True
        if all(t.status.value in ("succeeded", "failed", "cancelled") for t in tasks):
            return False
        time.sleep(0.2)
    return False


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #


def scenario_atomic_and_parity(base: str, tmp: Path) -> None:
    import nirs4all

    client = ClusterClient(base)
    t0 = time.time()
    job = client.submit_run(pipeline=str(PIPELINE), dataset=str(GRAPEVINE),
                            params={"random_state": 42, "refit": True}, name="atomic")
    submit_t = time.time()
    job = client.wait(job.id, timeout=180, poll=0.5)
    total = time.time() - t0
    ok = job.status.value == "succeeded"
    RESULTS["tests"]["1_atomic_job_n4a"] = ok

    # .n4a downloadable + valid zip
    out = client.download_best_model(job.id, tmp / "best.n4a")
    n4a_ok = out is not None and zipfile.is_zipfile(out)
    RESULTS["tests"]["1b_n4a_valid_bundle"] = n4a_ok

    # parity vs local
    local = nirs4all.run(pipeline=str(PIPELINE), dataset=str(GRAPEVINE),
                         workspace_path=str(tmp / "local_ws"), random_state=42, refit=True,
                         save_charts=False, verbose=0)
    diff = abs(job.aggregate.best_metric - local.best_rmse)
    RESULTS["tests"]["6_metric_parity_vs_local"] = diff < 1e-6
    RESULTS["measurements"]["metric_diff_vs_local"] = diff
    RESULTS["measurements"]["cluster_best_rmse"] = job.aggregate.best_metric
    RESULTS["measurements"]["local_best_rmse"] = local.best_rmse

    task = client.get_tasks(job.id)[0]
    exec_s = task.result.duration_seconds if task.result else None
    RESULTS["measurements"]["exec_seconds"] = exec_s
    RESULTS["measurements"]["total_wall_seconds"] = round(total, 3)
    RESULTS["measurements"]["server_overhead_seconds"] = round(total - (exec_s or 0), 3)
    RESULTS["measurements"]["n4a_size_bytes"] = out.stat().st_size if out else None
    RESULTS["measurements"]["submit_latency_seconds"] = round(submit_t - t0, 3)


def scenario_two_workers_parallel(base: str, tmp: Path) -> None:
    client = ClusterClient(base)
    j1 = client.submit_run(pipeline=str(PIPELINE), dataset=str(GRAPEVINE), params={"random_state": 1}, name="p1")
    j2 = client.submit_run(pipeline=str(PIPELINE), dataset=str(GRAPEVINE), params={"random_state": 2}, name="p2")
    j1 = client.wait(j1.id, timeout=180, poll=0.5)
    j2 = client.wait(j2.id, timeout=180, poll=0.5)
    workers = {client.get_tasks(j1.id)[0].worker_id, client.get_tasks(j2.id)[0].worker_id}
    ok = j1.status.value == "succeeded" and j2.status.value == "succeeded"
    RESULTS["tests"]["2_two_jobs_parallel"] = ok
    RESULTS["measurements"]["distinct_workers_used"] = len(workers)


def scenario_kill_retry(base: str, tmp: Path) -> None:
    """Worker killed mid-task -> lease expires -> second worker retries -> success."""
    client = ClusterClient(base)
    # A slower pipeline so we can catch the task mid-run.
    slow = tmp / "slow.yaml"
    slow.write_text(
        "pipeline:\n"
        "  - class: sklearn.preprocessing.StandardScaler\n"
        "  - model:\n"
        "      class: sklearn.cross_decomposition.PLSRegression\n"
        "    name: PLS-FT\n"
        "    finetune_params:\n"
        "      n_trials: 80\n"
        "      approach: single\n"
        "      model_params:\n"
        "        n_components: {type: int, low: 1, high: 20}\n",
        encoding="utf-8",
    )
    victim = start_worker(base, tmp / "victim", "victim")
    job = client.submit_run(pipeline=str(slow), dataset=str(GRAPEVINE), params={"random_state": 42}, name="kill")
    running = _wait_task_running(client, job.id, timeout=60)
    killed = False
    if running:
        victim.send_signal(signal.SIGKILL)
        victim.wait()
        killed = True
    # rescuer worker picks up the requeued task after the lease lapses
    rescuer = start_worker(base, tmp / "rescuer", "rescuer")
    try:
        job = client.wait(job.id, timeout=240, poll=1.0)
    finally:
        rescuer.send_signal(signal.SIGINT)
        with contextlib.suppress(Exception):
            rescuer.wait(timeout=10)
    attempts = max((t.attempt for t in client.get_tasks(job.id)), default=0)
    RESULTS["tests"]["3_worker_kill_retry"] = killed and job.status.value == "succeeded" and attempts >= 2
    RESULTS["measurements"]["kill_retry_attempts"] = attempts
    RESULTS["measurements"]["kill_caught_running"] = killed


def scenario_cancel_not_relaunched(base: str, tmp: Path) -> None:
    client = ClusterClient(base)
    slow = tmp / "slow.yaml"  # reuse from kill scenario
    worker = start_worker(base, tmp / "cancel_worker", "canceller")
    job = client.submit_run(pipeline=str(slow), dataset=str(GRAPEVINE), params={"random_state": 7}, name="cancel")
    running = _wait_task_running(client, job.id, timeout=60)
    client.cancel(job.id)
    # observe for a while that it does not relaunch / succeed
    time.sleep(12)
    final = client.get_job(job.id)
    tasks = client.get_tasks(job.id)
    no_success = all(t.status.value != "succeeded" for t in tasks)
    terminal_cancel = final.status.value in ("cancelled", "cancelling")
    with contextlib.suppress(Exception):
        worker.send_signal(signal.SIGINT)
        worker.wait(timeout=10)
    RESULTS["tests"]["4_cancel_not_relaunched"] = running and terminal_cancel and no_success


def scenario_matrix(base: str, tmp: Path) -> None:
    client = ClusterClient(base)
    corn = DATA / "CORN"
    datasets = [
        {"kind": "shared_path", "name": "moisture", "path": str(corn / "Corn_Moisture_80_WangStyle_m5spec")},
        {"kind": "shared_path", "name": "oil", "path": str(corn / "Corn_Oil_80_WangStyle_m5spec")},
        {"kind": "shared_path", "name": "protein", "path": str(corn / "Corn_Protein_80_WangStyle_m5spec")},
    ]
    job = client.submit_run(pipeline=str(PIPELINE), datasets=datasets, params={"random_state": 42}, name="matrix")
    job = client.wait(job.id, timeout=240, poll=1.0)
    ok = (job.status.value == "succeeded" and job.aggregate.num_tasks == 3
          and job.aggregate.num_succeeded == 3 and len(job.aggregate.ranking) == 3)
    RESULTS["tests"]["5_matrix_aggregation"] = ok
    RESULTS["measurements"]["matrix_ranking"] = [
        {"dataset": r["dataset"], "best_rmse": r.get("best_rmse")} for r in job.aggregate.ranking
    ]


def main() -> int:
    if not GRAPEVINE.exists():
        print("nirs4all-data not present; cannot run validation", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="n4cluster_val_") as td:
        tmp = Path(td)
        port = _free_port()
        with server(tmp / "server", port, lease_ttl=6.0) as base:
            w1 = start_worker(base, tmp / "w1", "w1")
            w2 = start_worker(base, tmp / "w2", "w2")
            time.sleep(3)  # let workers register
            try:
                scenario_atomic_and_parity(base, tmp)
                scenario_two_workers_parallel(base, tmp)
                scenario_matrix(base, tmp)
            finally:
                for w in (w1, w2):
                    w.send_signal(signal.SIGINT)
                    with contextlib.suppress(Exception):
                        w.wait(timeout=10)
            # kill / cancel scenarios manage their own workers
            scenario_kill_retry(base, tmp)
            scenario_cancel_not_relaunched(base, tmp)

            # containment check: only state dirs were written under tmp
            RESULTS["tests"]["7_state_dir_containment"] = _check_containment(tmp)

    print(json.dumps(RESULTS, indent=2))
    passed = sum(1 for v in RESULTS["tests"].values() if v)
    total = len(RESULTS["tests"])
    print(f"\n=== validation: {passed}/{total} tests passed ===")
    return 0 if passed == total else 1


def _check_containment(tmp: Path) -> bool:
    # Every path under tmp must live inside a server/ or worker state dir.
    allowed_roots = {"server", "w1", "w2", "victim", "rescuer", "cancel_worker", "local_ws"}
    for p in tmp.iterdir():
        if p.is_dir() and p.name not in allowed_roots and p.name != "slow.yaml":
            if not p.name.endswith(".n4a") and not p.name.endswith(".yaml"):
                return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
