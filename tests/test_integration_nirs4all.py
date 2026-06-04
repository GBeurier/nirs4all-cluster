"""End-to-end integration tests that actually run nirs4all on a worker.

Skipped automatically when nirs4all (or the sample data) is unavailable, so the
rest of the suite stays dependency-free. Covers the design's validation tests:
an atomic job producing a ``.n4a``, two workers running a matrix in parallel,
metric parity vs a local ``nirs4all.run()``, and state-dir containment.
"""

import socket
import threading
import time
from pathlib import Path

import httpx
import pytest

pytest.importorskip("nirs4all")

from nirs4all_cluster.client import ClusterClient  # noqa: E402
from nirs4all_cluster.server.app import ServerConfig, create_app  # noqa: E402
from nirs4all_cluster.worker.agent import WorkerAgent  # noqa: E402

DATA = Path("/home/delete/nirs4all/nirs4all-data/regression")
PIPELINE = Path(__file__).resolve().parents[1] / "examples" / "pipelines" / "pls.yaml"
GRAPEVINE = DATA / "GRAPEVINE_LeafTraits" / "PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra"

pytestmark = pytest.mark.skipif(not GRAPEVINE.exists(), reason="nirs4all-data not present")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LiveServer:
    def __init__(self, state_dir: str):
        import uvicorn

        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = ServerConfig(state_dir=state_dir, lease_ttl_s=30.0, reaper_interval_s=1.0)
        self._uvicorn = uvicorn.Server(
            uvicorn.Config(create_app(config), host="127.0.0.1", port=self.port, log_level="warning")
        )
        self._thread = threading.Thread(target=self._uvicorn.run, daemon=True)

    def __enter__(self) -> "_LiveServer":
        self._thread.start()
        for _ in range(100):
            try:
                if httpx.get(f"{self.base_url}/", timeout=1.0).status_code == 200:
                    return self
            except httpx.HTTPError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def __exit__(self, *exc) -> None:
        self._uvicorn.should_exit = True
        self._thread.join(timeout=10)


def _start_worker(base_url, state_dir, **kw) -> WorkerAgent:
    agent = WorkerAgent(base_url, state_dir=state_dir, poll_interval=0.5, **kw)
    agent.register()
    threading.Thread(target=agent.serve, daemon=True).start()
    return agent


def test_atomic_job_and_metric_parity(tmp_path):
    import nirs4all

    with _LiveServer(str(tmp_path / "server")) as server:
        worker = _start_worker(server.base_url, str(tmp_path / "worker"))
        try:
            client = ClusterClient(server.base_url)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                dataset=str(GRAPEVINE),
                params={"random_state": 42, "refit": True},
                name="parity",
            )
            job = client.wait(job.id, timeout=180, poll=1.0)
            assert job.status.value == "succeeded", job.aggregate.errors
            cluster_rmse = job.aggregate.best_metric
            assert cluster_rmse is not None and cluster_rmse == pytest.approx(cluster_rmse)  # finite

            # The exported best model is downloadable and is a real zip bundle.
            out = client.download_best_model(job.id, tmp_path / "best.n4a")
            assert out is not None and out.stat().st_size > 1000
            import zipfile

            assert zipfile.is_zipfile(out)
        finally:
            worker.stop()

    # Parity: a local run with identical inputs must match the cluster metric.
    local = nirs4all.run(
        pipeline=str(PIPELINE),
        dataset=str(GRAPEVINE),
        workspace_path=str(tmp_path / "local_ws"),
        random_state=42,
        refit=True,
        save_charts=False,
        verbose=0,
    )
    assert cluster_rmse == pytest.approx(local.best_rmse, abs=1e-6)


def test_two_workers_parallel_matrix(tmp_path):
    corn = DATA / "CORN"
    datasets = [
        str(corn / "Corn_Moisture_80_WangStyle_m5spec"),
        str(corn / "Corn_Oil_80_WangStyle_m5spec"),
    ]
    if not all(Path(d).exists() for d in datasets):
        pytest.skip("CORN datasets not present")

    with _LiveServer(str(tmp_path / "server")) as server:
        w1 = _start_worker(server.base_url, str(tmp_path / "w1"), name="w1")
        w2 = _start_worker(server.base_url, str(tmp_path / "w2"), name="w2")
        try:
            client = ClusterClient(server.base_url)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                datasets=datasets,
                params={"random_state": 42},
                name="matrix",
            )
            job = client.wait(job.id, timeout=240, poll=1.0)
            assert job.status.value == "succeeded", job.aggregate.errors
            assert job.aggregate.num_tasks == 2
            assert job.aggregate.num_succeeded == 2
            assert len(job.aggregate.ranking) == 2
            # Both workers should have participated.
            tasks = client.get_tasks(job.id)
            assert len({t.worker_id for t in tasks}) == 2
        finally:
            w1.stop()
            w2.stop()


def test_state_dir_containment(tmp_path):
    """The worker cleans its per-task workdir when keep_task_workspace is false."""
    with _LiveServer(str(tmp_path / "server")) as server:
        worker_state = tmp_path / "worker"
        worker = _start_worker(server.base_url, str(worker_state))
        try:
            client = ClusterClient(server.base_url)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                dataset=str(GRAPEVINE),
                params={"random_state": 42},
                outputs={"keep_task_workspace": False},
            )
            job = client.wait(job.id, timeout=180, poll=1.0)
            assert job.status.value == "succeeded"
        finally:
            worker.stop()
        tasks_dir = worker_state / "tasks"
        # No task workdirs should remain after cleanup.
        assert not tasks_dir.exists() or not any(tasks_dir.iterdir())
