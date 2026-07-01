"""Parity harness for the cluster client/server path.

This stays dependency-free by putting a deterministic fake ``nirs4all`` module on
``PYTHONPATH`` for the subprocess runner. The server, clients and worker-side
materializer/executor remain nirs4all-free; only the runner imports the fake.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from nirs4all_cluster.client import ClusterClient
from nirs4all_cluster.client_worker import WorkerClient
from nirs4all_cluster.schemas import RunMetrics, TaskPayload, TaskResult
from nirs4all_cluster.worker.executor import ExecutionResult, execute_task
from nirs4all_cluster.worker.materialize import build_runner_spec


def test_cluster_run_matches_deterministic_local_executor(cluster, tmp_path, monkeypatch):
    """A simple cluster-submitted nirs4all job preserves the local result envelope."""
    pipeline = tmp_path / "pls.yaml"
    pipeline.write_text("steps:\n  - class: PLS\n    n_components: 2\n", encoding="utf-8")
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "manifest.json").write_text('{"name": "fake"}\n', encoding="utf-8")
    _install_fake_nirs4all(tmp_path / "fake-lib", monkeypatch)

    local_spec = {
        "pipeline": {"mode": "path", "path": str(pipeline)},
        "dataset": {"mode": "path", "path": str(dataset)},
        "params": {"random_state": 42, "refit": True, "inner_n_jobs": 1},
        "outputs": {"export_best_model": True, "keep_task_workspace": False},
    }
    local = execute_task(local_spec, tmp_path / "local-task", poll_interval=0.01, timeout=10)
    assert local.returncode == 0, local.result
    expected = _normalize_execution_result(local)

    with (
        ClusterClient(cluster.base_url, token=cluster.submitter, timeout=10) as submitter,
        WorkerClient(cluster.base_url, token=cluster.executor, timeout=10) as worker,
    ):
        job = submitter.submit_nirs4all_run(
            pipeline=str(pipeline),
            dataset={"kind": "shared_path", "path": str(dataset), "name": "fake-dataset"},
            params={"random_state": 42, "refit": True},
            n_jobs=1,
            name="parity-harness",
            outputs={"export_best_model": True, "keep_task_workspace": False},
        )
        registered = worker.register(
            slots_total=1,
            version={"packages": {"nirs4all": "fake-parity-1"}},
            name="parity-worker",
        )
        assert "execute" in registered.rights
        assert "submit" not in registered.rights

        task = worker.lease()
        assert task is not None
        worker.start_task(task.task_id)
        spec = build_runner_spec(task, tmp_path / "distributed-task", worker.download_artifact)
        distributed = execute_task(spec, tmp_path / "distributed-task", poll_interval=0.01, timeout=10)
        assert distributed.returncode == 0, distributed.result
        assert _received_run_args(distributed) == _received_run_args(local)
        _complete_from_execution(worker, task, distributed, spec["pipeline_fingerprint"])

        job = submitter.wait(job.id, poll=0.05, timeout=10)
        assert job.status.value == "succeeded", job.aggregate.errors
        tasks = submitter.get_tasks(job.id)

    assert len(tasks) == 1
    result = tasks[0].result
    assert result is not None
    assert _normalize_task_result(result) == expected
    assert result.pipeline_fingerprint == spec["pipeline_fingerprint"]
    assert job.aggregate.num_tasks == 1
    assert job.aggregate.num_succeeded == 1
    assert job.aggregate.best_metric == pytest.approx(expected["metrics"]["best_rmse"], abs=1e-12)
    assert job.aggregate.ranking == [
        {
            "task_id": task.task_id,
            "dataset": "fake-dataset",
            "pipeline": "pls",
            "best_rmse": expected["metrics"]["best_rmse"],
            "metrics": expected["metrics"],
        }
    ]
    assert job.aggregate.best_model_artifact_id == result.artifacts["model"]


def _install_fake_nirs4all(fake_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_dir.mkdir()
    (fake_dir / "nirs4all.py").write_text(
        '''
from __future__ import annotations

import json
from pathlib import Path

__version__ = "fake-parity-1"


class _RunResult:
    best_score = 0.8125
    best_rmse = 0.123456
    best_r2 = 0.987654
    best_mae = 0.111111
    best_accuracy = None
    num_predictions = 7
    best = {"model_name": "FakePLS", "task_type": "regression", "metric": "best_rmse"}

    def __init__(self, workspace_path: str) -> None:
        self._workspace_path = Path(workspace_path)

    def export(self, path: str) -> None:
        Path(path).write_text("FAKE-MODEL\\n", encoding="utf-8")

    def close(self) -> None:
        (self._workspace_path / "closed.txt").write_text("closed\\n", encoding="utf-8")


def run(*, pipeline: str, dataset: str, workspace_path: str, n_jobs: int = 1, **params: object) -> _RunResult:
    workspace = Path(workspace_path)
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "received.json").write_text(
        json.dumps(
            {
                "pipeline": pipeline,
                "dataset": dataset,
                "n_jobs": n_jobs,
                "params": params,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return _RunResult(workspace_path)
'''.lstrip(),
        encoding="utf-8",
    )
    repo_root = Path(__file__).resolve().parents[1]
    pythonpath = [str(fake_dir), str(repo_root)]
    if os.environ.get("PYTHONPATH"):
        pythonpath.append(os.environ["PYTHONPATH"])
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(pythonpath))


def _complete_from_execution(
    worker: WorkerClient,
    task: TaskPayload,
    execution: ExecutionResult,
    pipeline_fingerprint: str,
) -> None:
    summary = execution.result
    artifacts: dict[str, str | None] = {"model": None, "logs": None, "workspace": None}
    model_path = (summary.get("produced") or {}).get("model")
    if model_path is not None:
        artifacts["model"] = worker.upload_artifact(task.task_id, model_path, role="model", kind="model")
    artifacts["logs"] = worker.upload_artifact(task.task_id, execution.log_path, role="logs", kind="log")
    result = TaskResult(
        status="succeeded",
        nirs4all_version=summary.get("nirs4all_version"),
        pipeline_fingerprint=pipeline_fingerprint,
        duration_seconds=float(summary.get("duration_seconds", 0.0) or 0.0),
        metrics=RunMetrics(**(summary.get("metrics") or {})),
        counts=summary.get("counts", {}),
        artifacts=artifacts,
        extra=summary.get("extra", {}),
    )
    worker.complete_task(task.task_id, result)


def _normalize_execution_result(execution: ExecutionResult) -> dict[str, Any]:
    summary = execution.result
    duration = summary["duration_seconds"]
    assert isinstance(duration, (int, float)) and duration >= 0
    return {
        "status": summary["status"],
        "nirs4all_version": summary["nirs4all_version"],
        "duration_seconds": "nonnegative",
        "metrics": summary["metrics"],
        "counts": summary["counts"],
        "extra": summary["extra"],
        "artifacts": {
            "model": bool((summary.get("produced") or {}).get("model")),
            "logs": execution.log_path.exists(),
            "workspace": False,
        },
    }


def _received_run_args(execution: ExecutionResult) -> dict[str, Any]:
    return json.loads((execution.workspace_path / "received.json").read_text(encoding="utf-8"))


def _normalize_task_result(result: TaskResult) -> dict[str, Any]:
    assert result.duration_seconds >= 0
    return {
        "status": result.status,
        "nirs4all_version": result.nirs4all_version,
        "duration_seconds": "nonnegative",
        "metrics": result.metrics.model_dump(),
        "counts": result.counts,
        "extra": result.extra,
        "artifacts": {
            "model": bool(result.artifacts.get("model")),
            "logs": bool(result.artifacts.get("logs")),
            "workspace": bool(result.artifacts.get("workspace")),
        },
    }
