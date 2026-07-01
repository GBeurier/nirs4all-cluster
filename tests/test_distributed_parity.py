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
from nirs4all_cluster.versioning import fingerprint_obj
from nirs4all_cluster.worker.executor import ExecutionResult, execute_task
from nirs4all_cluster.worker.materialize import build_runner_spec


def test_cluster_run_matches_deterministic_local_executor(cluster, tmp_path, monkeypatch):
    """A constrained cluster job is scheduled to an eligible worker and preserves the result envelope."""
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
            requirements={"labels": {"site": "release-lab"}, "min_gpu_count": 1},
            n_jobs=1,
            name="parity-harness",
            outputs={"export_best_model": True, "keep_task_workspace": False},
        )

        missing_nirs4all = worker.register(
            labels={"site": "release-lab"},
            capabilities={"gpu_count": 1},
            slots_total=1,
            version={"packages": {}},
            name="missing-nirs4all-worker",
        )
        assert "execute" in missing_nirs4all.rights
        assert worker.lease() is None

        wrong_site = worker.register(
            labels={"site": "other-lab"},
            capabilities={"gpu_count": 1},
            slots_total=1,
            version={"packages": {"nirs4all": "fake-parity-1"}},
            name="wrong-site-worker",
        )
        assert "execute" in wrong_site.rights
        assert worker.lease() is None

        cpu_only = worker.register(
            labels={"site": "release-lab"},
            capabilities={"gpu_count": 0},
            slots_total=1,
            version={"packages": {"nirs4all": "fake-parity-1"}},
            name="cpu-only-worker",
        )
        assert "execute" in cpu_only.rights
        assert worker.lease() is None

        assert submitter.get_job(job.id).status.value == "queued"
        assert submitter.stats().tasks_in_flight == 0

        registered = worker.register(
            labels={"site": "release-lab"},
            capabilities={"gpu_count": 1},
            slots_total=1,
            version={"packages": {"nirs4all": "fake-parity-1"}},
            name="parity-worker",
        )
        assert "execute" in registered.rights
        assert "submit" not in registered.rights

        task = worker.lease()
        assert task is not None
        assert task.scheduler is not None
        assert task.scheduler.shape == "atomic"
        assert task.submission is not None
        assert task.submission.principal == "alice"
        assert task.assignment is not None
        assert task.assignment.assigned_by == "server"
        assert task.assignment.executor_principal == "worker1"
        assert task.assignment.worker_id == registered.worker_id
        assert task.assignment.required_rights == ["execute"]
        assert set(task.assignment.granted_rights) == {"read", "execute"}
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
    assert result.provenance.reported_by_principal == "worker1"
    assert result.provenance.worker_id == registered.worker_id
    assert result.provenance.job_id == job.id
    assert result.provenance.task_id == task.task_id
    assert result.provenance.attempt == 1
    assert set(result.provenance.granted_rights) == {"read", "execute"}
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


def test_inline_dag_matrix_preserves_local_result_semantics(cluster, tmp_path, monkeypatch):
    """A DAG-shaped inline pipeline keeps local semantics through server scheduling."""
    _install_fake_nirs4all(tmp_path / "fake-lib", monkeypatch)
    dag_pipeline = {
        "pipeline": [
            {
                "id": "standardize",
                "class": "sklearn.preprocessing.StandardScaler",
                "params": {"with_mean": True},
            },
            {
                "id": "branch_snv",
                "class": "nirs4all.preprocessing.SNV",
                "after": ["standardize"],
                "params": {"scale": 1.2},
            },
            {
                "id": "branch_derivative",
                "class": "nirs4all.preprocessing.SavitzkyGolay",
                "after": ["standardize"],
                "params": {"window": 7},
            },
            {
                "id": "pls_cv",
                "class": "sklearn.cross_decomposition.PLSRegression",
                "after": ["branch_snv", "branch_derivative"],
                "params": {"n_components": 4},
            },
        ],
        "dagml": {
            "nodes": [
                {"id": "source", "op": "DATASET", "deps": []},
                {"id": "standardize", "op": "PREPROCESS", "deps": ["source"]},
                {"id": "branch_snv", "op": "PREPROCESS", "deps": ["standardize"], "params": {"scale": 1.2}},
                {"id": "branch_derivative", "op": "PREPROCESS", "deps": ["standardize"], "params": {"window": 7}},
                {
                    "id": "pls_cv",
                    "op": "FIT_CV",
                    "deps": ["branch_snv", "branch_derivative"],
                    "params": {"n_components": 4},
                },
                {"id": "select", "op": "SELECT", "deps": ["pls_cv"]},
                {"id": "refit", "op": "REFIT", "deps": ["select"], "params": {"enabled": True}},
            ],
            "rank_metric": "best_rmse",
        },
    }
    pipeline_file = tmp_path / "dag-pipeline.json"
    pipeline_file.write_text(json.dumps(dag_pipeline, sort_keys=True), encoding="utf-8")
    datasets = [
        _dataset(tmp_path, "leaf-low-noise", samples=32, baseline_rmse=0.18, target_shift=1.5),
        _dataset(tmp_path, "leaf-high-noise", samples=32, baseline_rmse=0.31, target_shift=3.0),
    ]

    expected: dict[str, dict[str, Any]] = {}
    expected_received: dict[str, dict[str, Any]] = {}
    for dataset in datasets:
        local = execute_task(
            {
                "pipeline": {"mode": "path", "path": str(pipeline_file)},
                "dataset": {"mode": "path", "path": str(dataset)},
                "params": {"random_state": 11, "refit": True, "dag_scale": 2, "inner_n_jobs": 2},
                "outputs": {"export_best_model": True, "keep_task_workspace": False},
            },
            tmp_path / f"local-{dataset.name}",
            poll_interval=0.01,
            timeout=10,
        )
        assert local.returncode == 0, local.result
        expected[dataset.name] = _normalize_execution_result(local)
        expected_received[dataset.name] = _received_run_args(local)

    by_dataset: dict[str, dict[str, Any]] = {}
    with (
        ClusterClient(cluster.base_url, token=cluster.submitter, timeout=10) as submitter,
        WorkerClient(cluster.base_url, token=cluster.executor, timeout=10) as worker,
    ):
        job = submitter.submit_nirs4all_run(
            pipeline={"kind": "inline_json", "inline": dag_pipeline},
            datasets=[{"kind": "shared_path", "path": str(dataset), "name": dataset.name} for dataset in datasets],
            params={"random_state": 11, "refit": True, "dag_scale": 2},
            n_jobs=2,
            name="real-dag-parity",
            outputs={"export_best_model": True, "keep_task_workspace": False},
        )
        assert job.aggregate.num_tasks == 2
        worker.register(
            slots_total=1,
            version={"packages": {"nirs4all": "fake-parity-1"}},
            name="dag-parity-worker",
        )

        for index in range(2):
            task = worker.lease()
            assert task is not None
            assert task.pipeline.kind == "inline_json"
            assert task.pipeline.expected_fingerprint == fingerprint_obj(dag_pipeline)
            worker.start_task(task.task_id)
            spec = build_runner_spec(task, tmp_path / f"distributed-{index}", worker.download_artifact)
            assert spec["pipeline_fingerprint"] == fingerprint_obj(dag_pipeline)
            distributed = execute_task(spec, tmp_path / f"distributed-{index}", poll_interval=0.01, timeout=10)
            assert distributed.returncode == 0, distributed.result

            label = task.dataset.label()
            received = _received_run_args(distributed)
            assert received["pipeline_digest"] == expected_received[label]["pipeline_digest"]
            assert received["dag_order"] == expected_received[label]["dag_order"]
            assert received["dataset"] == expected_received[label]["dataset"]
            assert received["n_jobs"] == expected_received[label]["n_jobs"]
            assert received["params"] == expected_received[label]["params"]

            _complete_from_execution(worker, task, distributed, spec["pipeline_fingerprint"])
            by_dataset[label] = {
                "task_id": task.task_id,
                "result": _normalize_execution_result(distributed),
            }

        assert worker.lease() is None
        job = submitter.wait(job.id, poll=0.05, timeout=10)
        assert job.status.value == "succeeded", job.aggregate.errors
        task_views = submitter.get_tasks(job.id)

    assert set(by_dataset) == {dataset.name for dataset in datasets}
    for label, execution in by_dataset.items():
        assert execution["result"] == expected[label]
        assert execution["result"]["extra"]["dag_trace"]["terminal"] == "refit"

    tasks_by_label = {view.dataset_label: view for view in task_views}
    assert set(tasks_by_label) == set(by_dataset)
    for label, view in tasks_by_label.items():
        assert view.result is not None
        assert _normalize_task_result(view.result) == expected[label]
        assert view.result.pipeline_fingerprint == fingerprint_obj(dag_pipeline)

    expected_ranking = sorted(
        [
            {
                "task_id": by_dataset[label]["task_id"],
                "dataset": label,
                "pipeline": "inline_json",
                "best_rmse": expected[label]["metrics"]["best_rmse"],
                "metrics": expected[label]["metrics"],
            }
            for label in by_dataset
        ],
        key=lambda row: row["best_rmse"],
    )
    assert job.aggregate.num_tasks == 2
    assert job.aggregate.num_succeeded == 2
    assert job.aggregate.ranking == expected_ranking
    assert job.aggregate.best_metric == pytest.approx(expected_ranking[0]["best_rmse"], abs=1e-12)
    assert job.aggregate.best_task_id == expected_ranking[0]["task_id"]
    best_result = tasks_by_label[expected_ranking[0]["dataset"]].result
    assert best_result is not None
    assert job.aggregate.best_model_artifact_id == best_result.artifacts["model"]


def _dataset(tmp_path: Path, name: str, *, samples: int, baseline_rmse: float, target_shift: float) -> Path:
    dataset = tmp_path / name
    dataset.mkdir()
    (dataset / "manifest.json").write_text(
        json.dumps(
            {
                "name": name,
                "samples": samples,
                "baseline_rmse": baseline_rmse,
                "target_shift": target_shift,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return dataset


def _install_fake_nirs4all(fake_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_dir.mkdir()
    (fake_dir / "nirs4all.py").write_text(
        """
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

__version__ = "fake-parity-1"


class _RunResult:
    def __init__(self, workspace_path: str, metrics: dict[str, float | None], counts: dict[str, int], extra: dict) -> None:
        self._workspace_path = Path(workspace_path)
        self.best_score = metrics["best_score"]
        self.best_rmse = metrics["best_rmse"]
        self.best_r2 = metrics["best_r2"]
        self.best_mae = metrics["best_mae"]
        self.best_accuracy = metrics["best_accuracy"]
        self.num_predictions = counts["num_predictions"]
        self.extra = extra
        self.best = {
            "model_name": f"FakeDAGPLS:{extra['dag_trace']['terminal']}",
            "task_type": "regression",
            "metric": "best_rmse",
        }

    def export(self, path: str) -> None:
        Path(path).write_text(
            json.dumps({"metrics": self.extra["metrics"], "dag": self.extra["dag_trace"]}, sort_keys=True),
            encoding="utf-8",
        )

    def close(self) -> None:
        (self._workspace_path / "closed.txt").write_text("closed\\n", encoding="utf-8")


def run(*, pipeline: str, dataset: str, workspace_path: str, n_jobs: int = 1, **params: object) -> _RunResult:
    workspace = Path(workspace_path)
    workspace.mkdir(parents=True, exist_ok=True)
    pipeline_doc = _load_pipeline_doc(Path(pipeline))
    dataset_info = _load_dataset_info(Path(dataset))
    dag_trace = _evaluate_dag(pipeline_doc, dataset_info, params, n_jobs)
    metrics = _metrics(dataset_info, dag_trace, params, n_jobs)
    counts = {"num_predictions": int(dataset_info.get("samples", 7))}
    extra = {
        "dataset_name": dataset_info["name"],
        "pipeline_digest": _digest(pipeline_doc),
        "dag_trace": dag_trace,
        "metrics": metrics,
    }
    (workspace / "received.json").write_text(
        json.dumps(
            {
                "pipeline": pipeline,
                "dataset": dataset,
                "n_jobs": n_jobs,
                "params": params,
                "pipeline_digest": extra["pipeline_digest"],
                "dag_order": dag_trace["order"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (workspace / "dag_result.json").write_text(json.dumps(extra, sort_keys=True), encoding="utf-8")
    return _RunResult(workspace_path, metrics, counts, extra)


def _load_pipeline_doc(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _load_dataset_info(path: Path) -> dict:
    manifest = path / "manifest.json"
    if manifest.exists():
        data = json.loads(manifest.read_text(encoding="utf-8"))
    else:
        data = {}
    data.setdefault("name", path.name)
    data.setdefault("samples", 7)
    data.setdefault("baseline_rmse", 0.123456)
    data.setdefault("target_shift", 0.0)
    return data


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _dag_nodes(pipeline_doc: dict) -> list[dict]:
    dag = pipeline_doc.get("dagml") or pipeline_doc.get("dag") or {}
    if dag.get("nodes"):
        return list(dag["nodes"])
    steps = pipeline_doc.get("pipeline") or pipeline_doc.get("steps") or []
    nodes = [{"id": "source", "op": "DATASET", "deps": []}]
    previous = "source"
    for index, step in enumerate(steps):
        node_id = str(step.get("id") or f"step_{index}")
        deps = step.get("after") or step.get("deps") or [previous]
        nodes.append(
            {
                "id": node_id,
                "op": step.get("class") or step.get("op") or "STEP",
                "deps": deps,
                "params": step.get("params") or {},
            }
        )
        previous = node_id
    return nodes


def _evaluate_dag(pipeline_doc: dict, dataset_info: dict, params: dict, n_jobs: int) -> dict:
    nodes = {str(node["id"]): node for node in _dag_nodes(pipeline_doc)}
    pending = dict(nodes)
    values = {}
    order = []
    dataset_base = (
        float(dataset_info.get("samples", 7)) * 0.01
        + float(dataset_info.get("baseline_rmse", 0.123456)) * 100
        + float(dataset_info.get("target_shift", 0.0))
    )
    while pending:
        progressed = False
        for node_id, node in list(pending.items()):
            deps = node.get("deps") or []
            if isinstance(deps, str):
                deps = [deps]
            if not all(dep in values for dep in deps):
                continue
            op = str(node.get("op") or node.get("class") or "STEP").lower()
            node_params = node.get("params") or {}
            base = sum(values[dep] for dep in deps) if deps else dataset_base
            weight = int(_digest({"op": op, "params": node_params})[:4], 16) % 17 + 1
            value = base + weight + float(params.get("dag_scale", 1))
            if "scale" in node_params:
                value *= float(node_params["scale"])
            if "n_components" in node_params:
                value -= float(node_params["n_components"]) * 2.5
            if "select" in op:
                value *= 0.7
            if "refit" in op and node_params.get("enabled", True):
                value -= 5.0
            value += n_jobs * 0.01
            values[node_id] = round(value, 6)
            order.append(node_id)
            del pending[node_id]
            progressed = True
        if not progressed:
            raise ValueError(f"cycle or unresolved DAG dependencies: {sorted(pending)}")
    return {"order": order, "terminal": order[-1], "values": values}


def _metrics(dataset_info: dict, dag_trace: dict, params: dict, n_jobs: int) -> dict[str, float | None]:
    terminal_value = float(dag_trace["values"][dag_trace["terminal"]])
    baseline = float(dataset_info.get("baseline_rmse", 0.123456))
    adjustment = (abs(terminal_value) % 13) / 1000.0
    scale = max(float(params.get("dag_scale", 1)), 1.0)
    best_rmse = round(baseline + adjustment / scale + (n_jobs - 1) * 0.0001, 6)
    return {
        "best_score": round(1.0 - best_rmse, 6),
        "best_rmse": best_rmse,
        "best_r2": round(1.0 - best_rmse / 2.0, 6),
        "best_mae": round(best_rmse * 0.8, 6),
        "best_accuracy": None,
    }
""".lstrip(),
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
