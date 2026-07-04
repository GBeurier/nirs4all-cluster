"""Ecosystem E2E entrypoint for the cluster DAG scheduler/rights contract.

The default proof deliberately stays in the cluster control plane: no import of
``nirs4all``. When ``N4A_CLUSTER_NUMERIC_ORACLE=1`` is set, the same test also
runs one real ``nirs4all.run`` task through the worker subprocess and compares
the cluster metric to a local Python-reference oracle.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from nirs4all_cluster import ClusterClient, ClusterPermissionError, WorkerClient
from nirs4all_cluster.schemas import RunMetrics, TaskResult
from nirs4all_cluster.versioning import fingerprint_obj
from nirs4all_cluster.worker.executor import execute_task
from nirs4all_cluster.worker.materialize import build_runner_spec

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parent
NUMERIC_PIPELINE = REPO_ROOT / "examples" / "pipelines" / "pls.yaml"
NUMERIC_DATASET_NAME = "_".join(("PSI", "spxyG70", "30", "byCultivar", "MicroNIR", "NeoSpectra"))
NUMERIC_DATASET = (
    WORKSPACE_ROOT
    / "nirs4all-data"
    / "regression"
    / "GRAPEVINE_LeafTraits"
    / NUMERIC_DATASET_NAME
)
NUMERIC_ORACLE_ARTIFACT = "local-vs-cluster-numeric.json"


def _dag_pipeline() -> dict[str, object]:
    return {
        "dagml": {
            "nodes": [
                {"id": "source", "op": "DATASET", "deps": []},
                {"id": "preprocess", "op": "SNV", "deps": ["source"]},
                {"id": "fit", "op": "PLS_CV", "deps": ["preprocess"]},
                {"id": "refit", "op": "REFIT_BEST", "deps": ["fit"]},
            ]
        },
        "metadata": {"name": "e2e-dag-rights"},
    }


def _linear_pipeline() -> dict[str, object]:
    return {
        "steps": [
            {"id": "snv", "class": "SNV"},
            {"id": "pls", "class": "PLSRegression", "after": ["snv"], "params": {"n_components": 3}},
        ]
    }


def _permission_probe_kwargs() -> dict[str, object]:
    return {
        "pipeline": {"kind": "inline_json", "inline": _linear_pipeline()},
        "dataset": {"kind": "shared_path", "path": "/shared/e2e.csv", "name": "permission-probe"},
        "name": "permission-probe",
    }


def _numeric_oracle_enabled() -> bool:
    return os.environ.get("N4A_CLUSTER_NUMERIC_ORACLE") == "1"


def _path_from_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser().resolve()


def _ensure_numeric_inputs(pipeline: Path, dataset: Path) -> None:
    if not pipeline.is_file():
        raise AssertionError(f"numeric oracle pipeline is missing: {pipeline}")
    if not dataset.exists():
        raise AssertionError(f"numeric oracle dataset is missing: {dataset}")


def _run_numeric_oracle(
    submitter: ClusterClient,
    worker: WorkerClient,
    *,
    artifacts_dir: Path,
    tmp_path: Path,
) -> dict[str, Any]:
    if not _numeric_oracle_enabled():
        payload = {
            "schema_version": "n4a.e2e.cluster-numeric-oracle/v1",
            "status": "not_requested",
            "enable_with": "N4A_CLUSTER_NUMERIC_ORACLE=1",
            "scope": "real_cluster_run_vs_local_python_reference",
        }
        (artifacts_dir / NUMERIC_ORACLE_ARTIFACT).write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return payload

    pipeline = _path_from_env("N4A_CLUSTER_NUMERIC_PIPELINE", NUMERIC_PIPELINE)
    dataset = _path_from_env("N4A_CLUSTER_NUMERIC_DATASET", NUMERIC_DATASET)
    _ensure_numeric_inputs(pipeline, dataset)

    try:
        import nirs4all
    except ModuleNotFoundError as exc:
        raise AssertionError(
            "numeric oracle requires the Python reference package; set PYTHONPATH to the nirs4all checkout"
        ) from exc

    job = submitter.submit_nirs4all_run(
        pipeline=str(pipeline),
        dataset=str(dataset),
        params={"random_state": 42, "refit": True},
        inner_n_jobs=1,
        requirements={"labels": {"site": "e2e-lab"}, "packages": {"nirs4all": ">=0.9"}},
        outputs={"export_best_model": True, "keep_task_workspace": False},
        name="e2e-cluster-numeric-oracle",
        rank_metric="best_rmse",
        rank_mode="min",
    )
    task = worker.lease()
    if task is None:
        raise AssertionError("numeric oracle job was not leased by the eligible worker")
    worker.start_task(task.task_id)

    workdir = tmp_path / "numeric-oracle-worker" / task.task_id
    spec = build_runner_spec(task, workdir, worker.download_artifact)
    pythonpath_entries = [str(REPO_ROOT), str(WORKSPACE_ROOT / "nirs4all")]
    previous_pythonpath = os.environ.get("PYTHONPATH")
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [*pythonpath_entries, *([previous_pythonpath] if previous_pythonpath else [])]
    )
    try:
        execution = execute_task(spec, workdir, python_exe=sys.executable, poll_interval=0.2)
    finally:
        if previous_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = previous_pythonpath
    if execution.result.get("status") != "succeeded":
        log_tail = execution.log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
        raise AssertionError(f"numeric oracle cluster task failed: {execution.result!r}\n{log_tail}")

    produced = execution.result.get("produced") or {}
    model_artifact_id = None
    model_path = produced.get("model")
    if model_path and Path(model_path).exists():
        model_artifact_id = worker.upload_artifact(task.task_id, model_path, role="model", kind="model")
    logs_artifact_id = worker.upload_artifact(task.task_id, execution.log_path, role="logs", kind="logs")
    metrics = RunMetrics.model_validate(execution.result.get("metrics") or {})
    worker.complete_task(
        task.task_id,
        TaskResult(
            nirs4all_version=execution.result.get("nirs4all_version"),
            pipeline_fingerprint=spec.get("pipeline_fingerprint"),
            duration_seconds=float(execution.result.get("duration_seconds") or 0.0),
            metrics=metrics,
            counts=execution.result.get("counts") or {},
            artifacts={"model": model_artifact_id, "logs": logs_artifact_id, "workspace": None},
            extra={**(execution.result.get("extra") or {}), "numeric_oracle": True},
        ),
    )

    final = submitter.wait(job.id, poll=0.2, timeout=180)
    if final.status.value != "succeeded":
        raise AssertionError(f"numeric oracle cluster job failed: {final.aggregate.errors}")

    local = nirs4all.run(
        pipeline=str(pipeline),
        dataset=str(dataset),
        workspace_path=str(tmp_path / "numeric-oracle-local-ws"),
        random_state=42,
        refit=True,
        save_charts=False,
        verbose=0,
        n_jobs=1,
    )
    cluster_best_rmse = float(final.aggregate.best_metric)
    local_best_rmse = float(local.best_rmse)
    abs_diff = abs(cluster_best_rmse - local_best_rmse)
    tolerance = 1e-6
    passed = abs_diff <= tolerance
    payload = {
        "schema_version": "n4a.e2e.cluster-numeric-oracle/v1",
        "status": "passed" if passed else "failed",
        "scope": "real_cluster_run_vs_local_python_reference",
        "job_id": final.id,
        "task_id": task.task_id,
        "pipeline": str(pipeline),
        "dataset": str(dataset),
        "nirs4all_version": getattr(nirs4all, "__version__", "unknown"),
        "cluster_best_rmse": cluster_best_rmse,
        "local_best_rmse": local_best_rmse,
        "abs_diff": abs_diff,
        "tolerance_abs": tolerance,
        "best_model_artifact_id": final.aggregate.best_model_artifact_id,
    }
    (artifacts_dir / NUMERIC_ORACLE_ARTIFACT).write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not passed:
        raise AssertionError(f"numeric oracle mismatch: {payload}")
    return payload


def test_cluster_dag_rights_core_client_handoff(cluster, artifacts_dir: Path, tmp_path: Path) -> None:
    """Submit, route, lease, complete, and persist the cluster scheduler proof."""

    dataset_payload = tmp_path / "reference-dataset.json"
    dataset_payload.write_text(
        json.dumps(
            {
                "dataset_id": "synthetic.reference.nirs",
                "spectra_shape": [12, 8],
                "targets": ["protein"],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with (
        ClusterClient(cluster.base_url, token=cluster.viewer, timeout=10) as viewer,
        ClusterClient(cluster.base_url, token=cluster.executor, timeout=10) as executor_as_client,
        ClusterClient(cluster.base_url, token=cluster.submitter, timeout=10) as submitter,
        WorkerClient(cluster.base_url, token=cluster.viewer, timeout=10) as viewer_as_worker,
        WorkerClient(cluster.base_url, token=cluster.executor, timeout=10) as worker,
    ):
        with pytest.raises(ClusterPermissionError) as viewer_submit:
            viewer.submit_nirs4all_run(**_permission_probe_kwargs())
        assert viewer_submit.value.principal == "dash"
        assert "submit" in viewer_submit.value.missing_rights

        with pytest.raises(ClusterPermissionError) as executor_submit:
            executor_as_client.submit_nirs4all_run(**_permission_probe_kwargs())
        assert executor_submit.value.principal == "worker1"
        assert "submit" in executor_submit.value.missing_rights

        with pytest.raises(ClusterPermissionError) as viewer_execute:
            viewer_as_worker.register(name="viewer-cannot-execute")
        assert viewer_execute.value.principal == "dash"
        assert "execute" in viewer_execute.value.missing_rights

        dataset_artifact_id = submitter.upload_artifact(dataset_payload, kind="dataset")
        job = submitter.submit_nirs4all_run(
            pipelines=[
                {"kind": "inline_json", "inline": _dag_pipeline()},
                {"kind": "inline_json", "inline": _linear_pipeline()},
            ],
            datasets=[
                {"kind": "artifact", "artifact_id": dataset_artifact_id, "name": "artifact-dataset"},
                {"kind": "shared_path", "path": "/shared/alternative-dataset", "name": "alternative-dataset"},
            ],
            params={"random_state": 13, "inner_n_jobs": 1},
            requirements={"labels": {"site": "e2e-lab"}, "packages": {"nirs4all": ">=0.9,<0.10"}},
            outputs={"export_best_model": True, "keep_task_workspace": False},
            name="e2e-cluster-dag-rights",
            rank_metric="best_rmse",
            rank_mode="min",
        )

        initial = submitter.get_job(job.id)
        assert initial.scheduler is not None
        assert initial.scheduler.shape == "dag_shaped_whole_run"
        assert initial.submission is not None
        assert initial.submission.principal == "alice"
        assert set(initial.submission.granted_rights) == {"submit", "read", "cancel"}
        assert len(submitter.get_tasks(job.id)) == 4

        missing_package = worker.register(
            labels={"site": "e2e-lab"},
            capabilities={"gpu_count": 0},
            slots_total=1,
            version={"packages": {}},
            name="missing-nirs4all",
        )
        assert "execute" in missing_package.rights
        assert worker.lease() is None

        wrong_version = worker.register(
            labels={"site": "e2e-lab"},
            capabilities={"gpu_count": 0},
            slots_total=1,
            version={"packages": {"nirs4all": "0.8.9"}},
            name="wrong-nirs4all-version",
        )
        assert "execute" in wrong_version.rights
        assert worker.lease() is None

        wrong_site = worker.register(
            labels={"site": "other-lab"},
            capabilities={"gpu_count": 0},
            slots_total=1,
            version={"packages": {"nirs4all": "0.9.5"}},
            name="wrong-site",
        )
        assert "execute" in wrong_site.rights
        assert worker.lease() is None

        registered = worker.register(
            labels={"site": "e2e-lab"},
            capabilities={"gpu_count": 0},
            slots_total=1,
            version={"packages": {"nirs4all": "0.9.5"}},
            name="e2e-executor",
        )
        assert set(registered.rights) == {"read", "execute"}

        completed_task_ids: list[str] = []
        model_artifact_ids: list[str] = []
        for index in range(4):
            task = worker.lease()
            assert task is not None
            assert task.scheduler is not None
            assert task.scheduler.shape == "dag_shaped_whole_run"
            assert task.submission is not None
            assert task.submission.principal == "alice"
            assert task.assignment is not None
            assert task.assignment.executor_principal == "worker1"
            assert task.assignment.worker_id == registered.worker_id
            assert task.assignment.required_rights == ["execute"]
            assert set(task.assignment.granted_rights) == {"read", "execute"}

            worker.start_task(task.task_id)
            model_path = tmp_path / f"model-{index}.json"
            model_path.write_text(
                json.dumps({"task_id": task.task_id, "pipeline": task.pipeline.kind}, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            model_artifact_id = worker.upload_artifact(task.task_id, model_path, role="model", kind="model")
            model_artifact_ids.append(model_artifact_id)
            worker.complete_task(
                task.task_id,
                TaskResult(
                    nirs4all_version="0.9.5",
                    pipeline_fingerprint=(
                        fingerprint_obj(task.pipeline.inline)
                        if task.pipeline.kind == "inline_json" and task.pipeline.inline is not None
                        else None
                    ),
                    duration_seconds=0.01 + index,
                    metrics=RunMetrics(best_rmse=0.25 + index * 0.01, best_r2=0.9 - index * 0.01),
                    counts={"samples": 12, "features": 8},
                    artifacts={"model": model_artifact_id, "logs": None, "workspace": None},
                    extra={"dataset": task.dataset.label(), "pipeline": task.pipeline.kind},
                ),
            )
            completed_task_ids.append(task.task_id)

        assert worker.lease() is None

        final = submitter.wait(job.id, poll=0.05, timeout=10)
        tasks = submitter.get_tasks(job.id)
        events = submitter.get_events(job.id)
        artifacts = submitter.list_artifacts(job.id)
        numeric_oracle = _run_numeric_oracle(
            submitter,
            worker,
            artifacts_dir=artifacts_dir,
            tmp_path=tmp_path,
        )

    assert final.status.value == "succeeded", final.aggregate.errors
    assert final.aggregate.num_tasks == 4
    assert final.aggregate.num_succeeded == 4
    assert final.aggregate.best_metric == pytest.approx(0.25, abs=1e-12)
    assert final.aggregate.best_task_id in completed_task_ids
    assert final.aggregate.best_model_artifact_id in model_artifact_ids
    assert {task.status.value for task in tasks} == {"succeeded"}
    assert all(task.result is not None for task in tasks)
    assert all(task.result and task.result.provenance.reported_by_principal == "worker1" for task in tasks)
    event_types = {event.type for event in events}
    assert {"job_submitted", "task_leased", "task_started", "task_completed", "job_succeeded"} <= event_types
    assert any(artifact["role"] == "best_model" for artifact in artifacts)

    out = artifacts_dir / "scheduler-run.json"
    out.write_text(
        json.dumps(
            {
                "scenario": "e2e-cluster-dag-rights-client-core",
                "job_id": final.id,
                "status": final.status.value,
                "scheduler": final.scheduler.model_dump(mode="json") if final.scheduler else None,
                "submission": final.submission.model_dump(mode="json") if final.submission else None,
                "rights_checks": {
                    "viewer_submit_missing": sorted(viewer_submit.value.missing_rights),
                    "executor_submit_missing": sorted(executor_submit.value.missing_rights),
                    "viewer_execute_missing": sorted(viewer_execute.value.missing_rights),
                    "executor_granted": sorted(registered.rights),
                },
                "routing_checks": {
                    "blocked_workers": [
                        missing_package.worker_id,
                        wrong_version.worker_id,
                        wrong_site.worker_id,
                    ],
                    "eligible_worker": registered.worker_id,
                    "completed_task_ids": completed_task_ids,
                },
                "aggregate": final.aggregate.model_dump(mode="json"),
                "numeric_oracle": numeric_oracle,
                "tasks": [task.model_dump(mode="json") for task in tasks],
                "events": [event.type for event in events],
                "artifacts": artifacts,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    assert out.exists()
