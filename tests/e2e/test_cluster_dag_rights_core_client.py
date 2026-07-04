"""Ecosystem E2E entrypoint for the cluster DAG scheduler/rights contract.

This deliberately stays in the cluster control plane: no import of ``nirs4all``.
The ecosystem runner consumes the produced ``scheduler-run.json`` in the next
core-client handoff step.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nirs4all_cluster import ClusterClient, ClusterPermissionError, WorkerClient
from nirs4all_cluster.schemas import RunMetrics, TaskResult
from nirs4all_cluster.versioning import fingerprint_obj


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
