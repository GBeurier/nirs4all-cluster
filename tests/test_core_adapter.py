"""Core-facing nirs4all.run adapter contracts.

These tests do not import nirs4all. They pin the request shape core / CLI code
can hand to the cluster and the local-vs-distributed parity expectations that
travel with that request.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from nirs4all_cluster import ClusterClient, build_nirs4all_run_request
from nirs4all_cluster.schemas import JobRequest, JobView
from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.versioning import fingerprint_obj


def _native_launch_payload() -> dict[str, object]:
    return {
        "legacyConfig": {
            "name": "Cluster Robustness Experiment",
            "dataset_ids": ["d1"],
            "pipeline_ids": ["p1"],
        },
        "manifest": {
            "version": "studio.native-launch-payload.v1",
            "robustnessEvidencePublicationHandoff": {
                "kind": "robustness_evidence_publication_handoff",
                "requested": True,
                "destination": "result_metadata.robustness_evidence",
                "failClosed": True,
                "alignmentStrategies": [
                    "sample_indices",
                    "full_dataset_length",
                    "unique_metadata_identity",
                    "relation_manifest_identity",
                ],
                "publishedFields": [
                    "prediction_arrays.X",
                    "result_metadata.robustness_evidence.X",
                    "result_metadata.robustness_evidence.predictor_bundle",
                ],
            },
        },
        "strictCampaignSpecs": {"splitSpecs": [], "skippedRunIds": []},
    }


def test_build_nirs4all_run_request_records_parity_contract(tmp_path):
    inline = {"steps": [{"class": "PLS", "n_components": 6}]}

    req = build_nirs4all_run_request(
        pipelines=[{"kind": "inline_json", "inline": inline}, "/shared/rf.yaml"],
        datasets=["/data/corn", "/data/wheat"],
        params={
            "random_state": 42,
            "refit": True,
            "workspace_path": str(tmp_path / "local-ws"),
            "n_jobs": 2,
        },
        workspace_path=tmp_path / "local-ws",
        name="dag-matrix",
        rank_metric="best_rmse",
    )

    assert req.name == "dag-matrix"
    assert req.pipeline is None and req.pipelines is not None
    assert req.pipelines[0].expected_fingerprint == fingerprint_obj(inline)
    assert req.dataset is None and req.datasets is not None
    assert req.params == {"random_state": 42, "refit": True, "inner_n_jobs": 2}
    assert req.parity is not None
    assert req.parity.scope == "pipeline_dataset_matrix"
    assert req.parity.task_granularity == "whole_nirs4all_run"
    assert req.parity.workspace_policy == "isolated_task_workspace"
    assert req.parity.translated_params == {"n_jobs": "inner_n_jobs"}
    assert req.parity.omitted_local_kwargs == ["workspace_path"]
    assert req.parity.preserved_params == ["random_state", "refit"]
    assert any("fold-level distribution" in item for item in req.parity.deferred)
    assert req.scheduler is not None
    assert req.scheduler.shape == "pipeline_dataset_matrix"
    assert req.scheduler.assignment_mode == "server_leased_executor"
    assert req.scheduler.result_provenance == "server_attested_worker_report"


def test_build_nirs4all_run_request_preserves_native_robustness_handoff():
    req = build_nirs4all_run_request(
        pipeline="/shared/pls.yaml",
        dataset="/data/corn",
        native_payload=_native_launch_payload(),
    )

    assert req.native_payload is not None
    assert req.native_payload.manifest.version == "studio.native-launch-payload.v1"
    handoff = req.native_payload.manifest.robustness_evidence_publication_handoff
    assert handoff is not None
    assert handoff.requested is True
    assert handoff.fail_closed is True
    assert handoff.destination == "result_metadata.robustness_evidence"
    assert "relation_manifest_identity" in handoff.alignment_strategies
    assert "prediction_arrays.X" in handoff.published_fields


def test_build_nirs4all_run_request_rejects_conflicting_parallelism():
    with pytest.raises(ValueError, match="conflicting"):
        build_nirs4all_run_request(
            pipeline="/p.yaml",
            dataset="/data",
            params={"n_jobs": 2},
            inner_n_jobs=3,
        )


def test_submit_nirs4all_run_uses_adapter_contract(monkeypatch):
    captured: list[JobRequest] = []
    client = object.__new__(ClusterClient)

    def fake_submit(job: JobRequest | dict[str, object]) -> JobView:
        captured.append(job if isinstance(job, JobRequest) else JobRequest.model_validate(job))
        return JobView(
            id="job_test",
            type="nirs4all.run",
            status="queued",
            created_at=0.0,
            updated_at=0.0,
        )

    monkeypatch.setattr(client, "submit", fake_submit)

    view = ClusterClient.submit_nirs4all_run(
        client,
        pipeline="/shared/pls.yaml",
        dataset="/data/corn",
        params={"random_state": 7},
        n_jobs=4,
    )

    assert view.id == "job_test"
    assert captured[0].params == {"random_state": 7, "inner_n_jobs": 4}
    assert captured[0].parity is not None
    assert captured[0].parity.scope == "atomic"
    assert captured[0].scheduler is not None
    assert captured[0].scheduler.shape == "atomic"


def test_adapter_contract_survives_server_persistence_and_decomposition(tmp_path):
    req = build_nirs4all_run_request(
        pipelines=["/shared/pls.yaml", "/shared/rf.yaml"],
        datasets=["/data/a", "/data/b"],
        params={"inner_n_jobs": 2},
        name="matrix",
        native_payload=_native_launch_payload(),
    )
    with TestClient(create_app(ServerConfig(state_dir=str(tmp_path / "state")))) as client:
        response = client.post("/v1/jobs", json=req.model_dump(mode="json"))
        response.raise_for_status()
        body = response.json()
        assert body["aggregate"]["num_tasks"] == 4

        stored = JobRequest.model_validate_json(client.app.state.db.get_job(body["id"])["request_json"])
        assert stored.parity is not None
        assert stored.parity.scope == "pipeline_dataset_matrix"
        assert stored.native_payload is not None
        stored_handoff = stored.native_payload.manifest.robustness_evidence_publication_handoff
        assert stored_handoff is not None
        assert stored_handoff.alignment_strategies[-1] == "relation_manifest_identity"

        task_rows = client.app.state.db.list_tasks_for_job(body["id"])
        assert len(task_rows) == 4
        for row in task_rows:
            payload = json.loads(row["payload_json"])
            assert payload["params"] == {"inner_n_jobs": 2}
            assert payload["native_payload"]["manifest"]["robustness_evidence_publication_handoff"][
                "published_fields"
            ] == [
                "prediction_arrays.X",
                "result_metadata.robustness_evidence.X",
                "result_metadata.robustness_evidence.predictor_bundle",
            ]


def test_only_subprocess_runner_imports_nirs4all():
    root = Path(__file__).resolve().parents[1] / "nirs4all_cluster"
    allowed = root / "runners" / "nirs4all_run.py"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path == allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "nirs4all" or alias.name.startswith("nirs4all."):
                        offenders.append(f"{path.relative_to(root)}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "nirs4all" or module.startswith("nirs4all."):
                    offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert offenders == []
