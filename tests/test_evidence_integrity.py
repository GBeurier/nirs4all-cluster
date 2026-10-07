"""Fail-closed transport/persistence regressions; no scientific SDK needed."""

import io
import types
import zipfile

import pytest
from fastapi.testclient import TestClient
from test_worker import _install_fake_numpy, _MiniArray, _native_payload

from nirs4all_cluster.runners import nirs4all_run as runner
from nirs4all_cluster.schemas import NativeExperimentLaunchPayload, TaskPayload
from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.worker.agent import WorkerAgent


def job(payload):
    return {
        "pipeline": {"kind": "path", "path": "/p"},
        "dataset": {"kind": "shared_path", "path": "/d"},
        "nativePayload": payload,
    }


@pytest.mark.parametrize(
    "canonical,alias",
    [
        ("fail_closed", "failClosed"),
        ("alignment_strategies", "alignmentStrategies"),
        ("published_fields", "publishedFields"),
    ],
)
def test_alias_shadow_refused_before_persistence_and_normal_leasing_works(tmp_path, canonical, alias):
    payload = _native_payload()
    handoff = payload["manifest"]["robustnessEvidencePublicationHandoff"]
    handoff[canonical] = "poison"
    with TestClient(create_app(ServerConfig(state_dir=str(tmp_path / "state")))) as client:
        response = client.post("/v1/jobs", json=job(payload))
        assert response.status_code == 422
        assert client.app.state.db.list_jobs() == []
        healthy = client.post("/v1/jobs", json=job(_native_payload()))
        assert healthy.status_code == 200
        worker = client.post("/v1/workers/register", json={"version": {"packages": {"nirs4all": "1.4.5"}}}).json()
        wid = worker["worker_id"]
        lease = client.post(f"/v1/workers/{wid}/lease")
        assert lease.status_code == 200
        task = TaskPayload.model_validate(lease.json()["task"])
        assert task.native_payload.manifest.robustness_evidence_publication_handoff.fail_closed is True
        assert client.get("/v1/jobs").status_code == 200
        assert client.post(f"/v1/workers/{wid}/heartbeat").status_code == 200


@pytest.mark.parametrize(
    "field,alias", [("legacy_config", "legacyConfig"), ("strict_campaign_specs", "strictCampaignSpecs")]
)
def test_top_level_alias_shadow_rejected(field, alias):
    with pytest.raises(ValueError, match="Ambiguous field"):
        NativeExperimentLaunchPayload.model_validate({field: {}, alias: {}})


def test_nullable_extensions_and_canonical_dump_roundtrip():
    payload = _native_payload()
    payload["extension"] = {"untouched": None}
    first = NativeExperimentLaunchPayload.model_validate(payload)
    assert NativeExperimentLaunchPayload.model_validate(first.model_dump()).model_dump() == first.model_dump()
    assert first.model_dump()["extension"] == {"untouched": None}


@pytest.mark.parametrize(
    "arrays,expected",
    [
        ({"y_true": [3, 1, 2], "sample_metadata": {"row_id": ["c", "a", "b"]}}, [[30, 31], [10, 11], [20, 21]]),
        ({"y_true": [1, 1, 3], "sample_metadata": {"row_id": ["a", "a", "c"]}}, None),
        ({"y_true": [1, 3], "y_pred": [1]}, None),
        ({"y_true": [1, 3], "sample_indices": [0]}, None),
        ({"y_true": [1, 2, 3], "sample_indices": [99, 98, 97]}, None),
        ({"y_true": [1, 3], "sample_indices": [0, 0]}, None),
        ({"y_true": [1, 3], "sample_metadata": {"row_id": ["missing", "a"]}}, None),
        ({"y_true": [1, 3], "sample_metadata": {"row_id": ["a"]}}, None),
        ({"y_true": [1, 3], "sample_metadata": {"row_id": ["c", "a"], "row_ids": ["a", "c"]}}, None),
        ({"y_true": [1, 3], "sample_indices": [0, 2], "weights": [0.2, 0.3, 0.5]}, [[10, 11], [30, 31]]),
    ],
)
def test_alignment_checks_explicit_identity_and_all_prediction_row_counts(monkeypatch, arrays, expected):
    _install_fake_numpy(monkeypatch)
    selected = runner._select_prediction_X(
        _MiniArray([[10, 11], [20, 21], [30, 31]]), arrays, dataset_metadata={"row_id": ["a", "b", "c"]}
    )
    assert (selected.tolist() if selected is not None else None) == expected


def test_partial_outcomes_do_not_claim_global_requested_predictor_or_X():
    handoff = _native_payload()["manifest"]["robustnessEvidencePublicationHandoff"]
    summary = {
        "status": "partial",
        "prediction_outcomes": [
            {"prediction_id": "good", "missing": []},
            {"prediction_id": "bad", "missing": ["X", "predictor_bundle"]},
        ],
    }
    trace = runner._robustness_handoff_trace(handoff, {"model": "/exists.n4a"}, summary)
    assert trace["status"] == "received_needs_array_publication"
    assert set(trace["missing"]) == set(handoff["publishedFields"])
    assert trace["published"] == {}
    assert trace["fail_closed"] is True


def test_evidence_workspace_uploaded_and_downloadable_after_default_cleanup(tmp_path):
    with TestClient(create_app(ServerConfig(state_dir=str(tmp_path / "server")))) as client:
        client.post("/v1/jobs", json=job(_native_payload())).raise_for_status()
        wid = client.post("/v1/workers/register", json={"version": {"packages": {"nirs4all": "1.4.5"}}}).json()[
            "worker_id"
        ]
        task = TaskPayload.model_validate(client.post(f"/v1/workers/{wid}/lease").json()["task"])
        client.post(f"/v1/tasks/{task.task_id}/start", params={"worker_id": wid}).raise_for_status()
        agent = WorkerAgent("http://unused", state_dir=tmp_path / "agent")
        agent._http.close()
        agent._http = client
        agent.worker_id = wid
        workdir = tmp_path / "work"
        workspace = workdir / "workspace"
        workspace.mkdir(parents=True)
        (workspace / "arrays.parquet").write_bytes(b"retained prediction arrays")
        log = workdir / "run.log"
        log.write_text("finished")
        trace = {
            "kind": "robustness_evidence_publication_trace",
            "status": "received_needs_array_publication",
            "publication_summary": {"workspace_required": True},
        }
        execution = types.SimpleNamespace(
            result={"extra": {"robustness_evidence_publication_trace": trace}}, workspace_path=workspace, log_path=log
        )
        assert task.outputs.keep_task_workspace is False
        agent._report_success(task, execution, None)
        artifact_id = trace["published_artifacts"]["workspace"]
        agent._cleanup(task, workdir)
        assert not workdir.exists()
        downloaded = client.get(f"/v1/artifacts/{artifact_id}")
        assert downloaded.status_code == 200
        with zipfile.ZipFile(io.BytesIO(downloaded.content)) as archive:
            assert archive.read("arrays.parquet") == b"retained prediction arrays"


def test_failed_evidence_workspace_upload_prevents_success_report(tmp_path, monkeypatch):
    task = TaskPayload(
        task_id="t",
        job_id="j",
        type="nirs4all.run",
        attempt=1,
        pipeline={"kind": "path", "path": "/p"},
        dataset={"kind": "shared_path", "path": "/d"},
        lease_expires_at=100,
    )
    agent = WorkerAgent("http://unused", state_dir=tmp_path / "agent")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "arrays.parquet").write_bytes(b"evidence")
    execution = types.SimpleNamespace(
        result={
            "extra": {"robustness_evidence_publication_trace": {"publication_summary": {"workspace_required": True}}}
        },
        workspace_path=workspace,
        log_path=tmp_path / "missing",
    )
    monkeypatch.setattr(agent, "_upload", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="not delivered"):
        agent._report_success(task, execution, None)
    agent._http.close()


def test_identical_feature_values_cannot_hide_conflicting_row_identity(monkeypatch):
    _install_fake_numpy(monkeypatch)
    assert (
        runner._select_prediction_X(
            _MiniArray([[1, 2], [1, 2]]),
            {"y_true": [10, 20], "sample_indices": [0, 1], "sample_metadata": {"row_id": ["b", "a"]}},
            dataset_metadata={"row_id": ["a", "b"]},
        )
        is None
    )


@pytest.mark.parametrize("indices", [[0.9, 1.9], [True, False], [[0], [1]]])
def test_indices_are_one_dimensional_integer_positions(monkeypatch, indices):
    _install_fake_numpy(monkeypatch)
    assert (
        runner._select_prediction_X(_MiniArray([[1, 2], [3, 4]]), {"y_true": [1, 2], "sample_indices": indices}) is None
    )


@pytest.mark.parametrize("fail_save", [False, True])
def test_array_upsert_preserves_all_existing_evidence_and_reports_commit_failure(tmp_path, monkeypatch, fail_save):
    import sys

    _install_fake_numpy(monkeypatch)

    class Dataset:
        def x(self, *a, **kw):
            return _MiniArray([[10, 11], [20, 21], [30, 31]])

    monkeypatch.setattr(runner, "_load_replay_dataset_from_spec", lambda _: Dataset())
    existing = {
        "y_true": [1, 3],
        "y_pred": [1.1, 2.9],
        "sample_indices": [0, 2],
        "spectra": [[100, 101], [300, 301]],
        "weights": [0.2, 0.3, 0.5],
        "sample_metadata": {},
        "result_metadata": {"previous": "retained"},
    }
    saved = []

    class Store:
        def __init__(self, path):
            self.array_store = self

        def query_predictions(self):
            return types.SimpleNamespace(
                iter_rows=lambda **kw: iter([{"prediction_id": "a", "dataset_name": "d", "fold_id": "final"}])
            )

        def load_single(self, *a, **kw):
            return dict(existing)

        def save_batch(self, records):
            if fail_save:
                raise OSError("atomic publication refused")
            saved.extend(records)

        def close(self):
            pass

    for name in [
        "nirs4all",
        "nirs4all.pipeline",
        "nirs4all.pipeline.storage",
        "nirs4all.pipeline.storage.workspace_store",
    ]:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules["nirs4all.pipeline.storage.workspace_store"].WorkspaceStore = Store
    summary = runner._publish_robustness_evidence_to_workspace(
        workspace_path=tmp_path, dataset_spec={}, model_path=None
    )
    if fail_save:
        assert summary["published_prediction_count"] == 0
        trace = runner._robustness_handoff_trace(
            _native_payload()["manifest"]["robustnessEvidencePublicationHandoff"], {}, summary
        )
        assert trace["published"] == {}
        assert len(trace["missing"]) == 3
    else:
        assert summary["published_prediction_count"] == 1
        for field in existing:
            if field != "result_metadata":
                assert saved[0][field] == existing[field]
        assert saved[0]["result_metadata"]["previous"] == "retained"
        assert summary["prediction_outcomes"][0]["missing"] == ["predictor_bundle"]
