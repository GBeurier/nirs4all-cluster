"""Worker agent tests: GPU declaration/detection, runner contract and zip-slip safety.

These don't need nirs4all (the agent never imports it) or a live server (the
httpx client connects lazily).
"""

import json
import sys
import types
import zipfile

import pytest

from nirs4all_cluster.runners import nirs4all_run
from nirs4all_cluster.schemas import TaskPayload
from nirs4all_cluster.versioning import fingerprint_file, fingerprint_obj
from nirs4all_cluster.worker.agent import WorkerAgent, _detect_gpu
from nirs4all_cluster.worker.materialize import _safe_extract, build_runner_spec


class _MiniIndexArray:
    def __init__(self, values):
        self.values = list(values)
        self.ndim = 1
        self.size = len(self.values)

    def reshape(self, *_shape):
        return self

    def min(self):
        return min(self.values)

    def max(self):
        return max(self.values)

    def tolist(self):
        return list(self.values)


class _MiniArray:
    def __init__(self, rows):
        self.rows = [list(row) for row in rows]
        self.ndim = 2
        self.shape = (len(self.rows), len(self.rows[0]) if self.rows else 0)

    def __getitem__(self, key):
        if isinstance(key, _MiniIndexArray):
            return _MiniArray([self.rows[index] for index in key.values])
        if isinstance(key, list):
            return _MiniArray([self.rows[index] for index in key])
        return self.rows[key]

    def tolist(self):
        return [list(row) for row in self.rows]


def _install_fake_numpy(monkeypatch):
    module = types.ModuleType("numpy")

    def asarray(value, dtype=None):
        del dtype
        if isinstance(value, (_MiniArray, _MiniIndexArray)):
            return value
        if isinstance(value, (list, tuple)) and value and isinstance(value[0], (list, tuple)):
            return _MiniArray(value)
        if isinstance(value, (list, tuple)):
            return _MiniIndexArray(value)
        return value

    module.asarray = asarray
    module.array = asarray
    module.ndarray = (_MiniArray, _MiniIndexArray)
    monkeypatch.setitem(sys.modules, "numpy", module)


def _assert_matrix_equal(actual, expected):
    rows = actual.tolist() if hasattr(actual, "tolist") else actual
    assert rows == expected


def test_detect_gpu_stable_shape():
    info = _detect_gpu()
    assert set(info) == {"cuda", "gpu_count", "gpu_names", "cuda_version", "driver_version"}
    assert isinstance(info["gpu_count"], int)
    assert isinstance(info["cuda"], bool)


def test_declare_gpu_override():
    agent = WorkerAgent("http://127.0.0.1:1", gpu_count=2)
    try:
        assert agent.capabilities["gpu_count"] == 2
        assert agent.capabilities["cuda"] is True
        assert agent.labels["cuda"] == "true"
    finally:
        agent._http.close()

    cpu = WorkerAgent("http://127.0.0.1:1", gpu_count=0)
    try:
        assert cpu.capabilities["gpu_count"] == 0
        assert cpu.labels["cuda"] == "false"
    finally:
        cpu._http.close()


def test_user_cuda_label_not_overwritten():
    agent = WorkerAgent("http://127.0.0.1:1", labels={"cuda": "true"}, gpu_count=0)
    try:
        # explicit user label wins over detection
        assert agent.labels["cuda"] == "true"
    finally:
        agent._http.close()


def test_safe_extract_rejects_zip_slip(tmp_path):
    archive = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("ok.txt", b"fine")
        zf.writestr("../escape.txt", b"nope")  # sibling-escape attempt
    dest = tmp_path / "out" / "dataset"
    with pytest.raises(ValueError):
        _safe_extract(archive, dest)


def test_safe_extract_rejects_absolute(tmp_path):
    archive = tmp_path / "abs.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("/etc/passwd", b"nope")
    with pytest.raises(ValueError):
        _safe_extract(archive, tmp_path / "out")


def test_safe_extract_ok(tmp_path):
    archive = tmp_path / "good.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("a/b.txt", b"hello")
    dest = tmp_path / "out"
    _safe_extract(archive, dest)
    assert (dest / "a" / "b.txt").read_bytes() == b"hello"


def _payload(pipeline, dataset_dir):
    return TaskPayload(
        task_id="t1",
        job_id="j1",
        type="nirs4all.run",
        attempt=1,
        pipeline=pipeline,
        dataset={"kind": "shared_path", "path": str(dataset_dir)},
        lease_expires_at=0.0,
    )


def _native_payload():
    return {
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
    }


def test_build_runner_spec_fingerprints_inline(tmp_path):
    inline = {"steps": [{"class": "PLS", "n": 5}]}
    task = _payload({"kind": "inline_json", "inline": inline}, tmp_path)
    spec = build_runner_spec(task, tmp_path / "wd", lambda artifact_id, dest: dest)
    # The worker's inline fingerprint matches the client's hash of the same dict.
    assert spec["pipeline_fingerprint"] == fingerprint_obj(inline)


def test_build_runner_spec_fingerprints_path(tmp_path):
    pipeline_file = tmp_path / "p.yaml"
    pipeline_file.write_text("steps: [a, b]\n", encoding="utf-8")
    task = _payload({"kind": "path", "path": str(pipeline_file)}, tmp_path)
    spec = build_runner_spec(task, tmp_path / "wd", lambda artifact_id, dest: dest)
    assert spec["pipeline_fingerprint"] == fingerprint_file(pipeline_file)


def test_build_runner_spec_carries_native_payload_to_subprocess(tmp_path):
    pipeline_file = tmp_path / "p.yaml"
    pipeline_file.write_text("steps: [a, b]\n", encoding="utf-8")
    task = TaskPayload.model_validate(
        {
            "task_id": "t1",
            "job_id": "j1",
            "type": "nirs4all.run",
            "attempt": 1,
            "pipeline": {"kind": "path", "path": str(pipeline_file)},
            "dataset": {"kind": "shared_path", "path": str(tmp_path)},
            "nativePayload": _native_payload(),
            "lease_expires_at": 0.0,
        }
    )

    spec = build_runner_spec(task, tmp_path / "wd", lambda artifact_id, dest: dest)

    handoff = spec["native_payload"]["manifest"]["robustnessEvidencePublicationHandoff"]
    assert handoff["kind"] == "robustness_evidence_publication_handoff"
    assert "relation_manifest_identity" in handoff["alignmentStrategies"]


def test_runner_maps_adapter_params_to_isolated_nirs4all_call(tmp_path, monkeypatch):
    seen = {}

    class _RunResult:
        best_score = 0.1
        best_rmse = 0.2
        best_r2 = 0.3
        best_mae = 0.4
        best_accuracy = None
        num_predictions = 5
        best = {"model_name": "PLS", "task_type": "regression", "metric": "best_rmse"}

        def close(self):
            seen["closed"] = True

    def fake_run(**kwargs):
        seen.update(kwargs)
        return _RunResult()

    monkeypatch.setitem(sys.modules, "nirs4all", types.SimpleNamespace(__version__="test", run=fake_run))
    task_file = tmp_path / "task.json"
    result_file = tmp_path / "result.json"
    task_file.write_text(
        json.dumps(
            {
                "pipeline": {"mode": "path", "path": "/shared/pls.yaml"},
                "dataset": {"mode": "path", "path": "/data/corn"},
                "params": {
                    "workspace_path": "/local/workspace",
                    "n_jobs": 99,
                    "inner_n_jobs": 3,
                    "random_state": 42,
                },
                "outputs": {"export_best_model": False},
            }
        ),
        encoding="utf-8",
    )

    rc = nirs4all_run.main(
        [
            "--task-file",
            str(task_file),
            "--workspace",
            str(tmp_path / "worker-workspace"),
            "--output-dir",
            str(tmp_path / "outputs"),
            "--result-file",
            str(result_file),
        ]
    )

    assert rc == 0
    assert seen["pipeline"] == "/shared/pls.yaml"
    assert seen["dataset"] == "/data/corn"
    assert seen["workspace_path"] == str(tmp_path / "worker-workspace")
    assert seen["n_jobs"] == 3
    assert seen["random_state"] == 42
    assert "inner_n_jobs" not in seen
    assert seen["closed"] is True
    summary = json.loads(result_file.read_text(encoding="utf-8"))
    assert summary["metrics"]["best_rmse"] == 0.2


def test_runner_records_robustness_handoff_trace_without_synthesizing_arrays(tmp_path, monkeypatch):
    class _RunResult:
        best_score = 0.1
        best_rmse = 0.2
        best_r2 = 0.3
        best_mae = 0.4
        best_accuracy = None
        num_predictions = 5
        best = {"model_name": "PLS", "task_type": "regression", "metric": "best_rmse"}

        def export(self, path):
            with open(path, "wb") as fh:
                fh.write(b"FAKE-N4A")

        def close(self):
            pass

    def fake_run(**kwargs):
        return _RunResult()

    monkeypatch.setitem(sys.modules, "nirs4all", types.SimpleNamespace(__version__="test", run=fake_run))
    task_file = tmp_path / "task.json"
    result_file = tmp_path / "result.json"
    task_file.write_text(
        json.dumps(
            {
                "pipeline": {"mode": "path", "path": "/shared/pls.yaml"},
                "dataset": {"mode": "path", "path": "/data/corn"},
                "params": {},
                "outputs": {"export_best_model": True},
                "native_payload": _native_payload(),
            }
        ),
        encoding="utf-8",
    )

    rc = nirs4all_run.main(
        [
            "--task-file",
            str(task_file),
            "--workspace",
            str(tmp_path / "worker-workspace"),
            "--output-dir",
            str(tmp_path / "outputs"),
            "--result-file",
            str(result_file),
        ]
    )

    assert rc == 0
    summary = json.loads(result_file.read_text(encoding="utf-8"))
    trace = summary["extra"]["robustness_evidence_publication_trace"]
    assert trace["status"] == "received_needs_array_publication"
    assert trace["published"]["result_metadata.robustness_evidence.predictor_bundle"].endswith("best_model.n4a")
    assert "prediction_arrays.X" in trace["missing"]
    assert "result_metadata.robustness_evidence.X" in trace["missing"]


def test_runner_publishes_row_aligned_robustness_evidence_to_workspace(tmp_path, monkeypatch):
    _install_fake_numpy(monkeypatch)

    class FakeDataset:
        def x(self, _selector=None, *, layout="2d"):
            assert layout == "2d"
            return [
                _MiniArray(
                    [
                        [10.0, 11.0],
                        [20.0, 21.0],
                        [30.0, 31.0],
                    ]
                )
            ]

    class FakeDatasetConfigs:
        def __init__(self, path):
            assert path == "/data/corn"

        def get_dataset_at(self, index):
            assert index == 0
            return FakeDataset()

    class FakeRows:
        def iter_rows(self, named=False):
            assert named is True
            return iter(
                [
                    {
                        "prediction_id": "pred-a",
                        "dataset_name": "dataset-a",
                        "model_name": "PLSRegression",
                        "fold_id": "final",
                        "partition": "test",
                        "metric": "rmse",
                        "val_score": None,
                        "task_type": "regression",
                    }
                ]
            )

    class FakeArrayStore:
        def __init__(self):
            self.saved = []

        def load_single(self, prediction_id, dataset_name=None):
            assert prediction_id == "pred-a"
            assert dataset_name == "dataset-a"
            return {
                "y_true": [1.0, 3.0],
                "y_pred": [1.1, 2.9],
                "sample_indices": [0, 2],
            }

        def save_batch(self, records):
            self.saved.extend(records)

    class FakeWorkspaceStore:
        def __init__(self, workspace_path):
            assert workspace_path == tmp_path / "workspace"
            self.array_store = FakeArrayStore()
            stores.append(self)

        def query_predictions(self):
            return FakeRows()

        def close(self):
            self.closed = True

    data_module = types.ModuleType("nirs4all.data")
    data_module.DatasetConfigs = FakeDatasetConfigs
    pipeline_module = types.ModuleType("nirs4all.pipeline")
    storage_module = types.ModuleType("nirs4all.pipeline.storage")
    workspace_store_module = types.ModuleType("nirs4all.pipeline.storage.workspace_store")
    workspace_store_module.WorkspaceStore = FakeWorkspaceStore
    monkeypatch.setitem(sys.modules, "nirs4all.data", data_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline", pipeline_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline.storage", storage_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline.storage.workspace_store", workspace_store_module)

    stores = []
    summary = nirs4all_run._publish_robustness_evidence_to_workspace(
        workspace_path=tmp_path / "workspace",
        dataset_spec={"mode": "path", "path": "/data/corn"},
        model_path="/worker/outputs/best_model.n4a",
    )

    assert summary == {
        "status": "published",
        "reason": None,
        "published_prediction_count": 1,
    }
    assert stores[0].closed is True
    saved = stores[0].array_store.saved[0]
    _assert_matrix_equal(saved["X"], [[10.0, 11.0], [30.0, 31.0]])
    assert saved["result_metadata"]["robustness_evidence"] == {
        "X": "prediction_arrays.X",
        "predictor_bundle": "/worker/outputs/best_model.n4a",
        "publisher": "nirs4all-cluster.runner",
    }

    trace = nirs4all_run._robustness_handoff_trace(
        _native_payload()["manifest"]["robustnessEvidencePublicationHandoff"],
        {"model": "/worker/outputs/best_model.n4a"},
        summary,
    )
    assert trace["status"] == "published"
    assert trace["missing"] == []
    assert trace["published"]["prediction_arrays.X"] == "task_workspace_prediction_arrays"


def test_runner_publishes_robustness_evidence_by_relation_manifest_identity(tmp_path, monkeypatch):
    _install_fake_numpy(monkeypatch)

    class FakeMetadata:
        columns = ["sample_id"]

        def get_column(self, column):
            assert column == "sample_id"
            return types.SimpleNamespace(to_list=lambda: ["s0", "s1", "s2"])

    class FakeDataset:
        def x(self, _selector=None, *, layout="2d"):
            assert layout == "2d"
            return [
                _MiniArray(
                    [
                        [10.0, 11.0],
                        [20.0, 21.0],
                        [30.0, 31.0],
                    ]
                )
            ]

        def metadata(self):
            return FakeMetadata()

    class FakeDatasetConfigs:
        def __init__(self, path):
            assert path == "/data/corn"

        def get_dataset_at(self, index):
            assert index == 0
            return FakeDataset()

    class FakeRows:
        def iter_rows(self, named=False):
            assert named is True
            return iter(
                [
                    {
                        "prediction_id": "pred-relation",
                        "dataset_name": "dataset-a",
                        "model_name": "PLSRegression",
                        "fold_id": "final",
                        "partition": "test",
                        "metric": "rmse",
                        "val_score": None,
                        "task_type": "regression",
                    }
                ]
            )

    class FakeArrayStore:
        def __init__(self):
            self.saved = []

        def load_single(self, prediction_id, dataset_name=None):
            assert prediction_id == "pred-relation"
            assert dataset_name == "dataset-a"
            return {
                "y_true": [3.0, 1.0],
                "y_pred": [2.9, 1.1],
                "result_metadata": {
                    "relation_replay_manifest": {
                        "materialization_manifest": {
                            "sample_ids": ["s2", "s0"],
                        },
                    },
                },
            }

        def save_batch(self, records):
            self.saved.extend(records)

    class FakeWorkspaceStore:
        def __init__(self, workspace_path):
            assert workspace_path == tmp_path / "workspace"
            self.array_store = FakeArrayStore()
            stores.append(self)

        def query_predictions(self):
            return FakeRows()

        def close(self):
            pass

    data_module = types.ModuleType("nirs4all.data")
    data_module.DatasetConfigs = FakeDatasetConfigs
    pipeline_module = types.ModuleType("nirs4all.pipeline")
    storage_module = types.ModuleType("nirs4all.pipeline.storage")
    workspace_store_module = types.ModuleType("nirs4all.pipeline.storage.workspace_store")
    workspace_store_module.WorkspaceStore = FakeWorkspaceStore
    monkeypatch.setitem(sys.modules, "nirs4all.data", data_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline", pipeline_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline.storage", storage_module)
    monkeypatch.setitem(sys.modules, "nirs4all.pipeline.storage.workspace_store", workspace_store_module)

    stores = []
    summary = nirs4all_run._publish_robustness_evidence_to_workspace(
        workspace_path=tmp_path / "workspace",
        dataset_spec={"mode": "path", "path": "/data/corn"},
        model_path="/worker/outputs/best_model.n4a",
    )

    assert summary["status"] == "published"
    saved = stores[0].array_store.saved[0]
    _assert_matrix_equal(saved["X"], [[30.0, 31.0], [10.0, 11.0]])
    assert saved["result_metadata"]["robustness_evidence"]["publisher"] == "nirs4all-cluster.runner"


def test_worker_enriches_robustness_trace_with_uploaded_artifact_ids():
    extra = {
        "robustness_evidence_publication_trace": {
            "kind": "robustness_evidence_publication_trace",
            "published": {},
        }
    }

    WorkerAgent._attach_robustness_artifact_refs(extra, {"model": "art_model", "workspace": "art_workspace"})

    assert extra["robustness_evidence_publication_trace"]["published_artifacts"] == {
        "predictor_bundle": "art_model",
        "workspace": "art_workspace",
    }
