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
