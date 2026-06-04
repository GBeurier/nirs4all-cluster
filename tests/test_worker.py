"""Worker agent tests: GPU declaration/detection and zip-slip safety.

These don't need nirs4all (the agent never imports it) or a live server (the
httpx client connects lazily).
"""

import zipfile

import pytest

from nirs4all_cluster.worker.agent import WorkerAgent, _detect_gpu
from nirs4all_cluster.worker.materialize import _safe_extract


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
