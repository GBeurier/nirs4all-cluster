"""Version-compatibility handshake + pipeline fingerprinting (no nirs4all needed)."""

import hashlib

import pytest
from fastapi.testclient import TestClient

from nirs4all_cluster import versioning as V
from nirs4all_cluster.server.app import ServerConfig, create_app


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(ServerConfig(state_dir=str(tmp_path / "state")))) as c:
        yield c


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_fingerprint_obj_deterministic():
    a = {"steps": [{"class": "PLS", "n": 5}]}
    b = {"steps": [{"class": "PLS", "n": 5}]}
    assert V.fingerprint_obj(a) == V.fingerprint_obj(b)
    assert V.fingerprint_obj(a) != V.fingerprint_obj({"steps": []})
    assert V.fingerprint_obj(a).startswith("sha256:")


def test_fingerprint_file(tmp_path):
    p = tmp_path / "p.yaml"
    p.write_bytes(b"hello")
    assert V.fingerprint_file(p) == "sha256:" + hashlib.sha256(b"hello").hexdigest()


def test_compatibility_helpers():
    assert V.is_incompatible(V.API_VERSION + 1) is True
    assert V.is_incompatible(V.API_VERSION) is False
    assert V.is_incompatible(None) is False
    assert V.is_divergent("999.0.0") is True
    assert V.is_divergent(V.CLUSTER_VERSION) is False
    assert V.is_divergent(None) is False


def test_client_attaches_inline_fingerprint():
    from nirs4all_cluster.client import _as_pipeline

    inline = {"steps": [1]}
    ref = _as_pipeline({"kind": "inline_json", "inline": inline})
    assert ref.expected_fingerprint == V.fingerprint_obj(inline)
    # A path pipeline carries no fingerprint (the client can't read a worker-side path).
    assert _as_pipeline("/shared/p.yaml").expected_fingerprint is None


# --------------------------------------------------------------------------- #
# Server middleware
# --------------------------------------------------------------------------- #


def test_response_carries_version_headers(client):
    r = client.get("/v1/jobs")
    assert r.headers[V.H_VERSION] == V.CLUSTER_VERSION
    assert r.headers[V.H_API] == str(V.API_VERSION)


def test_incompatible_protocol_rejected(client):
    r = client.get("/v1/jobs", headers={V.H_API: str(V.API_VERSION + 1)})
    assert r.status_code == 426
    # The handshake still advertises the server's own version on the rejection.
    assert r.headers[V.H_API] == str(V.API_VERSION)


def test_divergent_version_noted_once(client):
    headers = {V.H_API: str(V.API_VERSION), V.H_VERSION: "9.9.9", V.H_ROLE: "client"}
    assert client.get("/v1/jobs", headers=headers).status_code == 200
    client.get("/v1/jobs", headers=headers).raise_for_status()  # repeat — throttled
    events = client.app.state.db.list_recent_events()
    divergence = [e for e in events if e["type"] == "version_divergence"]
    assert len(divergence) == 1


def test_pipeline_fingerprint_mismatch_emits_event(client):
    inline = {"steps": [{"class": "sklearn.cross_decomposition.PLSRegression"}]}
    req = {
        "type": "nirs4all.run",
        "pipeline": {"kind": "inline_json", "inline": inline, "expected_fingerprint": V.fingerprint_obj(inline)},
        "dataset": {"kind": "shared_path", "path": "/a"},
    }
    client.post("/v1/jobs", json=req).raise_for_status()
    worker = client.post(
        "/v1/workers/register", json={"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}}
    ).json()["worker_id"]
    task = client.post(f"/v1/workers/{worker}/lease").json()["task"]
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
    body = {
        "status": "succeeded",
        "pipeline_fingerprint": "sha256:something-else",
        "metrics": {"best_rmse": 0.1},
        "artifacts": {},
    }
    client.post(f"/v1/tasks/{task['task_id']}/complete", params={"worker_id": worker}, json=body).raise_for_status()
    events = client.app.state.db.list_recent_events()
    assert any(e["type"] == "pipeline_fingerprint_mismatch" for e in events)
