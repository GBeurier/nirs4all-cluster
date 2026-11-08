"""End-to-end server API tests using a simulated worker (no nirs4all needed).

These exercise the full lifecycle the real worker drives — register, lease,
start, upload artifact, complete/fail, aggregate, cancel — but without running
nirs4all, so they are fast and dependency-free.
"""

import pytest
from auth_support import TestClient, create_app

from nirs4all_cluster.server.app import ServerConfig


@pytest.fixture
def client(tmp_path):
    config = ServerConfig(state_dir=str(tmp_path / "state"), lease_ttl_s=60.0)
    app = create_app(config)
    with TestClient(app) as c:
        yield c


def test_auth_required_by_default(tmp_path):
    from auth_support import TOKEN

    config = ServerConfig(state_dir=str(tmp_path / "state"))
    with TestClient(create_app(config)) as c:
        assert c.get("/v1/jobs", headers={"Authorization": ""}).status_code == 401
        assert c.get("/v1/jobs", headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert c.get("/v1/jobs", headers={"Authorization": "Bearer " + TOKEN}).status_code == 200


def test_input_artifact_size_limit(tmp_path):
    config = ServerConfig(state_dir=str(tmp_path / "state"), max_artifact_mb=0)  # 0 MB cap
    with TestClient(create_app(config)) as c:
        files = {"file": ("big.bin", b"x" * 4096, "application/octet-stream")}
        resp = c.post("/v1/artifacts", files=files)
        assert resp.status_code == 413
        # No orphan blob should remain in the object store.
        objects = tmp_path / "state" / "objects"
        leftover = [p for p in objects.rglob("*") if p.is_file()] if objects.exists() else []
        assert leftover == []


def _register(client, **kw):
    # Workers declare nirs4all by default so they satisfy the implicit
    # availability requirement the server adds to nirs4all.run jobs.
    body = {"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}}
    body.update(kw)
    resp = client.post("/v1/workers/register", json=body)
    resp.raise_for_status()
    return resp.json()["worker_id"]


def _lease(client, worker_id):
    resp = client.post(f"/v1/workers/{worker_id}/lease")
    resp.raise_for_status()
    return resp.json()["task"]


def _complete(client, worker_id, task_id, metrics, artifacts=None, delivery=None):
    body = {
        "status": "succeeded",
        "nirs4all_version": "0.9.1",
        "duration_seconds": 1.0,
        "metrics": metrics,
        "counts": {"num_predictions": 3},
        "delivery": delivery or {"status": "not_requested"},
        "artifacts": artifacts or {"model": None, "logs": None, "workspace": None},
    }
    resp = client.post(f"/v1/tasks/{task_id}/complete", params={"worker_id": worker_id}, json=body)
    resp.raise_for_status()
    return resp.json()


def _atomic_job(pipeline="/shared/pls.yaml", dataset="/shared/corn"):
    return {
        "type": "nirs4all.run",
        "name": "demo",
        "pipeline": {"kind": "path", "path": pipeline},
        "dataset": {"kind": "shared_path", "path": dataset},
        "params": {"random_state": 42},
        "outputs": {"export_best_model": False},
    }


def _native_payload():
    return {
        "legacyConfig": {"name": "Cluster Robustness Experiment", "dataset_ids": ["d1"], "pipeline_ids": ["p1"]},
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


def test_health(client):
    assert client.get("/").json()["ok"] is True


def test_healthz_and_version(client):
    assert client.get("/healthz").json() == {"ok": True}
    v = client.get("/version").json()
    assert v["service"] == "nirs4all-cluster"
    assert v["api_version"] >= 1
    assert client.get("/").json()["version"] == v["version"]


def test_dashboard_served(client):
    r = client.get("/ui")
    assert r.status_code == 200
    assert "nirs4all-cluster" in r.text


def test_stats_counts(client):
    client.post("/v1/jobs", json=_atomic_job()).raise_for_status()
    _register(client)
    s = client.get("/v1/stats").json()
    assert s["jobs_by_status"].get("queued") == 1
    assert s["workers_alive"] == 1
    assert s["api_version"] >= 1


def test_jobs_filter_by_status_and_name(client):
    client.post("/v1/jobs", json=_atomic_job()).raise_for_status()  # name="demo"
    other = _atomic_job()
    other["name"] = "other"
    client.post("/v1/jobs", json=other).raise_for_status()

    assert len(client.get("/v1/jobs", params={"status": "queued"}).json()) == 2
    assert client.get("/v1/jobs", params={"status": "succeeded"}).json() == []
    named = client.get("/v1/jobs", params={"name": "other"}).json()
    assert len(named) == 1 and named[0]["name"] == "other"


def test_workers_view_exposes_version_and_divergence(client):
    _register(client, version={"packages": {"nirs4all": "0.9.1"}, "nirs4all_cluster": "9.9.9"})
    w = client.get("/v1/workers").json()[0]
    assert w["cluster_version"] == "9.9.9"
    assert w["version_divergent"] is True  # 9.9.9 != the server's own version
    assert "capabilities" in w


def test_global_event_stream(client):
    with client.websocket_connect("/v1/events/stream") as ws:
        job = client.post("/v1/jobs", json=_atomic_job()).json()
        seen = False
        for _ in range(20):
            msg = ws.receive_json()
            if msg.get("type") == "job_submitted" and msg.get("job_id") == job["id"]:
                seen = True
                break
        assert seen


def test_large_json_request_rejected(tmp_path):
    config = ServerConfig(state_dir=str(tmp_path / "state"), max_request_mb=0)  # 0 MB cap
    with TestClient(create_app(config)) as c:
        r = c.post("/v1/jobs", json=_atomic_job())
        assert r.status_code == 413
        # Even the early rejection advertises the server's protocol version.
        assert r.headers["X-N4C-Version"] and r.headers["X-N4C-Api"]


def test_cors_header_when_enabled(tmp_path):
    config = ServerConfig(state_dir=str(tmp_path / "state"), cors_origins=["http://example.com"])
    with TestClient(create_app(config)) as c:
        r = c.get("/v1/jobs", headers={"Origin": "http://example.com"})
        assert r.headers.get("access-control-allow-origin") == "http://example.com"


def test_atomic_job_full_lifecycle(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    assert job["status"] == "queued"
    assert job["aggregate"]["num_tasks"] == 1

    worker = _register(client)
    task = _lease(client, worker)
    assert task is not None
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
    _complete(client, worker, task["task_id"], {"best_rmse": 0.42, "best_r2": 0.9})

    job = client.get(f"/v1/jobs/{job['id']}").json()
    assert job["status"] == "succeeded"
    assert job["aggregate"]["num_succeeded"] == 1
    assert job["aggregate"]["best_metric"] == 0.42
    assert len(job["aggregate"]["ranking"]) == 1


def test_native_payload_robustness_handoff_survives_submit_and_lease(client):
    req = _atomic_job()
    req["nativePayload"] = _native_payload()

    job = client.post("/v1/jobs", json=req).json()
    worker = _register(client)
    task = _lease(client, worker)

    assert task["nativePayload"]["manifest"]["robustnessEvidencePublicationHandoff"] == {
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
    }
    stored = client.app.state.db.get_job(job["id"])["request_json"]
    assert "native_payload" in stored
    assert "robustness_evidence_publication_handoff" in stored


def test_matrix_decomposition_and_ranking(client):
    req = {
        "type": "nirs4all.run",
        "pipeline": {"kind": "path", "path": "/p.yaml"},
        "datasets": [
            {"kind": "shared_path", "path": "/a", "name": "A"},
            {"kind": "shared_path", "path": "/b", "name": "B"},
            {"kind": "shared_path", "path": "/c", "name": "C"},
        ],
        "outputs": {"export_best_model": False},
        "rank_metric": "best_rmse",
        "rank_mode": "min",
    }
    job = client.post("/v1/jobs", json=req).json()
    assert job["aggregate"]["num_tasks"] == 3

    worker = _register(client, slots_total=3)
    rmses = {"A": 0.5, "B": 0.2, "C": 0.8}
    for _ in range(3):
        task = _lease(client, worker)
        client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
        _complete(client, worker, task["task_id"], {"best_rmse": rmses[task["dataset"]["name"]]})

    job = client.get(f"/v1/jobs/{job['id']}").json()
    assert job["status"] == "succeeded"
    assert job["aggregate"]["num_succeeded"] == 3
    ranking = job["aggregate"]["ranking"]
    assert [r["dataset"] for r in ranking] == ["B", "A", "C"]  # sorted ascending rmse
    assert job["aggregate"]["best_metric"] == 0.2


def test_artifact_upload_download_and_best_model_link(client):
    req = _atomic_job()
    req["outputs"]["export_best_model"] = True
    job = client.post("/v1/jobs", json=req).json()
    worker = _register(client)
    task = _lease(client, worker)
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()

    # Transport-only synthetic bytes: the server does not scientifically certify a trusted provider.
    files = {"file": ("best_model.n4a", b"FAKE-N4A-BYTES", "application/octet-stream")}
    up = client.post(
        f"/v1/tasks/{task['task_id']}/artifacts", params={"role": "model", "kind": "model"}, files=files
    ).json()
    artifact_id = up["artifact_id"]
    _complete(
        client,
        worker,
        task["task_id"],
        {"best_rmse": 0.3},
        artifacts={"model": artifact_id},
        delivery={
            "status": "exported",
            "profile": "synthetic-transport-test",
            "bundle_format_version": "test",
            "sha256": up["sha256"],
            "size_bytes": up["size_bytes"],
        },
    )

    arts = client.get(f"/v1/jobs/{job['id']}/artifacts").json()
    roles = {a["role"] for a in arts}
    assert "model" in roles and "best_model" in roles
    # download round-trips the bytes
    dl = client.get(f"/v1/artifacts/{artifact_id}")
    assert dl.content == b"FAKE-N4A-BYTES"


def _canonical_delivery(up, **overrides):
    delivery = {
        "status": "exported",
        "profile": "nirs4all.n4a.container.v1",
        "physical_profile": "synthetic.physical.v1",
        "container_id": "container:synthetic",
        "owner_identities": {"model": "fp-model", "run": "fp-run", "result": "fp-result", "score": "fp-score"},
        "owner_version": "9.9.9",
        "sha256": up["sha256"],
        "size_bytes": up["size_bytes"],
        "canonical_core": True,
    }
    delivery.update(overrides)
    return delivery


def test_canonical_delivery_metadata_is_stored_opaquely_and_gated(client):
    """A canonical delivery has no bundle version; the server stores it without parsing archives."""
    req = _atomic_job()
    req["outputs"]["export_best_model"] = True
    job = client.post("/v1/jobs", json=req).json()
    worker = _register(client)
    task = _lease(client, worker)
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
    up = client.post(
        f"/v1/tasks/{task['task_id']}/artifacts",
        params={"role": "model", "kind": "model"},
        files={"file": ("best_model.n4a", b"SYNTHETIC-CANONICAL-BYTES", "application/octet-stream")},
    ).json()
    complete = f"/v1/tasks/{task['task_id']}/complete"

    def post(delivery):
        body = {
            "status": "succeeded",
            "metrics": {"best_rmse": 0.3},
            "artifacts": {"model": up["artifact_id"]},
            "delivery": delivery,
        }
        return client.post(complete, params={"worker_id": worker}, json=body)

    # Refused: no profile, neither a bundle version nor a container id, or bytes that differ from the upload.
    assert post(_canonical_delivery(up, profile=None)).status_code == 409
    assert post(_canonical_delivery(up, container_id=None)).status_code == 409
    assert post(_canonical_delivery(up, sha256="0" * 64)).status_code == 409

    assert post(_canonical_delivery(up)).status_code == 200
    stored = client.get(f"/v1/jobs/{job['id']}/tasks").json()[0]["result"]["delivery"]
    assert stored["container_id"] == "container:synthetic"
    assert stored["physical_profile"] == "synthetic.physical.v1"
    assert stored["owner_identities"]["score"] == "fp-score"
    assert stored["bundle_format_version"] is None and stored["canonical_core"] is True
    assert client.get(f"/v1/artifacts/{up['artifact_id']}").content == b"SYNTHETIC-CANONICAL-BYTES"


def test_cancel_queued_job_not_relaunched(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    cancelled = client.post(f"/v1/jobs/{job['id']}/cancel").json()
    assert cancelled["status"] == "cancelled"
    # the queued task is now cancelled -> a worker leases nothing
    worker = _register(client)
    assert _lease(client, worker) is None


def test_cancel_running_job(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    worker = _register(client)
    task = _lease(client, worker)
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()

    j = client.post(f"/v1/jobs/{job['id']}/cancel").json()
    assert j["status"] == "cancelling"
    # heartbeat tells the worker to stop the in-flight task
    hb = client.post(f"/v1/workers/{worker}/heartbeat").json()
    assert task["task_id"] in hb["cancel_task_ids"]
    # worker reports the task failed/stopped -> job becomes cancelled, not failed
    client.post(
        f"/v1/tasks/{task['task_id']}/fail",
        params={"worker_id": worker},
        json={"error": "cancelled", "retriable": False},
    ).raise_for_status()
    job = client.get(f"/v1/jobs/{job['id']}").json()
    assert job["status"] == "cancelled"


def test_cancelled_success_report_emits_cancelled_not_completed(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    worker = _register(client)
    task = _lease(client, worker)
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
    client.post(f"/v1/jobs/{job['id']}/cancel").raise_for_status()

    _complete(client, worker, task["task_id"], {"best_rmse": 0.2})

    job = client.get(f"/v1/jobs/{job['id']}").json()
    assert job["status"] == "cancelled"
    events = client.get(f"/v1/jobs/{job['id']}/events").json()
    task_events = [event for event in events if event["task_id"] == task["task_id"]]
    assert any(event["type"] == "task_cancelled" for event in task_events)
    assert all(event["type"] != "task_completed" for event in task_events)


def test_running_task_retriable_failure_requeues_via_api(client):
    """A worker reporting a retriable failure on a started (running) task must get
    a 200 with requeued=True — not a 500 from an illegal running -> queued."""
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    worker = _register(client)
    task = _lease(client, worker)
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()

    resp = client.post(
        f"/v1/tasks/{task['task_id']}/fail",
        params={"worker_id": worker},
        json={"error": "runner boom", "retriable": True},
    )
    assert resp.status_code == 200
    assert resp.json()["requeued"] is True
    # Job stays running (task requeued), and the worker can lease it again.
    assert client.get(f"/v1/jobs/{job['id']}").json()["status"] == "running"
    retry = _lease(client, worker)
    assert retry["task_id"] == task["task_id"]
    assert retry["attempt"] == 2


def test_lease_expiry_retry_then_succeed(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    worker = _register(client)
    # Lease with a tiny TTL by leasing then forcing a reap.
    db = client.app.state.db
    payload = db.lease_next_task(worker, lease_ttl_s=0.01)
    assert payload.attempt == 1
    import time

    time.sleep(0.05)
    db.reap_expired_leases()

    task = _lease(client, worker)  # requeued, leasable again
    assert task["attempt"] == 2
    client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
    _complete(client, worker, task["task_id"], {"best_rmse": 0.1})
    assert client.get(f"/v1/jobs/{job['id']}").json()["status"] == "succeeded"


def test_idempotent_submit(client):
    req = _atomic_job()
    req["idempotency_key"] = "key-123"
    first = client.post("/v1/jobs", json=req).json()
    second = client.post("/v1/jobs", json=req).json()
    assert first["id"] == second["id"]


def test_python_entrypoint_rejected_without_flag(client):
    req = {
        "type": "nirs4all.run",
        "pipeline": {"kind": "python_entrypoint", "entrypoint": "mod:build"},
        "dataset": {"kind": "shared_path", "path": "/a"},
    }
    resp = client.post("/v1/jobs", json=req)
    assert resp.status_code == 400


def test_events_recorded(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    events = client.get(f"/v1/jobs/{job['id']}/events").json()
    assert any(e["type"] == "job_submitted" for e in events)


def test_worker_without_nirs4all_does_not_lease(client):
    """Availability: an nirs4all.run job is not routed to a worker lacking nirs4all."""
    client.post("/v1/jobs", json=_atomic_job()).raise_for_status()
    # Worker declares no packages -> nirs4all unavailable.
    resp = client.post("/v1/workers/register", json={"slots_total": 1, "version": {"packages": {}}})
    worker = resp.json()["worker_id"]
    assert _lease(client, worker) is None
    # A worker that has nirs4all can take it.
    ok_worker = _register(client)
    assert _lease(client, ok_worker) is not None


def test_version_constraint_routing(client):
    """A version range routes only to workers whose declared version satisfies it."""
    req = _atomic_job()
    req["requirements"] = {"packages": {"nirs4all": ">=0.9,<0.10"}}
    client.post("/v1/jobs", json=req).raise_for_status()

    too_old = client.post(
        "/v1/workers/register", json={"slots_total": 1, "version": {"packages": {"nirs4all": "0.8.5"}}}
    ).json()["worker_id"]
    assert _lease(client, too_old) is None  # 0.8.5 not in >=0.9,<0.10

    ok = client.post(
        "/v1/workers/register", json={"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}}
    ).json()["worker_id"]
    assert _lease(client, ok) is not None


def test_gpu_routing(client):
    """A min_gpu_count requirement routes only to workers declaring enough GPUs."""
    req = _atomic_job()
    req["requirements"] = {"min_gpu_count": 1}
    client.post("/v1/jobs", json=req).raise_for_status()

    cpu = client.post(
        "/v1/workers/register",
        json={"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}, "capabilities": {"gpu_count": 0}},
    ).json()["worker_id"]
    assert _lease(client, cpu) is None

    gpu = client.post(
        "/v1/workers/register",
        json={
            "slots_total": 1,
            "version": {"packages": {"nirs4all": "0.9.1"}},
            "capabilities": {"gpu_count": 2, "cuda": True},
            "labels": {"cuda": "true"},
        },
    ).json()["worker_id"]
    assert _lease(client, gpu) is not None


def test_invalid_version_specifier_rejected(client):
    req = _atomic_job()
    req["requirements"] = {"packages": {"nirs4all": "not-a-specifier"}}
    assert client.post("/v1/jobs", json=req).status_code == 422


def test_empty_pipelines_rejected(client):
    req = {
        "type": "nirs4all.run",
        "pipelines": [],
        "dataset": {"kind": "shared_path", "path": "/a"},
    }
    assert client.post("/v1/jobs", json=req).status_code == 422


def test_bad_event_level_rejected_at_boundary(client):
    job = client.post("/v1/jobs", json=_atomic_job()).json()
    worker = _register(client)
    task = _lease(client, worker)
    # An out-of-enum level is rejected with 422 at the boundary, so it can never
    # be persisted and 500 the events read path later.
    bad = client.post(f"/v1/tasks/{task['task_id']}/events", json={"level": "trace", "message": "x"})
    assert bad.status_code == 422
    # The events read path stays healthy.
    assert client.get(f"/v1/jobs/{job['id']}/events").status_code == 200


def test_best_model_uses_latest_winner_in_matrix(client):
    req = {
        "type": "nirs4all.run",
        "pipeline": {"kind": "path", "path": "/p.yaml"},
        "datasets": [
            {"kind": "shared_path", "path": "/a", "name": "A"},
            {"kind": "shared_path", "path": "/b", "name": "B"},
        ],
        "outputs": {"export_best_model": True},
        "rank_metric": "best_rmse",
        "rank_mode": "min",
    }
    job = client.post("/v1/jobs", json=req).json()
    worker = _register(client, slots_total=2)
    # Complete the worse one first (A=0.9), then the better one (B=0.1).
    order = {"A": 0.9, "B": 0.1}
    art_by_ds = {}
    for _ in range(2):
        task = _lease(client, worker)
        ds = task["dataset"]["name"]
        client.post(f"/v1/tasks/{task['task_id']}/start", params={"worker_id": worker}).raise_for_status()
        files = {"file": (f"{ds}.n4a", f"MODEL-{ds}".encode(), "application/octet-stream")}
        up = client.post(
            f"/v1/tasks/{task['task_id']}/artifacts", params={"role": "model", "kind": "model"}, files=files
        ).json()
        art_by_ds[ds] = up["artifact_id"]
        _complete(
            client,
            worker,
            task["task_id"],
            {"best_rmse": order[ds]},
            artifacts={"model": up["artifact_id"]},
            delivery={
                "status": "exported",
                "profile": "synthetic-transport-test",
                "bundle_format_version": "test",
                "sha256": up["sha256"],
                "size_bytes": up["size_bytes"],
            },
        )

    job = client.get(f"/v1/jobs/{job['id']}").json()
    # Exactly one best_model link, pointing at the better (B) model.
    arts = client.get(f"/v1/jobs/{job['id']}/artifacts").json()
    best_links = [a for a in arts if a["role"] == "best_model"]
    assert len(best_links) == 1
    assert job["aggregate"]["best_model_artifact_id"] == art_by_ds["B"]
