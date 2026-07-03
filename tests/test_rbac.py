"""RBAC: credential-bound rights for {submit, read, cancel, execute, admin}.

Two layers:
- unit tests over ``server/auth.py`` (role→rights, principal matching, dev mode);
- end-to-end route enforcement via ``TestClient`` (each role can only reach the
  routes its rights gate; the advisory ``X-N4C-Role`` header confers nothing).

No nirs4all needed — same as the rest of the API suite.
"""

import json
import time

import anyio
import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.server.auth import (
    ALL_RIGHTS,
    AuthError,
    Authorizer,
    Principal,
    Right,
    bearer_token,
    rights_from_roles,
)

# --------------------------------------------------------------------------- #
# Unit — server/auth.py
# --------------------------------------------------------------------------- #


def test_roles_resolve_to_expected_rights():
    assert rights_from_roles(["submitter"]) == frozenset({Right.SUBMIT, Right.READ, Right.CANCEL})
    assert rights_from_roles(["executor"]) == frozenset({Right.READ, Right.EXECUTE})
    assert rights_from_roles(["viewer"]) == frozenset({Right.READ})
    assert rights_from_roles(["admin"]) == ALL_RIGHTS
    # Composing roles unions their rights.
    assert rights_from_roles(["submitter", "executor"]) == frozenset(
        {Right.SUBMIT, Right.READ, Right.CANCEL, Right.EXECUTE}
    )


def test_unknown_role_rejected():
    with pytest.raises(ValueError):
        rights_from_roles(["root"])


def test_admin_is_a_wildcard():
    admin = Principal.from_roles("ops", "t", ["admin"])
    assert all(admin.has(r) for r in Right)
    viewer = Principal.from_roles("v", "t", ["viewer"])
    assert viewer.has(Right.READ)
    assert not viewer.has(Right.SUBMIT)


def test_bearer_token_parsing():
    assert bearer_token("Bearer abc") == "abc"
    assert bearer_token("bearer abc") == "abc"  # scheme is case-insensitive
    assert bearer_token("Token abc") is None
    assert bearer_token("Bearer ") is None
    assert bearer_token(None) is None


def test_open_mode_grants_everything():
    authz = Authorizer()  # no principals → dev/open mode
    assert not authz.enforced
    granted = authz.check(None, [Right.ADMIN, Right.SUBMIT, Right.EXECUTE])
    assert granted.name == "dev"


def test_enforced_rejects_unknown_or_missing_token():
    authz = Authorizer([Principal.from_roles("a", "s", ["viewer"])])
    assert authz.enforced
    with pytest.raises(AuthError) as wrong:
        authz.check("n", [Right.READ])
    assert wrong.value.status == 401
    with pytest.raises(AuthError) as missing:
        authz.check(None, [Right.READ])
    assert missing.value.status == 401


def test_enforced_missing_right_is_403():
    authz = Authorizer([Principal.from_roles("v", "s", ["viewer"])])
    assert authz.check("s", [Right.READ]).name == "v"
    with pytest.raises(AuthError) as exc:
        authz.check("s", [Right.SUBMIT])
    assert exc.value.status == 403


# --------------------------------------------------------------------------- #
# End-to-end route enforcement
# --------------------------------------------------------------------------- #

SUBMITTER = "s"
EXECUTOR = "e"
EXECUTOR_2 = "x"
VIEWER = "v"
ADMIN = "a"
SUBMIT_ONLY = "u"


def _rbac_app(tmp_path):
    config = ServerConfig(
        state_dir=str(tmp_path / "state"),
        principals=[
            Principal.from_roles("alice", SUBMITTER, ["submitter"]),
            Principal.from_roles("worker1", EXECUTOR, ["executor"]),
            Principal.from_roles("worker2", EXECUTOR_2, ["executor"]),
            Principal.from_roles("dash", VIEWER, ["viewer"]),
            Principal.from_roles("ops", ADMIN, ["admin"]),
            Principal(name="uploadbot", token=SUBMIT_ONLY, rights=frozenset({Right.SUBMIT})),
        ],
    )
    return create_app(config)


def _hdr(token):
    return {"Authorization": f"Bearer {token}"}


def _job():
    return {
        "type": "nirs4all.run",
        "pipeline": {"kind": "path", "path": "/p.yaml"},
        "dataset": {"kind": "shared_path", "path": "/a"},
    }


def _dag_job():
    return {
        "type": "nirs4all.run",
        "name": "dag-rights",
        "pipeline": {
            "kind": "inline_json",
            "inline": {
                "dagml": {
                    "nodes": [
                        {"id": "source", "op": "DATASET", "deps": []},
                        {"id": "preproc", "op": "PREPROCESS", "deps": ["source"]},
                        {"id": "fit", "op": "FIT_CV", "deps": ["preproc"]},
                        {"id": "refit", "op": "REFIT", "deps": ["fit"]},
                    ]
                }
            },
        },
        "dataset": {"kind": "shared_path", "path": "/a", "name": "A"},
        "rank_metric": "best_rmse",
    }


def _worker_body():
    return {"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}}


def _assert_missing_right(response, right: str) -> None:
    assert response.status_code == 403
    detail = response.json()["detail"]
    assert "principal 'dash'" in detail
    assert f"required right(s): {right}" in detail


def _upload_input_artifact(client, token, payload=b"artifact payload"):
    return client.post(
        "/v1/artifacts",
        files={"file": ("input.bin", payload, "application/octet-stream")},
        headers=_hdr(token),
    )


def _receive_ws_json_matching(ws, predicate, *, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    last_payload = None
    while time.monotonic() < deadline:
        try:
            message = ws.portal.call(ws._send_rx.receive_nowait)
        except anyio.WouldBlock:
            time.sleep(0.01)
            continue
        ws._raise_on_close(message)
        payload = json.loads(message["text"] if "text" in message else message["bytes"].decode("utf-8"))
        last_payload = payload
        if predicate(payload):
            return payload
    raise AssertionError(f"timed out waiting for websocket payload; last={last_payload!r}")


def _wait_for_event_subscriber(client, *, job_id=None, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        broker = client.app.state.broker
        if job_id is None:
            if broker._global:
                return
        elif broker._subscribers.get(job_id):
            return
        time.sleep(0.01)
    target = "global feed" if job_id is None else f"job {job_id}"
    raise AssertionError(f"timed out waiting for websocket subscriber on {target}")


@pytest.fixture
def rbac_client(tmp_path):
    with TestClient(_rbac_app(tmp_path)) as c:
        yield c


def test_missing_token_401_when_enforced(rbac_client):
    assert rbac_client.get("/v1/jobs").status_code == 401
    assert rbac_client.post("/v1/jobs", json=_job()).status_code == 401


def test_submitter_can_submit_read_cancel_not_execute(rbac_client):
    r = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(SUBMITTER))
    assert r.status_code == 200
    job_id = r.json()["id"]
    assert rbac_client.get("/v1/jobs", headers=_hdr(SUBMITTER)).status_code == 200
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(SUBMITTER)).status_code == 200
    assert rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(SUBMITTER)).status_code == 200
    # execute routes are denied for a submitter
    assert rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(SUBMITTER)).status_code == 403


def test_executor_can_register_lease_read_not_submit_cancel(rbac_client):
    reg = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR))
    assert reg.status_code == 200
    worker_id = reg.json()["worker_id"]
    # lease + heartbeat are execute routes
    assert rbac_client.post(f"/v1/workers/{worker_id}/lease", headers=_hdr(EXECUTOR)).status_code == 200
    assert rbac_client.post(f"/v1/workers/{worker_id}/heartbeat", headers=_hdr(EXECUTOR)).status_code == 200
    # executor also holds read
    assert rbac_client.get("/v1/jobs", headers=_hdr(EXECUTOR)).status_code == 200
    # but cannot submit or cancel
    assert rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(EXECUTOR)).status_code == 403
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    assert rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(EXECUTOR)).status_code == 403


def test_viewer_is_read_only(rbac_client):
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    worker_id = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()[
        "worker_id"
    ]
    task_id = rbac_client.post(f"/v1/workers/{worker_id}/lease", headers=_hdr(EXECUTOR)).json()["task"]["task_id"]

    assert rbac_client.get("/v1/jobs", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get(f"/v1/jobs/{job_id}/tasks", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get(f"/v1/jobs/{job_id}/events", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get(f"/v1/jobs/{job_id}/artifacts", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get("/v1/stats", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get("/v1/workers", headers=_hdr(VIEWER)).status_code == 200

    _assert_missing_right(rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(VIEWER)), "submit")
    _assert_missing_right(rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(VIEWER)), "cancel")
    _assert_missing_right(
        rbac_client.post(
            "/v1/artifacts",
            params={"kind": "input"},
            files={"file": ("pipeline.yaml", b"pipeline: {}", "text/yaml")},
            headers=_hdr(VIEWER),
        ),
        "submit",
    )
    _assert_missing_right(
        rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(VIEWER)),
        "execute",
    )
    _assert_missing_right(rbac_client.post(f"/v1/workers/{worker_id}/heartbeat", headers=_hdr(VIEWER)), "execute")
    _assert_missing_right(rbac_client.post(f"/v1/workers/{worker_id}/lease", headers=_hdr(VIEWER)), "execute")
    _assert_missing_right(
        rbac_client.post(f"/v1/tasks/{task_id}/start", params={"worker_id": worker_id}, headers=_hdr(VIEWER)),
        "execute",
    )
    _assert_missing_right(
        rbac_client.post(
            f"/v1/tasks/{task_id}/events",
            json={"level": "info", "message": "viewer cannot mutate"},
            headers=_hdr(VIEWER),
        ),
        "execute",
    )
    _assert_missing_right(
        rbac_client.post(
            f"/v1/tasks/{task_id}/artifacts",
            params={"role": "logs", "kind": "log"},
            files={"file": ("log.txt", b"viewer cannot mutate", "text/plain")},
            headers=_hdr(VIEWER),
        ),
        "execute",
    )
    _assert_missing_right(
        rbac_client.post(
            f"/v1/tasks/{task_id}/complete",
            params={"worker_id": worker_id},
            json={"status": "succeeded", "duration_seconds": 0.1, "metrics": {"best_rmse": 0.2}},
            headers=_hdr(VIEWER),
        ),
        "execute",
    )
    _assert_missing_right(
        rbac_client.post(
            f"/v1/tasks/{task_id}/fail",
            params={"worker_id": worker_id},
            json={"error": "viewer cannot mutate"},
            headers=_hdr(VIEWER),
        ),
        "execute",
    )


def test_admin_can_do_everything(rbac_client):
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(ADMIN)).status_code == 200
    assert rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(ADMIN)).status_code == 200
    assert rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(ADMIN)).status_code == 200


def test_input_artifact_upload_requires_submit(rbac_client):
    assert _upload_input_artifact(rbac_client, VIEWER).status_code == 403
    assert _upload_input_artifact(rbac_client, EXECUTOR).status_code == 403

    submit_only = _upload_input_artifact(rbac_client, SUBMIT_ONLY)
    assert submit_only.status_code == 200
    assert submit_only.json()["artifact_id"]

    submitter = _upload_input_artifact(rbac_client, SUBMITTER)
    assert submitter.status_code == 200
    assert submitter.json()["artifact_id"]

    admin = _upload_input_artifact(rbac_client, ADMIN)
    assert admin.status_code == 200
    assert admin.json()["artifact_id"]


def test_artifact_download_requires_read_after_upload(rbac_client):
    payload = b"dataset bytes"
    artifact_id = _upload_input_artifact(rbac_client, SUBMITTER, payload=payload).json()["artifact_id"]

    for token in (VIEWER, EXECUTOR, SUBMITTER):
        downloaded = rbac_client.get(f"/v1/artifacts/{artifact_id}", headers=_hdr(token))
        assert downloaded.status_code == 200
        assert downloaded.content == payload

    readless = rbac_client.get(f"/v1/artifacts/{artifact_id}", headers=_hdr(SUBMIT_ONLY))
    assert readless.status_code == 403
    assert "read" in readless.json()["detail"]
    assert rbac_client.get(f"/v1/artifacts/{artifact_id}", headers=_hdr("n")).status_code == 401
    assert rbac_client.get(f"/v1/artifacts/{artifact_id}").status_code == 401


def test_register_echoes_granted_rights(rbac_client):
    reg = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()
    assert set(reg["rights"]) == {"read", "execute"}


def test_worker_lifecycle_is_bound_to_registering_principal(rbac_client):
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    worker1 = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()[
        "worker_id"
    ]
    worker2 = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR_2)).json()[
        "worker_id"
    ]

    assert rbac_client.post(f"/v1/workers/{worker2}/heartbeat", headers=_hdr(EXECUTOR_2)).status_code == 200
    stolen_heartbeat = rbac_client.post(f"/v1/workers/{worker1}/heartbeat", headers=_hdr(EXECUTOR_2))
    assert stolen_heartbeat.status_code == 403
    assert "not registered for worker" in stolen_heartbeat.json()["detail"]
    assert rbac_client.post(f"/v1/workers/{worker1}/lease", headers=_hdr(EXECUTOR_2)).status_code == 403

    leased = rbac_client.post(f"/v1/workers/{worker1}/lease", headers=_hdr(EXECUTOR)).json()["task"]
    task_id = leased["task_id"]

    assert (
        rbac_client.post(
            f"/v1/tasks/{task_id}/start",
            params={"worker_id": worker1},
            headers=_hdr(EXECUTOR_2),
        ).status_code
        == 403
    )
    assert (
        rbac_client.post(
            f"/v1/tasks/{task_id}/events",
            json={"level": "info", "message": "spoofed"},
            headers=_hdr(EXECUTOR_2),
        ).status_code
        == 403
    )
    artifact = rbac_client.post(
        f"/v1/tasks/{task_id}/artifacts",
        params={"role": "logs", "kind": "log"},
        files={"file": ("log.txt", b"spoofed", "text/plain")},
        headers=_hdr(EXECUTOR_2),
    )
    assert artifact.status_code == 403

    result_body = {
        "status": "succeeded",
        "duration_seconds": 0.1,
        "metrics": {"best_rmse": 0.2},
        "artifacts": {},
    }
    assert (
        rbac_client.post(
            f"/v1/tasks/{task_id}/complete",
            params={"worker_id": worker1},
            json=result_body,
            headers=_hdr(EXECUTOR_2),
        ).status_code
        == 403
    )
    assert (
        rbac_client.post(
            f"/v1/tasks/{task_id}/fail",
            params={"worker_id": worker1},
            json={"error": "spoofed"},
            headers=_hdr(EXECUTOR_2),
        ).status_code
        == 403
    )

    rbac_client.post(f"/v1/tasks/{task_id}/start", params={"worker_id": worker1}, headers=_hdr(EXECUTOR)).raise_for_status()
    rbac_client.post(
        f"/v1/tasks/{task_id}/complete",
        params={"worker_id": worker1},
        json=result_body,
        headers=_hdr(EXECUTOR),
    ).raise_for_status()
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(ADMIN)).json()["status"] == "succeeded"


def test_cancelling_task_report_requires_assigned_executor(rbac_client):
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    worker1 = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()[
        "worker_id"
    ]
    worker2 = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR_2)).json()[
        "worker_id"
    ]
    leased = rbac_client.post(f"/v1/workers/{worker1}/lease", headers=_hdr(EXECUTOR)).json()["task"]
    task_id = leased["task_id"]
    rbac_client.post(f"/v1/tasks/{task_id}/start", params={"worker_id": worker1}, headers=_hdr(EXECUTOR)).raise_for_status()
    rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(ADMIN)).raise_for_status()

    spoofed_failure = rbac_client.post(
        f"/v1/tasks/{task_id}/fail",
        params={"worker_id": worker2},
        json={"error": "cancelled", "retriable": False},
        headers=_hdr(EXECUTOR_2),
    )
    assert spoofed_failure.status_code == 403
    spoofed_success = rbac_client.post(
        f"/v1/tasks/{task_id}/complete",
        params={"worker_id": worker2},
        json={"status": "succeeded", "duration_seconds": 0.1, "metrics": {"best_rmse": 0.2}},
        headers=_hdr(EXECUTOR_2),
    )
    assert spoofed_success.status_code == 403

    rbac_client.post(
        f"/v1/tasks/{task_id}/fail",
        params={"worker_id": worker1},
        json={"error": "cancelled", "retriable": False},
        headers=_hdr(EXECUTOR),
    ).raise_for_status()
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(ADMIN)).json()["status"] == "cancelled"


def test_dag_job_leases_only_to_capable_executor(rbac_client):
    req = _dag_job()
    req["requirements"] = {"labels": {"site": "lab-b"}, "min_gpu_count": 1}
    rbac_client.post("/v1/jobs", json=req, headers=_hdr(SUBMITTER)).raise_for_status()

    wrong_site = _worker_body()
    wrong_site.update({"labels": {"site": "lab-a"}, "capabilities": {"gpu_count": 2}})
    worker_a = rbac_client.post("/v1/workers/register", json=wrong_site, headers=_hdr(EXECUTOR)).json()["worker_id"]
    assert rbac_client.post(f"/v1/workers/{worker_a}/lease", headers=_hdr(EXECUTOR)).json()["task"] is None

    no_gpu = _worker_body()
    no_gpu.update({"labels": {"site": "lab-b"}, "capabilities": {"gpu_count": 0}})
    worker_cpu = rbac_client.post("/v1/workers/register", json=no_gpu, headers=_hdr(EXECUTOR)).json()["worker_id"]
    assert rbac_client.post(f"/v1/workers/{worker_cpu}/lease", headers=_hdr(EXECUTOR)).json()["task"] is None

    capable = _worker_body()
    capable.update({"labels": {"site": "lab-b"}, "capabilities": {"gpu_count": 1}})
    worker_gpu = rbac_client.post("/v1/workers/register", json=capable, headers=_hdr(EXECUTOR)).json()["worker_id"]
    leased = rbac_client.post(f"/v1/workers/{worker_gpu}/lease", headers=_hdr(EXECUTOR)).json()["task"]
    assert leased is not None
    assert leased["scheduler"]["shape"] == "dag_shaped_whole_run"
    assert leased["assignment"]["executor_principal"] == "worker1"


def test_dag_scheduler_contract_records_rights_and_result_provenance(rbac_client):
    submitted = rbac_client.post("/v1/jobs", json=_dag_job(), headers=_hdr(SUBMITTER))
    assert submitted.status_code == 200
    job = submitted.json()
    assert job["submission"]["mode"] == "client_submitted"
    assert job["submission"]["principal"] == "alice"
    assert job["submission"]["required_rights"] == ["submit"]
    assert set(job["submission"]["granted_rights"]) == {"submit", "read", "cancel"}
    assert job["scheduler"] == {
        "shape": "dag_shaped_whole_run",
        "task_granularity": "whole_nirs4all_run",
        "assignment_mode": "server_leased_executor",
        "result_provenance": "server_attested_worker_report",
        "submit_rights_required": ["submit"],
        "execute_rights_required": ["execute"],
    }

    reg = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()
    worker_id = reg["worker_id"]
    leased = rbac_client.post(f"/v1/workers/{worker_id}/lease", headers=_hdr(EXECUTOR)).json()["task"]
    assert leased["submission"]["principal"] == "alice"
    assert leased["scheduler"]["shape"] == "dag_shaped_whole_run"
    assert leased["assignment"]["mode"] == "server_leased_executor"
    assert leased["assignment"]["assigned_by"] == "server"
    assert leased["assignment"]["executor_principal"] == "worker1"
    assert leased["assignment"]["worker_id"] == worker_id
    assert leased["assignment"]["required_rights"] == ["execute"]
    assert set(leased["assignment"]["granted_rights"]) == {"read", "execute"}

    result_body = {
        "status": "succeeded",
        "nirs4all_version": "0.9.1",
        "duration_seconds": 0.5,
        "metrics": {"best_rmse": 0.12, "best_r2": 0.8},
        "counts": {"num_predictions": 3},
        "artifacts": {"model": None, "logs": None, "workspace": None},
        "extra": {"dag_trace": {"terminal": "refit"}, "stable_field": "preserved"},
    }
    denied = rbac_client.post(
        f"/v1/tasks/{leased['task_id']}/complete",
        params={"worker_id": worker_id},
        json=result_body,
        headers=_hdr(SUBMITTER),
    )
    assert denied.status_code == 403
    assert "execute" in denied.json()["detail"]

    rbac_client.post(
        f"/v1/tasks/{leased['task_id']}/start", params={"worker_id": worker_id}, headers=_hdr(EXECUTOR)
    ).raise_for_status()
    rbac_client.post(
        f"/v1/tasks/{leased['task_id']}/complete",
        params={"worker_id": worker_id},
        json=result_body,
        headers=_hdr(EXECUTOR),
    ).raise_for_status()

    task_view = rbac_client.get(f"/v1/jobs/{job['id']}/tasks", headers=_hdr(SUBMITTER)).json()[0]
    result = task_view["result"]
    assert result["metrics"]["best_rmse"] == 0.12
    assert result["extra"] == {"dag_trace": {"terminal": "refit"}, "stable_field": "preserved"}
    assert result["provenance"]["source"] == "worker_report"
    assert result["provenance"]["reported_by_principal"] == "worker1"
    assert result["provenance"]["worker_id"] == worker_id
    assert result["provenance"]["job_id"] == job["id"]
    assert result["provenance"]["task_id"] == leased["task_id"]
    assert result["provenance"]["attempt"] == 1
    assert result["provenance"]["required_rights"] == ["execute"]
    assert set(result["provenance"]["granted_rights"]) == {"read", "execute"}

    finished = rbac_client.get(f"/v1/jobs/{job['id']}", headers=_hdr(SUBMITTER)).json()
    assert finished["status"] == "succeeded"
    assert finished["aggregate"]["best_metric"] == 0.12


def test_scheduler_shape_is_server_normalized(rbac_client):
    req = _dag_job()
    req["scheduler"] = {
        "shape": "atomic",
        "submit_rights_required": ["admin"],
        "execute_rights_required": ["admin"],
    }

    submitted = rbac_client.post("/v1/jobs", json=req, headers=_hdr(SUBMITTER))
    assert submitted.status_code == 200
    job = submitted.json()
    assert job["scheduler"] == {
        "shape": "dag_shaped_whole_run",
        "task_granularity": "whole_nirs4all_run",
        "assignment_mode": "server_leased_executor",
        "result_provenance": "server_attested_worker_report",
        "submit_rights_required": ["submit"],
        "execute_rights_required": ["execute"],
    }


def test_server_reinfers_attested_scheduler_shape_from_payload(rbac_client):
    req = _dag_job()
    req["scheduler"] = {"shape": "atomic"}
    req["submission"] = {"principal": "admin"}

    submitted = rbac_client.post("/v1/jobs", json=req, headers=_hdr(ADMIN))
    assert submitted.status_code == 200
    job = submitted.json()
    assert job["scheduler"]["shape"] == "dag_shaped_whole_run"
    assert job["submission"]["principal"] == "ops"

    reg = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()
    leased = rbac_client.post(f"/v1/workers/{reg['worker_id']}/lease", headers=_hdr(EXECUTOR)).json()["task"]
    assert leased["scheduler"]["shape"] == "dag_shaped_whole_run"
    assert leased["submission"]["principal"] == "ops"


def test_role_header_confers_nothing(rbac_client):
    # A viewer self-asserting admin via the advisory X-N4C-Role header gains
    # nothing — rights come from the credential, not the header.
    r = rbac_client.post("/v1/jobs", json=_job(), headers={**_hdr(VIEWER), "X-N4C-Role": "admin"})
    assert r.status_code == 403


def test_ws_stream_requires_read(tmp_path):
    with TestClient(_rbac_app(tmp_path)) as c:
        # A viewer credential can stream the global feed.
        with c.websocket_connect(f"/v1/events/stream?token={VIEWER}") as ws:
            _wait_for_event_subscriber(c)
            c.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).raise_for_status()
            _receive_ws_json_matching(ws, lambda msg: msg.get("type") == "job_submitted")
        # An unknown token is rejected before the socket is accepted.
        for token in ("n", SUBMIT_ONLY):
            with pytest.raises(WebSocketDisconnect):
                with c.websocket_connect(f"/v1/events/stream?token={token}"):
                    pass


def test_job_specific_ws_stream_requires_read(tmp_path):
    with TestClient(_rbac_app(tmp_path)) as c:
        job = c.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()

        with c.websocket_connect(f"/v1/jobs/{job['id']}/events/stream?token={VIEWER}") as ws:
            _receive_ws_json_matching(
                ws,
                lambda msg: msg.get("type") == "job_submitted" and msg.get("job_id") == job["id"],
            )
            _wait_for_event_subscriber(c, job_id=job["id"])
            c.post(f"/v1/jobs/{job['id']}/cancel", headers=_hdr(ADMIN)).raise_for_status()
            _receive_ws_json_matching(
                ws,
                lambda msg: msg.get("type") == "job_cancel_requested" and msg.get("job_id") == job["id"],
            )

        for token in ("n", SUBMIT_ONLY):
            with pytest.raises(WebSocketDisconnect):
                with c.websocket_connect(f"/v1/jobs/{job['id']}/events/stream?token={token}"):
                    pass


def test_single_token_is_admin_equivalent(tmp_path):
    # Backward compatibility: a bare ``token`` still unlocks every route.
    config = ServerConfig(state_dir=str(tmp_path / "state"), token="solo")
    with TestClient(create_app(config)) as c:
        assert c.get("/v1/jobs").status_code == 401
        assert c.get("/v1/jobs", headers=_hdr("solo")).status_code == 200
        assert c.post("/v1/jobs", json=_job(), headers=_hdr("solo")).status_code == 200
        assert c.post("/v1/workers/register", json=_worker_body(), headers=_hdr("solo")).status_code == 200
