"""RBAC: credential-bound rights for {submit, read, cancel, execute, admin}.

Two layers:
- unit tests over ``server/auth.py`` (role→rights, principal matching, dev mode);
- end-to-end route enforcement via ``TestClient`` (each role can only reach the
  routes its rights gate; the advisory ``X-N4C-Role`` header confers nothing).

No nirs4all needed — same as the rest of the API suite.
"""

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
VIEWER = "v"
ADMIN = "a"


def _rbac_app(tmp_path):
    config = ServerConfig(
        state_dir=str(tmp_path / "state"),
        principals=[
            Principal.from_roles("alice", SUBMITTER, ["submitter"]),
            Principal.from_roles("worker1", EXECUTOR, ["executor"]),
            Principal.from_roles("dash", VIEWER, ["viewer"]),
            Principal.from_roles("ops", ADMIN, ["admin"]),
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


def _worker_body():
    return {"slots_total": 1, "version": {"packages": {"nirs4all": "0.9.1"}}}


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
    assert rbac_client.get("/v1/jobs", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get("/v1/stats", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.get("/v1/workers", headers=_hdr(VIEWER)).status_code == 200
    assert rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(VIEWER)).status_code == 403
    assert rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(VIEWER)).status_code == 403


def test_admin_can_do_everything(rbac_client):
    job_id = rbac_client.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).json()["id"]
    assert rbac_client.get(f"/v1/jobs/{job_id}", headers=_hdr(ADMIN)).status_code == 200
    assert rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(ADMIN)).status_code == 200
    assert rbac_client.post(f"/v1/jobs/{job_id}/cancel", headers=_hdr(ADMIN)).status_code == 200


def test_register_echoes_granted_rights(rbac_client):
    reg = rbac_client.post("/v1/workers/register", json=_worker_body(), headers=_hdr(EXECUTOR)).json()
    assert set(reg["rights"]) == {"read", "execute"}


def test_role_header_confers_nothing(rbac_client):
    # A viewer self-asserting admin via the advisory X-N4C-Role header gains
    # nothing — rights come from the credential, not the header.
    r = rbac_client.post("/v1/jobs", json=_job(), headers={**_hdr(VIEWER), "X-N4C-Role": "admin"})
    assert r.status_code == 403


def test_ws_stream_requires_read(tmp_path):
    with TestClient(_rbac_app(tmp_path)) as c:
        # A viewer credential can stream the global feed.
        with c.websocket_connect(f"/v1/events/stream?token={VIEWER}") as ws:
            c.post("/v1/jobs", json=_job(), headers=_hdr(ADMIN)).raise_for_status()
            seen = False
            for _ in range(20):
                if ws.receive_json().get("type") == "job_submitted":
                    seen = True
                    break
            assert seen
        # An unknown token is rejected before the socket is accepted.
        with pytest.raises(WebSocketDisconnect):
            with c.websocket_connect("/v1/events/stream?token=nope"):
                pass


def test_single_token_is_admin_equivalent(tmp_path):
    # Backward compatibility: a bare ``token`` still unlocks every route.
    config = ServerConfig(state_dir=str(tmp_path / "state"), token="solo")
    with TestClient(create_app(config)) as c:
        assert c.get("/v1/jobs").status_code == 401
        assert c.get("/v1/jobs", headers=_hdr("solo")).status_code == 200
        assert c.post("/v1/jobs", json=_job(), headers=_hdr("solo")).status_code == 200
        assert c.post("/v1/workers/register", json=_worker_body(), headers=_hdr("solo")).status_code == 200
