"""Protocol-v2 credential and project isolation counterexamples (no open harness)."""

import hashlib
import json
import time
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from nirs4all_access import (
    AccessError,
    CredentialStoreError,
    ProjectPrincipal,
    ProjectTokenVerifier,
    dump_credential_store,
    issue_credential,
    parse_credential_store,
)
from starlette.testclient import WebSocketDenialResponse

from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.server.auth import ALL_RIGHTS, rights_from_roles
from nirs4all_cluster.server.state import bootstrap_state


def credential(project="alpha", name="consumer", roles=("admin",)):
    return issue_credential(
        ProjectPrincipal(project, name, frozenset({"cluster"}), frozenset(rights_from_roles(roles)))
    )


def job(**updates):
    return dict(
        type="nirs4all.run",
        pipeline={"kind": "path", "path": "/science/pls.yaml"},
        dataset={"kind": "shared_path", "path": "/science/data"},
        outputs={"export_best_model": False},
        **updates,
    )


def header(token):
    return {"Authorization": "Bearer " + token}


@pytest.fixture
def services(tmp_path):
    credentials = {
        name: credential(project, name, roles)
        for name, project, roles in [
            ("a", "alpha", ("admin",)),
            ("b", "beta", ("admin",)),
            ("v", "alpha", ("viewer",)),
            ("p", "alpha", ("executor",)),
            ("s", "alpha", ("submitter",)),
        ]
    }
    credentials["u"] = issue_credential(
        ProjectPrincipal("alpha", "upload-only", frozenset({"cluster"}), frozenset({"cluster.submit"}))
    )
    state = tmp_path / "state"
    bootstrap_state(state, tuple(r for _, r in credentials.values()))
    app = create_app(ServerConfig(str(state), reaper_interval_s=1000))
    with TestClient(app) as client:
        yield client, {k: t for k, (t, _) in credentials.items()}, app, state


def test_roles_expand_without_leaf_wildcards():
    assert rights_from_roles(["admin"]) == ALL_RIGHTS
    assert rights_from_roles(["executor"]) == {"cluster.read", "cluster.execute"}
    with pytest.raises(ValueError):
        rights_from_roles(["root"])
    with pytest.raises(ValueError):
        rights_from_roles([])


def test_leaf_snapshot_and_duplicate_rejection():
    token, record = credential()
    verifier = ProjectTokenVerifier([record])
    assert verifier.verify(token, service="cluster").project_id == "alpha"
    with pytest.raises(CredentialStoreError):
        ProjectTokenVerifier([record, replace(record, key_id="f" * 16)])
    with pytest.raises(CredentialStoreError):
        ProjectTokenVerifier([record, replace(record, token_hash="sha256:" + "f" * 64)])
    for invalid in [None, "wrong", "non-ascii-é", token + "\n"]:
        with pytest.raises(AccessError) as exc:
            verifier.verify(invalid, service="cluster")
        assert exc.value.status == 401
    with pytest.raises(AccessError):
        ProjectTokenVerifier([]).verify(token, service="cluster")
    assert parse_credential_store(dump_credential_store([record])) == (record,)


def test_fail_closed_and_legacy_untouched(tmp_path):
    state = tmp_path / "legacy"
    state.mkdir()
    (state / "store.sqlite").write_bytes(b"legacy bytes")

    def snapshot():
        return [
            (
                str(p.relative_to(state)),
                p.stat().st_mtime_ns,
                hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None,
            )
            for p in sorted(state.rglob("*"))
        ]

    before = snapshot()
    from nirs4all_cluster.cli import main

    assert main(["init-state", "--state", str(state)]) == 2
    assert (
        main(["token-create", "--state", str(state), "--project", "alpha", "--principal", "c", "--role", "admin"]) == 2
    )
    with pytest.raises(ValueError):
        create_app(ServerConfig(str(state)))
    assert snapshot() == before
    bootstrap_state(tmp_path / "empty", ())
    with pytest.raises(ValueError):
        create_app(ServerConfig(str(tmp_path / "empty")))
    with pytest.raises(ValueError):
        create_app(ServerConfig(str(tmp_path / "absent")))
    assert not (tmp_path / "absent").exists()


def test_all_boundaries_isolate(services):
    c, t, app, state = services
    a, b = header(t["a"]), header(t["b"])
    aj = c.post("/v1/jobs", headers=a, json=job(idempotency_key="same")).json()
    bj = c.post("/v1/jobs", headers=b, json=job(idempotency_key="same")).json()
    assert aj["id"] != bj["id"]
    assert c.get("/v1/jobs", headers=b).json()[0]["id"] == bj["id"]
    for suffix in ["", "/tasks", "/events", "/artifacts"]:
        assert c.get("/v1/jobs/" + aj["id"] + suffix, headers=b).status_code == 404
    assert c.post("/v1/jobs/" + aj["id"] + "/cancel", headers=b).status_code == 404
    assert c.get("/v1/stats", headers=b).json()["jobs_by_status"] == {"queued": 1}
    register(c, t["a"])
    assert len(c.get("/v1/workers", headers=a).json()) == 1
    assert c.get("/v1/workers", headers=b).json() == []
    assert c.get("/v1/stats", headers=b).json()["workers_alive"] == 0
    arts = []
    for h in [a, b]:
        r = c.post("/v1/artifacts", headers=h, files={"file": ("input", b"identical", "application/octet-stream")})
        assert r.status_code == 200
        arts.append(r.json())
    assert arts[0]["sha256"] == arts[1]["sha256"]
    assert arts[0]["artifact_id"] != arts[1]["artifact_id"]
    assert c.get("/v1/artifacts/" + arts[0]["artifact_id"], headers=b).status_code == 404
    paths = [app.state.services[p].store._path_for(arts[0]["sha256"]) for p in ["alpha", "beta"]]
    assert all(p.is_file() for p in paths) and paths[0] != paths[1]
    assert c.post("/v1/jobs", headers=a, json=job(idempotency_key="same", name="changed")).status_code == 409
    assert c.post("/v1/jobs", headers=a, json=job(idempotency_key="same")).json()["id"] == aj["id"]
    for ref in [
        {"pipeline": {"kind": "artifact", "artifact_id": arts[1]["artifact_id"]}},
        {"pipelines": [{"kind": "artifact", "artifact_id": "missing"}], "pipeline": None},
        {"pipeline": {"kind": "python_entrypoint", "entrypoint": "x:y", "bundle_artifact_id": "missing"}},
        {"datasets": [{"kind": "artifact", "artifact_id": "missing"}], "dataset": None},
    ]:
        body = job()
        body.update(ref)
        assert c.post("/v1/jobs", headers=a, json=body).status_code == 404
    valid = job()
    valid["pipeline"] = {"kind": "artifact", "artifact_id": arts[0]["artifact_id"]}
    assert c.post("/v1/jobs", headers=a, json=valid).status_code == 200


def register(c, token):
    r = c.post("/v1/workers/register", headers=header(token), json={"version": {"packages": {"nirs4all": "1.4.15"}}})
    assert r.status_code == 200, r.text
    reg = r.json()
    return reg, {**header(token), "X-N4C-Worker-Secret": reg["worker_secret"]}


def test_worker_session_attempt_and_artifact_fences(services):
    c, t, app, state = services
    a = header(t["a"])
    j = c.post("/v1/jobs", headers=a, json=job(retry={"max_attempts": 2})).json()
    w, h = register(c, t["p"])
    w2, h2 = register(c, t["p"])
    assert c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h2).status_code == 403
    assert c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=header(t["b"])).status_code == 404
    task = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"]
    tid = task["task_id"]
    url = "/v1/tasks/" + tid
    fence = {"worker_id": w["worker_id"], "attempt": 1}
    assert (
        c.post(url + "/complete", headers=h, params=fence, json={"delivery": {"status": "not_requested"}}).status_code
        == 409
    )
    assert c.post(url + "/start", headers=h, params=fence).status_code == 200
    assert c.post(url + "/start", headers=h, params=fence).status_code == 200
    assert c.post(url + "/events", headers=h, params=fence, json={"type": "job_succeeded"}).status_code == 400
    assert c.post(url + "/events", headers=h2, params=fence, json={"type": "log"}).status_code == 403
    old = c.post(
        url + "/artifacts", headers=h, params={**fence, "role": "model"}, files={"file": ("model", b"old")}
    ).json()
    assert c.post(url + "/fail", headers=h, params=fence, json={"error": "retry", "retriable": True}).status_code == 200
    task2 = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"]
    assert task2["attempt"] == 2
    for endpoint, kwargs in [
        ("start", {}),
        ("events", {"json": {"type": "log"}}),
        ("artifacts", {"files": {"file": ("late", b"x")}}),
        ("complete", {"json": {}}),
        ("fail", {"json": {"error": "stale"}}),
    ]:
        assert c.post(url + "/" + endpoint, headers=h, params=fence, **kwargs).status_code == 409
    fence["attempt"] = 2
    assert c.post(url + "/start", headers=h, params=fence).status_code == 200
    assert (
        c.post(
            url + "/complete", headers=h, params=fence, json={"artifacts": {"model": old["artifact_id"]}}
        ).status_code
        == 409
    )
    assert (
        c.post(url + "/complete", headers=h, params=fence, json={"artifacts": {"logs": "art_guessed"}}).status_code
        == 409
    )
    result = {"delivery": {"status": "not_requested"}}
    assert c.post(url + "/complete", headers=h, params=fence, json=result).status_code == 200
    assert c.post(url + "/complete", headers=h, params=fence, json=result).status_code == 200
    assert c.post(url + "/fail", headers=h, params=fence, json={"error": "late"}).status_code == 409
    assert c.post(url + "/artifacts", headers=h, params=fence, files={"file": ("late", b"x")}).status_code == 409
    final = c.get("/v1/jobs/" + j["id"], headers=a).json()
    assert final["aggregate"]["best_model_artifact_id"] is None
    assert final["aggregate"]["completeness"] == "not_requested"
    assert c.get("/v1/jobs/" + j["id"] + "/artifacts", headers=a).json() == []


def test_requested_export_reference_checks(services):
    c, t, app, state = services
    a = header(t["a"])
    body = job()
    body["outputs"] = {"export_best_model": True}
    j = c.post("/v1/jobs", headers=a, json=body).json()
    w, h = register(c, t["p"])
    task = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"]
    url = "/v1/tasks/" + task["task_id"]
    q = {"worker_id": w["worker_id"], "attempt": 1}
    c.post(url + "/start", headers=h, params=q).raise_for_status()
    assert c.post(url + "/complete", headers=h, params=q, json={}).status_code == 409
    assert (
        c.post(
            url + "/artifacts", headers=h, params={**q, "role": "best_model"}, files={"file": ("bad", b"x")}
        ).status_code
        == 400
    )
    art = c.post(
        url + "/artifacts", headers=h, params={**q, "role": "model"}, files={"file": ("model", b"transport fixture")}
    ).json()
    evidence = {
        "status": "exported",
        "profile": "synthetic-transport-test",
        "bundle_format_version": "test",
        "sha256": art["sha256"],
        "size_bytes": art["size_bytes"],
    }
    result = {"artifacts": {"model": art["artifact_id"]}, "delivery": evidence}
    bad = json.loads(json.dumps(result))
    bad["delivery"]["sha256"] = "0" * 64
    assert c.post(url + "/complete", headers=h, params=q, json=bad).status_code == 409
    no_profile = json.loads(json.dumps(result))
    del no_profile["delivery"]["profile"]
    assert c.post(url + "/complete", headers=h, params=q, json=no_profile).status_code == 409
    no_identity = json.loads(json.dumps(result))
    del no_identity["delivery"]["bundle_format_version"]  # neither a bundle version nor a container id
    assert c.post(url + "/complete", headers=h, params=q, json=no_identity).status_code == 409
    assert c.post(url + "/complete", headers=h, params=q, json=result).status_code == 200
    assert c.get("/v1/jobs/" + j["id"], headers=a).json()["aggregate"]["best_model_artifact_id"] == art["artifact_id"]


def test_heartbeat_does_not_renew_orphans(services):
    c, t, app, state = services
    c.post("/v1/jobs", headers=header(t["a"]), json=job(retry={"max_attempts": 2})).raise_for_status()
    w, h = register(c, t["p"])
    task = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"]
    db = app.state.services["alpha"].db
    db._conn.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (time.time() - 10, task["task_id"]))
    db._conn.commit()
    assert (
        c.post("/v1/workers/" + w["worker_id"] + "/heartbeat", headers=h, json={"active_attempts": []}).status_code
        == 200
    )
    assert db.reap_expired_leases() == [(task["task_id"], task["job_id"])]


def test_auth_order_discovery_headers_ws(services):
    c, t, app, state = services
    for path in ["/docs", "/redoc", "/openapi.json", "/unknown", "/v1/jobs", "/v1/stats", "/v1/workers"]:
        assert c.get(path).status_code == 401
    assert c.post("/v1/jobs", headers={**header(t["v"]), "X-N4C-Role": "admin"}, json=job()).status_code == 403
    assert c.get("/v1/jobs", headers={**header(t["b"]), "X-N4C-Project": "alpha"}).status_code == 400
    assert c.get("/v1/project", headers=header(t["b"])).json()["project_id"] == "beta"
    assert c.post("/v1/consumers/register", headers=header(t["s"])).json()["principal_id"] == "s"
    j = c.post("/v1/jobs", headers=header(t["a"]), json=job()).json()
    for path, h, status in [
        ("/v1/events/stream", {}, 401),
        ("/v1/events/stream", header("bad"), 401),
        ("/v1/events/stream", header(t["v"]), 200),
        ("/v1/jobs/" + j["id"] + "/events/stream", header(t["b"]), 404),
        ("/v1/events/stream?token=synthetic-query", header(t["v"]), 401),
        ("/v1/events/stream", {**header(t["v"]), "X-N4C-Api": "999"}, 426),
    ]:
        if status == 200:
            with c.websocket_connect(path, headers=h):
                pass
        else:
            with pytest.raises(WebSocketDenialResponse) as exc:
                with c.websocket_connect(path, headers=h):
                    pass
            assert exc.value.status_code == status
            assert exc.value.headers["X-N4C-Api"] == "2"


@pytest.mark.asyncio
async def test_asgi_unauth_reads_zero_chunks_and_chunked_json_bounded(services):
    c, t, app, state = services
    before = len(app.state.services["alpha"].db.list_all_events())

    async def invoke(auth, path, body, maximum=None):
        messages = []
        reads = 0

        async def receive():
            nonlocal reads
            reads += 1
            return {"type": "http.request", "body": body, "more_body": reads < 64}

        async def send(message):
            messages.append(message)

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [(b"authorization", auth), (b"x-n4c-version", b"9.9.9")],
            "server": ("localhost", 80),
            "client": ("127.0.0.1", 1),
        }
        await app(scope, receive, send)
        return reads, next(x["status"] for x in messages if x["type"] == "http.response.start")

    reads, status = await invoke(b"Bearer malformed", "/v1/artifacts", b"x" * 1000)
    assert reads == 0 and status == 401
    assert len(app.state.services["alpha"].db.list_all_events()) == before
    reads, status = await invoke(b"Bearer \xff", "/v1/jobs", b"{}")
    assert reads == 0 and status == 401
    # 17 MiB is above the 16 MiB JSON guard with no Content-Length.
    reads, status = await invoke(("Bearer " + t["a"]).encode(), "/v1/jobs", b"x" * (1024 * 1024))
    assert reads == 17 and status == 413


def test_pinned_owner_fixture_transport_provenance_and_idempotency(services):
    c, t, app, state = services
    fixture = Path(__file__).parent / "fixtures/owner-python-1.0.n4a"
    provenance = json.loads(fixture.with_suffix(".json").read_text())
    data = fixture.read_bytes()
    assert hashlib.sha256(data).hexdigest() == provenance["sha256"]
    assert len(data) == provenance["size_bytes"]
    body = job()
    body["outputs"] = {"export_best_model": True}
    accepted = []
    for token, project in [(t["a"], "alpha"), (t["b"], "beta")]:
        h = header(token)
        j = c.post("/v1/jobs", headers=h, json=body).json()
        w, wh = register(c, token)
        task = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=wh).json()["task"]
        q = {"worker_id": w["worker_id"], "attempt": task["attempt"]}
        url = "/v1/tasks/" + task["task_id"]
        c.post(url + "/start", headers=wh, params=q).raise_for_status()
        up = []
        for _ in range(2):
            r = c.post(
                url + "/artifacts", headers=wh, params={**q, "role": "model"}, files={"file": ("owner.n4a", data)}
            )
            assert r.status_code == 200
            up.append(r.json())
        assert up[0] == up[1]
        delivery = {
            k: provenance[k]
            for k in ["profile", "bundle_format_version", "owner_version", "sha256", "size_bytes", "canonical_core"]
        }
        delivery["status"] = "exported"
        result = {
            "nirs4all_version": provenance["owner_version"],
            "artifacts": {"model": up[0]["artifact_id"]},
            "delivery": delivery,
        }
        for _ in range(2):
            assert c.post(url + "/complete", headers=wh, params=q, json=result).status_code == 200
        accepted_result = c.get("/v1/jobs/" + j["id"] + "/tasks", headers=h).json()[0]["result"]
        assert accepted_result["provenance"]["project_id"] == project
        assert accepted_result["provenance"]["worker_id"] == w["worker_id"]
        for _ in range(2):
            dl = c.get("/v1/artifacts/" + up[0]["artifact_id"], headers=h)
            assert dl.content == data and dl.headers["X-N4C-SHA256"] == provenance["sha256"]
        accepted.append(up[0]["artifact_id"])
    assert accepted[0] != accepted[1]
    assert c.get("/v1/artifacts/" + accepted[0], headers=header(t["b"])).status_code == 404


def test_ws_paged_replay_then_live_is_project_scoped(services):
    c, t, app, state = services
    h = header(t["a"])
    j = c.post("/v1/jobs", headers=h, json=job()).json()
    service = app.state.services["alpha"]
    for i in range(605):
        service.broker.emit(level="info", type="log", job_id=j["id"], data={"index": i})
    other = app.state.services["beta"]
    other.broker.emit(level="info", type="foreign-marker", data={"foreign": True})
    with c.websocket_connect("/v1/jobs/" + j["id"] + "/events/stream", headers=h) as ws:
        replay = [ws.receive_json() for _ in range(606)]
        assert len({x["id"] for x in replay}) == 606
        assert replay[-1]["data"]["index"] == 604
        service.broker.emit(level="info", type="live-marker", job_id=j["id"])
        assert ws.receive_json()["type"] == "live-marker"
    with c.websocket_connect("/v1/events/stream", headers=h) as ws:
        replay = [ws.receive_json() for _ in range(607)]
        assert all(x["type"] != "foreign-marker" for x in replay)
        other.broker.emit(level="info", type="foreign-marker", data={"foreign": True})
        service.broker.emit(level="info", type="local-next")
        assert ws.receive_json()["type"] == "local-next"
    page = c.get("/v1/jobs/" + j["id"] + "/events", headers=h, params={"limit": 500}).json()
    second = c.get(
        "/v1/jobs/" + j["id"] + "/events", headers=h, params={"after_id": page[-1]["id"], "limit": 500}
    ).json()
    assert len(page) == 500 and len(second) == 107
    assert c.get("/v1/jobs", headers=header(t["b"]), params={"created_before": time.time(), "limit": 1}).json() == []


def test_route_auth_precedes_lookup_and_foreign_task_mutations(services):
    c, t, app, state = services
    reads = [
        "/v1/project",
        "/v1/jobs",
        "/v1/stats",
        "/v1/workers",
        "/v1/artifacts/unknown",
        "/v1/jobs/unknown",
        "/v1/jobs/unknown/tasks",
        "/v1/jobs/unknown/events",
        "/v1/jobs/unknown/artifacts",
    ]
    writes = [
        "/v1/jobs",
        "/v1/artifacts",
        "/v1/jobs/unknown/cancel",
        "/v1/workers/register",
        "/v1/workers/unknown/lease",
        "/v1/workers/unknown/heartbeat",
        *["/v1/tasks/unknown/" + x for x in ["start", "events", "artifacts", "complete", "fail"]],
    ]
    for path in reads:
        assert c.get(path).status_code == 401
        assert c.get(path, headers=header(t["u"])).status_code == 403
    for path in writes:
        assert c.post(path).status_code == 401
        assert c.post(path, headers=header(t["v"])).status_code == 403
    with pytest.raises(WebSocketDenialResponse) as exc:
        with c.websocket_connect("/v1/events/stream", headers=header(t["u"])):
            pass
    assert exc.value.status_code == 403
    c.post("/v1/jobs", headers=header(t["a"]), json=job()).raise_for_status()
    wa, ha = register(c, t["a"])
    wb, hb = register(c, t["b"])
    task = c.post("/v1/workers/" + wa["worker_id"] + "/lease", headers=ha).json()["task"]
    q = {"worker_id": wb["worker_id"], "attempt": 1}
    for action, kwargs in [
        ("start", {}),
        ("events", {"json": {"type": "log"}}),
        ("artifacts", {"files": {"file": ("a", b"x")}}),
        ("complete", {"json": {}}),
        ("fail", {"json": {"error": "foreign"}}),
    ]:
        assert c.post("/v1/tasks/" + task["task_id"] + "/" + action, headers=hb, params=q, **kwargs).status_code == 404


def test_all_projects_open_and_cleanup(tmp_path):
    import sqlite3

    tokens = [credential(project) for project in ["first", "second"]]
    state = tmp_path / "state"
    bootstrap_state(state, tuple(record for _, record in tokens))
    app = create_app(ServerConfig(str(state)))
    with TestClient(app):
        assert set(app.state.services) == {"first", "second"}
        dbs = [service.db for service in app.state.services.values()]
    for db in dbs:
        with pytest.raises(sqlite3.ProgrammingError):
            db.list_jobs()
