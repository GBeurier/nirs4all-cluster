"""Regression probes for the concrete W1–W10 independent review failures."""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from nirs4all_access import load_credential_store
from privacy_support import finalize_diagnostic, scan_logs, unsafe_control
from test_rbac import credential, header, job, register
from test_rbac import services as _services

from nirs4all_cluster import ClusterClient, WorkerClient
from nirs4all_cluster.cli import build_parser, main
from nirs4all_cluster.client_errors import ClusterConflictError, ClusterError
from nirs4all_cluster.credentials import read_bearer_file
from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.server.state import bootstrap_state
from nirs4all_cluster.worker import agent as agent_module
from nirs4all_cluster.worker.agent import WorkerAgent

services = _services


@pytest.mark.parametrize("kind", ["sdk", "worker", "agent"])
@pytest.mark.parametrize("existing", [True, False])
@pytest.mark.parametrize(
    "failure",
    [
        "wrong_hash",
        "missing_hash",
        "invalid_hash",
        "truncated",
        "bad_size",
        "missing_size",
        "stream_drop",
        "http_failure",
    ],
)
def test_failed_download_is_atomic(tmp_path, kind, existing, failure):
    good = b"qualified existing bytes"
    new = b"new transport bytes"
    sha = hashlib.sha256(new).hexdigest()
    dest = tmp_path / "result.n4a"
    if existing:
        dest.write_bytes(good)

    class Drop(httpx.SyncByteStream):
        def __iter__(self):
            yield new[:4]
            raise httpx.ReadError("bounded stream failure")

    def handler(request):
        headers = {"X-N4C-SHA256": sha, "X-N4C-Size": str(len(new))}
        body = new
        if failure == "wrong_hash":
            headers["X-N4C-SHA256"] = "0" * 64
        if failure == "missing_hash":
            headers.pop("X-N4C-SHA256")
        if failure == "invalid_hash":
            headers["X-N4C-SHA256"] = "../invalid"
        if failure == "truncated":
            body = new[:4]
        if failure == "bad_size":
            headers["X-N4C-Size"] = "invalid"
        if failure == "missing_size":
            headers.pop("X-N4C-Size")
        if failure == "stream_drop":
            return httpx.Response(200, headers=headers, stream=Drop())
        if failure == "http_failure":
            return httpx.Response(500)
        return httpx.Response(200, headers=headers, content=body)

    transport = httpx.MockTransport(handler)
    if kind == "sdk":
        client = ClusterClient("http://127.0.0.1:1", transport=transport)
        download = client.download_artifact
    elif kind == "worker":
        client = WorkerClient("http://127.0.0.1:1", transport=transport)
        download = client.download_artifact
    else:
        client = WorkerAgent.__new__(WorkerAgent)
        client._http = httpx.Client(base_url="http://127.0.0.1:1", transport=transport)
        download = client._download_artifact
    try:
        with pytest.raises((ValueError, httpx.HTTPError, RuntimeError, ClusterError)):
            download("art_synthetic", dest)
    finally:
        client._http.close()
    assert dest.read_bytes() == good if existing else not dest.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == (["result.n4a"] if existing else [])


def test_worker_client_never_relabels_old_attempt(cluster, tmp_path):
    with (
        ClusterClient(cluster.base_url, token=cluster.submitter) as c,
        WorkerClient(cluster.base_url, token=cluster.executor) as w,
    ):
        j = c.submit_run(
            pipeline={"kind": "inline_json", "inline": {}},
            dataset="/tmp",
            outputs={"export_best_model": False},
            retry={"max_attempts": 2},
        )
        w.register(version={"packages": {"nirs4all": "1.4.15"}})
        first = w.lease()
        assert first
        w.start_task(first.task_id, attempt=first.attempt)
        from nirs4all_cluster.schemas import TaskFailure

        w.fail_task(first.task_id, TaskFailure(error="retry"), attempt=first.attempt)
        second = w.lease()
        assert second and second.attempt == 2
        w.start_task(second.task_id, attempt=second.attempt)
        out = tmp_path / "old"
        out.write_bytes(b"old attempt bytes")
        with pytest.raises(ClusterConflictError):
            w.upload_artifact(first.task_id, out, attempt=first.attempt)
        assert (second.task_id, second.attempt) in w._attempts - w._terminal
        assert w.heartbeat().ok
        w.fail_task(second.task_id, TaskFailure(error="finished", retriable=False), attempt=second.attempt)
        assert c.get_job(j.id).status.value == "failed"


def test_same_agent_overlap_keeps_attempt2(tmp_path, monkeypatch):
    ct, cr = credential("overlap", "consumer", ("submitter",))
    pt, pr = credential("overlap", "provider", ("executor",))
    state = tmp_path / "state"
    bootstrap_state(state, (cr, pr))
    app = create_app(ServerConfig(str(state), lease_ttl_s=0.3, reaper_interval_s=3600))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.01)
    assert server.started
    gates = {i: threading.Event() for i in (1, 2)}
    entered = {i: threading.Event() for i in (1, 2)}

    def execute(spec, workdir, **kwargs):
        attempt = int(workdir.name)
        workdir.mkdir(parents=True, exist_ok=True)
        model = workdir / "model.n4a"
        data = f"attempt-{attempt}".encode()
        model.write_bytes(data)
        log = workdir / "run.log"
        log.write_text("transport stub")
        entered[attempt].set()
        assert gates[attempt].wait(10)
        return SimpleNamespace(
            cancelled=False,
            log_path=log,
            workspace_path=workdir / "none",
            result={
                "status": "succeeded",
                "produced": {"model": str(model)},
                "delivery": {
                    "status": "exported",
                    "profile": "nirs4all-python-n4a-1.0",
                    "bundle_format_version": "1.0",
                    "owner_version": "1.4.15",
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                },
            },
        )

    monkeypatch.setattr(agent_module, "execute_task", execute)
    monkeypatch.setattr(agent_module, "_environment_version", lambda exe=None: {"packages": {"nirs4all": "1.4.15"}})
    a = WorkerAgent(
        f"http://127.0.0.1:{port}",
        token=pt,
        state_dir=str(tmp_path / "worker"),
        slots=2,
        gpu_count=0,
        allowed_roots=(tmp_path,),
    )
    handlers = []
    try:
        a.register()
        with ClusterClient(f"http://127.0.0.1:{port}", token=ct) as c:
            j = c.submit_run(
                pipeline={"kind": "inline_json", "inline": {}}, dataset=str(tmp_path), retry={"max_attempts": 2}
            )
            first = a._lease()
            assert first
            a._inc_active()
            a._attempts.add((first.task_id, first.attempt))
            h = threading.Thread(target=a._handle_task, args=(first,))
            h.start()
            handlers.append(h)
            assert entered[1].wait(5)
            db = app.state.services["overlap"].db
            with db.project_guard():
                db._conn.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (time.time() - 1, first.task_id))
                db._conn.commit()
            assert db.reap_expired_leases()
            second = a._lease()
            assert second and second.attempt == 2
            db.heartbeat_worker(a.worker_id, 30, [(second.task_id, 2)])
            a._inc_active()
            a._attempts.add((second.task_id, 2))
            h = threading.Thread(target=a._handle_task, args=(second,))
            h.start()
            handlers.append(h)
            assert entered[2].wait(5)
            gates[1].set()
            handlers[0].join(5)
            assert not handlers[0].is_alive()
            assert db.get_task_artifact(second.task_id, 2, "model") is None
            assert (second.task_id, 2) in a._attempts
            gates[2].set()
            handlers[1].join(5)
            assert not handlers[1].is_alive()
            final = c.get_job(j.id)
            assert final.status.value == "succeeded" and final.aggregate.completeness == "complete"
            assert (
                db.get_task_artifact(second.task_id, 2, "model")["sha256"] == hashlib.sha256(b"attempt-2").hexdigest()
            )
            assert not a._attempts
    finally:
        for gate in gates.values():
            gate.set()
        for h in handlers:
            h.join(5)
        a._http.close()
        server.should_exit = True
        thread.join(5)


def test_cancel_sweep_serializes_failure_and_rolls_back(services, monkeypatch):
    c, t, app, state = services
    w, h = register(c, t["p"])
    j = c.post("/v1/jobs", headers=header(t["a"]), json=job(retry={"max_attempts": 3})).json()
    task = c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"]
    q = {"worker_id": w["worker_id"], "attempt": 1}
    url = "/v1/tasks/" + task["task_id"]
    c.post(url + "/start", headers=h, params=q).raise_for_status()
    db = app.state.services["alpha"].db
    original = db.cancel_job_tasks
    swept = threading.Event()
    release = threading.Event()

    def sweep(*args, **kwargs):
        result = original(*args, **kwargs)
        swept.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(db, "cancel_job_tasks", sweep)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        cancel = pool.submit(c.post, "/v1/jobs/" + j["id"] + "/cancel", headers=header(t["a"]))
        assert swept.wait(5)
        fail = pool.submit(c.post, url + "/fail", headers=h, params=q, json={"error": "transient", "retriable": True})
        with pytest.raises(concurrent.futures.TimeoutError):
            fail.result(timeout=0.1)
        release.set()
        assert cancel.result(5).status_code == 200
        assert fail.result(5).status_code == 200
    assert c.post("/v1/workers/" + w["worker_id"] + "/lease", headers=h).json()["task"] is None
    assert c.get("/v1/jobs/" + j["id"], headers=header(t["a"])).json()["status"] == "cancelled"
    monkeypatch.setattr(db, "cancel_job_tasks", original)
    queued = c.post("/v1/jobs", headers=header(t["a"]), json=job()).json()["id"]

    def crashed(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("injected transaction failure")

    monkeypatch.setattr(db, "cancel_job_tasks", crashed)
    with pytest.raises(RuntimeError):
        db.request_job_cancel(queued)
    assert db.get_job(queued)["status"] == "queued" and db.list_tasks_for_job(queued)[0]["status"] == "queued"


def test_ws_400_structured_lifetimes_no_cancelled_error(services):
    c, t, app, state = services
    service = app.state.services["alpha"]
    for i in range(400):
        with c.websocket_connect("/v1/events/stream", headers=header(t["v"])) as ws:
            if i % 2:
                service.broker.emit(level="info", type="churn")
                ws.receive_json()
    assert not service.broker._global and not service.broker._subscribers


@pytest.mark.parametrize(
    "command",
    [
        "server",
        "worker",
        "jobs",
        "run",
        "submit",
        "status",
        "logs",
        "cancel",
        "artifacts",
        "workers",
        "init-state",
        "token-create",
        "token-revoke",
    ],
)
@pytest.mark.parametrize("option", ["--token", "--principal", "--auth-file"])
def test_legacy_options_no_echo(command, option, capsys):
    if command == "token-create" and option == "--principal":
        parsed = build_parser().parse_args(
            ["token-create", "--state", "unused", "--project", "p", "--principal", "identity", "--role", "admin"]
        )
        assert parsed.principal == "identity"
        return  # Identifier remains valid provisioning vocabulary.
    for args in [[command, option, "SYNTHETIC-PRIVATE-VALUE"], [command, option + "=SYNTHETIC-PRIVATE-VALUE"]]:
        with pytest.raises(SystemExit) as exc:
            main(args)
        assert exc.value.code == 2
        text = capsys.readouterr().err
        assert "SYNTHETIC-PRIVATE-VALUE" not in text and "init-state" in text and "project" in text
    assert build_parser().allow_abbrev is False


def test_private_token_file_nonregular_refusal(tmp_path):
    token, _ = credential()
    valid = tmp_path / "valid"
    valid.write_text(token)
    valid.chmod(0o600)
    assert read_bearer_file(valid) == token
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo, 0o600)
    link = tmp_path / "link"
    link.symlink_to(fifo)
    public = tmp_path / "public"
    public.write_text(token)
    public.chmod(0o644)
    large = tmp_path / "large"
    large.write_bytes(b"x" * 2048)
    large.chmod(0o600)
    before = len(list(Path("/proc/self/fd").iterdir()))
    for _ in range(8):
        for p in [fifo, link, tmp_path, Path("/dev/null"), public, large, tmp_path / token]:
            started = time.monotonic()
            with pytest.raises(ValueError) as exc:
                read_bearer_file(p)
            assert token not in str(exc.value) and time.monotonic() - started < 1
    assert len(list(Path("/proc/self/fd").iterdir())) == before


def test_runtime_failure_is_visible_and_neutral(tmp_path, monkeypatch, caplog):
    shadow = tmp_path / "nirs4all"
    shadow.mkdir()
    monkeypatch.chdir(tmp_path)
    seen = []

    def unavailable(argv, **kwargs):
        seen.append(kwargs["cwd"])
        assert Path(kwargs["cwd"]) != tmp_path
        return SimpleNamespace(returncode=1, stderr="ModuleNotFoundError: no science owner", stdout="")

    monkeypatch.setattr(agent_module.subprocess, "run", unavailable)
    with caplog.at_level("ERROR"):
        report = agent_module._environment_version("/explicit/python")
    assert not report["packages"] and "ModuleNotFoundError" in caplog.text and seen


def test_cli_revoke_requires_restart_and_preserves_projects(tmp_path):
    root = Path(__file__).resolve().parents[1]
    ct, cr = credential("revocation", "consumer", ("admin",))
    ot, other = credential("revocation", "other", ("admin",))
    bt, br = credential("foreign", "consumer", ("admin",))
    state = tmp_path / "state"
    bootstrap_state(state, (cr, other, br))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), os.environ.get("PYTHONPATH", "")]))
    for name in ["NIRS4ALL_PROJECT_TOKEN", "N4CLUSTER_TOKEN"]:
        env.pop(name, None)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    logs = []

    def start(index):
        path = tmp_path / f"server-{index}.log"
        logs.append(path)
        stream = path.open("w")
        proc = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-m",
                "nirs4all_cluster.cli",
                "server",
                "--state",
                str(state),
                "--port",
                str(port),
                "--log-level",
                "info",
                "--cors-origin",
                "https://example.com",
            ],
            cwd=tmp_path,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
        )
        stream.close()
        for _ in range(150):
            try:
                if httpx.get(base + "/healthz", timeout=0.3).status_code == 200:
                    return proc
            except httpx.HTTPError:
                pass
            assert proc.poll() is None
            time.sleep(0.02)
        proc.terminate()
        proc.wait(5)
        raise AssertionError("CLI server startup timeout")

    def stop(proc):
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(5)

    first = start(1)
    try:
        with httpx.Client(base_url=base, timeout=5) as c:
            for query in [[("x", ct)], [("x", ct), ("x", "ordinary")], [("x", "ordinary"), ("x", ct)]]:
                for cors_headers in [
                    {},
                    {"Access-Control-Request-Method": "GET"},
                    {"Origin": "https://example.com", "Access-Control-Request-Method": "GET"},
                ]:
                    denied = c.options("/v1/jobs", params=query, headers=cors_headers)
                    assert denied.status_code == 401
                    if "Origin" in cors_headers:
                        assert denied.headers["access-control-allow-origin"] == cors_headers["Origin"]
            assert (
                c.options(
                    "/v1/jobs",
                    params={"x": ct},
                    headers={"Origin": "https://example.com", "Access-Control-Request-Method": "GET"},
                ).status_code
                == 401
            )
            assert (
                c.options(
                    "/v1/jobs",
                    params={"limit": "1"},
                    headers={"Origin": "https://example.com", "Access-Control-Request-Method": "GET"},
                ).status_code
                == 200
            )
            a = c.post("/v1/jobs", headers=header(ct), json=job()).json()["id"]
            b = c.post("/v1/jobs", headers=header(bt), json=job()).json()["id"]
            assert c.post(f"/v1/jobs/{a}/cancel", headers=header(ct)).status_code == 200
            c.post("/v1/jobs", headers=header(ct), json=job()).raise_for_status()
            finalize_diagnostic(base, ct)
            revoked = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "nirs4all_cluster.cli",
                    "token-revoke",
                    "--state",
                    str(state),
                    "--key-id",
                    cr.key_id,
                ],
                cwd=tmp_path,
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert revoked.returncode == 0 and "restart" in revoked.stdout
            records = load_credential_store(state / "auth/credentials.json")
            assert [r.revoked for r in records if r.key_id == cr.key_id] == [True]
            assert c.get("/v1/jobs/" + a, headers=header(ct)).status_code == 200  # immutable active snapshot
    finally:
        stop(first)
    second = start(2)
    try:
        with httpx.Client(base_url=base, timeout=5) as c:
            assert c.get("/v1/jobs/" + a, headers=header(ct)).status_code == 401
            assert c.get("/v1/jobs/" + a, headers=header(ot)).status_code == 200
            assert c.get("/v1/jobs/" + b, headers=header(bt)).status_code == 200
            assert c.get("/v1/jobs/" + a, headers=header(bt)).status_code == 404
            diagnostic_job = c.post("/v1/jobs", headers=header(ot), json=job()).json()["id"]
            assert c.post(f"/v1/jobs/{diagnostic_job}/cancel", headers=header(ot)).status_code == 200
            c.post("/v1/jobs", headers=header(ot), json=job()).raise_for_status()
            finalize_diagnostic(base, ot)
    finally:
        stop(second)
    for log in logs:
        text = log.read_text()
        counts = scan_logs(text, [ct, ot, bt])
        assert counts["lines"] > 0 and "nirs4all_cluster.server" in text and "finalized:" in text
        assert all(counts[k] == 0 for k in ["credential_lines", "header_lines", "target_lines"])
    assert unsafe_control(tmp_path / "unsafe-control")["credential_lines"] > 0
    assert all(token not in (state / "auth/credentials.json").read_text() for token in [ct, ot, bt])


def test_query_credentials_any_key_are_refused_without_breaking_queries(services):
    c, t, app, state = services
    from starlette.testclient import WebSocketDenialResponse

    for name in ["api_key", "auth", "bearer", "t", "x", "other"]:
        for h in [{}, header(t["a"])]:
            assert c.get("/v1/jobs", headers=h, params={name: t["a"]}).status_code == 401
            with pytest.raises(WebSocketDenialResponse) as exc:
                with c.websocket_connect("/v1/events/stream?" + name + "=" + t["a"], headers=h):
                    pass
            assert exc.value.status_code == 401
    assert c.get("/v1/jobs", headers=header(t["a"]), params={"limit": 1, "name": "ordinary"}).status_code == 200


@pytest.mark.parametrize("origin", [None, "http://allowed.invalid"])
@pytest.mark.parametrize("token_first", [True, False])
def test_cors_query_without_origin_never_500(tmp_path, origin, token_first):
    token, record = credential()
    state = tmp_path / "state"
    bootstrap_state(state, (record,))
    app = create_app(ServerConfig(str(state), cors_origins=["http://allowed.invalid"]))
    query = [("x", token), ("x", "ordinary")]
    if not token_first:
        query.reverse()
    headers = {} if origin is None else {"Origin": origin, "Access-Control-Request-Method": "GET"}
    with TestClient(app, raise_server_exceptions=False) as c:
        denied = c.options("/v1/jobs", params=query, headers=headers)
        assert denied.status_code == 401
        assert denied.headers["www-authenticate"] == "Bearer"
        if origin is not None:
            assert denied.headers["access-control-allow-origin"] == origin
            assert c.options("/v1/jobs?limit=1", headers=headers).status_code == 200


# --------------------------------------------------------------------------- #
# Canonical Core ``.n4a`` delivery: runner glue against a stubbed SDK module.
# These make no native claim; the real Core producer needs the cohort venv.
# --------------------------------------------------------------------------- #

_CANONICAL_PROFILE = "nirs4all.n4a.container.v1"
_CANONICAL_BYTES = b"sealed-by-core-stub"


class _FakeCanonicalResult:
    """Stands in for ``nirs4all.api.native_n4a.CanonicalN4aRunResult``."""

    def __init__(self, written: bytes = _CANONICAL_BYTES) -> None:
        self._written = written

    best_score = 0.1
    best_rmse = 0.1
    num_predictions = 1
    best = {"model_name": "pls", "task_type": "regression", "metric": "rmse"}

    @property
    def native_archive_reference(self):
        return {"container_id": "container:stub", "archive_sha256": hashlib.sha256(_CANONICAL_BYTES).hexdigest()}

    @property
    def canonical_view(self):
        return {
            "manifest": {
                "schema": _CANONICAL_PROFILE,
                "container_id": "container:stub",
                "physical_profile": "stub.physical.v1",
                "artifacts": [
                    {"kind": kind, "identity": {"fingerprint": f"fp-{kind}"}}
                    for kind in ("model", "run", "result", "score")
                ],
            },
            "receipt": {
                "container_sha256": hashlib.sha256(_CANONICAL_BYTES).hexdigest(),
                "byte_length": len(_CANONICAL_BYTES),
            },
        }

    def export(self, path):
        Path(path).write_bytes(self._written)

    def close(self) -> None:
        pass


def _run_runner(tmp_path, monkeypatch, *, params, outputs, result, handoff=None):
    import json
    import types

    from nirs4all_cluster.runners import nirs4all_run as runner

    calls: list[dict] = []

    def fake_run(**kwargs):
        calls.append(kwargs)
        return result

    native_module = types.ModuleType("nirs4all.api.native_n4a")
    native_module.CANONICAL_N4A_NATIVE_PROFILE = _CANONICAL_PROFILE
    native_module.CanonicalN4aRunResult = _FakeCanonicalResult
    root = types.ModuleType("nirs4all")
    root.__version__ = "9.9.9"
    root.run = fake_run
    for name, module in {"nirs4all": root, "nirs4all.api": types.ModuleType("nirs4all.api"), "nirs4all.api.native_n4a": native_module}.items():
        monkeypatch.setitem(sys.modules, name, module)
    if handoff is not None:
        monkeypatch.setattr(runner, "_robustness_handoff_from_spec", lambda spec: handoff)
    task_file = tmp_path / "task.json"
    task_file.write_text(
        json.dumps(
            {
                "pipeline": {"mode": "path", "path": "/p/recipe.json"},
                "dataset": {"mode": "path", "path": "/d/record.json"},
                "params": params,
                "outputs": outputs,
            }
        )
    )
    result_file = tmp_path / "result.json"
    code = runner.main(
        [
            "--task-file",
            str(task_file),
            "--workspace",
            str(tmp_path / "ws"),
            "--output-dir",
            str(tmp_path / "out"),
            "--result-file",
            str(result_file),
        ]
    )
    return code, json.loads(result_file.read_text()), calls


_CANONICAL_PARAMS = {"engine": "native", "native_profile": _CANONICAL_PROFILE, "source_id": "source:stub"}


def test_runner_canonical_delivery_binds_sealed_bytes_and_owner_identities(tmp_path, monkeypatch):
    code, report, calls = _run_runner(
        tmp_path, monkeypatch, params=dict(_CANONICAL_PARAMS), outputs={"export_best_model": True}, result=_FakeCanonicalResult()
    )
    assert code == 0, report
    # Canonical profile persists nothing in a workspace, so none is forwarded; the other options pass untouched.
    assert "workspace_path" not in calls[0]
    assert calls[0]["engine"] == "native" and calls[0]["native_profile"] == _CANONICAL_PROFILE
    assert calls[0]["source_id"] == "source:stub"
    delivery = report["delivery"]
    assert delivery == {
        "status": "exported",
        "profile": _CANONICAL_PROFILE,
        "physical_profile": "stub.physical.v1",
        "container_id": "container:stub",
        "owner_identities": {"model": "fp-model", "run": "fp-run", "result": "fp-result", "score": "fp-score"},
        "owner_version": "9.9.9",
        "sha256": hashlib.sha256(_CANONICAL_BYTES).hexdigest(),
        "size_bytes": len(_CANONICAL_BYTES),
        "canonical_core": True,
    }
    assert (tmp_path / "out" / "best_model.n4a").read_bytes() == _CANONICAL_BYTES


def test_runner_canonical_refuses_bytes_that_differ_from_the_sealed_container(tmp_path, monkeypatch):
    code, report, _ = _run_runner(
        tmp_path,
        monkeypatch,
        params=dict(_CANONICAL_PARAMS),
        outputs={"export_best_model": True},
        result=_FakeCanonicalResult(written=b"another container"),
    )
    assert code == 1 and report["retriable"] is False
    assert report["delivery"]["status"] == "incomplete"


@pytest.mark.parametrize("unsupported", ["keep_task_workspace", "robustness_handoff"])
def test_runner_canonical_refuses_workspace_dependent_outputs_before_running(tmp_path, monkeypatch, unsupported):
    code, report, calls = _run_runner(
        tmp_path,
        monkeypatch,
        params=dict(_CANONICAL_PARAMS),
        outputs={"export_best_model": True, "keep_task_workspace": unsupported == "keep_task_workspace"},
        result=_FakeCanonicalResult(),
        handoff={"requested": True} if unsupported == "robustness_handoff" else None,
    )
    assert code == 1 and report["retriable"] is False
    assert "no task workspace" in report["error"]
    assert calls == []  # nothing was fitted


def test_runner_canonical_refuses_explicit_workspace_path_before_any_fit(tmp_path, monkeypatch):
    code, report, calls = _run_runner(
        tmp_path,
        monkeypatch,
        params={**_CANONICAL_PARAMS, "workspace_path": str(tmp_path / "user-workspace")},
        outputs={"export_best_model": True},
        result=_FakeCanonicalResult(),
    )
    assert code == 1 and report["retriable"] is False
    assert "workspace_path" in report["error"]
    assert calls == []  # the explicit workspace was refused, not discarded, and nothing was fitted
    assert report["delivery"]["status"] == "incomplete"


@pytest.mark.parametrize("option", ["n_jobs", "inner_n_jobs"])
def test_runner_canonical_refuses_explicit_parallelism_before_any_fit(tmp_path, monkeypatch, option):
    code, report, calls = _run_runner(
        tmp_path,
        monkeypatch,
        params={**_CANONICAL_PARAMS, option: 4},
        outputs={"export_best_model": True},
        result=_FakeCanonicalResult(),
    )
    assert code == 1 and report["retriable"] is False
    assert "n_jobs" in report["error"]
    assert calls == []  # refused, not dropped, and nothing was fitted

    # The neutral default (inner_n_jobs=1, no n_jobs) stays accepted, and the legacy route still drops nothing it never did.
    code, _, calls = _run_runner(
        tmp_path, monkeypatch, params={**_CANONICAL_PARAMS, "inner_n_jobs": 1}, outputs={"export_best_model": True}, result=_FakeCanonicalResult()
    )
    assert code == 0 and "n_jobs" not in calls[0]


def test_runner_non_canonical_results_can_never_claim_canonical_core(tmp_path, monkeypatch):
    from nirs4all_cluster.runners import nirs4all_run as runner

    monkeypatch.setattr(
        runner,
        "_verify_owner_model",
        lambda path, version: {"status": "exported", "profile": "nirs4all-python-n4a-1.0", "canonical_core": False},
    )

    class V2Result:  # plain result object, as the interim native V2 lane returns
        num_predictions = 1
        best = {}
        best_score = best_rmse = 0.2

        def export(self, path):
            Path(path).write_bytes(b"v2 bundle")

    # Default job: workspace semantics unchanged, owner bundle verification, canonical_core stays False.
    code, report, calls = _run_runner(
        tmp_path, monkeypatch, params={"random_state": 1}, outputs={"export_best_model": True}, result=V2Result()
    )
    assert code == 0 and report["delivery"]["canonical_core"] is False
    assert calls[0]["workspace_path"] == str(tmp_path / "ws")

    # Interim PLS native profile (not canonical): same bundle verification path, never canonical_core=True.
    code, report, calls = _run_runner(
        tmp_path,
        monkeypatch,
        params={"engine": "native", "native_profile": "n4m.pls_role_pipeline.v1"},
        outputs={"export_best_model": True},
        result=V2Result(),
    )
    assert code == 0 and report["delivery"]["canonical_core"] is False
    assert calls[0]["workspace_path"] == str(tmp_path / "ws")

    # The canonical profile cannot be satisfied by a non-canonical result object.
    code, report, _ = _run_runner(
        tmp_path, monkeypatch, params=dict(_CANONICAL_PARAMS), outputs={"export_best_model": True}, result=V2Result()
    )
    assert code == 1 and report["retriable"] is False and "non-canonical" in report["error"]
