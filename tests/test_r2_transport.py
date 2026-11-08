"""R2: shared base-URL admission and URL-free owned HTTP failure reporting."""

import argparse

import httpx
import pytest
from nirs4all_access import generate_token

from nirs4all_cluster import ClusterClient, WorkerClient, cli
from nirs4all_cluster.client_errors import ClusterConnectionError
from nirs4all_cluster.client_transport import make_http_client
from nirs4all_cluster.transport_diagnostics import http_failure_diagnostic
from nirs4all_cluster.worker.agent import WorkerAgent

BAD_URLS = [
    "http://u:{secret}@127.0.0.1:1",
    "http://u@127.0.0.1:1",
    "http://@127.0.0.1:1",
    "http://:@127.0.0.1:1",
    "http://%75:%70%77@127.0.0.1:1",
    "http://127.0.0.1:1?",
    "http://127.0.0.1:1/?x={secret}",
    "http://127.0.0.1:1/base?",
    "http://127.0.0.1:1#",
    "http://127.0.0.1:1/base#{secret}",
    "http://127.0.0.1:1?#",
    "http://[broken",
    "http://127.0.0.1:bad",
    "http://127.0.0.1:70000",
    "http:///missing",
    "not-a-url",
    "ftp://localhost/base",
    "http://127.0.0.1:1/\n{secret}",
]
REFUSAL = "invalid server base URL; require HTTP(S) without userinfo, query or fragment"


@pytest.mark.parametrize("constructor", ["sdk", "worker-client", "agent"])
@pytest.mark.parametrize("with_token", [False, True])
@pytest.mark.parametrize("url", BAD_URLS)
def test_shared_url_guard_before_client_creation(tmp_path, monkeypatch, constructor, with_token, url):
    secret = generate_token()

    def forbidden(*args, **kwargs):
        raise AssertionError("HTTP client construction must follow admission")

    monkeypatch.setattr(httpx, "Client", forbidden)
    target = url.format(secret=secret)
    with pytest.raises(ValueError) as exc:
        if constructor == "agent":
            WorkerAgent(target, token=secret if with_token else None, state_dir=str(tmp_path / "no-state"))
        else:
            (ClusterClient if constructor == "sdk" else WorkerClient)(target, token=secret if with_token else None)
    assert str(exc.value) == REFUSAL
    assert not (tmp_path / "no-state").exists()
    assert exc.value.__suppress_context__


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8765",
        "http://[::1]:8765",
        "http://localhost:8765/base/",
        "https://example.invalid/base/path",
        "https://example.invalid/base%3Fpart%23part",
    ],
)
@pytest.mark.parametrize("constructor", [ClusterClient, WorkerClient])
@pytest.mark.parametrize("with_token", [False, True])
def test_valid_url_preserves_supported_basepaths(url, constructor, with_token):
    seen = []

    def respond(request):
        seen.append(request)
        return httpx.Response(200, json={})

    with constructor(
        url, token=generate_token() if with_token else None, transport=httpx.MockTransport(respond)
    ) as client:
        client._http.get("/version").raise_for_status()
    assert len(seen) == 1 and seen[0].url.host == httpx.URL(url).host
    assert seen[0].url.path.startswith(httpx.URL(url).path.rstrip("/"))


@pytest.mark.parametrize("value", [None, 42, b"http://localhost"])
def test_transport_nonstring_url_is_fixed_refusal(value):
    with pytest.raises(ValueError, match="invalid server base URL"):
        make_http_client(value, token=None, role="client", timeout=1)


def error_case(kind, secret):
    request = httpx.Request(
        "GET", "http://127.0.0.1:1/v1/artifacts/" + secret, headers={"Authorization": "Bearer " + secret}
    )
    if isinstance(kind, int):
        return httpx.HTTPStatusError(
            "private message " + secret, request=request, response=httpx.Response(kind, request=request)
        )
    return kind("private message " + str(request.url), request=request)


@pytest.mark.parametrize(
    "kind", [401, 403, 404, 405, 500, httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError]
)
def test_cli_http_errors_never_render_messages_or_chains(monkeypatch, capsys, kind):
    secret = generate_token()
    error = error_case(kind, secret)

    def fail(args):
        try:
            raise RuntimeError("private cause " + secret)
        except RuntimeError as cause:
            raise error from cause

    monkeypatch.setattr(
        cli, "build_parser", lambda: argparse.Namespace(parse_args=lambda argv: argparse.Namespace(func=fail))
    )
    assert cli.main([]) == (3 if kind in [401, 403] else 4)
    text = capsys.readouterr()
    expected = f"HTTP {kind}" if isinstance(kind, int) else kind.__name__
    assert text.out == "" and text.err == f"[n4cluster] transport failure: {expected}\n"
    assert http_failure_diagnostic(error) == expected
    assert secret not in text.err and "http://" not in text.err and "Traceback" not in text.err


def test_cli_wrapped_connection_error_is_generic(monkeypatch, capsys):
    secret = generate_token()

    def fail(args):
        raise ClusterConnectionError("http://127.0.0.1/" + secret) from error_case(httpx.ConnectError, secret)

    monkeypatch.setattr(
        cli, "build_parser", lambda: argparse.Namespace(parse_args=lambda argv: argparse.Namespace(func=fail))
    )
    assert cli.main([]) == 4
    assert capsys.readouterr().err == "[n4cluster] cannot reach server\n"


@pytest.mark.parametrize("kind", [404, 405, httpx.ConnectError])
def test_agent_http_failure_roundtrip_is_status_only(cluster, tmp_path, monkeypatch, caplog, kind):
    secret = generate_token()
    with ClusterClient(cluster.base_url, token=cluster.submitter) as consumer:
        source = tmp_path / "input.zip"
        source.write_bytes(b"synthetic transport placeholder")
        aid = consumer.upload_artifact(source)
        job = consumer.submit_run(
            pipeline={"kind": "inline_json", "inline": {}},
            dataset={"kind": "artifact", "artifact_id": aid},
            outputs={"export_best_model": False},
            retry={"max_attempts": 1},
        )
        monkeypatch.setattr(
            "nirs4all_cluster.worker.agent._environment_version", lambda exe: {"packages": {"nirs4all": "1.4.15"}}
        )
        agent = WorkerAgent(cluster.base_url, token=cluster.executor, state_dir=str(tmp_path / "agent"), gpu_count=0)
        agent.register()

        def fail(*args):
            raise error_case(kind, secret) from RuntimeError("private cause " + secret)

        monkeypatch.setattr(agent, "_download_artifact", fail)
        task = agent._lease()
        assert task and task.job_id == job.id
        agent._attempts.add((task.task_id, task.attempt))
        agent._inc_active()
        with caplog.at_level("WARNING", logger="nirs4all_cluster.worker"):
            agent._handle_task(task)
        expected = "agent error: " + (f"HTTP {kind}" if isinstance(kind, int) else kind.__name__)
        result = consumer.get_tasks(job.id)[0]
        assert result.error == expected and result.status.value == "failed" and result.attempt == task.attempt
        assert expected in caplog.text and "retriable=True" in caplog.text
        assert secret not in caplog.text and "http://" not in result.error and "/v1/" not in result.error
        assert not agent._attempts
        agent._http.close()


def test_real_worker_cli_admission_auth_and_baseline(tmp_path):
    import os
    import subprocess
    import sys
    import time
    from dataclasses import replace
    from pathlib import Path

    from conftest import running_server
    from test_rbac import credential

    from nirs4all_cluster.server.app import ServerConfig
    from nirs4all_cluster.server.state import bootstrap_state

    provider, record = credential("r2", "provider", ("executor",))
    revoked, old_record = credential("r2", "revoked", ("executor",))
    viewer, viewer_record = credential("r2", "viewer", ("viewer",))
    secret = generate_token()
    state = tmp_path / "server"
    bootstrap_state(state, (record, replace(old_record, revoked=True), viewer_record))
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), os.environ.get("PYTHONPATH", "")]))
    with running_server(ServerConfig(str(state))) as base:
        urls = [
            (base.replace("http://", "http://u:" + secret + "@"), provider, 2, REFUSAL),
            (base + "/?x=" + secret, provider, 2, REFUSAL),
            (base + "/#" + secret, provider, 2, REFUSAL),
            (base, secret, 3, "HTTP 401"),
            (base, revoked, 3, "HTTP 401"),
            (base, viewer, 3, "HTTP 403"),
            (base + "/absent", provider, 3, "HTTP 403"),
        ]
        for index, (url, token, code, message) in enumerate(urls):
            log = tmp_path / f"error-{index}.log"
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "nirs4all_cluster.cli",
                    "worker",
                    "--server",
                    url,
                    "--state",
                    str(tmp_path / f"worker-{index}"),
                    "--gpus",
                    "0",
                    "--log-level",
                    "debug",
                    "--log-file",
                    str(log),
                ],
                cwd=tmp_path,
                env={**env, "NIRS4ALL_PROJECT_TOKEN": token},
                capture_output=True,
                text=True,
                timeout=10,
            )
            text = result.stdout + result.stderr + (log.read_text() if log.exists() else "")
            assert result.returncode == code and message in text
            assert all(t not in text for t in [provider, revoked, viewer, secret])
            assert "Traceback" not in text and "http://" not in text and "/v1/" not in text
        log = tmp_path / "baseline.log"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-m",
                "nirs4all_cluster.cli",
                "worker",
                "--server",
                base,
                "--state",
                str(tmp_path / "baseline"),
                "--gpus",
                "0",
                "--poll-interval",
                "0.05",
                "--log-level",
                "debug",
                "--log-file",
                str(log),
            ],
            cwd=tmp_path,
            env={**env, "NIRS4ALL_PROJECT_TOKEN": provider},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            for _ in range(100):
                if log.exists() and "registered as" in log.read_text():
                    break
                assert proc.poll() is None
                time.sleep(0.03)
            else:
                raise AssertionError("baseline registration diagnostic missing")
        finally:
            proc.terminate()
            out, err = proc.communicate(timeout=5)
        text = out + err + log.read_text()
        assert "registered as" in text and provider not in text
        assert "http://" not in text and "/v1/" not in text and "Traceback" not in text


@pytest.mark.parametrize("status", [404, 405])
def test_real_worker_cli_http_status_is_visible_without_trace(tmp_path, status):
    import os
    import subprocess
    import sys
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path

    class Response(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(status)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Response)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = Path(__file__).resolve().parents[1]
    token = generate_token()
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "nirs4all_cluster.cli",
                "worker",
                "--server",
                f"http://127.0.0.1:{server.server_port}",
                "--state",
                str(tmp_path / "worker"),
                "--gpus",
                "0",
            ],
            cwd=tmp_path,
            env=dict(
                os.environ,
                PYTHONPATH=os.pathsep.join([str(root), os.environ.get("PYTHONPATH", "")]),
                NIRS4ALL_PROJECT_TOKEN=token,
            ),
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        server.shutdown()
        thread.join(5)
        server.server_close()
    text = result.stdout + result.stderr
    assert result.returncode == 4 and f"HTTP {status}" in text
    assert token not in text and "http://" not in text and "/v1/" not in text and "Traceback" not in text
