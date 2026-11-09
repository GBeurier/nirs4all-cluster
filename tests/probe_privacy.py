"""Real owned CLI privacy matrix; tokens stay in memory, unsafe controls retain counts only."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

import httpx
from nirs4all_access import ProjectPrincipal, generate_token, issue_credential
from privacy_support import finalize_diagnostic, scan_logs, unsafe_control
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect

from nirs4all_cluster.logging_setup import LOG_LEVELS
from nirs4all_cluster.server.state import bootstrap_state

ROOT = Path(__file__).resolve().parents[1]
SHAPES = {
    "exact": (lambda t: "x=" + t, True),
    "named": (lambda t: "token=" + t, True),
    "duplicate_first": (lambda t: f"x={t}&x=ordinary", True),
    "duplicate_last": (lambda t: f"x=ordinary&x={t}", True),
    "upper_name": (lambda t: "ACCESS_TOKEN=" + t, True),
    "encoded_name": (lambda t: "%74oken=" + t, True),
    "arbitrary_key": (lambda t: "api_key=" + t, True),
    "encoded_value": (lambda t: "x=" + "".join(f"%{ord(c):02x}" for c in t), True),
    "bearer_space": (lambda t: "x=Bearer%20" + t, False),
    "nested": (lambda t: "next=" + quote("/a?token=" + t, safe=""), False),
    "token_key": (lambda t: t + "=1", False),
    "bare": (lambda t: t, False),
    "punctuation": (lambda t: "x=(" + t + ")", False),
    "prefix_suffix": (lambda t: "x=prefix_" + t + "_suffix", False),
    "json_opaque": (lambda t: "x=" + quote(json.dumps({"credential": t}), safe=""), False),
}


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def terminate(proc):
    proc.terminate()
    try:
        return proc.wait(5)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.wait(5)


def raw(p, payload):
    with socket.create_connection(("127.0.0.1", p), timeout=1) as sock:
        sock.settimeout(0.3)
        sock.sendall(payload)
        try:
            sock.recv(2048)
        except OSError:
            pass


def main(evidence):
    evidence.mkdir(parents=True, exist_ok=False)
    assert shutil.disk_usage(ROOT).free >= 2147483648
    results = {"cases": {}, "processes": []}
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1")
    for key in ["NIRS4ALL_PROJECT_TOKEN", "N4CLUSTER_TOKEN"]:
        env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="n4privacy-") as tmp:
        tmp = Path(tmp)
        credentials = {}
        for name, service, rights in [
            (
                "admin",
                "cluster",
                {"cluster.read", "cluster.submit", "cluster.execute", "cluster.cancel", "cluster.admin"},
            ),
            ("provider", "cluster", {"cluster.read", "cluster.execute"}),
            ("denied", "arena", {"arena.read"}),
        ]:
            credentials[name] = issue_credential(
                ProjectPrincipal("privacy", name, frozenset({service}), frozenset(rights))
            )
        bootstrap_state(tmp / "state", tuple(r for _, r in credentials.values()))
        secrets = [t for t, _ in credentials.values()]
        auth = {"Authorization": "Bearer " + credentials["admin"][0]}
        for level in LOG_LEVELS:
            assert shutil.disk_usage(ROOT).free >= 2147483648
            p = port()
            base = f"http://127.0.0.1:{p}"
            argv = [
                sys.executable,
                "-B",
                "-m",
                "nirs4all_cluster.cli",
                "server",
                "--state",
                str(tmp / "state"),
                "--port",
                str(p),
                "--log-level",
                level,
                "--log-file",
                str(evidence / f"{level}-application.log"),
                "--cors-origin",
                "https://allowed.invalid",
            ]
            paths = [evidence / f"{level}-{channel}.log" for channel in ["stdout", "stderr"]]
            with paths[0].open("w") as out, paths[1].open("w") as err:
                srv = subprocess.Popen(argv, cwd=tmp, env=env, stdout=out, stderr=err)
                case = {"statuses": {}, "workers": {}}
                try:
                    for _ in range(150):
                        try:
                            if httpx.get(base + "/healthz", timeout=0.2).status_code == 200:
                                break
                        except httpx.HTTPError:
                            pass
                        assert srv.poll() is None, "server stopped during startup"
                        time.sleep(0.02)
                    else:
                        raise AssertionError("server startup timeout")
                    with httpx.Client(base_url=base, timeout=3) as client:
                        for shape, (query, refused) in SHAPES.items():
                            specs = [
                                ("public", "GET", "/healthz", {}, 200),
                                ("authenticated", "GET", "/v1/jobs", auth, 200),
                                ("unauthenticated", "GET", "/v1/jobs", {}, 401),
                                (
                                    "forbidden",
                                    "GET",
                                    "/v1/jobs",
                                    {"Authorization": "Bearer " + credentials["denied"][0]},
                                    403,
                                ),
                                ("unknown", "GET", "/v1/jobs/unknown", auth, 404),
                                (
                                    "preflight",
                                    "OPTIONS",
                                    "/v1/jobs",
                                    {"Origin": "https://allowed.invalid", "Access-Control-Request-Method": "GET"},
                                    200,
                                ),
                                ("options_no_origin", "OPTIONS", "/v1/jobs", {}, 401),
                            ]
                            for name, method, path, headers, expected in specs:
                                token = generate_token()
                                secrets.append(token)
                                response = client.request(method, path + "?" + query(token), headers=headers)
                                case["statuses"][shape + ":" + name] = response.status_code
                                assert response.status_code == (401 if refused else expected), (
                                    level,
                                    shape,
                                    name,
                                    response.status_code,
                                )
                            for name, headers, expected in [
                                ("ws_authenticated", auth, "accepted"),
                                ("ws_denied", {}, 401),
                            ]:
                                token = generate_token()
                                secrets.append(token)
                                try:
                                    with connect(
                                        base.replace("http:", "ws:") + "/v1/events/stream?" + query(token),
                                        additional_headers=headers,
                                        open_timeout=3,
                                        close_timeout=0.2,
                                    ):
                                        status = "accepted"
                                except InvalidStatus as exc:
                                    status = exc.response.status_code
                                case["statuses"][shape + ":" + name] = status
                                assert status == (401 if refused else expected), (level, shape, name, status)
                        for query, expected in [("limit=3", 200), ("status=queued", 200), ("limit=0", 422)]:
                            status = client.get("/v1/jobs?" + query, headers=auth).status_code
                            assert status == expected
                            case["statuses"]["routing:" + query] = status
                        response = client.get("/v1/jobs", headers={**auth, "X-N4C-Api": "999"})
                        assert response.status_code == 426
                        case["statuses"]["protocol"] = 426
                        # Existing INFO finalization diagnostic, through actual public APIs.
                        client.post(
                            "/v1/jobs",
                            headers=auth,
                            json={
                                "pipeline": {"kind": "inline_json", "inline": {"pipeline": []}},
                                "dataset": {"kind": "shared_path", "path": "/tmp"},
                            },
                        ).raise_for_status()
                        finalize_diagnostic(base, credentials["admin"][0])
                    # Malformed wire inputs are below the ASGI application boundary.
                    for kind in range(6):
                        token = generate_token()
                        secrets.append(token)
                        payloads = [
                            f"GET /v1/events/stream?x={token} HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nAuthorization: Bearer {token}\r\n\r\n",
                            f"GET /v1/events/stream?x={token} HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {token}\r\nSec-WebSocket-Version: 13\r\n\r\n",
                            f"GET /v1/jobs?x={token} HTTP/9.9\r\nbad header {token}\r\n\r\n",
                            f"NOTHTTP {token}\r\n\r\n",
                            f"POST /v1/jobs?x={token} HTTP/1.1\r\nHost: a\r\nContent-Length: nope\r\nAuthorization: Bearer {token}\r\n\r\n",
                            f"GET /v1/events/stream?x={token} HTTP/1.1\r\nHost: a\r\n",
                        ]
                        raw(p, payloads[kind].encode())
                    for failing in [False, True]:
                        worker_name = "failure" if failing else "registered"
                        token = generate_token() if failing else credentials["provider"][0]
                        secrets.append(token)
                        wpaths = [
                            evidence / f"{level}-worker-{worker_name}-{c}.log"
                            for c in ["stdout", "stderr", "application"]
                        ]
                        wargv = [
                            sys.executable,
                            "-B",
                            "-m",
                            "nirs4all_cluster.cli",
                            "worker",
                            "--server",
                            base,
                            "--state",
                            str(tmp / f"worker-{level}-{worker_name}"),
                            "--gpus",
                            "0",
                            "--poll-interval",
                            "0.05",
                            "--python-exe",
                            str(tmp / "unavailable-python"),
                            "--log-level",
                            level,
                            "--log-file",
                            str(wpaths[2]),
                        ]
                        initial_workers = len(httpx.get(base + "/v1/workers", headers=auth).json())
                        with wpaths[0].open("w") as wout, wpaths[1].open("w") as werr:
                            worker = subprocess.Popen(
                                wargv, cwd=tmp, env={**env, "NIRS4ALL_PROJECT_TOKEN": token}, stdout=wout, stderr=werr
                            )
                            if failing:
                                code = worker.wait(10)
                                assert code != 0
                            else:
                                for _ in range(100):
                                    workers = httpx.get(base + "/v1/workers", headers=auth).json()
                                    if len(workers) > initial_workers:
                                        break
                                    assert worker.poll() is None
                                    time.sleep(0.05)
                                else:
                                    raise AssertionError("worker did not register")
                                time.sleep(0.1)
                                code = terminate(worker)
                        case["workers"][worker_name] = {"pid": worker.pid, "exit_code": code, "channels": {}}
                        for path in wpaths:
                            text = path.read_text()
                            counts = scan_logs(text, secrets)
                            assert all(counts[k] == 0 for k in ["credential_lines", "header_lines", "target_lines"]), (
                                level,
                                worker_name,
                                counts,
                            )
                            case["workers"][worker_name]["channels"][path.name] = counts
                        wt = wpaths[1].read_text()
                        assert ("environment unavailable" in wt) == (level != "critical")
                        assert ("registered as" in wt) == (not failing and level in ["info", "debug"])
                        if failing:
                            assert ("worker credential rejected" in wt) == (level != "critical")
                finally:
                    exit_code = terminate(srv)
                results["processes"].append({"level": level, "pid": srv.pid, "argv": argv, "exit_code": exit_code})
            case["channels"] = {}
            assert "project-token server" in paths[0].read_text(), "stdout capture missing"
            for path in [*paths, evidence / f"{level}-application.log"]:
                text = path.read_text()
                counts = scan_logs(text, secrets)
                assert all(counts[k] == 0 for k in ["credential_lines", "header_lines", "target_lines"]), (
                    level,
                    counts,
                )
                if path != paths[0]:
                    assert ("finalized:" in text) == (level in ["info", "debug"])
                case["channels"][path.name] = counts
            assert len([k for k in case["statuses"] if ":" in k and not k.startswith("routing:")]) == 135
            case["malformed_inputs"] = 6
            results["cases"][level] = case
            (evidence / "partial.json").write_text(json.dumps(results, indent=2) + "\n")
        results["unsafe_positive_control"] = unsafe_control(tmp / "unsafe-control")
    results["exit_code"] = 0
    (evidence / "result.json").write_text(json.dumps(results, indent=2) + "\n")
    print(
        json.dumps(
            {
                "exit_code": 0,
                "accepted_levels": list(results["cases"]),
                "matrix_per_level": "15 query shapes x 9 request types + 6 malformed inputs",
                "unsafe_control": results["unsafe_positive_control"],
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    main(parser.parse_args().evidence)
