"""Test-only log scanners and unsafe direct-hosting positive control."""

from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
from nirs4all_access import ProjectPrincipal, issue_credential
from websockets.sync.client import connect

from nirs4all_cluster.server.state import bootstrap_state


def scan_logs(text: str, secrets: list[str]) -> dict[str, int]:
    lines = text.splitlines()
    fragments = [part for secret in secrets for part in [secret, secret.rsplit(".", 1)[-1]]]
    return {
        "lines": len(lines),
        "credential_lines": sum(any(part in line for part in fragments) for line in lines),
        "header_lines": sum(
            bool(re.search(r"authorization|x-n4c-worker-secret|sec-websocket-key", line, re.I)) for line in lines
        ),
        "target_lines": sum(
            bool(re.search(r"(?:GET|POST|OPTIONS|HEAD) /|WebSocket /|HTTP Request:", line)) for line in lines
        ),
    }


def unsafe_control(directory: Path) -> dict[str, int]:
    """Run unsafe scratch ASGI hosting; retain counts only, never raw leaked secrets."""
    directory.mkdir(parents=True)
    token, record = issue_credential(
        ProjectPrincipal("control", "control", frozenset({"cluster"}), frozenset({"cluster.read"}))
    )
    bootstrap_state(directory / "state", (record,))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    script = (
        "import sys,uvicorn; from nirs4all_cluster.server.app import create_app,ServerConfig; "
        "uvicorn.run(create_app(ServerConfig(sys.argv[1])),host='127.0.0.1',port=int(sys.argv[2]),log_level='debug')"
    )
    proc = subprocess.Popen(
        [sys.executable, "-B", "-c", script, str(directory / "state"), str(port)],
        env=dict(os.environ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(150):
            try:
                if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            assert proc.poll() is None, "unsafe scratch control startup failed"
            time.sleep(0.02)
        else:
            raise AssertionError("unsafe scratch control timeout")
        with connect(
            f"ws://127.0.0.1:{port}/v1/events/stream?x=Bearer%20{token}",
            additional_headers={"Authorization": "Bearer " + token, "X-N4C-Worker-Secret": token},
            open_timeout=3,
            close_timeout=0.2,
        ):
            pass
    finally:
        proc.terminate()
        stdout, stderr = proc.communicate(timeout=5)
    counts = scan_logs(stdout + stderr, [token])
    assert all(counts[k] > 0 for k in ["lines", "credential_lines", "header_lines", "target_lines"]), (
        "unsafe control did not expose detectable leaks"
    )
    return counts


def finalize_diagnostic(base: str, token: str) -> None:
    """Exercise a real terminal transition that emits the existing INFO diagnostic."""
    from nirs4all_cluster import WorkerClient
    from nirs4all_cluster.schemas import TaskFailure

    with WorkerClient(base, token=token) as worker:
        worker.register(version={"packages": {"nirs4all": "1.4.15"}})
        task = worker.lease()
        assert task is not None
        worker.start_task(task.task_id, attempt=task.attempt)
        worker.fail_task(
            task.task_id,
            TaskFailure(error="privacy qualification terminal failure", retriable=False),
            attempt=task.attempt,
        )
