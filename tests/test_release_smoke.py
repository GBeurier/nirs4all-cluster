"""Release smoke proof for the installable wheel.

This test intentionally stays below the real ``nirs4all.run`` path: it proves
that the built distribution exposes the installed CLI/server/worker/API surface
without requiring ``nirs4all`` in the environment. Metric parity remains covered
by the heavier validation harness.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "r"
STARTUP_TIMEOUT_SECONDS = 60

pytestmark = pytest.mark.release_smoke


def _clean_env() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("N4CLUSTER_SERVER", None)
    env.pop("N4CLUSTER_TOKEN", None)
    env.pop("VIRTUAL_ENV", None)
    env["PYTHONNOUSERSITE"] = "1"
    return env


def _uv() -> str:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required for the release smoke test")
    return uv


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _build_wheel(uv: str, tmp_path: Path) -> Path:
    wheelhouse = tmp_path / "dist"
    result = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(wheelhouse)],
        cwd=ROOT,
        env=_clean_env(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    wheels = sorted(wheelhouse.glob("nirs4all_cluster-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _uv_run_cmd(uv: str, wheel: Path, *args: str) -> list[str]:
    return [
        uv,
        "run",
        "--isolated",
        "--no-project",
        "--python",
        sys.executable,
        "--with",
        str(wheel),
        *args,
    ]


def _run_installed_cli(uv: str, wheel: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _uv_run_cmd(uv, wheel, "n4cluster", *args),
        cwd=cwd,
        env=_clean_env(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def _start_installed_cli(uv: str, wheel: Path, cwd: Path, *args: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        _uv_run_cmd(uv, wheel, "n4cluster", *args),
        cwd=cwd,
        env=_clean_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _stop_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.send_signal(signal.SIGINT)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=10)
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=10)


def _process_output(proc: subprocess.Popen[str]) -> str:
    if proc.poll() is None:
        return "<process still running>"
    stdout, stderr = proc.communicate(timeout=1)
    return f"stdout:\n{stdout}\nstderr:\n{stderr}"


def _await_health(base_url: str, proc: subprocess.Popen[str]) -> None:
    deadline = time.time() + STARTUP_TIMEOUT_SECONDS
    last_error = ""
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"installed server exited before becoming healthy\n{_process_output(proc)}")
        try:
            response = httpx.get(f"{base_url}/healthz", timeout=0.5)
            if response.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.1)
    pytest.fail(f"installed server did not become healthy: {last_error}")


def _await_worker(base_url: str, name: str, proc: subprocess.Popen[str]) -> None:
    deadline = time.time() + STARTUP_TIMEOUT_SECONDS
    headers = {"Authorization": f"Bearer {TOKEN}"}
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"installed worker exited before registering\n{_process_output(proc)}")
        with contextlib.suppress(httpx.HTTPError):
            workers = httpx.get(f"{base_url}/v1/workers", headers=headers, timeout=0.5).json()
            if any(worker.get("name") == name for worker in workers):
                return
        time.sleep(0.1)
    pytest.fail(f"installed worker {name!r} did not register")


def test_release_wheel_runs_installed_cli_server_and_worker_without_nirs4all(tmp_path: Path) -> None:
    uv = _uv()
    wheel = _build_wheel(uv, tmp_path)
    run_cwd = tmp_path / "run"
    run_cwd.mkdir()

    probe_code = (
        "import importlib.util, json, nirs4all_cluster; "
        "print(json.dumps({"
        "'cluster_file': nirs4all_cluster.__file__, "
        "'has_nirs4all': importlib.util.find_spec('nirs4all') is not None"
        "}))"
    )
    probe = subprocess.run(
        _uv_run_cmd(uv, wheel, "python", "-c", probe_code),
        cwd=run_cwd,
        env=_clean_env(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr or probe.stdout
    import_info = json.loads(probe.stdout)
    assert not Path(import_info["cluster_file"]).resolve().is_relative_to(ROOT)
    assert import_info["has_nirs4all"] is False

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    server = _start_installed_cli(
        uv,
        wheel,
        run_cwd,
        "server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--state",
        str(tmp_path / "server-state"),
        "--token",
        TOKEN,
        "--log-level",
        "warning",
    )
    worker = None
    try:
        _await_health(base_url, server)
        dashboard = httpx.get(f"{base_url}/ui", timeout=5)
        assert dashboard.status_code == 200
        assert "nirs4all-cluster" in dashboard.text
        assert httpx.get(f"{base_url}/v1/jobs", timeout=5).status_code == 401

        worker = _start_installed_cli(
            uv,
            wheel,
            run_cwd,
            "worker",
            "--server",
            base_url,
            "--token",
            TOKEN,
            "--state",
            str(tmp_path / "worker-state"),
            "--name",
            "release-smoke-worker",
            "--poll-interval",
            "0.2",
            "--gpus",
            "0",
            "--log-level",
            "warning",
        )
        _await_worker(base_url, "release-smoke-worker", worker)

        workers = _run_installed_cli(uv, wheel, run_cwd, "workers", "--server", base_url, "--token", TOKEN)
        assert workers.returncode == 0, workers.stderr or workers.stdout
        assert "release-smoke-worker" in workers.stdout

        submitted = _run_installed_cli(
            uv,
            wheel,
            run_cwd,
            "run",
            "--pipeline",
            "/shared/pls.yaml",
            "--dataset",
            "/data/corn",
            "--name",
            "release-smoke",
            "--server",
            base_url,
            "--token",
            TOKEN,
        )
        assert submitted.returncode == 0, submitted.stderr or submitted.stdout
        assert "submitted nirs4all.run job" in submitted.stdout

        jobs = _run_installed_cli(uv, wheel, run_cwd, "jobs", "--server", base_url, "--token", TOKEN)
        assert jobs.returncode == 0, jobs.stderr or jobs.stdout
        assert "queued" in jobs.stdout
        assert "release-smoke" in jobs.stdout
    finally:
        if worker is not None:
            _stop_process(worker)
        _stop_process(server)
