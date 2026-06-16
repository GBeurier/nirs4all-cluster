"""Run the task runner as a child process with capture + cancellation.

Running ``nirs4all.run()`` in a subprocess (rather than in-process) gives three
things the prototype needs: crash isolation (a segfault in a native backend does
not take down the worker), real cancellation (the parent can ``terminate()`` the
child), and a clean nirs4all-free import graph for the agent itself.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("nirs4all_cluster.worker.executor")


@dataclass
class ExecutionResult:
    returncode: int
    cancelled: bool
    result: dict[str, Any]
    log_path: Path
    workspace_path: Path
    output_dir: Path


def execute_task(
    spec: dict[str, Any],
    workdir: Path,
    *,
    allow_python: bool = False,
    python_exe: str | None = None,
    poll_interval: float = 1.0,
    cancel_check: Callable[[], bool] = lambda: False,
    on_tick: Callable[[float], None] = lambda elapsed: None,
    tick_every: float = 15.0,
    timeout: float | None = None,
) -> ExecutionResult:
    workdir.mkdir(parents=True, exist_ok=True)
    spec_file = workdir / "task.json"
    workspace = workdir / "workspace"
    output_dir = workdir / "outputs"
    result_file = workdir / "result.json"
    log_file = workdir / "run.log"
    spec_file.write_text(json.dumps(spec, indent=2), encoding="utf-8")

    cmd = [
        python_exe or sys.executable,
        "-m",
        "nirs4all_cluster.runners.nirs4all_run",
        "--task-file",
        str(spec_file),
        "--workspace",
        str(workspace),
        "--output-dir",
        str(output_dir),
        "--result-file",
        str(result_file),
    ]
    if allow_python:
        cmd.append("--allow-python")

    cancelled = False
    start = time.time()
    last_tick = start
    with open(log_file, "wb") as log:
        # Run in the task workdir (all spec paths are absolute). This also avoids
        # an `import nirs4all` resolving to a namespace-package shadow if the
        # worker happened to be launched from a directory that contains a
        # `nirs4all/` source tree.
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=str(workdir))
        logger.info("runner started (pid=%s) in %s", proc.pid, workdir)
        while True:
            ret = proc.poll()
            if ret is not None:
                break
            now = time.time()
            if cancel_check():
                cancelled = True
                logger.info("cancelling runner (pid=%s)", proc.pid)
                _terminate(proc)
                break
            if timeout is not None and (now - start) > timeout:
                logger.warning("runner timed out after %.0fs (pid=%s)", now - start, proc.pid)
                _terminate(proc)
                break
            if now - last_tick >= tick_every:
                on_tick(now - start)
                last_tick = now
            time.sleep(poll_interval)

    returncode = proc.returncode if proc.returncode is not None else -1
    logger.info("runner exited rc=%s cancelled=%s", returncode, cancelled)
    result: dict[str, Any] = {}
    if result_file.exists():
        try:
            result = json.loads(result_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            result = {}
    if cancelled:
        result = {"status": "cancelled", "error": "task cancelled by server"}
    elif not result:
        tail = _log_tail(log_file)
        result = {
            "status": "failed",
            "error": f"runner exited with code {returncode} and produced no result",
            "traceback": tail,
        }
    return ExecutionResult(
        returncode=returncode,
        cancelled=cancelled,
        result=result,
        log_path=log_file,
        workspace_path=workspace,
        output_dir=output_dir,
    )


def _terminate(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _log_tail(log_file: Path, max_chars: int = 4000) -> str:
    try:
        text = log_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:]
