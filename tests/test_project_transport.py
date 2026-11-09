"""Worker transport, input fencing and child environment production failure paths."""

import json
import sys
import types
from pathlib import Path

import httpx
import pytest

from nirs4all_cluster.client import ClusterClient
from nirs4all_cluster.schemas import TaskPayload, WorkerRegistered
from nirs4all_cluster.worker.agent import WorkerAgent
from nirs4all_cluster.worker.executor import execute_task
from nirs4all_cluster.worker.materialize import build_runner_spec


def task(tmp_path, export=False):
    return TaskPayload(
        task_id="task_synthetic",
        job_id="job_synthetic",
        type="nirs4all.run",
        attempt=1,
        pipeline={"kind": "inline_json", "inline": {}},
        dataset={"kind": "shared_path", "path": str(tmp_path)},
        outputs={"export_best_model": export},
        lease_expires_at=100,
    )


def agent(tmp_path, status):
    a = WorkerAgent("http://127.0.0.1:1", state_dir=str(tmp_path / "agent"), gpu_count=0)
    a._http.close()
    a._http = httpx.Client(
        base_url="http://127.0.0.1:1", transport=httpx.MockTransport(lambda req: httpx.Response(status, json={}))
    )
    a.worker_id = "worker_synthetic"
    a._attempts.add(("task_synthetic", 1))
    return a


def test_upload_http_failure_cannot_be_reported_as_success(tmp_path):
    a = agent(tmp_path, 507)
    path = tmp_path / "model"
    path.write_bytes(b"transport bytes")
    with pytest.raises(httpx.HTTPStatusError):
        a._upload("task_synthetic", path, attempt=1, role="model", kind="model")
    a._http.close()


def test_complete_http_failure_is_observed(tmp_path):
    a = agent(tmp_path, 409)
    result = types.SimpleNamespace(
        result={"delivery": {"status": "not_requested"}},
        log_path=tmp_path / "missing",
        workspace_path=tmp_path / "missing",
    )
    with pytest.raises(httpx.HTTPStatusError):
        a._report_success(task(tmp_path), result, None)
    a._http.close()


def test_missing_requested_model_refuses_success(tmp_path):
    a = agent(tmp_path, 200)
    result = types.SimpleNamespace(result={}, log_path=tmp_path / "missing", workspace_path=tmp_path / "missing")
    with pytest.raises(ValueError, match="incomplete"):
        a._report_success(task(tmp_path, True), result, None)
    a._http.close()


def test_auth_rejection_stops_worker(tmp_path):
    a = agent(tmp_path, 401)
    with pytest.raises(httpx.HTTPStatusError):
        a._post("/v1/workers/register")
    assert a._stop.is_set() and a._fatal_auth
    a._http.close()


def test_worker_secret_repr_redacted():
    r = WorkerRegistered(
        worker_id="worker_synthetic", worker_secret="synthetic-worker-private-value", project_id="p", principal_id="c"
    )
    assert r.worker_secret not in repr(r)


def test_path_roots_and_pinned_input_hash(tmp_path):
    with pytest.raises(ValueError, match="allowed roots"):
        build_runner_spec(task(tmp_path), tmp_path / "wd", lambda aid, dest: dest)
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "root"
    root.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    t = task(tmp_path).model_copy(
        update={"dataset": task(tmp_path).dataset.model_copy(update={"path": str(root / "link")})}
    )
    with pytest.raises(ValueError, match="allowed roots"):
        build_runner_spec(t, tmp_path / "wd", lambda aid, dest: dest, allowed_roots=(root,))
    t = task(tmp_path).model_copy(
        update={
            "pipeline": task(tmp_path).pipeline.model_copy(update={"kind": "artifact", "artifact_id": "art_synthetic"}),
            "input_artifacts": {"art_synthetic": "0" * 64},
        }
    )

    def download(aid, dest):
        dest.write_bytes(b"wrong bytes")
        return dest

    with pytest.raises(ValueError, match="identity mismatch"):
        build_runner_spec(t, tmp_path / "wd", download, allowed_roots=(tmp_path,))


def test_child_process_cannot_read_control_credentials(tmp_path, monkeypatch):
    # Real subprocess stub at the same runner module boundary; no science dependency.
    pkg = tmp_path / "stub/nirs4all_cluster"
    (pkg / "runners").mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "runners/__init__.py").write_text("")
    (pkg / "runners/nirs4all_run.py").write_text("""import os,json,sys
from pathlib import Path
args=sys.argv[1:]
keys=["N4CLUSTER_TOKEN","NIRS4ALL_PROJECT_TOKEN","N4CLUSTER_WORKER_SECRET","N4CLUSTER_TOKEN_FILE"]
r={"status":"succeeded","credential_present":any(k in os.environ for k in keys),"threads":os.environ.get("OMP_NUM_THREADS"),"provisioning":bool(os.environ.get("PYTHONPATH"))}
Path(args[args.index("--result-file")+1]).write_text(json.dumps(r))
""")
    monkeypatch.setenv("PYTHONPATH", str(pkg.parent))
    for name in ["N4CLUSTER_TOKEN", "NIRS4ALL_PROJECT_TOKEN", "N4CLUSTER_WORKER_SECRET", "N4CLUSTER_TOKEN_FILE"]:
        monkeypatch.setenv(name, "synthetic-secret-for-child-test")
    monkeypatch.setenv("OMP_NUM_THREADS", "1")
    result = execute_task({}, tmp_path / "work", python_exe=sys.executable, poll_interval=0.01, timeout=5)
    assert result.returncode == 0 and not result.result["credential_present"]
    assert result.result["threads"] == "1" and result.result["provisioning"]
    assert "synthetic-secret-for-child-test" not in result.log_path.read_text()


def test_sdk_rejects_insecure_nonloopback():
    for url in ["http://example.com", "http://0.0.0.0", "http://localhost.example"]:
        with pytest.raises(ValueError):
            ClusterClient(url, token="synthetic")


@pytest.mark.parametrize("mode", ["export_raises", "export_missing", "invalid_archive"])
def test_requested_export_failure_is_nonretriable(tmp_path, monkeypatch, mode):
    from nirs4all_cluster.runners import nirs4all_run as runner

    class Result:
        def export(self, path):
            if mode == "export_raises":
                raise RuntimeError("exact owner refusal")
            if mode == "invalid_archive":
                Path(path).write_bytes(b"invalid archive")

        def close(self):
            pass

    fake = types.SimpleNamespace(__version__="test", run=lambda **kwargs: Result())
    monkeypatch.setitem(sys.modules, "nirs4all", fake)
    if mode == "invalid_archive":
        monkeypatch.setattr(
            runner, "_verify_owner_model", lambda *args: (_ for _ in ()).throw(ValueError("owner invalid checksum"))
        )
    task_file = tmp_path / "task.json"
    task_file.write_text(
        json.dumps(
            {
                "pipeline": {"mode": "path", "path": "/p"},
                "dataset": {"mode": "path", "path": "/d"},
                "outputs": {"export_best_model": True},
            }
        )
    )
    result = tmp_path / "result.json"
    code = runner.main(
        [
            "--task-file",
            str(task_file),
            "--workspace",
            str(tmp_path / "ws"),
            "--output-dir",
            str(tmp_path / "out"),
            "--result-file",
            str(result),
        ]
    )
    assert code == 1
    report = json.loads(result.read_text())
    assert report["status"] == "failed" and report["retriable"] is False
    if mode == "export_raises":
        assert "exact owner refusal" in report["error"]
    assert report["delivery"]["status"] == "incomplete"


def test_cli_wait_failed_or_partial_nonzero(tmp_path, monkeypatch):
    from nirs4all_cluster import cli
    from nirs4all_cluster.schemas import JobAggregate, JobStatus

    fakejob = types.SimpleNamespace(
        id="job_test", status=JobStatus.SUCCEEDED, aggregate=JobAggregate(completeness="incomplete")
    )

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, request):
            return fakejob

        def wait(self, *args, **kwargs):
            return fakejob

    monkeypatch.setattr(cli, "_client", lambda args: Client())
    file = tmp_path / "job.json"
    file.write_text("{}")
    assert cli.main(["submit", str(file), "--wait"]) == 1
    fakejob.status = JobStatus.FAILED
    assert cli.main(["submit", str(file), "--wait"]) == 1


def test_advertisement_uses_bound_child_environment(tmp_path, monkeypatch):
    from nirs4all_cluster.worker.agent import _environment_version

    calls = []

    def probe(argv, **kwargs):
        calls.append((argv, kwargs))
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "python": "3.12.9",
                    "executable": "/provisioned/python",
                    "packages": {"nirs4all": "1.4.15", "numpy": "2.0.0"},
                }
            ),
        )

    monkeypatch.setenv("NIRS4ALL_PROJECT_TOKEN", "synthetic-child-secret")
    monkeypatch.setattr("nirs4all_cluster.worker.agent.subprocess.run", probe)
    version = _environment_version("/provisioned/python")
    assert version["python"] == "3.12.9" and version["packages"] == {"nirs4all": "1.4.15", "numpy": "2.0.0"}
    assert calls[0][0][0] == "/provisioned/python"
    assert "NIRS4ALL_PROJECT_TOKEN" not in calls[0][1]["env"]
