"""CLI adapter tests."""

from __future__ import annotations

import json

from nirs4all_cluster.cli import _run_job_from_args, build_parser, main


def test_run_command_builds_nirs4all_run_adapter_request():
    args = build_parser().parse_args(
        [
            "run",
            "--pipeline",
            "/shared/pls.yaml",
            "--pipeline",
            "/shared/rf.yaml",
            "--dataset",
            "/data/corn",
            "--param",
            "random_state=42",
            "--param",
            "refit=true",
            "--n-jobs",
            "2",
            "--require-label",
            "site=lab-a",
            "--require-label",
            "cuda=false",
            "--no-export-best-model",
            "--keep-task-workspace",
            "--rank-mode",
            "min",
        ]
    )

    req = _run_job_from_args(args)

    assert req.pipelines is not None and [p.path for p in req.pipelines] == ["/shared/pls.yaml", "/shared/rf.yaml"]
    assert req.dataset is not None and req.dataset.path == "/data/corn"
    assert req.params == {"random_state": 42, "refit": True, "inner_n_jobs": 2}
    assert req.requirements.labels == {"site": "lab-a", "cuda": "false"}
    assert req.outputs.export_best_model is False
    assert req.outputs.keep_task_workspace is True
    assert req.parity is not None
    assert req.parity.scope == "pipeline_dataset_matrix"


def test_run_command_rejects_bad_param_before_connecting(capsys):
    rc = main(["run", "--pipeline", "/p.yaml", "--dataset", "/data", "--param", "not-a-pair"])

    assert rc == 2
    assert "invalid input" in capsys.readouterr().err


def test_cli_forbidden_message_exposes_missing_right(cluster, tmp_path, capsys, monkeypatch):
    job_file = tmp_path / "job.json"
    job_file.write_text(
        json.dumps(
            {
                "type": "nirs4all.run",
                "pipeline": {"kind": "path", "path": "/p.yaml"},
                "dataset": {"kind": "shared_path", "path": "/data"},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("NIRS4ALL_PROJECT_TOKEN", cluster.viewer)
    rc = main(["submit", str(job_file), "--server", cluster.base_url])

    assert rc == 3
    err = capsys.readouterr().err
    assert "forbidden (as dash)" in err
    assert "right(s): cluster.submit" in err


def test_provision_hash_only_restart_contract(tmp_path, capsys):
    from nirs4all_access import ProjectTokenVerifier, load_credential_store

    state = tmp_path / "state"
    assert main(["init-state", "--state", str(state)]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "token-create",
                "--state",
                str(state),
                "--project",
                "lab",
                "--principal",
                "consumer",
                "--role",
                "submitter",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    secret = captured.out.strip()
    assert "restart" in captured.err
    path = state / "auth/credentials.json"
    assert secret not in path.read_text()
    assert path.stat().st_mode & 0o777 == 0o600
    records = load_credential_store(path)
    principal = ProjectTokenVerifier(records).verify(secret, service="cluster", required_rights=["cluster.submit"])
    assert principal.project_id == "lab" and principal.principal_id == "consumer"
    assert "cluster.execute" not in principal.rights
    before = path.read_bytes()
    assert (
        main(["token-create", "--state", str(state), "--project", "../bad", "--principal", "c", "--role", "admin"]) == 2
    )
    assert path.read_bytes() == before
