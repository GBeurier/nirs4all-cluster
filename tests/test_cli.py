"""CLI adapter tests."""

from __future__ import annotations

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
