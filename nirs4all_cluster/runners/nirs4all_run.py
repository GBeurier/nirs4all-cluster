"""Subprocess entrypoint: run one ``nirs4all.run()`` task and summarize it.

Invoked by the worker executor as::

    python -m nirs4all_cluster.runners.nirs4all_run \
        --task-file spec.json --workspace ws/ --output-dir out/ --result-file result.json

This is the *only* module that imports ``nirs4all``. Running it as a child
process gives crash isolation and real cancellability (the parent can terminate
it), and keeps nirs4all entirely out of the server/agent import graph.

The task spec is pre-resolved by the worker (all refs are local paths)::

    {
      "pipeline": {"mode": "path", "path": "/abs/pipeline.yaml"}
                | {"mode": "entrypoint", "entrypoint": "mod.sub:build", "sys_path": ["/abs/bundle"]},
      "dataset":  {"mode": "path", "path": "/abs/dataset_dir"}
                | {"mode": "spec", "spec": {...}},
      "params":   {"verbose": 0, "random_state": 42, "refit": true, "inner_n_jobs": 1},
      "outputs":  {"export_best_model": true, "keep_task_workspace": false},
      "rank_metric": "best_rmse"
    }
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import time
import traceback
from pathlib import Path
from typing import Any


def _sanitize(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _load_pipeline(spec: dict[str, Any], allow_python: bool) -> Any:
    mode = spec.get("mode")
    if mode == "path":
        return spec["path"]  # nirs4all.run accepts a YAML/JSON path string
    if mode == "entrypoint":
        if not allow_python:
            raise PermissionError("python_entrypoint pipeline requires --allow-python")
        import sys

        for p in spec.get("sys_path", []):
            if p not in sys.path:
                sys.path.insert(0, p)
        module_name, _, func_name = spec["entrypoint"].partition(":")
        module = importlib.import_module(module_name)
        builder = getattr(module, func_name or "build_pipeline")
        return builder()
    raise ValueError(f"unsupported pipeline mode: {mode!r}")


def _load_dataset(spec: dict[str, Any]) -> Any:
    mode = spec.get("mode")
    if mode == "path":
        return spec["path"]  # folder path string
    if mode == "spec":
        return spec["spec"]  # dict config for DatasetConfigs
    raise ValueError(f"unsupported dataset mode: {mode!r}")


def _summarize(result: Any, nirs4all_version: str, duration: float) -> dict[str, Any]:
    def attr(name: str) -> Any:
        try:
            return _sanitize(getattr(result, name))
        except Exception:
            return None

    return {
        "status": "succeeded",
        "nirs4all_version": nirs4all_version,
        "duration_seconds": round(duration, 4),
        "metrics": {
            "best_score": attr("best_score"),
            "best_rmse": attr("best_rmse"),
            "best_r2": attr("best_r2"),
            "best_mae": attr("best_mae"),
            "best_accuracy": attr("best_accuracy"),
        },
        "counts": {"num_predictions": int(getattr(result, "num_predictions", 0) or 0)},
        "extra": {
            "best_model": (result.best or {}).get("model_name") if hasattr(result, "best") else None,
            "task_type": (result.best or {}).get("task_type") if hasattr(result, "best") else None,
            "metric": (result.best or {}).get("metric") if hasattr(result, "best") else None,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="nirs4all-cluster task runner")
    parser.add_argument("--task-file", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--allow-python", action="store_true")
    args = parser.parse_args(argv)

    result_path = Path(args.result_file)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        spec = json.loads(Path(args.task_file).read_text(encoding="utf-8"))
        import nirs4all  # lazy: only the runner imports nirs4all

        pipeline = _load_pipeline(spec["pipeline"], args.allow_python)
        dataset = _load_dataset(spec["dataset"])

        params = dict(spec.get("params") or {})
        inner_n_jobs = params.pop("inner_n_jobs", 1)
        params.setdefault("verbose", 0)
        params.setdefault("save_charts", False)
        # Worker writes nirs4all artifacts into its own task workspace.
        params.pop("workspace_path", None)
        params.pop("n_jobs", None)

        start = time.time()
        run_result = nirs4all.run(
            pipeline=pipeline,
            dataset=dataset,
            workspace_path=str(Path(args.workspace)),
            n_jobs=inner_n_jobs,
            **params,
        )
        duration = time.time() - start

        summary = _summarize(run_result, getattr(nirs4all, "__version__", "unknown"), duration)

        produced: dict[str, Any] = {"model": None}
        outputs = spec.get("outputs") or {}
        if outputs.get("export_best_model", True):
            try:
                model_path = output_dir / "best_model.n4a"
                run_result.export(str(model_path))
                if model_path.exists():
                    produced["model"] = str(model_path)
            except Exception as exc:  # export is best-effort; record but don't fail the task
                summary["extra"]["export_error"] = f"{type(exc).__name__}: {exc}"
        summary["produced"] = produced

        if hasattr(run_result, "close"):
            try:
                run_result.close()
            except Exception:
                pass

        result_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return 0
    except Exception as exc:
        failure = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        result_path.write_text(json.dumps(failure, indent=2), encoding="utf-8")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
