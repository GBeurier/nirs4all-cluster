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


def _jsonable(value: Any) -> Any:
    value = _sanitize(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return repr(value)


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

    raw_extra = attr("extra")
    extra = _jsonable(raw_extra) if isinstance(raw_extra, dict) else {}
    extra.update(
        {
            "best_model": (result.best or {}).get("model_name") if hasattr(result, "best") else None,
            "task_type": (result.best or {}).get("task_type") if hasattr(result, "best") else None,
            "metric": (result.best or {}).get("metric") if hasattr(result, "best") else None,
        }
    )
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
        "extra": extra,
    }


def _get_mapping_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _robustness_handoff_from_spec(spec: dict[str, Any]) -> dict[str, Any] | None:
    native_payload = spec.get("native_payload") or spec.get("nativePayload")
    if not isinstance(native_payload, dict):
        return None
    manifest = native_payload.get("manifest")
    if not isinstance(manifest, dict):
        return None
    handoff = _get_mapping_value(
        manifest,
        "robustnessEvidencePublicationHandoff",
        "robustness_evidence_publication_handoff",
    )
    return handoff if isinstance(handoff, dict) else None


def _coerce_dataset_X(dataset_object: Any) -> Any | None:
    try:
        X = dataset_object.x({}, layout="2d")
    except TypeError:
        try:
            X = dataset_object.x(layout="2d")
        except Exception:
            return None
    except Exception:
        return None

    if isinstance(X, list) and X:
        X = X[0]
    try:
        import numpy as np

        X_array = np.asarray(X, dtype=float)
    except Exception:
        return None
    if X_array.ndim != 2 or X_array.shape[0] == 0:
        return None
    return X_array


def _load_replay_X_from_dataset_spec(dataset_spec: dict[str, Any]) -> Any | None:
    if dataset_spec.get("mode") != "path" or not dataset_spec.get("path"):
        return None
    try:
        from nirs4all.data import DatasetConfigs
    except Exception:
        return None
    try:
        dataset_object = DatasetConfigs(str(dataset_spec["path"])).get_dataset_at(0)
    except Exception:
        return None
    return _coerce_dataset_X(dataset_object)


def _load_replay_dataset_from_spec(dataset_spec: dict[str, Any]) -> Any | None:
    if dataset_spec.get("mode") != "path" or not dataset_spec.get("path"):
        return None
    try:
        from nirs4all.data import DatasetConfigs
    except Exception:
        return None
    try:
        return DatasetConfigs(str(dataset_spec["path"])).get_dataset_at(0)
    except Exception:
        return None


_ROBUSTNESS_REPLAY_IDENTITY_KEYS: tuple[tuple[str, ...], ...] = (
    ("sample_id", "sample_ids"),
    ("physical_sample_id", "physical_sample_ids"),
    ("origin_sample_id", "origin_sample_ids"),
    ("row_id", "row_ids"),
    ("unit_id", "unit_ids"),
    ("observation_id", "observation_ids"),
    ("internal_sample_id", "internal_sample_ids"),
)


def _sequence_or_none(value: Any) -> list[Any] | None:
    if value is None or isinstance(value, (str, bytes)):
        return None
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            if value.ndim == 0:
                return None
            return value.reshape(-1).tolist()
    except Exception:
        pass
    if isinstance(value, (list, tuple)):
        return list(value)
    return None


def _dataset_metadata_columns(dataset_object: Any) -> dict[str, list[Any]]:
    metadata_method = getattr(dataset_object, "metadata", None)
    if not callable(metadata_method):
        return {}
    try:
        metadata = metadata_method()
    except Exception:
        return {}
    columns = getattr(metadata, "columns", None)
    if not columns:
        return {}

    result: dict[str, list[Any]] = {}
    for column in columns:
        try:
            if hasattr(metadata, "get_column"):
                values = metadata.get_column(column).to_list()
            elif hasattr(metadata, "__getitem__"):
                values = list(metadata[column])
            else:
                continue
        except Exception:
            continue
        result[str(column)] = values
    return result


def _prediction_row_count(arrays: dict[str, Any]) -> int | None:
    for key in ("y_true", "y_pred", "sample_indices"):
        values = _sequence_or_none(arrays.get(key))
        if values:
            return len(values)
    sample_metadata = arrays.get("sample_metadata")
    if isinstance(sample_metadata, dict):
        for value in sample_metadata.values():
            values = _sequence_or_none(value)
            if values:
                return len(values)
    return None


def _unique_identity_index(values: list[Any], *, expected_len: int) -> dict[str, int] | None:
    if len(values) != expected_len:
        return None
    index: dict[str, int] = {}
    for position, value in enumerate(values):
        if value is None:
            return None
        key = str(value)
        if key in index:
            return None
        index[key] = position
    return index


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _collect_prediction_identity_columns_from_mapping(
    payload: dict[str, Any],
    *,
    expected_len: int,
    result: dict[str, list[Any]],
    depth: int = 0,
) -> None:
    if depth > 2:
        return

    for aliases in _ROBUSTNESS_REPLAY_IDENTITY_KEYS:
        canonical = aliases[0]
        if canonical in result:
            continue
        for alias in aliases:
            values = _sequence_or_none(payload.get(alias))
            if values is not None and len(values) == expected_len:
                result[canonical] = values
                break

    for nested_key in (
        "row_identity",
        "sample_identity",
        "prediction_identity",
        "materialization_manifest",
        "relation_replay_manifest",
        "relation_materialization_manifest",
    ):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            _collect_prediction_identity_columns_from_mapping(
                nested,
                expected_len=expected_len,
                result=result,
                depth=depth + 1,
            )


def _prediction_identity_columns(arrays: dict[str, Any], *, row_count: int) -> dict[str, list[Any]]:
    result: dict[str, list[Any]] = {}
    for payload in (
        _mapping_or_empty(arrays.get("sample_metadata")),
        _mapping_or_empty(arrays.get("result_metadata")),
    ):
        if payload:
            _collect_prediction_identity_columns_from_mapping(
                payload,
                expected_len=row_count,
                result=result,
            )
    return result


def _select_prediction_X_by_identity(
    X: Any,
    arrays: dict[str, Any],
    dataset_metadata: dict[str, list[Any]],
) -> Any | None:
    row_count = _prediction_row_count(arrays)
    if not row_count or not dataset_metadata:
        return None
    prediction_metadata = _prediction_identity_columns(arrays, row_count=row_count)
    if not prediction_metadata:
        return None

    for aliases in _ROBUSTNESS_REPLAY_IDENTITY_KEYS:
        dataset_key = next((alias for alias in aliases if alias in dataset_metadata), None)
        prediction_key = aliases[0] if aliases[0] in prediction_metadata else None
        if dataset_key is None or prediction_key is None:
            continue

        dataset_values = dataset_metadata.get(dataset_key)
        prediction_values = prediction_metadata.get(prediction_key)
        if dataset_values is None or prediction_values is None or len(prediction_values) != row_count:
            continue

        dataset_index = _unique_identity_index(dataset_values, expected_len=X.shape[0])
        prediction_index = _unique_identity_index(prediction_values, expected_len=row_count)
        if dataset_index is None or prediction_index is None:
            continue

        positions: list[int] = []
        for value in prediction_values:
            key = str(value)
            if key not in dataset_index:
                positions = []
                break
            positions.append(dataset_index[key])
        if len(positions) == row_count:
            try:
                import numpy as np

                return X[np.asarray(positions, dtype=int)]
            except Exception:
                return None
    return None


def _select_prediction_X(
    X: Any,
    arrays: dict[str, Any],
    *,
    dataset_metadata: dict[str, list[Any]] | None = None,
) -> Any | None:
    try:
        import numpy as np

        sample_indices = arrays.get("sample_indices")
        if sample_indices is not None:
            indices = np.asarray(sample_indices, dtype=int).reshape(-1)
            if indices.size > 0 and indices.min() >= 0 and indices.max() < X.shape[0]:
                return X[indices]
    except Exception:
        pass

    try:
        for key in ("y_true", "y_pred"):
            values = arrays.get(key)
            if values is not None and len(values) == X.shape[0]:
                return X
    except Exception:
        pass
    if dataset_metadata:
        return _select_prediction_X_by_identity(X, arrays, dataset_metadata)
    return None


def _publish_robustness_evidence_to_workspace(
    *,
    workspace_path: Path,
    dataset_spec: dict[str, Any],
    model_path: str | None,
) -> dict[str, Any]:
    if not model_path:
        return {"status": "skipped", "reason": "predictor_bundle_unavailable", "published_prediction_count": 0}

    dataset_object = _load_replay_dataset_from_spec(dataset_spec)
    if dataset_object is None:
        return {"status": "skipped", "reason": "dataset_unavailable", "published_prediction_count": 0}

    X = _coerce_dataset_X(dataset_object)
    if X is None:
        return {"status": "skipped", "reason": "dataset_X_unavailable", "published_prediction_count": 0}
    dataset_metadata = _dataset_metadata_columns(dataset_object)

    try:
        from nirs4all.pipeline.storage.workspace_store import WorkspaceStore
    except Exception as exc:
        return {
            "status": "skipped",
            "reason": "workspace_store_unavailable",
            "error": f"{type(exc).__name__}: {exc}",
            "published_prediction_count": 0,
        }

    records: list[dict[str, Any]] = []
    store = WorkspaceStore(workspace_path)
    try:
        rows_df = store.query_predictions()
        for row in rows_df.iter_rows(named=True):
            prediction_id = row.get("prediction_id")
            dataset_name = row.get("dataset_name")
            if not prediction_id or not dataset_name:
                continue
            arrays = store.array_store.load_single(str(prediction_id), dataset_name=str(dataset_name))
            if not isinstance(arrays, dict):
                continue
            prediction_X = _select_prediction_X(X, arrays, dataset_metadata=dataset_metadata)
            if prediction_X is None:
                continue

            raw_result_metadata = arrays.get("result_metadata")
            result_metadata: dict[str, Any] = raw_result_metadata if isinstance(raw_result_metadata, dict) else {}
            robustness_evidence = result_metadata.get("robustness_evidence")
            if not isinstance(robustness_evidence, dict):
                robustness_evidence = {}
            robustness_evidence.update(
                {
                    "X": "prediction_arrays.X",
                    "predictor_bundle": model_path,
                    "publisher": "nirs4all-cluster.runner",
                }
            )
            result_metadata = {
                **result_metadata,
                "robustness_evidence": robustness_evidence,
            }

            records.append(
                {
                    "prediction_id": str(prediction_id),
                    "dataset_name": str(dataset_name),
                    "model_name": row.get("model_name") or "",
                    "fold_id": str(row.get("fold_id") or ""),
                    "partition": row.get("partition") or "",
                    "metric": row.get("metric") or "",
                    "val_score": row.get("val_score"),
                    "task_type": row.get("task_type") or "",
                    "y_true": arrays.get("y_true"),
                    "y_pred": arrays.get("y_pred"),
                    "y_proba": arrays.get("y_proba"),
                    "sample_indices": arrays.get("sample_indices"),
                    "weights": arrays.get("weights"),
                    "sample_metadata": arrays.get("sample_metadata"),
                    "X": prediction_X,
                    "result_metadata": result_metadata,
                }
            )

        if records:
            store.array_store.save_batch(records)
        return {
            "status": "published" if records else "skipped",
            "reason": None if records else "no_row_aligned_predictions",
            "published_prediction_count": len(records),
        }
    except Exception as exc:
        return {
            "status": "skipped",
            "reason": "publication_error",
            "error": f"{type(exc).__name__}: {exc}",
            "published_prediction_count": len(records),
        }
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()


def _robustness_handoff_trace(
    handoff: dict[str, Any],
    produced: dict[str, Any],
    publication_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    published_fields = _get_mapping_value(handoff, "publishedFields", "published_fields") or []
    if not isinstance(published_fields, list):
        published_fields = []
    alignment_strategies = _get_mapping_value(handoff, "alignmentStrategies", "alignment_strategies") or []
    if not isinstance(alignment_strategies, list):
        alignment_strategies = []

    model_path = produced.get("model")
    published: dict[str, Any] = {}
    missing = [field for field in published_fields if isinstance(field, str)]
    if model_path:
        published["result_metadata.robustness_evidence.predictor_bundle"] = model_path
        missing = [field for field in missing if field != "result_metadata.robustness_evidence.predictor_bundle"]

    if publication_summary and publication_summary.get("status") == "published":
        for field in (
            "prediction_arrays.X",
            "result_metadata.robustness_evidence.X",
        ):
            published[field] = "task_workspace_prediction_arrays"
            missing = [item for item in missing if item != field]

    return {
        "kind": "robustness_evidence_publication_trace",
        "publisher": "nirs4all-cluster.runner",
        "status": "received_needs_array_publication" if missing else "published",
        "requested": bool(handoff.get("requested", True)),
        "destination": _get_mapping_value(handoff, "destination") or "result_metadata.robustness_evidence",
        "fail_closed": bool(_get_mapping_value(handoff, "failClosed", "fail_closed") is not False),
        "alignment_strategies": [str(item) for item in alignment_strategies],
        "published_fields": [str(item) for item in published_fields],
        "published": published,
        "missing": missing,
        "publication_summary": publication_summary
        or {
            "status": "skipped",
            "reason": "not_attempted",
            "published_prediction_count": 0,
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
        robustness_handoff = _robustness_handoff_from_spec(spec)

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
        if robustness_handoff is not None:
            publication_summary = _publish_robustness_evidence_to_workspace(
                workspace_path=Path(args.workspace),
                dataset_spec=spec["dataset"],
                model_path=produced.get("model"),
            )
            summary["extra"]["robustness_evidence_publication_trace"] = _robustness_handoff_trace(
                robustness_handoff,
                produced,
                publication_summary,
            )

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
