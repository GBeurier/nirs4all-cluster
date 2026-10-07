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
import hashlib
import importlib
import io
import json
import math
import re
import shutil
import time
import traceback
import zipfile
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
    from nirs4all_cluster.schemas import NativeExperimentLaunchPayload

    native_payload = NativeExperimentLaunchPayload.model_validate(native_payload).model_dump()
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

        X_array = np.asarray(X)
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
    counts: list[int] = []
    for key in ("y_true", "y_pred", "y_proba", "sample_indices"):
        value = arrays.get(key)
        if value is not None:
            try:
                counts.append(len(value))
            except TypeError:
                return None
    sample_metadata = arrays.get("sample_metadata")
    if isinstance(sample_metadata, dict):
        for value in sample_metadata.values():
            values = _sequence_or_none(value)
            if values is not None:
                counts.append(len(values))
    return counts[0] if counts and counts[0] > 0 and len(set(counts)) == 1 else None


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
        for alias in aliases:
            if alias not in payload:
                continue
            values = _sequence_or_none(payload[alias])
            if values is None or len(values) != expected_len:
                raise ValueError("Invalid prediction identity length")
            if canonical in result and result[canonical] != values:
                raise ValueError("Conflicting prediction identities")
            result[canonical] = values

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


def _prediction_identity_positions(
    X: Any,
    arrays: dict[str, Any],
    dataset_metadata: dict[str, list[Any]],
) -> list[int] | None:
    row_count = _prediction_row_count(arrays)
    if not row_count or not dataset_metadata:
        return None
    prediction_metadata = _prediction_identity_columns(arrays, row_count=row_count)
    selected: list[int] | None = None
    for aliases in _ROBUSTNESS_REPLAY_IDENTITY_KEYS:
        values = prediction_metadata.get(aliases[0])
        dataset_columns = [dataset_metadata[alias] for alias in aliases if alias in dataset_metadata]
        if values is None or not dataset_columns:
            continue
        if any(column != dataset_columns[0] for column in dataset_columns):
            return None
        dataset_index = _unique_identity_index(dataset_columns[0], expected_len=X.shape[0])
        if dataset_index is None or _unique_identity_index(values, expected_len=row_count) is None:
            return None
        try:
            positions = [dataset_index[str(value)] for value in values]
        except KeyError:
            return None
        if selected is not None and selected != positions:
            return None
        selected = positions
    return selected


def _select_prediction_X_by_identity(
    X: Any,
    arrays: dict[str, Any],
    dataset_metadata: dict[str, list[Any]],
) -> Any | None:
    positions = _prediction_identity_positions(X, arrays, dataset_metadata)
    if positions is None:
        return None
    import numpy as np

    return X[np.asarray(positions, dtype=int)]


def _select_prediction_X(
    X: Any,
    arrays: dict[str, Any],
    *,
    dataset_metadata: dict[str, list[Any]] | None = None,
) -> Any | None:
    row_count = _prediction_row_count(arrays)
    if row_count is None:
        return None
    try:
        import numpy as np

        identities = _prediction_identity_columns(arrays, row_count=row_count)
        identity_positions = None
        if any(_unique_identity_index(values, expected_len=row_count) is None for values in identities.values()):
            return None
        comparable_identity = any(
            aliases[0] in identities and any(alias in (dataset_metadata or {}) for alias in aliases)
            for aliases in _ROBUSTNESS_REPLAY_IDENTITY_KEYS
        )
        if comparable_identity:
            identity_positions = _prediction_identity_positions(X, arrays, dataset_metadata or {})
            if identity_positions is None:
                return None
        sample_indices = arrays.get("sample_indices")
        if sample_indices is not None:
            raw = np.asarray(sample_indices)
            if raw.ndim != 1 or len(raw) != row_count or raw.dtype.kind not in "iu":
                return None
            indices = np.asarray(sample_indices, dtype=int)
            if len(set(indices.tolist())) != row_count or indices.min() < 0 or indices.max() >= X.shape[0]:
                return None
            selected = X[indices]
            if identity_positions is not None and indices.tolist() != identity_positions:
                return None
            return selected
        if identity_positions is not None:
            return X[np.asarray(identity_positions, dtype=int)]
        if not identities and row_count == X.shape[0]:
            return X
    except Exception:
        return None
    return None


def _select_legacy_bundle_fold(path: Path, fold_id: str, input_dtype: str, weights: Any = None) -> None:
    """Retain the requested captured fold closure in an SDK-produced bundle.

    The SDK legacy exporter includes every fitted fold and REFIT model even for
    source=one prediction. The standard loader otherwise prefers REFIT. Select
    archived states only; never refit or reimplement their numerical execution.
    """
    if fold_id not in {"final", "avg", "w_avg"} and not fold_id.isdecimal():
        raise ValueError("Unsupported prediction fold identity")
    with zipfile.ZipFile(path) as archive:
        entries = [(item, archive.read(item)) for item in archive.infolist()]
    captured_folds = {
        match.group(1)
        for item, _ in entries
        if item.filename.startswith("artifacts/") and (match := re.search(r"_fold([^_/]+)_", item.filename))
    }
    # SDK final-source export may number its sole captured model fold0.
    selected_fold = "0" if fold_id == "final" and captured_folds == {"0"} else fold_id
    fold_weights = None
    if fold_id == "w_avg" and weights is not None:
        import numpy as np

        values = np.asarray(weights)
        numeric_folds = sorted(int(value) for value in captured_folds if value.isdecimal())
        if values.ndim != 1 or numeric_folds != list(range(len(values))) or not np.isfinite(values).all():
            raise ValueError("Captured ensemble weights do not identify the exported folds")
        fold_weights = {str(index): float(value) for index, value in enumerate(values)}
    selected = []
    found = False
    for item, data in entries:
        match = re.search(r"_fold([^_/]+)_", item.filename) if item.filename.startswith("artifacts/") else None
        if match:
            captured = match.group(1)
            keep = captured == "all" or (
                captured == selected_fold if fold_id not in {"avg", "w_avg"} else captured.isdecimal()
            )
            if not keep:
                continue
            found = found or captured == selected_fold or (fold_id in {"avg", "w_avg"} and captured.isdecimal())
        if item.filename == "manifest.json":
            manifest = json.loads(data)
            manifest["fold_strategy"] = (
                "average" if fold_id == "avg" else "weighted_average" if fold_id == "w_avg" else "single"
            )
            data = json.dumps(manifest).encode()
        if item.filename == "fold_weights.json":
            data = (
                json.dumps(fold_weights).encode() if fold_weights is not None else data if fold_id == "w_avg" else b"{}"
            )
        selected.append((item, data))
    if fold_weights is not None and not any(item.filename == "fold_weights.json" for item, _ in selected):
        selected.append((zipfile.ZipInfo("fold_weights.json"), json.dumps(fold_weights).encode()))
    if not found:
        raise ValueError("Selected fold artifact is absent from the exported bundle")
    # ArrayStore persists X as float64. Bind the original raw-input dtype to the
    # captured entry operator so ordinary bundle.predict on reloaded X retains
    # the training representation, without requiring consumers to read metadata.
    import joblib
    import numpy as np
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer

    operators = [
        (index, int(match.group(1)))
        for index, (item, _) in enumerate(selected)
        if item.filename.startswith("artifacts/") and (match := re.search(r"/step_(\d+)_", item.filename))
    ]
    if not operators:
        raise ValueError("Exported bundle has no captured entry operator")
    first_step = min(step for _, step in operators)
    first = [index for index, step in operators if step == first_step]
    if len(first) != 1:
        raise ValueError("Raw input dtype needs an unambiguous captured entry operator")
    index = first[0]
    item, data = selected[index]
    captured_operator = joblib.load(io.BytesIO(data))
    restored = Pipeline(
        [
            ("raw_dtype", FunctionTransformer(np.asarray, kw_args={"dtype": input_dtype})),
            ("captured", captured_operator),
        ]
    )
    encoded = io.BytesIO()
    joblib.dump(restored, encoded)
    selected[index] = (item, encoded.getvalue())
    temporary = path.with_suffix(".n4a.tmp")
    try:
        with zipfile.ZipFile(temporary, "w") as archive:
            for item, data in selected:
                archive.writestr(item, data)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _prediction_bundle(
    run_result: Any,
    row: dict[str, Any],
    arrays: dict[str, Any],
    X: Any,
    model_path: str | None,
    workspace_path: Path,
    final_row: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    """Export only the fitted model belonging to this record; verify its baseline replay."""
    if run_result is None:
        return None, "prediction_model_identity_unavailable"
    destination: Path | None = None
    try:
        import nirs4all
        import numpy as np

        prediction_id = str(row["prediction_id"])
        destination = (
            workspace_path / "robustness_models" / (hashlib.sha256(prediction_id.encode()).hexdigest() + ".n4a")
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        if run_result._is_dagml_engine():
            final = run_result.final
            if not final or str(row.get("fold_id")) != "final":
                return None, "native_fold_predictor_not_exportable"
            if final_row is None or any(
                row.get(key) != final_row.get(key)
                for key in ("pipeline_id", "model_name", "branch_id", "dataset_name", "source_index")
            ):
                return None, "native_final_model_identity_mismatch"
            if not model_path:
                return None, "predictor_bundle_unavailable"
            shutil.copyfile(model_path, destination)
        else:
            source = run_result.predictions.get_prediction_by_id(prediction_id, load_arrays=False)
            if not source:
                return None, "prediction_model_identity_unavailable"
            run_result.export(str(destination), source=source)
            _select_legacy_bundle_fold(destination, str(row.get("fold_id")), str(X.dtype), arrays.get("weights"))
        replay = nirs4all.predict(
            model=str(destination),
            data={"X": X},
            verbose=0,
            engine="dag-ml" if run_result._is_dagml_engine() else "legacy",
            workspace_path=str(workspace_path),
        )
        baseline = np.asarray(arrays.get("y_pred"))
        actual = np.asarray(replay.y_pred)
        if baseline.size != actual.size or not np.array_equal(baseline.reshape(-1), actual.reshape(-1)):
            destination.unlink(missing_ok=True)
            return None, "predictor_baseline_replay_mismatch"
        return str(destination), None
    except Exception as exc:
        if destination is not None:
            destination.unlink(missing_ok=True)
        return None, f"predictor_export_unavailable:{type(exc).__name__}"


def _publish_robustness_evidence_to_workspace(
    *,
    workspace_path: Path,
    dataset_spec: dict[str, Any],
    model_path: str | None,
    run_result: Any = None,
) -> dict[str, Any]:
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
    outcomes: list[dict[str, Any]] = []
    store = WorkspaceStore(workspace_path)
    try:
        rows = list(store.query_predictions().iter_rows(named=True))
        final = run_result.final if run_result is not None else None
        final_id = (final.get("id") or final.get("prediction_id")) if final else None
        final_row = next((row for row in rows if row.get("prediction_id") == final_id), None)
        for row in rows:
            prediction_id, dataset_name = row.get("prediction_id"), row.get("dataset_name")
            outcome: dict[str, Any] = {
                "prediction_id": prediction_id,
                "dataset_name": dataset_name,
                "status": "unavailable",
                "missing": ["X", "predictor_bundle"],
            }
            outcomes.append(outcome)
            if not prediction_id or not dataset_name:
                outcome["reason"] = "prediction_identity_unavailable"
                continue
            arrays = store.array_store.load_single(str(prediction_id), dataset_name=str(dataset_name))
            if not isinstance(arrays, dict):
                outcome["reason"] = "prediction_arrays_unavailable"
                continue
            prediction_X = _select_prediction_X(X, arrays, dataset_metadata=dataset_metadata)
            if prediction_X is None:
                outcome["reason"] = "row_alignment_unavailable"
                continue
            bundle, reason = _prediction_bundle(
                run_result, row, arrays, prediction_X, model_path, workspace_path, final_row
            )
            metadata = _mapping_or_empty(arrays.get("result_metadata"))
            evidence = _mapping_or_empty(metadata.get("robustness_evidence"))
            # Discard stale predictor claims from earlier publication attempts.
            evidence.pop("predictor_bundle", None)
            evidence.pop("predictor_bundle_relative_path", None)
            evidence.update({"X": "prediction_arrays.X", "publisher": "nirs4all-cluster.runner"})
            if bundle:
                relative = Path(bundle).relative_to(workspace_path).as_posix()
                evidence.update({"predictor_bundle": bundle, "predictor_bundle_relative_path": relative})
                outcome.update({"status": "published", "missing": [], "predictor_bundle_relative_path": relative})
            else:
                outcome.update({"status": "partial", "missing": ["predictor_bundle"], "reason": reason})
            evidence["publication"] = dict(outcome)
            # ArrayStore upserts replace whole records: preserve every existing array/metadata field.
            records.append(
                {**row, **arrays, "X": prediction_X, "result_metadata": {**metadata, "robustness_evidence": evidence}}
            )
        if records:
            store.array_store.save_batch(records)
        complete = bool(outcomes) and all(item["status"] == "published" for item in outcomes)
        return {
            "status": "published" if complete else "partial" if records else "skipped",
            "reason": None if complete else "incomplete_prediction_evidence",
            "published_prediction_count": len(records),
            "prediction_outcomes": outcomes,
            "workspace_required": bool(records),
        }
    except Exception as exc:
        for outcome in outcomes:
            outcome.update(
                {"status": "unavailable", "missing": ["X", "predictor_bundle"], "reason": "publication_error"}
            )
        return {
            "status": "skipped",
            "reason": "publication_error",
            "error": f"{type(exc).__name__}: {exc}",
            "published_prediction_count": 0,
            "prediction_outcomes": outcomes,
        }
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()


def _robustness_handoff_trace(
    handoff: dict[str, Any], produced: dict[str, Any], publication_summary: dict[str, Any] | None = None
) -> dict[str, Any]:
    fields = _get_mapping_value(handoff, "publishedFields", "published_fields") or []
    strategies = _get_mapping_value(handoff, "alignmentStrategies", "alignment_strategies") or []
    summary = publication_summary or {"status": "skipped", "reason": "not_attempted", "published_prediction_count": 0}
    outcomes = summary.get("prediction_outcomes") or []
    published: dict[str, Any] = {}
    if outcomes and all("X" not in outcome["missing"] for outcome in outcomes):
        published.update(
            {
                "prediction_arrays.X": "task_workspace_prediction_arrays",
                "result_metadata.robustness_evidence.X": "task_workspace_prediction_arrays",
            }
        )
    if outcomes and all("predictor_bundle" not in outcome["missing"] for outcome in outcomes):
        published["result_metadata.robustness_evidence.predictor_bundle"] = "task_workspace_prediction_bundles"
    missing = [field for field in fields if field not in published]
    return {
        "kind": "robustness_evidence_publication_trace",
        "publisher": "nirs4all-cluster.runner",
        "status": "received_needs_array_publication" if missing else "published",
        "requested": handoff.get("requested", True),
        "destination": handoff.get("destination") or "result_metadata.robustness_evidence",
        "fail_closed": _get_mapping_value(handoff, "failClosed", "fail_closed") is not False,
        "alignment_strategies": strategies,
        "published_fields": fields,
        "published": published,
        "missing": missing,
        "publication_summary": summary,
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
        robustness_handoff = _robustness_handoff_from_spec(spec)
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
        # ``1`` is the cluster's neutral/default worker-local parallelism.  Do
        # not forward it to DAG-ML, whose public V1 API rejects the legacy
        # PipelineRunner-only option.  Non-neutral requests remain explicit so
        # an engine that cannot honor them fails closed instead of silently
        # changing the requested execution contract.
        if inner_n_jobs != 1:
            params["n_jobs"] = inner_n_jobs

        start = time.time()
        run_result = nirs4all.run(
            pipeline=pipeline,
            dataset=dataset,
            workspace_path=str(Path(args.workspace)),
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
        if robustness_handoff is not None and robustness_handoff.get("requested", True):
            publication_summary = _publish_robustness_evidence_to_workspace(
                workspace_path=Path(args.workspace),
                dataset_spec=spec["dataset"],
                model_path=produced.get("model"),
                run_result=run_result,
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
