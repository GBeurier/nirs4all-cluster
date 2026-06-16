"""Resolve a task's pipeline/dataset references into local paths.

The worker resolves every reference to a concrete on-disk path (downloading
uploaded artifacts, extracting zips) and emits a *runner spec* — a plain dict the
subprocess runner consumes. Nothing here imports nirs4all; the runner does that.
"""

from __future__ import annotations

import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import yaml

from ..schemas import DatasetRef, PipelineRef, TaskPayload
from ..versioning import fingerprint_file, fingerprint_obj

# A callable the agent provides to fetch an uploaded artifact's bytes to a path.
DownloadFn = Callable[[str, Path], Path]


class _Materializer(Protocol):
    def __call__(self, ref: Any, inputs_dir: Path, download: DownloadFn) -> dict[str, Any]: ...


def _materialize_pipeline(ref: PipelineRef, inputs_dir: Path, download: DownloadFn) -> dict[str, Any]:
    if ref.kind == "path":
        path = Path(ref.path)  # type: ignore[arg-type]
        if not path.exists():
            raise FileNotFoundError(f"pipeline path not accessible on worker: {path}")
        return {"mode": "path", "path": str(path)}
    if ref.kind == "inline_json":
        dest = inputs_dir / "pipeline.yaml"
        dest.write_text(yaml.safe_dump(ref.inline, sort_keys=False), encoding="utf-8")
        return {"mode": "path", "path": str(dest)}
    if ref.kind == "artifact":
        dest = inputs_dir / "pipeline.yaml"
        download(ref.artifact_id, dest)  # type: ignore[arg-type]
        return {"mode": "path", "path": str(dest)}
    if ref.kind == "python_entrypoint":
        sys_path: list[str] = []
        if ref.bundle_artifact_id:
            bundle_zip = inputs_dir / "bundle.zip"
            download(ref.bundle_artifact_id, bundle_zip)
            bundle_dir = inputs_dir / "bundle"
            _safe_extract(bundle_zip, bundle_dir)
            sys_path.append(str(bundle_dir))
        return {"mode": "entrypoint", "entrypoint": ref.entrypoint, "sys_path": sys_path}
    raise ValueError(f"unsupported pipeline kind: {ref.kind!r}")


def _materialize_dataset(ref: DatasetRef, inputs_dir: Path, download: DownloadFn) -> dict[str, Any]:
    if ref.kind in ("shared_path", "worker_local"):
        # NOTE: 'worker_local' currently behaves like 'shared_path' (the path must
        # exist on whichever worker leases the task). True locality (routing only
        # to labelled workers) is a future federated-mode feature; for now pin it
        # yourself via requirements.labels.
        path = Path(ref.path)  # type: ignore[arg-type]
        if not path.exists():
            raise FileNotFoundError(f"dataset path not accessible on worker: {path}")
        return {"mode": "path", "path": str(path)}
    if ref.kind == "artifact":
        archive = inputs_dir / "dataset.zip"
        download(ref.artifact_id, archive)  # type: ignore[arg-type]
        dataset_dir = inputs_dir / "dataset"
        _safe_extract(archive, dataset_dir)
        return {"mode": "path", "path": str(_dataset_root(dataset_dir))}
    if ref.kind == "catalog":
        # Planned (design dataset kind #3). Resolving a DOI to a local dir uses
        # nirs4all_datasets.load()/resolve_config(); deferred in the prototype to
        # keep the worker free of that dependency. Use shared_path/artifact.
        raise NotImplementedError(
            "dataset kind 'catalog' is not implemented in the prototype worker; "
            "use 'shared_path' or 'artifact'"
        )
    raise ValueError(f"unsupported dataset kind: {ref.kind!r}")


def build_runner_spec(task: TaskPayload, workdir: Path, download: DownloadFn) -> dict[str, Any]:
    inputs_dir = workdir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    pipeline_spec = _materialize_pipeline(task.pipeline, inputs_dir, download)
    dataset_spec = _materialize_dataset(task.dataset, inputs_dir, download)
    return {
        "pipeline": pipeline_spec,
        "dataset": dataset_spec,
        "params": dict(task.params),
        "outputs": task.outputs.model_dump(),
        # Content fingerprint of the pipeline the worker actually runs. The runner
        # ignores this key; the agent reads it back onto the TaskResult so results
        # are traceable to an exact pipeline (and the server can compare it against
        # any expected_fingerprint the client pinned).
        "pipeline_fingerprint": _pipeline_fingerprint(task.pipeline, pipeline_spec),
    }


def _pipeline_fingerprint(ref: PipelineRef, spec: dict[str, Any]) -> str:
    """sha256 of the pipeline content, matching the client's hash for inline pipelines."""
    if ref.kind == "inline_json":
        return fingerprint_obj(ref.inline)
    if spec.get("mode") == "path":
        return fingerprint_file(spec["path"])
    if ref.kind == "python_entrypoint":
        return fingerprint_obj({"entrypoint": ref.entrypoint})
    return fingerprint_obj(spec)


def _safe_extract(archive: Path, dest: Path) -> None:
    """Extract a zip, refusing path traversal (zip-slip), absolute members and symlinks."""
    dest.mkdir(parents=True, exist_ok=True)
    dest_resolved = dest.resolve()
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            member = info.filename
            if member.startswith("/") or Path(member).is_absolute():
                raise ValueError(f"absolute path in archive: {member}")
            # Reject symlinks (high bits of external_attr encode the unix mode).
            mode = info.external_attr >> 16
            if (mode & 0o170000) == 0o120000:
                raise ValueError(f"symlink in archive: {member}")
            target = (dest / member).resolve()
            if not target.is_relative_to(dest_resolved):
                raise ValueError(f"unsafe path in archive: {member}")
        zf.extractall(dest)


def _dataset_root(extracted: Path) -> Path:
    """If a zip contained a single top-level folder, descend into it."""
    entries = [p for p in extracted.iterdir() if not p.name.startswith("__MACOSX")]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return extracted
