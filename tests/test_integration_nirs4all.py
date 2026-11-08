"""End-to-end integration tests that actually run nirs4all on a worker.

Skipped automatically when nirs4all (or the sample data) is unavailable, so the
rest of the suite stays dependency-free. Covers the design's validation tests:
an atomic job producing a ``.n4a``, two workers running a matrix in parallel,
metric parity vs a local ``nirs4all.run()``, and state-dir containment.
"""

import hashlib
import json
import os
import socket
import struct
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path

import httpx
import pytest

pytest.importorskip("nirs4all")

from auth_support import TOKEN, create_app  # noqa: E402

from nirs4all_cluster.client import ClusterClient  # noqa: E402
from nirs4all_cluster.server.app import ServerConfig
from nirs4all_cluster.worker.agent import WorkerAgent  # noqa: E402

DATA = Path("/home/delete/nirs4all/nirs4all-data/regression")
PIPELINE = Path(__file__).resolve().parents[1] / "examples" / "pipelines" / "pls.yaml"
GRAPEVINE = DATA / "GRAPEVINE_LeafTraits" / "PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra"

# Only the GRAPEVINE/CORN tests need nirs4all-data; the canonical test brings its own explicit cohort inputs.
requires_grapevine = pytest.mark.skipif(not GRAPEVINE.exists(), reason="nirs4all-data not present")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LiveServer:
    def __init__(self, state_dir: str):
        import uvicorn

        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = ServerConfig(state_dir=state_dir, lease_ttl_s=30.0, reaper_interval_s=1.0)
        self._uvicorn = uvicorn.Server(
            uvicorn.Config(create_app(config), host="127.0.0.1", port=self.port, log_level="warning")
        )
        self._thread = threading.Thread(target=self._uvicorn.run, daemon=True)

    def __enter__(self) -> "_LiveServer":
        self._thread.start()
        for _ in range(100):
            try:
                if httpx.get(f"{self.base_url}/", timeout=1.0).status_code == 200:
                    return self
            except httpx.HTTPError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def __exit__(self, *exc) -> None:
        self._uvicorn.should_exit = True
        self._thread.join(timeout=10)


def _start_worker(base_url, state_dir, **kw) -> WorkerAgent:
    agent = WorkerAgent(
        base_url, token=TOKEN, state_dir=state_dir, poll_interval=0.5, allowed_roots=(DATA, PIPELINE.parent), **kw
    )
    agent.register()
    threading.Thread(target=agent.serve, daemon=True).start()
    return agent


@requires_grapevine
def test_atomic_job_and_metric_parity(tmp_path):
    import nirs4all

    with _LiveServer(str(tmp_path / "server")) as server:
        worker = _start_worker(server.base_url, str(tmp_path / "worker"))
        try:
            client = ClusterClient(server.base_url, token=TOKEN)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                dataset=str(GRAPEVINE),
                params={"random_state": 42, "refit": True},
                name="parity",
            )
            job = client.wait(job.id, timeout=180, poll=1.0)
            assert job.status.value == "succeeded", job.aggregate.errors
            cluster_rmse = job.aggregate.best_metric
            assert cluster_rmse is not None and cluster_rmse == pytest.approx(cluster_rmse)  # finite

            # The exported best model is downloadable and is a real zip bundle.
            out = client.download_best_model(job.id, tmp_path / "best.n4a")
            assert out is not None and out.stat().st_size > 1000
            import zipfile

            assert zipfile.is_zipfile(out)
        finally:
            worker.stop()

    # Parity: a local run with identical inputs must match the cluster metric.
    local = nirs4all.run(
        pipeline=str(PIPELINE),
        dataset=str(GRAPEVINE),
        workspace_path=str(tmp_path / "local_ws"),
        random_state=42,
        refit=True,
        save_charts=False,
        verbose=0,
    )
    assert cluster_rmse == pytest.approx(local.best_rmse, abs=1e-6)


@requires_grapevine
def test_two_workers_parallel_matrix(tmp_path):
    corn = DATA / "CORN"
    datasets = [
        str(corn / "Corn_Moisture_80_WangStyle_m5spec"),
        str(corn / "Corn_Oil_80_WangStyle_m5spec"),
    ]
    if not all(Path(d).exists() for d in datasets):
        pytest.skip("CORN datasets not present")

    with _LiveServer(str(tmp_path / "server")) as server:
        w1 = _start_worker(server.base_url, str(tmp_path / "w1"), name="w1")
        w2 = _start_worker(server.base_url, str(tmp_path / "w2"), name="w2")
        try:
            client = ClusterClient(server.base_url, token=TOKEN)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                datasets=datasets,
                params={"random_state": 42},
                name="matrix",
            )
            job = client.wait(job.id, timeout=240, poll=1.0)
            assert job.status.value == "succeeded", job.aggregate.errors
            assert job.aggregate.num_tasks == 2
            assert job.aggregate.num_succeeded == 2
            assert len(job.aggregate.ranking) == 2
            # Both workers should have participated.
            tasks = client.get_tasks(job.id)
            assert len({t.worker_id for t in tasks}) == 2
        finally:
            w1.stop()
            w2.stop()


@requires_grapevine
def test_state_dir_containment(tmp_path):
    """The worker cleans its per-task workdir when keep_task_workspace is false."""
    with _LiveServer(str(tmp_path / "server")) as server:
        worker_state = tmp_path / "worker"
        worker = _start_worker(server.base_url, str(worker_state))
        try:
            client = ClusterClient(server.base_url, token=TOKEN)
            job = client.submit_run(
                pipeline=str(PIPELINE),
                dataset=str(GRAPEVINE),
                params={"random_state": 42},
                outputs={"keep_task_workspace": False},
            )
            job = client.wait(job.id, timeout=180, poll=1.0)
            assert job.status.value == "succeeded"
        finally:
            worker.stop()
        tasks_dir = worker_state / "tasks"
        # No task workdirs should remain after cleanup.
        assert not tasks_dir.exists() or not any(tasks_dir.iterdir())


# Call-through probe for the Cluster child and the fresh prediction process. It is installed as a
# ``sitecustomize`` module on PYTHONPATH, so it runs inside every separate interpreter the job spawns and
# wraps the REAL ``nirs4all_core`` functions (the real Core still executes). Each call is appended to the
# file named by N4A_CORE_PROBE_LOG, which is how an honest call count crosses the process boundary.
_CORE_PROBE = """
import importlib.abc, importlib.util, json, os, sys

_LOG = os.environ.get("N4A_CORE_PROBE_LOG")
_WRAPPED = ("produce_n4a_native", "inspect_n4a_bytes", "predict_n4a_native", "open_n4a_file")


def _append(record):
    with open(_LOG, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(record) + "\\n")


def _wrap(name, function):
    def call_through(*args, **kwargs):
        record = {"pid": os.getpid(), "function": name, "run_id": kwargs.get("run_id")}
        _append({**record, "phase": "start"})
        try:
            result = function(*args, **kwargs)
        except BaseException as error:
            _append({**record, "phase": "refused", "error": type(error).__name__})
            raise
        _append({**record, "phase": "returned"})
        return result

    return call_through


class _Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != "nirs4all_core":
            return None
        sys.meta_path.remove(self)
        try:
            spec = importlib.util.find_spec(fullname)
        finally:
            sys.meta_path.insert(0, self)
        if spec is None or spec.loader is None:
            return spec
        real = spec.loader

        class _Loader(importlib.abc.Loader):
            def create_module(self, spec):
                return real.create_module(spec)

            def exec_module(self, module):
                real.exec_module(module)
                for name in _WRAPPED:
                    function = getattr(module, name, None)
                    if callable(function):
                        setattr(module, name, _wrap(name, function))

        spec.loader = _Loader()
        return spec


if _LOG:
    sys.meta_path.insert(0, _Finder())
"""

# Fresh cohort-interpreter process: validates, predicts (external record through the SDK facade, stored
# record through Core's final facade) and dumps every real owner output as JSON. No other training route.
_FRESH_PROCESS = """
import hashlib, json, sys, uuid

import nirs4all
import nirs4all_core

mode, archive, record, partition, library, out = sys.argv[1:7]
data = {"record": record, "partition": partition}
archive_bytes = open(archive, "rb").read()
if mode == "corrupt":
    nirs4all.predict(model=archive, data=data, engine="native", methods_library_path=library)
    raise SystemExit("a corrupted container was not refused")
library_sha256 = hashlib.sha256(open(library, "rb").read()).hexdigest()
view_before = nirs4all.inspect_n4a(archive)
external = nirs4all.predict(model=archive, data=data, engine="native", methods_library_path=library)
stored = nirs4all_core.predict_n4a_native(
    archive_bytes, record=None, entrypoint="predict", source_id=view_before["training"]["source_id"],
    partition=partition, request_id="request:cluster-test." + uuid.uuid4().hex, run_id="run:cluster-test." + uuid.uuid4().hex,
    methods_library_path=library, methods_library_sha256=library_sha256, require_training_schema=True,
)
view_after = nirs4all.inspect_n4a(archive)
json.dump({
    "archive_sha256_after": hashlib.sha256(open(archive, "rb").read()).hexdigest(),
    "view_before": view_before, "view_after": view_after,
    "external": {"y_pred": external.y_pred.tolist(), "sample_ids": external.sample_ids, "metadata": external.metadata},
    "stored": stored,
}, open(out, "w"))
"""


def _cohort_process(tmp_path: Path, mode: str, archive: Path, record: str, partition: str, library: str, probe_log: Path, tag: str = ""):
    scripts = tmp_path / "scripts"
    scripts.mkdir(exist_ok=True)
    script = scripts / "fresh_process.py"
    script.write_text(_FRESH_PROCESS)
    work = tmp_path / "fresh"
    work.mkdir(exist_ok=True)
    out = work / f"{mode}{tag}.json"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(tmp_path / "probe"), os.environ.get("PYTHONPATH", "")]).rstrip(os.pathsep)}
    env["N4A_CORE_PROBE_LOG"] = str(probe_log)
    done = subprocess.run(
        [sys.executable, str(script), mode, str(archive), record, partition, library, str(out)],
        capture_output=True, text=True, env=env, cwd=work, timeout=600, check=False,
    )
    return done, out


def _probe_calls(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _flip_payload_byte(source: Path, target: Path, member: str) -> None:
    """Corrupt the first stored data byte of one non-manifest member, leaving the routing manifest readable."""
    data = bytearray(source.read_bytes())
    with zipfile.ZipFile(source) as archive:
        header_offset = archive.getinfo(member).header_offset
    name_length, extra_length = struct.unpack_from("<HH", data, header_offset + 26)
    data[header_offset + 30 + name_length + extra_length] ^= 0xFF
    target.write_bytes(bytes(data))


def _rotated_record(document: dict, shift: int) -> dict:
    """The same samples with the record rows rotated by ``shift``: every row-aligned field moves together.

    Written for the cohort IO record layout (``origin_ids``/``fold_ids`` plus ``dataset.sample_ids``,
    ``y``, ``partitions`` and per-source ``sample_ids``/``array``); it is only used to build a predict-only record.
    """
    dataset = document["dataset"]
    count = len(dataset["sample_ids"])
    order = [(row + shift) % count for row in range(count)]

    def pick(rows: list) -> list:
        assert len(rows) == count
        return [rows[row] for row in order]

    rotated = json.loads(json.dumps(document))
    rotated["origin_ids"], rotated["fold_ids"] = pick(document["origin_ids"]), pick(document["fold_ids"])
    target = rotated["dataset"]
    target["sample_ids"], target["y"]["values"] = pick(dataset["sample_ids"]), pick(dataset["y"]["values"])
    target["partitions"]["values"] = pick(dataset["partitions"]["values"])
    for source, original in zip(target["sources"], dataset["sources"], strict=True):
        source["sample_ids"], source["array"]["values"] = pick(original["sample_ids"]), pick(original["array"]["values"])
    return rotated


def test_canonical_n4a_fit_export_download_predict(tmp_path, monkeypatch):
    """Real canonical path; runs only in the cohort venv with a matching Core, Methods and IO record.

    Required environment: ``N4A_CANONICAL_RECIPE`` (native recipe JSON/YAML), ``N4A_CANONICAL_RECORD``
    (IO record JSON), ``N4A_CANONICAL_SOURCE_ID``, ``N4A_CANONICAL_PARTITION`` (the record partition to
    replay) and ``N4M_LIBRARY_PATH``. The recipe and record must lie under directories the worker may read.
    The test process itself makes no native call: every Core call happens in the Cluster child or in a
    fresh interpreter (``sys.executable``, the cohort interpreter).

    Evidence boundaries (not mocked, not guessed): the single Core fit is counted by a call-through probe
    around the real ``produce_n4a_native`` inside the Cluster child (a separate interpreter); the fit count
    INSIDE Core (folds x candidates + one refit) is not exposed by any owner field, so it is not asserted
    here; the owner only evidences that the refit completed and that replays run phase ``PREDICT`` (the owner's SCREAMING_SNAKE_CASE ``Phase``) bound to
    the sealed training outcome. Axes are checked against Core's own stored-vs-external replay and output
    binding, not against an independent reference; that remains for the cohort reference data.
    """
    import importlib

    names = ("N4A_CANONICAL_RECIPE", "N4A_CANONICAL_RECORD", "N4A_CANONICAL_SOURCE_ID", "N4A_CANONICAL_PARTITION", "N4M_LIBRARY_PATH")
    if not all(os.environ.get(name) for name in names):
        pytest.skip("canonical cohort inputs not provided")
    recipe, record, source_id, partition, library = (os.environ[name] for name in names)
    try:
        core = importlib.import_module("nirs4all_core")
    except ImportError:
        pytest.skip("nirs4all_core is not installed")
    if not all(callable(getattr(core, name, None)) for name in ("produce_n4a_native", "inspect_n4a_bytes", "predict_n4a_native")):
        pytest.skip("installed nirs4all_core has no canonical .n4a bridge")
    from nirs4all.api.native_n4a import CANONICAL_N4A_NATIVE_PROFILE

    record_bytes = Path(record).read_bytes()

    probe_dir = tmp_path / "probe"
    probe_dir.mkdir()
    (probe_dir / "sitecustomize.py").write_text(_CORE_PROBE)
    worker_log = tmp_path / "worker-probe.jsonl"
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(probe_dir), os.environ.get("PYTHONPATH", "")]).rstrip(os.pathsep))
    monkeypatch.setenv("N4A_CORE_PROBE_LOG", str(worker_log))  # inherited by the runner child (a separate interpreter)

    with _LiveServer(str(tmp_path / "server")) as server:
        agent = WorkerAgent(
            server.base_url,
            token=TOKEN,
            state_dir=str(tmp_path / "worker"),
            poll_interval=0.5,
            allowed_roots=(Path(recipe).parent, Path(record).parent),
        )
        agent.register()
        threading.Thread(target=agent.serve, daemon=True).start()
        try:
            client = ClusterClient(server.base_url, token=TOKEN)
            job = client.submit_run(
                pipeline=recipe,
                dataset=record,
                params={
                    "engine": "native",
                    "native_profile": CANONICAL_N4A_NATIVE_PROFILE,
                    "source_id": source_id,
                    "methods_library_path": library,
                },
            )
            job = client.wait(job.id, timeout=300, poll=1.0)
            assert job.status.value == "succeeded", job.aggregate.errors
            result = client.get_tasks(job.id)[0].result
            downloaded = client.download_best_model(job.id, tmp_path / "best.n4a")
        finally:
            agent.stop()

    # --- Delivery metadata and transport identity.
    delivery = result.delivery
    assert delivery.status == "exported" and delivery.canonical_core is True
    assert delivery.profile == CANONICAL_N4A_NATIVE_PROFILE and delivery.bundle_format_version is None
    assert zipfile.is_zipfile(downloaded)
    sha256 = hashlib.sha256(downloaded.read_bytes()).hexdigest()
    assert sha256 == delivery.sha256 and downloaded.stat().st_size == delivery.size_bytes

    # --- Exactly one real Core produce in the Cluster child, and nothing else in it (no re-inspection/replay).
    child_calls = _probe_calls(worker_log)
    produced = [c for c in child_calls if c["function"] == "produce_n4a_native"]
    assert [c["phase"] for c in produced] == ["start", "returned"], child_calls
    assert produced[0]["run_id"].startswith("run:nirs4all.native_n4a."), "produce was not called through the SDK bridge"
    assert {c["function"] for c in child_calls} == {"produce_n4a_native"}, child_calls

    # --- Fresh cohort-interpreter process on the downloaded bytes.
    child_log = tmp_path / "fresh-probe.jsonl"
    done, out = _cohort_process(tmp_path, "predict", downloaded, record, partition, library, child_log)
    assert done.returncode == 0, done.stderr[-3000:]
    fresh = json.loads(out.read_text())
    fresh_calls = _probe_calls(child_log)
    assert not [c for c in fresh_calls if c["function"] == "produce_n4a_native"], "prediction trained: a produce call was made"
    assert [c["phase"] for c in fresh_calls if c["function"] == "predict_n4a_native"] == ["start", "returned"] * 2  # external + stored
    assert fresh["archive_sha256_after"] == sha256 and fresh["view_before"] == fresh["view_after"]

    # --- Owner-selected OOF decision, read from the real TrainingOutcome.
    view = fresh["view_after"]
    outcome = view["training"]["outcome"]
    variant_id = outcome["selected_variant_id"]
    output = [o for o in outcome["outputs"] if o["binding"]["binding_id"] == outcome["selection_output_id"]]
    assert len(output) == 1
    selections = outcome["execution_bundle"]["selections"]
    assert len(selections) == 1
    (policy_id, decision), = selections.items()
    assert decision["policy_id"] == policy_id and decision["selected_candidate_id"] == variant_id
    assert decision["evaluation_scope"] == "oof"
    assert outcome["score_set"]["selection_metric"] == decision["metric_name"]
    assert outcome["refit"]["requested"] is True and outcome["refit"]["status"] == "completed"
    selected_score = decision["selected_score"]
    # The public Cluster metrics are that OOF score, never a retained-test value or a report-order winner.
    assert result.metrics.best_score == selected_score
    assert result.metrics.best_rmse == (selected_score if decision["metric_name"] == "rmse" else None)
    # When the score set carries the selected variant's OOF average for the selection metric, it agrees.
    averages = [
        r["metrics"][decision["metric_name"]]
        for r in outcome["score_set"]["reports"]
        if r["partition"] == "validation" and r.get("fold_id") in ("avg", "macro") and r.get("variant_id") in (variant_id, None)
        and decision["metric_name"] in r["metrics"]
    ]
    assert not averages or any(a == pytest.approx(selected_score) for a in averages)

    # --- Model/run/result/score identities survive produce -> export -> download -> fresh inspection.
    manifest = view["manifest"]
    assert manifest["schema"] == delivery.profile and manifest["container_id"] == delivery.container_id
    assert manifest["physical_profile"] == delivery.physical_profile
    assert view["receipt"]["container_sha256"] == sha256
    assert set(delivery.owner_identities) == {"model", "run", "result", "score"}
    for kind, fingerprint in delivery.owner_identities.items():
        artifacts = [a for a in manifest["artifacts"] if a["kind"] == kind]
        assert len(artifacts) == 1 and artifacts[0]["identity"]["fingerprint"] == fingerprint
    model_id = next(a["id"] for a in manifest["artifacts"] if a["kind"] == "model")
    assert hashlib.sha256(record_bytes).hexdigest() in {m["sha256"] for m in manifest["members"]}  # embedded training record

    # --- Real replay: phase, owner bindings and axes, external (SDK) versus stored (Core facade).
    external, stored = fresh["external"]["metadata"], fresh["stored"]
    sources = {"external": (external["replay"], external["receipt"], "predict-external"), "stored": (stored["replay"], stored["receipt"], "predict")}
    blocks = {}
    for label, (replay, receipt, entrypoint) in sources.items():
        assert replay["phase"] == "PREDICT"
        assert replay["source_training_outcome"]["outcome_fingerprint"] == outcome["outcome_fingerprint"]
        assert replay["source_training_outcome"]["outcome_id"] == outcome["outcome_id"]
        assert receipt["binding"]["container_sha256"] == sha256 and receipt["binding"]["entrypoint_id"] == entrypoint
        assert receipt["binding"]["root_id"] == model_id
        assert receipt["record_sha256"] == hashlib.sha256(record_bytes).hexdigest()
        assert len(replay["outputs"]) == 1 and len(replay["outputs"][0]["predictions"]) == 1
        assert replay["outputs"][0]["binding"]["binding_id"] == outcome["selection_output_id"]
        blocks[label] = replay["outputs"][0]["predictions"][0]
    block = blocks["external"]
    target_names = output[0]["binding"]["target_names"]
    assert block["target_names"] == target_names and len(target_names) >= 1
    assert block["sample_ids"] and len(set(block["sample_ids"])) == len(block["sample_ids"])
    assert len(block["values"]) == len(block["sample_ids"]) and all(len(row) == len(target_names) for row in block["values"])
    # The embedded training record replays the same partition with identical signed blocks.
    assert blocks["stored"] == block and stored["receipt"]["io_content_identity"] == external["receipt"]["io_content_identity"]

    # --- Row order. Core's block is ordered by sample id, NOT by record row; Core separately returns the
    # physical source/partition order, which the SDK must apply to y_pred. The record is read here only as an
    # independent oracle (the SDK never parses it).
    document = json.loads(record_bytes)
    dataset = document["dataset"]
    record_ids = [i for i, part in zip(dataset["sample_ids"], dataset["partitions"]["values"], strict=True) if part == partition]
    assert record_ids != sorted(record_ids), "the cohort record must not already be in sample-id order, or this test cannot discriminate"
    owner_ids = fresh["external"]["sample_ids"]
    assert owner_ids == record_ids == external["sample_ids"] == stored["sample_ids"]
    assert sorted(owner_ids) == sorted(block["sample_ids"])  # the same set as the signed block, nothing extra or missing
    y_pred = fresh["external"]["y_pred"]
    block_row = {sample_id: row for row, sample_id in enumerate(block["sample_ids"])}
    assert len(y_pred) == len(owner_ids)
    for row, sample_id in enumerate(owner_ids):  # positional prediction == the signed block row of the SAME sample id
        assert y_pred[row] == block["values"][block_row[sample_id]]
    assert dataset["target_names"] == target_names
    record_row = {sample_id: row for row, sample_id in enumerate(dataset["sample_ids"])}
    y_true = [dataset["y"]["values"][record_row[sample_id]] for sample_id in owner_ids]  # record order, by id
    aligned = (sum((p - t) ** 2 for pr, tr in zip(y_pred, y_true, strict=True) for p, t in zip(pr, tr, strict=True)) / (len(y_pred) * len(target_names))) ** 0.5
    positional = (sum((p - t) ** 2 for pr, tr in zip(block["values"], y_true, strict=True) for p, t in zip(pr, tr, strict=True)) / (len(y_pred) * len(target_names))) ** 0.5
    assert aligned < positional  # using the sorted block by position would silently misalign samples

    # --- Real permutation, prediction only (no fit): the same samples with rotated rows keep their per-id values.
    rotated_document = _rotated_record(document, len(dataset["sample_ids"]) // 2)
    rotated_ids = [
        i for i, part in zip(rotated_document["dataset"]["sample_ids"], rotated_document["dataset"]["partitions"]["values"], strict=True) if part == partition
    ]
    rotated = tmp_path / "rotated-record.json"
    rotated.write_text(json.dumps(rotated_document))
    rotated_log = tmp_path / "rotated-probe.jsonl"
    done, rotated_out = _cohort_process(tmp_path, "predict", downloaded, str(rotated), partition, library, rotated_log, tag="-rotated")
    assert done.returncode == 0, done.stderr[-3000:]
    permuted = json.loads(rotated_out.read_text())["external"]
    assert not [c for c in _probe_calls(rotated_log) if c["function"] == "produce_n4a_native"]
    assert permuted["sample_ids"] == rotated_ids != owner_ids and sorted(permuted["sample_ids"]) == sorted(owner_ids)
    assert dict(zip(permuted["sample_ids"], map(tuple, permuted["y_pred"]), strict=True)) == dict(zip(owner_ids, map(tuple, y_pred), strict=True))

    # --- A corrupted payload is a Core refusal: no output, no fallback, no replay, no training.
    members = sorted((m for m in manifest["members"] if m["path"] != "manifest.json"), key=lambda m: m["byte_length"])
    flipped = members[-1]["path"]
    corrupt = tmp_path / "corrupt.n4a"
    _flip_payload_byte(downloaded, corrupt, flipped)
    refused_log = tmp_path / "refused-probe.jsonl"
    refused, refused_out = _cohort_process(tmp_path, "corrupt", corrupt, record, partition, library, refused_log)
    assert refused.returncode != 0 and not refused_out.exists()
    refused_calls = _probe_calls(refused_log)
    # Core's file open refuses the container on the flipped member's ZIP CRC, before any inspection, replay or training.
    opened = [c for c in refused_calls if c["function"] == "open_n4a_file"]
    assert [c["phase"] for c in opened] == ["start", "refused"] and opened[-1]["error"] == "ValueError", refused_calls
    assert f"n4a integrity refusal: ZIP CRC mismatch for `{flipped}`" in refused.stderr, refused.stderr[-3000:]
    assert not [c for c in refused_calls if c["function"] in ("produce_n4a_native", "predict_n4a_native")]
