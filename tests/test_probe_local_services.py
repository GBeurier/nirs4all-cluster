"""Qualification output must not regenerate the independent pinned transport fixture."""

import hashlib
import json
from types import SimpleNamespace

import probe_local_services as probe
import pytest

from nirs4all_cluster.schemas import ModelDelivery, ResultProvenance, TaskResult


@pytest.mark.parametrize("explicit_evidence", [False, True])
def test_main_retains_export_only_in_evidence(tmp_path, monkeypatch, explicit_evidence):
    product = tmp_path / "product"
    fixtures = product / "tests/fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "owner-python-1.0.n4a").write_bytes(b"immutable pinned archive")
    (fixtures / "owner-python-1.0.json").write_text('{"immutable":"pinned provenance"}\n')
    data = product / "data"
    data.mkdir()
    science = product / "python"
    science.write_text("")
    evidence = tmp_path / "evidence"
    monkeypatch.setattr(probe, "ROOT", product)
    monkeypatch.setattr(probe, "EVIDENCE", evidence)
    monkeypatch.setattr(probe, "DATA", data)
    monkeypatch.setattr(probe, "SCIENCE", science)
    monkeypatch.setattr(probe, "bootstrap_state", lambda *args: None)
    monkeypatch.setattr(probe.httpx, "get", lambda *args, **kwargs: SimpleNamespace(status_code=200))

    class Process:
        def __init__(self, *args, **kwargs):
            # This fixture-mutation test stubs process execution, not privacy qualification.
            kwargs["stdout"].write(
                "INFO nirs4all_cluster.server job finalized: succeeded\nINFO nirs4all_cluster.worker registered as worker_stub\n"
            )
            kwargs["stdout"].flush()

        def poll(self):
            return None

        def terminate(self):
            pass

        def wait(self, timeout):
            return 0

    monkeypatch.setattr(probe.subprocess, "Popen", Process)

    def denied(*args, **kwargs):
        raise probe.InvalidStatus(SimpleNamespace(status_code=401))

    monkeypatch.setattr(probe, "connect", denied)
    current_export = b"distinct current owner export (transport stub)"
    digest = hashlib.sha256(current_export).hexdigest()
    exported = TaskResult(
        nirs4all_version="1.4.15",
        delivery=ModelDelivery(
            status="exported",
            profile="nirs4all-python-n4a-1.0",
            bundle_format_version="1.0",
            owner_version="1.4.15",
            sha256=digest,
            size_bytes=len(current_export),
        ),
        provenance=ResultProvenance(project_id="probe", reported_by_principal="provider"),
    )

    class Client:
        def __init__(self, *args, **kwargs):
            self._http = SimpleNamespace(get=lambda *args: SimpleNamespace(status_code=404))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def list_workers(self):
            return [{"version": {"packages": {"nirs4all": "1.4.15"}}}]

        def list_jobs(self):
            return []

        def submit_run(self, **kwargs):
            identity = (
                "noexport" if kwargs.get("outputs") else "branch" if isinstance(kwargs["pipeline"], dict) else "model"
            )
            return SimpleNamespace(id=identity)

        def wait(self, identity, **kwargs):
            return SimpleNamespace(
                id=identity,
                status=SimpleNamespace(value="failed" if identity == "branch" else "succeeded"),
                aggregate=SimpleNamespace(
                    completeness="not_requested" if identity == "noexport" else "complete",
                    errors={},
                    best_model_artifact_id=None if identity == "branch" else "model_artifact",
                ),
            )

        def get_tasks(self, identity):
            result = TaskResult(delivery=ModelDelivery(status="not_requested")) if identity == "noexport" else exported
            return [SimpleNamespace(result=result, attempt=1, error="owner export unsupported")]

        def download_best_model(self, identity, path):
            path.write_bytes(current_export)
            return path

    monkeypatch.setattr(probe, "ClusterClient", Client)
    monkeypatch.setattr(probe, "unsafe_control", lambda path: {"credential_lines": 1})

    def snapshot():
        return {
            str(p.relative_to(product)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in product.rglob("*")
            if p.is_file()
        }

    before = snapshot()
    if explicit_evidence:
        evidence = tmp_path / "explicit-evidence"
        assert probe.main(evidence=evidence) == 0
    else:
        evidence.mkdir()
        assert probe.main() == 0
    assert snapshot() == before
    assert (evidence / "owner-export.n4a").read_bytes() == current_export
    provenance = json.loads((evidence / "owner-export.json").read_text())
    assert provenance["sha256"] == digest
    assert provenance["result_provenance"]["project_id"] == "probe"
    output = json.loads((evidence / "real-process-result.json").read_text())
    assert output["checks"]["genuine_owner_export_retained"] is True
