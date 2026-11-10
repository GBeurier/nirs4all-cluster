"""Bounded real OS-process server/provider/consumer acceptance (local loopback only)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
from nirs4all_access import ProjectPrincipal, issue_credential
from privacy_support import scan_logs, unsafe_control
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect

from nirs4all_cluster import ClusterClient
from nirs4all_cluster.server.auth import rights_from_roles
from nirs4all_cluster.server.state import bootstrap_state

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT.parent / "_implementation/2026-11-03-ecosystem-consolidation/phase6-cluster-localservices-parallel"
DATA = ROOT.parent / "nirs4all-data/regression/GRAPEVINE_LeafTraits/PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra"
SCIENCE = ROOT.parent / "nirs4all/.venv/bin/python"


def _retain_owner_export(evidence: Path, data: bytes, provenance: dict) -> None:
    """Retain this qualification run under evidence; pinned product fixtures are independent."""
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "owner-export.n4a").write_bytes(data)
    (evidence / "owner-export.json").write_text(json.dumps(provenance, indent=2) + "\n")


def main(*, evidence: Path | None = None):
    evidence = Path(evidence if evidence is not None else EVIDENCE)
    evidence.mkdir(parents=True, exist_ok=True)
    assert shutil.disk_usage(ROOT).free >= 2147483648, "disk floor"
    assert DATA.is_dir() and SCIENCE.is_file(), "real science prerequisite missing"
    output = {"topology": "server=Cluster venv, provider/runner=full Python venv, consumer=Cluster venv", "checks": {}}
    credentials = {}
    for key, project, role in [
        ("consumer", "probe", "submitter"),
        ("provider", "probe", "executor"),
        ("foreign", "other", "admin"),
    ]:
        credentials[key] = issue_credential(
            ProjectPrincipal(project, key, frozenset({"cluster"}), frozenset(rights_from_roles([role])))
        )
    secrets = [token for token, _ in credentials.values()]
    with tempfile.TemporaryDirectory(prefix="n4cluster-real-") as tmp:
        tmp = Path(tmp)
        bootstrap_state(tmp / "server", tuple(r for _, r in credentials.values()))
        env = dict(
            os.environ,
            PYTHONDONTWRITEBYTECODE="1",
            OMP_NUM_THREADS="1",
            OPENBLAS_NUM_THREADS="1",
            MKL_NUM_THREADS="1",
            NUMEXPR_NUM_THREADS="1",
            PYTHONPATH=os.pathsep.join([str(ROOT), os.environ.get("PYTHONPATH", "")]),
        )
        for key in ["NIRS4ALL_PROJECT_TOKEN", "N4CLUSTER_TOKEN"]:
            env.pop(key, None)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        processes = []
        logfiles = []
        try:
            logfiles.append(open(tmp / "server.log", "w"))
            server = subprocess.Popen(
                [
                    str(ROOT / ".venv/bin/python"),
                    "-B",
                    "-m",
                    "nirs4all_cluster.cli",
                    "server",
                    "--port",
                    str(port),
                    "--state",
                    str(tmp / "server"),
                    "--log-level",
                    "info",
                ],
                cwd=tmp,
                env=env,
                stdout=logfiles[-1],
                stderr=subprocess.STDOUT,
            )
            processes.append(server)
            for _ in range(100):
                try:
                    if httpx.get(base + "/healthz", timeout=0.5).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                assert server.poll() is None, "server startup failed"
                time.sleep(0.1)
            else:
                raise AssertionError("server startup timeout")
            # Real uvicorn must never log even rejected query credentials.
            wsbase = base.replace("http:", "ws:")
            query_secret = credentials["consumer"][0]
            for path, headers, status in [
                ("/v1/events/stream?token=" + query_secret, {}, 401),
                ("/v1/events/stream", {}, 401),
                ("/v1/events/stream", {"Authorization": "Bearer malformed"}, 401),
            ]:
                try:
                    with connect(wsbase + path, additional_headers=headers, open_timeout=3):
                        pass
                except InvalidStatus as exc:
                    assert exc.response.status_code == status
                else:
                    raise AssertionError("WS denial expected")
            output["checks"]["real_ws_denials"] = True
            logfiles.append(open(tmp / "provider.log", "w"))
            provider = subprocess.Popen(
                [
                    str(SCIENCE),
                    "-B",
                    "-m",
                    "nirs4all_cluster.cli",
                    "worker",
                    "--server",
                    base,
                    "--state",
                    str(tmp / "provider"),
                    "--allow-path-root",
                    str(DATA),
                    "--allow-path-root",
                    str(ROOT / "examples"),
                    "--poll-interval",
                    "0.1",
                    "--gpus",
                    "0",
                    "--log-level",
                    "info",
                ],
                cwd=tmp,
                env={
                    **env,
                    "NIRS4ALL_PROJECT_TOKEN": credentials["provider"][0],
                    "N4CLUSTER_TOKEN": credentials["provider"][0],
                },
                stdout=logfiles[-1],
                stderr=subprocess.STDOUT,
            )
            processes.append(provider)
            with ClusterClient(base, token=credentials["consumer"][0], timeout=15) as consumer:
                for _ in range(200):
                    workers = consumer.list_workers()
                    if workers:
                        break
                    assert provider.poll() is None, "provider startup failed"
                    time.sleep(0.1)
                else:
                    raise AssertionError("provider startup timeout")
                advertised = workers[0]["version"]["packages"].get("nirs4all")
                output["advertised_owner_version"] = advertised
                requested = consumer.submit_run(
                    pipeline=str(ROOT / "examples/pipelines/pls.yaml"),
                    dataset=str(DATA),
                    params={"verbose": 0, "random_state": 42},
                    retry={"max_attempts": 1},
                    idempotency_key="real-science",
                )
                final = consumer.wait(requested.id, poll=0.25, timeout=120)
                tasks = consumer.get_tasks(final.id)
                output["scientific_attempt"] = {
                    "status": final.status.value,
                    "completeness": final.aggregate.completeness,
                    "errors": final.aggregate.errors,
                }
                assert final.status.value == "succeeded", final.aggregate.errors
                result = tasks[0].result
                assert result is not None and result.delivery.status == "exported"
                assert result.nirs4all_version == advertised == result.delivery.owner_version
                p = consumer.download_best_model(final.id, tmp / "model.n4a")
                assert p
                first = p.read_bytes()
                p2 = consumer.download_best_model(final.id, tmp / "model-again.n4a")
                assert p2.read_bytes() == first
                assert hashlib.sha256(first).hexdigest() == result.delivery.sha256
                assert result.provenance.project_id == "probe" and result.provenance.reported_by_principal == "provider"
                replay = consumer.submit_run(
                    pipeline=str(ROOT / "examples/pipelines/pls.yaml"),
                    dataset=str(DATA),
                    params={"verbose": 0, "random_state": 42},
                    retry={"max_attempts": 1},
                    idempotency_key="real-science",
                )
                assert replay.id == final.id
                output["delivery"] = result.delivery.model_dump()
                output["provenance"] = result.provenance.model_dump()
                output["checks"]["retrieval_and_submission_idempotency"] = True
                with ClusterClient(base, token=credentials["foreign"][0]) as foreign:
                    assert foreign.list_jobs() == []
                    for path in [
                        "/v1/jobs/" + final.id,
                        "/v1/jobs/" + final.id + "/events",
                        "/v1/jobs/" + final.id + "/artifacts",
                        "/v1/artifacts/" + final.aggregate.best_model_artifact_id,
                    ]:
                        assert foreign._http.get(path).status_code == 404
                output["checks"]["real_project_isolation"] = True
                noexport = consumer.submit_run(
                    pipeline=str(ROOT / "examples/pipelines/pls.yaml"),
                    dataset=str(DATA),
                    params={"verbose": 0, "random_state": 42},
                    outputs={"export_best_model": False},
                    retry={"max_attempts": 1},
                )
                ne = consumer.wait(noexport.id, poll=0.25, timeout=120)
                assert ne.status.value == "succeeded" and ne.aggregate.completeness == "not_requested"
                nr = consumer.get_tasks(ne.id)[0].result
                assert nr.delivery.status == "not_requested" and nr.artifacts.get("model") is None
                output["checks"]["real_no_export"] = True
                # Current owner branch export refusal must be explicit and non-retriable.
                branch = consumer.submit_run(
                    pipeline={
                        "kind": "inline_json",
                        "inline": {
                            "pipeline": [
                                {"class": "sklearn.preprocessing.StandardScaler"},
                                {
                                    "branch": [
                                        [
                                            {
                                                "class": "sklearn.cross_decomposition.PLSRegression",
                                                "params": {"n_components": 3},
                                            }
                                        ],
                                        [
                                            {
                                                "class": "sklearn.cross_decomposition.PLSRegression",
                                                "params": {"n_components": 5},
                                            }
                                        ],
                                    ]
                                },
                            ]
                        },
                    },
                    dataset=str(DATA),
                    params={"verbose": 0, "random_state": 42},
                    retry={"max_attempts": 2},
                )
                bf = consumer.wait(branch.id, poll=0.25, timeout=120)
                bt = consumer.get_tasks(branch.id)[0]
                output["branch_attempt"] = {"status": bf.status.value, "attempt": bt.attempt, "error": bt.error}
                assert bf.status.value == "failed" and bt.attempt == 1 and "export" in (bt.error or "").lower()
                assert bf.aggregate.best_model_artifact_id is None
                output["checks"]["real_export_refusal"] = True
                # Qualification exports are evidence, never regenerated product fixtures.
                provenance = {
                    "profile": result.delivery.profile,
                    "bundle_format_version": result.delivery.bundle_format_version,
                    "owner_version": result.delivery.owner_version,
                    "sha256": result.delivery.sha256,
                    "size_bytes": len(first),
                    "producer": "current sibling nirs4all.run then run_result.export; validated via owner load_bundle",
                    "dataset": "public nirs4all-data GRAPEVINE_LeafTraits PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra",
                    "pipeline": "examples/pipelines/pls.yaml",
                    "random_state": 42,
                    "canonical_core": False,
                }
                provenance["result_provenance"] = result.provenance.model_dump()
                _retain_owner_export(evidence, first, provenance)
                output["checks"]["genuine_owner_export_retained"] = True
        finally:
            for proc in reversed(processes):
                proc.terminate()
                try:
                    proc.wait(10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(5)
            for log in logfiles:
                log.close()
            for name in ["server", "provider"]:
                path = tmp / (name + ".log")
                if path.exists():
                    log = path.read_text(errors="replace")
                    assert not any(secret in log for secret in secrets), "credential leaked in process log"
                    # Logs may include trusted paths, never headers/secrets. Keep compact raw evidence.
                    (evidence / (name + "-real.log")).write_text(log[-18000:])
            server_log = (tmp / "server.log").read_text()
            provider_log = (tmp / "provider.log").read_text()
            assert "nirs4all_cluster.server" in server_log and "finalized:" in server_log
            assert "nirs4all_cluster.worker" in provider_log and "registered as" in provider_log
            for text in [server_log, provider_log]:
                counts = scan_logs(text, secrets)
                assert counts["lines"] > 0 and all(counts[k] == 0 for k in ["credential_lines", "header_lines", "target_lines"])
            output["privacy_positive_control"] = unsafe_control(tmp / "unsafe-direct-host")
            output["checks"]["nonvacuous_application_logs"] = True
            output["checks"]["no_credentials_in_actual_uvicorn_logs"] = True
            (evidence / "real-process-result.json").write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence", type=Path, help="scratch directory for this qualification run's artifacts and logs"
    )
    raise SystemExit(main(evidence=parser.parse_args().evidence))
