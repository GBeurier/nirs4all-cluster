"""Explicit fixture-authoring tool; qualification never invokes this generator.

All input rows are new deterministic synthetic numbers, dedicated to CC0-1.0.
Training, archive production and archive validation run only in the existing
scientific owner runner, using an explicitly provisioned Python interpreter.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import shutil
import tempfile
from pathlib import Path

from nirs4all_cluster.worker.executor import execute_task

SEED = 20261104


def generate_inputs(directory: Path) -> dict[str, str]:
    directory.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    for partition, count in [("train", 64), ("test", 16)]:
        rows = []
        targets = []
        for _ in range(count):
            latent = [rng.uniform(-1, 1) for _ in range(3)]
            rows.append(
                [
                    sum(latent[k] * math.sin((j + 1) * (k + 1) * 0.37) for k in range(3)) + rng.uniform(-0.02, 0.02)
                    for j in range(8)
                ]
            )
            targets.append([2 * latent[0] - 0.8 * latent[1] + 0.3 * latent[2] + rng.uniform(-0.01, 0.01)])
        for kind, values, headers in [
            ("X", rows, [f"synthetic_{i}" for i in range(8)]),
            ("Y", targets, ["synthetic_target"]),
        ]:
            with (directory / f"{kind}{partition}.csv").open("w", newline="") as stream:
                writer = csv.writer(stream, delimiter=";", lineterminator="\n")
                writer.writerow(headers)
                writer.writerows([[format(v, ".17g") for v in row] for row in values])
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir()) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--python-exe", required=True)
    parser.add_argument(
        "--replace-fixture", action="store_true", required=True, help="intentional authorized fixture replacement"
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    assert shutil.disk_usage(root).free >= 2147483648, "disk floor"
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    inputs = evidence / "synthetic-inputs"
    input_hashes = generate_inputs(inputs)
    pipeline = root / "examples/pipelines/pls.yaml"
    spec = {
        "pipeline": {"mode": "path", "path": str(pipeline)},
        "dataset": {"mode": "path", "path": str(inputs)},
        "params": {"verbose": 0, "random_state": SEED},
        "outputs": {"export_best_model": True},
    }
    with tempfile.TemporaryDirectory(prefix="synthetic-owner-", dir=evidence) as temporary:
        execution = execute_task(spec, Path(temporary), python_exe=args.python_exe, poll_interval=0.2, timeout=90)
        (evidence / "owner-run.log").write_bytes(execution.log_path.read_bytes())
        (evidence / "owner-result.json").write_text(json.dumps(execution.result, indent=2) + "\n")
        assert execution.returncode == 0 and execution.result.get("status") == "succeeded", execution.result
        delivery = execution.result["delivery"]
        assert delivery["status"] == "exported"
        archive = Path(execution.result["produced"]["model"]).read_bytes()
        assert hashlib.sha256(archive).hexdigest() == delivery["sha256"]
        fixture = root / "tests/fixtures/owner-python-1.0.n4a"
        provenance = {
            k: delivery[k]
            for k in ["profile", "bundle_format_version", "owner_version", "sha256", "size_bytes", "canonical_core"]
        }
        provenance.update(
            {
                "producer": "nirs4all.run -> RunResult.export -> owner load_bundle and artifact provider (existing runner)",
                "dataset": "deterministic synthetic latent spectra; no external corpus or row-level source data",
                "rights": {
                    "synthetic_inputs": "CC0-1.0",
                    "external_data": False,
                    "archive_distribution": "nirs4all-cluster project dual license (CeCILL-2.1 OR AGPL-3.0-or-later)",
                },
                "generator": "tests/fixtures/generate_synthetic_fixture.py",
                "generator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "seed": SEED,
                "input_hashes": input_hashes,
                "rows": {"train": 64, "test": 16},
                "features": 8,
                "pipeline": "examples/pipelines/pls.yaml",
                "pipeline_sha256": hashlib.sha256(pipeline.read_bytes()).hexdigest(),
                "random_state": SEED,
                "provenance": {
                    "purpose": "synthetic model transport fixture",
                    "scientific_owner": "nirs4all",
                    "control_credentials_supplied": False,
                },
            }
        )
        fixture.write_bytes(archive)
        fixture.with_suffix(".json").write_text(json.dumps(provenance, indent=2) + "\n")
        (evidence / "fixture-generation.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(provenance, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
