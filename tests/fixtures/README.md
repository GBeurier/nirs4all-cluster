# Synthetic owner-produced transport fixture

`owner-python-1.0.n4a` is a genuine export trained on **deterministic synthetic
spectra**, not an external/public dataset. The generator is
`generate_synthetic_fixture.py` (seed 20261104): 64 training and 16 test rows,
eight features, synthetic latent signals plus seeded noise, no acquired samples.
The generated input values are dedicated to CC0-1.0; the archive is distributed
under this project's CeCILL-2.1 OR AGPL-3.0-or-later terms. The JSON sidecar records
rights, generator/input/pipeline hashes, owner version, format version and archive
SHA256/size. Any retained row-level model scores therefore describe synthetic rows.

Training, genuine `RunResult.export`, and archive/model loading run through the
existing `nirs4all_cluster.runners.nirs4all_run` owner boundary. This is the
`nirs4all-python-n4a-1.0` profile, **canonical_core=false**. Canonical Core migration
remains open pending upstream qualification. No science/parser is reimplemented
in the server, clients, worker or generator.

The static bytes support deterministic transport/idempotency tests. Ordinary
qualification (`tests/probe_local_services.py`) retains its fresh output **only in
scratch evidence** and never regenerates this fixture or its sidecar. Replacement
requires the explicit authoring tool's `--replace-fixture` flag and review of the
new sidecar/provenance together; it is not part of qualification.
