# Release checklist — nirs4all-cluster

Publishing is via **PyPI Trusted Publishing (OIDC)** — no stored token. `release.yml` runs when a
GitHub Release is published (or via `workflow_dispatch`). Pushing to `main` never publishes.

## Pre-release

- [ ] Green gate passes locally and on CI (see `quality_gates.md`).
- [ ] `CHANGELOG.md` has a dated entry for the target version (move `[0.1.1] — unreleased` to a date).
- [ ] Version single-sourced in `nirs4all_cluster/__init__.py` matches the intended release.
- [ ] **Tag scheme reconciled**: adopt `vX.Y.Z` so `version-guard` actually compares (today only
      `n4a-v1-*` tags exist and the guard is inert). The release **tag must point at the exact
      release commit** already containing the final manifest/changelog (Codex Gate 0/B4).
- [ ] Trusted Publisher configured on PyPI (project `nirs4all-cluster`, owner `GBeurier`,
      workflow `release.yml`, environment `pypi`).
- [ ] `uv build && uvx twine check --strict dist/*` clean.

## Release

- [ ] Create the annotated tag `vX.Y.Z` on the release commit and push it.
- [ ] Publish the GitHub Release (this triggers `release.yml` → build → OIDC publish).
- [ ] Confirm the `release.yml` run is green and the version appears on PyPI.

## Post-release

- [ ] `pip install nirs4all-cluster==X.Y.Z` in a clean venv; smoke `n4cluster --help` and
      `python -c "import nirs4all_cluster as m; print(m.__version__)"`.
- [ ] Start the next `## [Unreleased]` CHANGELOG section.

## Notes / residual risks

- Multi-registry fan-out is single-registry (PyPI) here, so no cross-registry partial-failure risk.
- CeCILL-2.1 OR AGPL-3.0-or-later SPDX metadata is validated by `twine check --strict`.
