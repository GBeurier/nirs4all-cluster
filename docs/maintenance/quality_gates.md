# Quality gates — nirs4all-cluster

## Local green gate (run before every push)

```bash
uv sync --extra dev                 # install (nirs4all intentionally NOT installed)
uv run ruff check .                 # lint (line-length 120, py311)
uv run mypy nirs4all_cluster        # types (22 source files)
uv run pytest -q                    # unit/API suite (integration self-skips without nirs4all)
uv run --extra docs sphinx-build -W -b html docs docs/_build/html   # docs (warnings = errors)
uv build && uvx twine check --strict dist/*            # package + metadata
```

Optional local hooks mirroring the gate (`pre-commit` is fetched on demand via `uvx`):

```bash
uvx pre-commit install
uvx pre-commit run --all-files
```

Integration / end-to-end (need the sibling `nirs4all` venv + `nirs4all-data`; self-skip otherwise):

```bash
/home/delete/nirs4all/nirs4all/.venv/bin/python -m pytest tests/test_integration_nirs4all.py -q
/home/delete/nirs4all/nirs4all/.venv/bin/python scripts/validation.py
```

## CI gates (`.github/workflows/`)

| workflow | trigger | gate |
|---|---|---|
| `ci.yml` | push/PR on `main`, `rc/**` | secret scan (detect-secrets + `secret_shape_guard.py`); test matrix **3.11/3.12/3.13** (ruff + mypy + pytest); docs (`sphinx -W`); package (`uv build` + `twine check --strict`) |
| `version-guard.yml` | push/PR | blocks a manifest version ahead of the latest release tag |
| `release.yml` | GitHub Release published / `workflow_dispatch` | build + PyPI Trusted Publishing (OIDC) — **not** triggered by push |

All workflows now declare least-privilege `permissions: contents: read` (the publish job narrows to `id-token: write`), and all third-party actions are **SHA-pinned** with a version comment (kept current by Dependabot).

## Known gaps (deepest-hardening roadmap)

- **Coverage is measured but not gated.** `coverage.xml` is produced locally; add `--cov=nirs4all_cluster --cov-fail-under=<floor>` to CI once a floor is agreed, then ratchet.
- **CI is ubuntu-only** while classifiers say OS-Independent; add a Windows/macOS smoke lane for the subprocess / zip-extraction / `nvidia-smi` paths.
- **No SAST / dependency scan** (CodeQL, `pip-audit`) — add a supply-chain lane.
- **`version-guard` is currently inert**: it keys on `VG_TAG_PREFIX=v` but only `n4a-v1-*` tags exist, so 0.1.1 is untagged/unenforced. Reconcile to a single `vX.Y.Z` scheme (release-sprint task).
