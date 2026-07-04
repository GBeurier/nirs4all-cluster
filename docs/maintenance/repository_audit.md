# Repository audit — nirs4all-cluster

> Generated from the automated pre-release audit (workflow wf_1fc87351-29f); the **Deepest hardening roadmap** section records the fullest realistic hardening even where the pragmatic pass does not implement it. Reviewed at Codex Gate 1.

- **Mode:** IN SCOPE — pragmatic hardening + push
- **Baseline HEAD:** `97f8331`
- **Role:** Distributed job/scheduler cluster service (Python): FastAPI server + SQLite/object-store + polling worker agents + client SDK/CLI for nirs4all.run(); trusted-LAN beta, auth via single static bearer token, WebSocket event feed, release e2e/smoke suite.
- **Stack:** Python >=3.11 (CI matrix 3.11/3.12/3.13). Package manager: uv (uv.lock committed). Build backend: hatchling>=1.27 (dynamic version from nirs4all_cluster/__init__.py). Runtime deps: fastapi>=0.110, uvicorn[standard], httpx, pydantic>=2.6, PyYAML, python-multipart, websockets, packaging. Dev: pytest, pytest-asyncio, ruff, mypy. Docs: sphinx>=8.1, myst-parser, furo, sphinx-copybutton, sphinxcontrib-mermaid. nirs4all itself is intentionally NOT a dependency (imported only inside runners/nirs4all_run.py subprocess).

## Release-readiness verdict
nirs4all-cluster is a well-structured beta-quality Python service with a mature CI/release setup for its stage: a 3-version test matrix, ruff+mypy+pytest gates, warnings-as-errors Sphinx docs on RTD, uv build + twine --strict packaging, OIDC Trusted-Publishing release, an active detect-secrets + secret-shape guard, and ~124 tests. All CI is currently green and a light secret scan of tracked source found no leaks. The main release-readiness gaps are governance/hardening rather than functional: the version-guard is silently inert due to a tag-prefix mismatch (tags are n4a-v1-* not v*), CHANGELOG lags the 0.1.1 manifest, CI lacks least-privilege token permissions and SHA-pinned actions, and there is no enforced coverage or SAST. None of these block a push to main, but the un-tagged 0.1.1 + inert version-guard + Release-triggered PyPI publish are the items to reconcile before the next release is cut.

## Gate commands (detected)
| key | value |
|---|---|
| `install` | uv sync --extra dev --python 3.12  (or: uv pip install -e ".[dev]") |
| `test` | uv run pytest -q |
| `lint` | uv run ruff check . |
| `typecheck` | uv run mypy nirs4all_cluster |
| `format` | — |
| `docs_build` | uv run sphinx-build -W -b html docs docs/_build/html |
| `package_build` | uv build |

## CI
- **Latest status:** All green. gh run list --limit 8 shows the 4 most recent CI runs and 4 version-guard runs all [ok] on main; no failing run to triage.
- **Workflows:**
- .github/workflows/ci.yml — push/PR on main & rc/**; jobs: secret scan (detect-secrets baseline + scripts/secret_shape_guard.py), test (matrix 3.11/3.12/3.13: ruff+mypy+pytest), docs (sphinx -W), package (uv build + twine check --strict + upload-artifact)
- .github/workflows/release.yml — on GitHub Release published + workflow_dispatch; build (uv build + twine check) then publish-pypi via PyPI Trusted Publishing (OIDC, environment: pypi, id-token: write)
- .github/workflows/version-guard.yml — push/PR on main & rc/**; blocks in-repo __version__ being ahead of the latest v-prefixed tag
- **Gaps:**
- CI runs ubuntu-latest only, despite 'Operating System :: OS Independent' classifier — no Windows/macOS validation of the subprocess/nvidia-smi/zip-extraction paths
- No coverage measurement or threshold in CI (pytest has no --cov; coverage.xml is only produced locally and is untracked)
- No SAST/dependency scanning (CodeQL, bandit, pip-audit/uv audit) in CI
- GitHub Actions pinned to floating major tags (actions/checkout@v4, astral-sh/setup-uv@v5, pypa/gh-action-pypi-publish@release/v1), not commit SHAs
- ci.yml has no top-level permissions block, so GITHUB_TOKEN uses the repo-default scope instead of least-privilege contents: read

## Standard files
- **Present:** readme, changelog, contributing, security, license, gitignore
- **Missing:** code_of_conduct, citation, editorconfig, precommit, pr_template, issue_template, dependabot

## Packaging
- **name:** `nirs4all-cluster` — **version:** `0.1.1`
- **issues:**
- CHANGELOG.md documents only [0.1.0] (2026-06-16); current __version__ is 0.1.1 — no 0.1.1 entry exists
- No v-prefixed release tag exists for 0.1.1 (only tags present: n4a-v1-2026.07-refactor, n4a-v1-rc1-2026.07-refactor); manifest is an un-tagged bump
- license = 'CeCILL-2.1 OR AGPL-3.0-or-later' SPDX expression with license-files globs (PEP 639) — validated by twine check --strict in CI, worth confirming on the release runner
- Development Status :: 4 - Beta (consistent with beta framing; pre-1.0, no GA commitment)

## Tests
- **framework:** pytest + pytest-asyncio (asyncio_mode=auto), testpaths=[tests]; custom marker release_smoke for installed-wheel smoke
- **estimate:** ~124 test functions across 14 files (scheduler, server_api, state_machine, rbac, worker, artifacts, versioning, distributed_parity, release_smoke, integration_nirs4all, core_adapter, cli, client_errors + conftest). Unit/API suite runs WITHOUT nirs4all installed; integration/parity suites self-skip unless the sibling nirs4all venv is present.
- **coverage:** No coverage config or enforced threshold. pytest-cov is not a dependency and CI does not pass --cov; a coverage.xml exists locally but is untracked and not gated.

## Docs
- **system:** Sphinx + MyST (furo theme, copybutton, mermaid, autodoc/napoleon/intersphinx). Source in docs/ (index + quickstart/installation/configuration/cli-reference/rest-api/python-sdk/job-spec/operations/security-and-scope/versioning/dashboard + concepts/ + design/). Hosted on Read the Docs (.readthedocs.yaml v2, python 3.12, extra_requirements docs, formats pdf+htmlzip).
- **status:** Buildable and gated: both CI and RTD build with warnings-as-errors (sphinx -W / fail_on_warning: true). conf.py imports nirs4all_cluster only (import-safe, never pulls nirs4all), version derived from __version__. Healthy.

## Risks
| severity | area | detail |
|---|---|---|
| medium | version-guard / release integrity | .github/workflows/version-guard.yml keys on VG_TAG_PREFIX="v", but the only tags are n4a-v1-2026.07-refactor / n4a-v1-rc1-2026.07-refactor (non-v). With no v* tag the guard hits its 'no matching tags yet; nothing to compare against' branch and passes trivially — the intended 'manifest must not be ahead of tag' protection is currently inert and 0.1.1 sits un-tagged/unenforced. Reconcile the tag scheme (adopt v0.1.1 tags) or fix VG_TAG_PREFIX. |
| medium | CI least-privilege | .github/workflows/ci.yml has NO permissions: block; the workflow-default GITHUB_TOKEN scope applies to secret-scan/test/docs/package jobs that need only read. Add top-level 'permissions: contents: read'. release.yml scopes id-token: write only on the publish job (good) but its build job also lacks an explicit read-only permissions block. |
| low | supply chain | All GitHub Actions use floating major tags (actions/checkout@v4, astral-sh/setup-uv@v5, actions/upload-artifact@v4, actions/download-artifact@v4, pypa/gh-action-pypi-publish@release/v1) rather than pinned commit SHAs — a compromised upstream tag would flow into the release/publish path. |
| low | coverage / regression safety | No coverage gate; a drop in the ~124-test suite would not fail CI. For a scheduler with leasing/retry/cancellation race logic, an enforced coverage floor on server/ and worker/ is warranted. |
| low | portability | CI is ubuntu-only while classifiers advertise OS Independent; worker paths use nvidia-smi detection and zip extraction (zip-slip guards in materialize.py) that would benefit from at least a Windows/macOS smoke lane. |
| low | changelog drift | CHANGELOG.md stops at 0.1.0 while shipping 0.1.1 — release notes are out of sync with the manifest. |

## Security
- **info** — Light secret scan over tracked source (nirs4all_cluster/, scripts/) for private keys / aws_secret / api_key= / token="..." found NO plausible real leaks. The repo actively defends this: detect-secrets baseline (.secrets.baseline) + detect-secrets-hook CI step over all tracked files, plus scripts/secret_shape_guard.py rejecting token-shaped CLI examples (recent commits harden this).
- **info** — Authentication is a single static bearer token by design (trusted-LAN beta), explicitly documented as out-of-scope-for-hardening in SECURITY.md (no mTLS/OIDC/rotation; python_entrypoint jobs run arbitrary Python, double-gated behind --allow-python-jobs + --allow-python). Not a leak, but the security-posture ceiling to keep in mind before any wider deployment.
- **info** — PyPI publishing uses OIDC Trusted Publishing (no long-lived API token/secret stored) — good baseline. Ensure the trusted publisher + 'pypi' environment (ideally with required reviewers) are configured so only intended releases publish.

## Quick wins (pragmatic scope — safe to apply now)
- Add a [0.1.1] section to CHANGELOG.md so release notes match the shipped __version__.
- Add top-level 'permissions: contents: read' to .github/workflows/ci.yml (and to the build job of release.yml) for least-privilege GITHUB_TOKEN.
- Add .editorconfig (line length 120, LF, utf-8, final newline) to match ruff config.
- Add .pre-commit-config.yaml wiring ruff check, ruff format, mypy, and detect-secrets so the CI green-gate is reproducible locally.
- Add .github/dependabot.yml for the pip ecosystem and github-actions (keeps floating action tags patched).
- Add .github/PULL_REQUEST_TEMPLATE.md and .github/ISSUE_TEMPLATE/ (bug/feature) — currently none exist.
- Add CODE_OF_CONDUCT.md and CITATION.cff (both absent) to complete standard OSS metadata.
- Reconcile version-guard: either tag releases as v0.1.1 or set VG_TAG_PREFIX to the actual scheme so the guard compares against a real tag instead of silently skipping.

## Deepest hardening roadmap (fullest realistic hardening)
- Wire coverage: add pytest-cov, run pytest --cov=nirs4all_cluster --cov-report=xml in CI, publish to Codecov, and set an enforced floor (~85%+ on server/scheduler/worker given leasing/retry/cancellation race paths).
- Pin every GitHub Action to a full commit SHA (comment naming the tag) and let Dependabot bump them; hardens the release/publish supply chain.
- Add build provenance/attestations to release.yml (actions/attest-build-provenance or gh-action-pypi-publish attestations) and generate an SBOM (cyclonedx) as a release asset.
- Add security scanning lanes: CodeQL (python), bandit, and pip-audit/uv audit over uv.lock on schedule and on PR.
- Add a lockfile-consistency check (uv lock --check / uv sync --frozen) to CI so uv.lock never drifts from pyproject.toml.
- Expand CI matrix to Windows and macOS (at least a smoke subset) to back the OS-Independent claim and exercise nvidia-smi absence + zip extraction on non-Linux.
- Introduce structured release automation: towncrier or release-please to generate CHANGELOG entries and drive the tag/version bump, eliminating manual CHANGELOG drift and the un-tagged 0.1.1 situation.
- Verify the Read the Docs project builds the v-tag versions; add a docs link-check (sphinx linkcheck) lane.
- Formalize the trusted-LAN → production security track from SECURITY.md (mTLS/OIDC, per-task sandbox/quotas, artifact encryption/retention, multi-tenant isolation) as tracked issues gating any 1.0.
- Add end-to-end release verification: run the release_smoke marker against the built wheel in the release workflow (install wheel, import, exercise n4cluster entrypoint) before publish.
- Add a GitHub Actions 'concurrency' guard and a manual-approval environment to release.yml so two overlapping releases cannot race to PyPI.

## Push-safety notes
- release.yml triggers on GitHub Release 'published' (and workflow_dispatch) and publishes to PyPI via OIDC Trusted Publishing — publishing a Release with tag v0.1.x pushes to PyPI. Plain pushes to main do NOT publish (safe), but cutting a Release is irreversible on PyPI. Confirm the 'pypi' environment has required reviewers before any release.
- version-guard.yml is meant to block merging a version bump to main ahead of its tag, but currently no-ops because no v* tag exists (tags are n4a-v1-*). So the manifest at 0.1.1 is effectively unguarded — a further bump could merge to main without the tripwire firing. Fix the tag prefix/scheme before relying on it.
- workflow_dispatch on release.yml means anyone with Actions write can trigger a PyPI publish out-of-band from a Release — restrict who can dispatch / who approves the 'pypi' environment.
- ci.yml and version-guard.yml run on push to main and rc/** with concurrency cancel-in-progress; ci.yml's package job uploads dist artifacts on every push (storage only, not a publish).
- No cross-repo coupling detected in the workflows (no submodule bump or repository_dispatch to sibling nirs4all repos); the repo is self-contained for release, lowering push risk relative to the rest of the ecosystem.
