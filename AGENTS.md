# Cluster contributor instructions

## Scope and runner boundary

Cluster owns trusted private project providers/consumers, jobs, worker lifecycle and artifact delivery. Read [PROTOTYPE_DESIGN](PROTOTYPE_DESIGN.md) with its November authoritative amendment before changing the wire contract, state machines or matching policy. Use [architecture](docs/concepts/architecture.md), [job specification](docs/job-spec.md) and [WORKLOG](WORKLOG.md) for navigation and measured history; historical prototype/GA/backend comparisons do not certify current capability.

Only `nirs4all_cluster/runners/nirs4all_run.py` may import `nirs4all`, in the child process `python -m nirs4all_cluster.runners.nirs4all_run`. Keep server/client/worker/materializer/executor/shared modules SDK-free. Extend the runner summary rather than importing science in the agent; preserve crash isolation and subprocess cancellation. Worker environments are provisioned before startup, not dynamically installed.

Keep `schemas.py` the shared boundary validation contract; trust internal invariants. Preserve one-of pipeline/pipelines and dataset/datasets Cartesian decomposition. Shared `versioning.py` owns protocol/handshake and fingerprint helpers; `logging_setup.py` owns daemon logging. No hostile multi-tenancy, Python sandbox, K8s/Ray/Dask-class scheduler, shared SDK workspace writers or fold distribution. The prototype restriction on other-library changes applies to that work, not a universal veto of currently authorized owner fixes.

## Scheduling, state and materialization

State changes go through scheduler transitions before DB enforcement, never ad-hoc status UPDATEs. Keep the single-process SQLite WAL/reentrant-lock design, live task-derived in-flight slot usage rather than mutable counters, and atomic/idempotent terminal aggregation/best-model selection. Reaper lease TTL/retries and worker-death handling must never relaunch cancelled jobs. Persist events with bounded drop-oldest queues and bound CAS uploads before publication.

Keep labels/memory/fail-closed GPU/PEP-440 matching and automatic SDK presence requirements unless explicitly ranged. GPU discovery uses `nvidia-smi`, not framework imports. `worker_local` currently follows shared-path behavior; use `requirements.labels` for placement. `catalog` datasets remain not implemented. Materialization rejects zip-slip, absolute and symlink members; the executor captures logs/result.json and preserves cooperative cancellation/timeouts. Keep default `inner_n_jobs=1`; explicit-variant parity requires its own measured evidence, not a promise.

## Project security and logging

Use the independent stdlib [Access contract](packages/nirs4all-access/CONTRACT.md), never copied verification or an open/shared-token mode. Explicit init-state/token-create provisioning and immutable restart snapshots select project DB/CAS/broker. The ASGI front door authenticates and selects project services before body reads, enforces the version handshake and bounded request sizes, and sanitizes denials; retain the [configuration](docs/configuration.md#configuration) contract. Exact project-scoped rights grant no groups, wildcards or caller selector authority. Require worker sessions and current task/attempt/lease fencing for reports and artifacts; verify transport hashes and links without claiming scientific truth.

Consumer rights grant trusted provider code execution: owner YAML may construct arbitrary Python classes. The server `--allow-python-jobs` and worker `--allow-python` switches gate only `python_entrypoint`, not that owner loader behavior. Preserve OS/project identity isolation, denied-by-default entry-path roots and credential removal before runner/version probes; entry-path allowlists do not sandbox embedded science paths. Use TLS outside loopback or explicit trusted TLS-proxy acknowledgement.

Follow [operations logging](docs/operations.md#logging-and-observability) and [URL/error policy](docs/operations.md#client-url-admission-and-failure-diagnostics). `--log-level` is application-only critical/error/warning/info/debug; other namespaces remain WARNING or stricter. Keep `log_config=None, log_level="warning", access_log=False`, no raw targets/headers or verbosity bypass, and no query credentials. Direct ASGI/proxy/host logging remains operator-owned; SDK/WorkerClient do not configure it. Preserve fixed non-echoing URL admission and status/class-only diagnostics/exit codes.

The old global WebSocket/dashboard description is superseded: v2 events are project-local and the unchanged `/ui` live feed is unavailable. Do not weaken authentication to restore it; frontend migration requires a later joint batch. Preserve owner export refusal, supported archive profile/version metadata and transport-verification limits in [security and scope](docs/security-and-scope.md); do not claim pending canonical Core routes.

## Commands and prerequisites

Run from this Cluster checkout. The original transport-only editable setup and unit/API/targeted commands remain distinct from the full push gate below. SDK integration/validation require the provisioned sibling `../nirs4all/.venv` and `nirs4all-data`; they self-skip without prerequisites. The two relative interpreter commands intentionally replace workstation-specific paths.

```bash
# Install (server + client + worker transport only; nirs4all is NOT a dependency)
uv pip install -e ".[dev]"

# Green gate
ruff check .
mypy nirs4all_cluster
pytest -q                         # unit/API gate; science modules may skip without dependencies

# A single test / file / pattern
pytest tests/test_scheduler.py -q
pytest tests/test_server_api.py::<name> -q
pytest -k "lease and retry" -q

# Integration + end-to-end (need the nirs4all venv AND nirs4all-data present;
# tests skip themselves otherwise — see tests/test_integration_nirs4all.py)
../nirs4all/.venv/bin/python -m pytest tests/test_integration_nirs4all.py -q
../nirs4all/.venv/bin/python scripts/validation.py   # real OS processes; SIGKILLs a worker to prove recovery
```

`tests/probe_local_services.py` uses split real processes; the full SDK venv lacks server dependencies. For local gates that require Access, use the installed leaf on PYTHONPATH, and never alter provisioned environments or hand-edit uv.lock.

For the full local gate before every push, retain the owner [quality gates](docs/maintenance/quality_gates.md#local-green-gate-run-before-every-push), including warning-as-error docs and built-distribution metadata checks:

```bash
uv sync --extra dev                 # install (nirs4all intentionally NOT installed)
uv run ruff check .                 # lint (line-length 120, py311)
uv run mypy nirs4all_cluster        # types
uv run pytest -q                    # unit/API suite (integration self-skips without nirs4all)
uv run --extra docs sphinx-build -W -b html docs docs/_build/html   # docs (warnings = errors)
uv build && uvx twine check --strict dist/*            # package + metadata
```

Optional `uvx pre-commit install` / `uvx pre-commit run --all-files` fetch hooks on demand. Contributor checks before PRs, package release smoke, secret scanners and CI/platform/release limitations retain their separate conditions in [CONTRIBUTING](CONTRIBUTING.md), [README tests](README.md#tests-and-validation), [SECURITY](SECURITY.md) and quality gates. Commands are references, not authorization to publish or run science in a docs-only batch.

Keep Python 3.11+, public types/Google docstrings and configured Ruff line length 120/py311, including `E501`/`UP042` ignores and `str, Enum` rather than StrEnum. No dead code or backward-compatibility shims. Follow [LICENSING](LICENSING.md), its dual `CeCILL-2.1 OR AGPL-3.0-or-later` policy/commercial option, and private security-reporting rules.

## Working rules

Read applicable local and deeper instructions before editing. Preserve dirty/concurrent work and make only authorized changes. When a workspace policy exists at `../AGENTS.md`, read it within the current user scope; this local policy also works in a standalone checkout. An existing `CLAUDE.md` is a compatibility entrypoint, not a second policy dependency.

Use CodeGraph FIRST for implementation navigation only when this checkout has its own `.codegraph/`; use its exact projectPath or run the CLI here. Never auto-index or substitute an ancestor index. Unindexed code, docs and configs use ordinary source navigation; source, tests and build tools remain final authority.

Keep secrets, private data, generated workspaces, caches and build outputs out of Git. Respect owner licenses and fixture restrictions, record actual commands/exits/skips, and preserve scoped authorization: command examples do not grant installation, science, publishing, hosting or frontend work.

All application and project content must be in English, including UI text, subtitles, tooltips, errors, documentation, comments and docstrings, test and fixture prose, reports, pull requests, commits, and generated content. Conversation with the maintainer may be in French. Preserve proper names, identifiers, filenames, source quotations, and historical or provenance records where translation would change their meaning or provenance.
