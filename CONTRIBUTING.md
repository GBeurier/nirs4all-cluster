# Contributing to nirs4all-cluster

Thanks for your interest. `nirs4all-cluster` is a small, deliberately scoped distributed
job queue for `nirs4all.run()`. Please read `PROTOTYPE_DESIGN.md` (the source of truth for
the data/security/recovery model and the **non-goals**) before proposing changes.

## Development setup

```bash
# Server + client + worker transport (nirs4all is NOT a dependency)
uv pip install -e ".[dev]"
```

The unit/API test suite runs **without** `nirs4all` installed. The integration suite
(`tests/test_integration_nirs4all.py`) and `scripts/validation.py` need the sibling
`nirs4all` environment and self-skip otherwise.

## Green gate (run before opening a PR)

```bash
ruff check .
mypy nirs4all_cluster
pytest -q
```

For documentation changes, also build the docs warning-free:

```bash
uv run --extra docs sphinx-build -W -b html docs docs/_build/html
```

## The load-bearing invariant

**Only `nirs4all_cluster/runners/nirs4all_run.py` may import `nirs4all`.** The server,
client, worker agent, materializer, executor and shared modules must stay `nirs4all`-free
so the server/client run without the library installed. Don't move `nirs4all` logic up the
stack — if the runner needs more from a run, extend the summary it emits.

## Respect the non-goals

PRs that cross the documented non-goals are out of scope: no modifying other ecosystem
libraries, no open multi-tenancy, no secure sandbox for arbitrary Python, no
Kubernetes/Ray/Dask-class scheduler, no concurrent writes to a shared `nirs4all`
workspace, no fold distribution. State changes must go through the scheduler's state
machines (`server/scheduler.py`), which the DB then enforces.

## Conventions

- Python ≥ 3.11, Google-style docstrings, type hints on public APIs.
- ruff (line length 120, `E501` ignored) for lint; mypy for types.
- Validate only at boundaries (`schemas.py` is the wire contract); trust internal code.
- No dead code, no backward-compatibility shims.
