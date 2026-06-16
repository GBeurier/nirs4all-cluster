# Installation

`nirs4all-cluster` needs **Python ≥ 3.11**. The server and client depend only on
FastAPI / uvicorn / httpx / pydantic; **`nirs4all` is not a dependency** of the package.

```{code-block} bash
:caption: install the server + client + worker transport
pip install nirs4all-cluster
# or, from a checkout:
uv pip install -e ".[dev]"
```

## Server / client hosts

Nothing else is required. The coordinator and the client SDK/CLI run on the base package
alone — you can submit and monitor jobs from a machine that has never seen `nirs4all`.

## Worker hosts

A worker runs `nirs4all.run()` in a subprocess, so a worker host must already have a
working **`nirs4all` environment** (plus whatever the pipelines need: scikit-learn,
torch, tensorflow, …). Install `nirs4all-cluster` *on top of* that environment:

```{code-block} bash
:caption: on a worker, inside the nirs4all environment
pip install nirs4all-cluster
```

The worker advertises the versions it found (`nirs4all`, `numpy`, `torch`, … and its own
`nirs4all-cluster` version) so the server can route jobs to a compatible worker — see
{doc}`versioning`.

## Documentation toolchain (optional)

```{code-block} bash
pip install "nirs4all-cluster[docs]"   # sphinx + myst + furo, to build these docs
```
