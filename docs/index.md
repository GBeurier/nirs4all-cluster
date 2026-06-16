# nirs4all-cluster

```{note}
**Beta · trusted-LAN.** `nirs4all-cluster` is a small distributed job queue for
`nirs4all.run()` — a coordinator that receives jobs and dispatches work to polling
workers. It is built for a **trusted local network**, not the open internet or untrusted
multi-tenant use. See {doc}`security-and-scope` for the documented limits.
```

The server and client run **without** `nirs4all` installed; only the worker's subprocess
runner imports it. That subprocess boundary buys crash isolation and real cancellation,
and keeps the control plane a thin, library-free orchestration layer.

```{code-block} bash
:caption: 60-second tour
n4cluster server --state ./cluster-state          # coordinator (+ dashboard at /ui)
n4cluster worker --server http://HOST:8765        # one or more workers
n4cluster submit examples/job.shared-path.yaml --wait --out ./results
```

```{toctree}
:maxdepth: 2
:caption: Getting started
installation
quickstart
```

```{toctree}
:maxdepth: 2
:caption: Concepts
concepts/architecture
concepts/job-decomposition
versioning
```

```{toctree}
:maxdepth: 2
:caption: Reference
cli-reference
configuration
job-spec
python-sdk
rest-api
```

```{toctree}
:maxdepth: 2
:caption: Running it
operations
dashboard
security-and-scope
```

```{toctree}
:maxdepth: 1
:caption: Design & internals
design/index
```
