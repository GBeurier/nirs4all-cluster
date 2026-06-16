# Version compatibility & pipeline fingerprints

`nirs4all-cluster` is a *distributed* system: the client, server and worker are often
different installs. The beta **tracks and warns** on version drift (it does not route or
enforce on it).

## The handshake

Every `/v1` request and response carries three headers:

- `X-N4C-Version` — the peer's `nirs4all-cluster` package version,
- `X-N4C-Api` — the protocol major (`API_VERSION`),
- `X-N4C-Role` — `client` / `worker` / `server`.

The rule is intentionally simple:

- **Same protocol major ⇒ compatible.** Different package versions are fine.
- **Different protocol major ⇒ incompatible.** The server replies **HTTP 426** and the
  client/worker raises `ClusterVersionError`. `API_VERSION` only changes on a breaking
  wire-contract change, independent of the package version.

When a *compatible* peer runs a *different package version*, the server logs it and emits a
one-shot `version_divergence` event (visible in the dashboard and event history); the
client/worker logs the reverse direction once. This makes "divergent but compatible"
visible without getting in the way.

```{note}
Workers also **declare** their full environment at registration (interpreter, `nirs4all`,
`numpy`, `torch`, …, and their own `nirs4all-cluster` version). Job
`requirements.packages` route on those declared versions — see {doc}`operations`.
```

## Pipeline fingerprints

Every successful `TaskResult` records `pipeline_fingerprint`: a sha256 of the pipeline
content the worker actually ran. For an **inline** pipeline the client computes the same
canonical hash and pins it as `expected_fingerprint`; if the worker's fingerprint differs,
the server emits a `pipeline_fingerprint_mismatch` event (non-fatal — the result still
stands; this is traceability). For `path`/`artifact` pipelines the fingerprint is the
sha256 of the file bytes.
