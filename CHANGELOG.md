# Changelog

All notable changes to `nirs4all-cluster` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] — 2026-07-04

Hardening release accumulated on top of the 0.1.0 beta. No breaking
changes to the wire contract or public API.

### Added
- **Credential-scoped RBAC**: per-credential rights, a DAG rights-provenance contract, and
  worker lifecycle bound to the issuing principal.
- **Core `nirs4all.run` adapter** and typed client/worker transport errors.
- **`version-guard` CI** workflow blocking a manifest version ahead of the latest release tag.
- Reproducible installs/CI via a tracked `uv.lock`.

### Changed
- Scheduler attests the inferred job shape; the server attests scheduler shape from the job payload.

### Fixed
- Balanced same-priority lease assignment across workers.
- Requeue running-task failures through the `failed` state; deterministic DAG worker-loss handling.

### Security
- Documentation and CI hardening: removed literal cluster credentials / token-shaped examples and
  added `scripts/secret_shape_guard.py` to block token-shaped CLI examples in CI.

### Tests
- RBAC rights, artifact and WebSocket boundary coverage; distributed-parity harness; real-DAG
  parity; installed-wheel release smoke; end-to-end rights-handoff proof.

## [0.1.0] — 2026-06-16

First **beta** release. The project graduates from a `0.0.x` validation prototype to a
packaged, documented beta for **trusted-LAN** use. The documented non-goals still hold
(no mTLS, no sandbox for arbitrary Python, single SQLite server, no Ray/Dask-class
scheduler) — see `SECURITY.md` and `PROTOTYPE_DESIGN.md`.

### Added
- **Version compatibility (track & warn).** Client, worker and server advertise their
  `nirs4all-cluster` package version and a protocol `API_VERSION` on every `/v1` call.
  The server rejects an incompatible protocol major with **HTTP 426** and logs/emits a
  one-shot `version_divergence` event when a compatible peer runs a different package
  version. New module `nirs4all_cluster.versioning`.
- **Pipeline fingerprints.** Every task result records a sha256 of the pipeline content
  the worker actually ran. The client pins an `expected_fingerprint` for inline
  pipelines; the server emits a `pipeline_fingerprint_mismatch` event on divergence.
- **Built-in web dashboard** at `/ui` (single self-contained page, no build step): live
  jobs + workers via a new global WebSocket stream `GET /v1/events/stream`, job detail
  (ranking + recent events), and a cancel button.
- **Richer job listing**: `GET /v1/jobs` filters by `status`/`name` with `created_before`
  cursor pagination; new `GET /v1/stats`; `GET /v1/workers` now reports each worker's
  version and a divergence flag; new `n4cluster jobs` CLI command.
- **Structured logging** across the server and worker (`--log-level`, `--log-file`).
- **Ops endpoints**: `GET /healthz`, `GET /version`; opt-in CORS (`--cors-origin`); a JSON
  request-body size guard (`max_request_mb`, default 16 MB; multipart uploads exempt).
- Packaging: PEP 639 SPDX license metadata, `py.typed`, PyPI classifiers and URLs,
  GitHub Actions CI + Trusted-Publishing release, and Read the Docs documentation.

### Changed
- Version is now single-sourced from `nirs4all_cluster/__init__.py`; the server response
  and OpenAPI metadata no longer hardcode it.
- Status reframed from "public alpha / validation prototype" to **beta (trusted-LAN)**,
  keeping the documented non-goals and the open native-vs-Dask question.
