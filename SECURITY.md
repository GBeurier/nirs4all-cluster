# Security policy

## Supported scope — trusted LAN only

`nirs4all-cluster` is a **beta** distributed job queue designed for a **trusted local
network** (a lab, a CI runner pool, a single operator's machines). It is **not** hardened
for the open internet or for untrusted multi-tenant use. By design (see
`PROTOTYPE_DESIGN.md` §Non-goals and `PROTOTYPE_TO_PRODUCTION.md` §4) it currently provides:

- **A single static bearer token** for authentication — no mTLS, no OIDC, no per-identity
  credentials, no token rotation.
- **No sandbox** for the work it runs. A worker executes `nirs4all.run()` in a subprocess
  with the worker's own privileges. `python_entrypoint` jobs run **arbitrary Python** and
  are therefore gated behind **both** the server's `--allow-python-jobs` and the worker's
  `--allow-python`; never enable them when a third party can submit.
- **A single SQLite-backed server process** and a local object store — no network storage,
  no encryption at rest.

Run the server and workers only on a network you control, behind your own firewall/VPN.
Treat the token as a shared secret and prefer per-deployment tokens. The opt-in
`--cors-origin` flag is off by default; enable it only for origins you trust.

What is **out of scope** for this beta (tracked for a future production track): mTLS /
OIDC, per-task container sandboxing and quotas, encryption/retention of artifacts, and
multi-tenant isolation. Do not rely on this software for any of those properties.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public GitHub issue.

Email **nirs4all-admin@cirad.fr** with a description, affected version, and reproduction
steps. We will acknowledge receipt and coordinate a fix and disclosure timeline with you.
