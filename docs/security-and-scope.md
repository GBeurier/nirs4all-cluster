# Security & scope

```{warning}
**Trusted-LAN beta.** Run `nirs4all-cluster` only on a network you control. It is not
hardened for the open internet or for untrusted multi-tenant use.
```

This beta provides, **by design**:

- **Credential-bound RBAC over static bearer tokens.** Each principal is a named
  identity bound to a static token and granted rights from
  `{submit, read, cancel, execute, admin}` (composed into the roles `submitter`,
  `executor` = read+execute, `viewer`, `admin`). Rights are derived from the
  credential, **never** from the advisory `X-N4C-Role` header. A bare `--token`
  remains supported as a single all-rights admin principal; with neither a token
  nor principals the server runs **open (dev mode)**. Still **no mTLS, no OIDC, no
  rotation** — tokens are shared secrets, only safe on a trusted LAN.
- **No sandbox.** A worker runs `nirs4all.run()` in a subprocess with its own privileges.
  `python_entrypoint` jobs run **arbitrary Python** and are gated behind **both**
  `--allow-python-jobs` (server) and `--allow-python` (worker) — never enable them when a
  third party can submit.
- **A single SQLite-backed server** and a local object store — no network storage, no
  encryption at rest.

The full disclosure and the private reporting address are in
[`SECURITY.md`](https://github.com/GBeurier/nirs4all-cluster/blob/main/SECURITY.md).

## Non-goals (this beta does not do these)

- modifying other ecosystem libraries;
- open / multi-tenant access;
- a secure sandbox for arbitrary Python;
- a Kubernetes / Ray / Dask-class scheduler;
- concurrent writes to a shared `nirs4all` workspace;
- fold distribution (Level 3), or promised parity for explicit variants (Level 2).

These are documented in `design/prototype-design` and `design/prototype-to-production`.

## Where this is headed

The ecosystem's default recommendation remains an **opt-in execution backend in
`nirs4all`** (e.g. a Dask backend) rather than a default-operated home cluster. This
repository is a public, auditable beta that de-risks the decision; the broader
native-vs-Dask question is still open. The production track (mTLS/OIDC, per-task
sandboxing + quotas, Postgres, network object storage) is described in
`design/prototype-to-production`.
