# Security policy

## Supported scope — trusted LAN only

`nirs4all-cluster` is a **beta** distributed job queue designed for a **trusted local
network** (a lab, a CI runner pool, a single operator's machines). It is **not** hardened
for the open internet or for untrusted multi-tenant use. By design (see
`PROTOTYPE_DESIGN.md` §Non-goals and `PROTOTYPE_TO_PRODUCTION.md` §4) it currently provides:

- **Static bearer-token authentication with optional credential-bound RBAC.** Each principal
  is a named identity bound to a static token and granted rights from
  `{submit, read, cancel, execute, admin}` (composed into `submitter` / `executor` /
  `viewer` / `admin` roles); rights derive from the credential, never from the advisory
  `X-N4C-Role` header. A bare `--token` remains a single all-rights admin principal, and with
  neither a token nor principals the server runs open (dev mode). Still **no mTLS, no OIDC, no
  token rotation** — tokens are shared secrets, only safe on a trusted LAN. See
  `docs/security-and-scope.md`.
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

## If a token-shaped value lands in git history

Treat any committed bearer token or `--principal` credential as exposed, even if it only
appeared in docs, examples, or a private branch.

1. Revoke or rotate the affected token(s) first. If the exposure was a `--principal`
   example, rotate every principal token present in the string before any cleanup.
2. Replace the committed value with placeholders such as `<auth-token>` and
   `<principal-spec>`, or load credentials from a shell variable / secret manager.
3. If the value reached published history, scrub that history and any generated docs or
   release artifacts that embedded it.
4. Re-run the repo secret gates before pushing the remediation:
   `git ls-files -z | xargs -0 uvx --from detect-secrets detect-secrets-hook --baseline .secrets.baseline`
   and `python3 scripts/secret_shape_guard.py`.

The secret-shape guard exists specifically to reject token-shaped CLI examples before they
reach history. Keep examples schematic; never commit a realistic-looking credential.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public GitHub issue.

Email **nirs4all-admin@cirad.fr** with a description, affected version, and reproduction
steps. We will acknowledge receipt and coordinate a fix and disclosure timeline with you.
