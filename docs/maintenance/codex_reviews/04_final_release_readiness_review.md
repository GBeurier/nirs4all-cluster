# Codex Gate 4 — final release-readiness review (nirs4all-cluster)

**Reviewer:** Codex CLI 0.142.5 — `codex exec` (read-only), 2026-07-04, after commit `d460d4d` (CI green).

**Codex verdict:** *"Not ready to tag/publish as-is … credible for a trusted-LAN beta, but I would block this exact release until the release metadata and gates match reality."* — which is expected: **this pass deliberately does not tag or publish.** The findings are triaged below.

## Findings & disposition

| # | Codex finding | class | disposition |
|---|---|---|---|
| 1 | `quality_gates.md` claims all actions SHA-pinned, but `version-guard.yml` still floated `actions/checkout@v4` + `actions/setup-python@v5`. | **introduced by this pass** | **FIXED** — both pinned to SHAs; claim now accurate. |
| 2 | `release_checklist.md` smoke `n4cluster --version` — CLI has no global `--version`. | **introduced by this pass** | **FIXED** — smoke now `n4cluster --help` + `python -c "import nirs4all_cluster …"`. |
| 3 | `SECURITY.md` says "no per-identity credentials", but credential-bound RBAC is now implemented (`docs/security-and-scope.md`). | pre-existing doc drift | **FIXED** — `SECURITY.md` updated to describe credential-bound RBAC while keeping the no-mTLS/OIDC/rotation caveats. |
| 4 | `CHANGELOG [0.1.1]` marked `unreleased` while `__init__` reports `0.1.1`. | release-sprint | **Documented** — `[0.1.1] — unreleased` is the honest KaC label for an un-tagged version; it gets dated when the `v0.1.1` tag is cut. |
| 5 | `version-guard` inert until `vX.Y.Z` tags exist (only `n4a-v1-*` today). | release-sprint | **Documented** in `release_checklist.md` + `repository_audit.md`; reconcile the tag scheme before the next release. |
| 6 | `release_smoke` deselected from default CI. | release-sprint | **Documented** — add a release-smoke lane / run installed-wheel smoke before publishing (checklist). |
| 7 | README "What it does" formatting damaged (glued inline code, collapsed list). | NICE-TO-HAVE | **Flagged, not fixed** — README/branding is owned by a concurrent branding pass; left to avoid a collision (see final_report residual risks). |
| — | Trusted-LAN non-goals (no sandbox/mTLS/OIDC), no coverage floor, no SAST, Ubuntu-only CI. | accepted / roadmap | Documented in `quality_gates.md` (Known gaps) + `repository_audit.md` (Deepest roadmap). Codex agrees these do not block the beta framing. |

## Net
Two accuracy defects introduced by the pass (1, 2) were caught and fixed; one pre-existing doc drift (3) fixed. All remaining Codex "release-blockers" are genuine **release-sprint** prerequisites (tag scheme, dated changelog, release-smoke gate) — out of scope for this push-only hardening pass and explicitly documented. No correctness/security regression in the shipped changes.
