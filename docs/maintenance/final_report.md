# Final hardening report — nirs4all-cluster

**Date:** 2026-07-04 · **Branch:** `main` · **Operator:** Claude (Opus 4.8) · **Reviewer:** Codex CLI 0.142.5

## Summary
Pragmatic pre-release hardening of the `nirs4all-cluster` trusted-LAN beta: added the missing
community-health + tooling set, hardened CI/release workflows (least-privilege token scope +
SHA-pinned actions), caught up the CHANGELOG, and added a `docs/maintenance/` trail (audit,
quality gates, release checklist, Codex reviews). **No code or public-API changes.**

## Commits
- **Baseline (start):** `e50c8fa` (actor's HEAD at start; reconciled from `origin/main`).
- **Commit 1:** `d460d4d` — community-health + CI/release hardening + CHANGELOG + docs/maintenance.
- **Commit 2:** *(this commit)* — Gate 4 fixes: pin `version-guard.yml` actions, fix release-checklist
  smoke, align `SECURITY.md` with credential-bound RBAC; add Gate 4 review + this report.

## Files changed
Added: `CODE_OF_CONDUCT.md`, `CITATION.cff`, `.editorconfig`, `.pre-commit-config.yaml`,
`.github/dependabot.yml`, `.github/PULL_REQUEST_TEMPLATE.md`, `.github/ISSUE_TEMPLATE/{bug_report,feature_request,config}`,
`docs/maintenance/{repository_audit,quality_gates,release_checklist,final_report}.md`,
`docs/maintenance/codex_reviews/{01,03,04}_*.md`.
Modified: `.github/workflows/{ci,release,version-guard}.yml` (permissions + SHA pins), `CHANGELOG.md`
(`[0.1.1]`), `SECURITY.md` (RBAC), `docs/maintenance/quality_gates.md` + PR template (doc fixes).

## Checks
- Local gate (green): `uv run ruff check .` ✓, `uv run mypy nirs4all_cluster` ✓ (22 files),
  `uv run pytest -q` ✓ (**147 passed, 1 skipped**). YAML/CFF syntax validated.
- **Codex Gate 1** (audit) — consolidated into Gate 0 + Gate 3 (see `codex_reviews/01`).
- **Codex Gate 3** (diff, `codex exec review --uncommitted`) — "structurally sound"; 2 minor doc nits fixed.
- **Codex Gate 4** (final readiness) — 3 defects fixed (2 introduced, 1 pre-existing), rest documented as release-sprint.
- Not run locally: full `scripts/validation.py` (needs sibling `nirs4all` venv + data); OS-matrix (CI ubuntu-only).

## GitHub Actions (commit `d460d4d`, push event)
- `CI [push]`: **success** (#28691154624) — secret-scan + test matrix 3.11/3.12/3.13 + docs + package.
- `version-guard [push]`: **success** (#28691154613).
- Dependabot (`pip`, `github-actions`): update jobs **success** (config live on landing).
- Commit 2 CI/version-guard: verified green post-push (see run list for this commit).

## Residual risks
- **README "What it does" section formatting is damaged** (glued inline code, collapsed list) —
  from an automated branding pass; left untouched to avoid colliding with the concurrent branding
  agent. **Recommend the branding owner reflow it.**
- Release prerequisites (NOT done here, documented): reconcile the inert `version-guard` tag scheme
  to `vX.Y.Z`, date the CHANGELOG at tag time, add a release-smoke CI lane, confirm PyPI Trusted Publisher.
- Roadmap gaps: coverage floor, SAST/dependency scan, OS matrix (see `repository_audit.md`).

## Release readiness
**Push-hardening: complete and CI-green.** **Release: blocked on the documented release-sprint items**
(tag scheme, dated changelog, release-smoke). The trusted-LAN non-goals are intentional and do not
block the beta framing.

## 12-month maintenance routine
- Merge weekly Dependabot PRs (actions + pip) after CI green.
- Keep `CHANGELOG.md` current; date `[Unreleased]` → `vX.Y.Z` at each tag (tag the exact release commit).
- Run `uvx pre-commit run --all-files` before large changes; keep ruff/mypy green.
- Before any release: full green gate + `scripts/validation.py` + the `release_checklist.md`.
- Revisit the roadmap gaps (coverage floor, SAST, OS matrix) once the tag scheme is reconciled.
