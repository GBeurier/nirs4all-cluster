<!-- Thanks for contributing to nirs4all-cluster! -->

## Summary

<!-- What does this PR change, and why? -->

## Related issues

<!-- e.g. Closes #123 -->

## Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change
- [ ] Docs / CI / chore

## Checklist

- [ ] Green gate passes locally: `ruff check .`, `mypy nirs4all_cluster`, `pytest -q`
- [ ] Docs build if docs changed: `uv run --extra docs sphinx-build -W -b html docs docs/_build/html`
- [ ] Changes respect the documented non-goals (`PROTOTYPE_DESIGN.md` §Non-goals)
- [ ] State changes go through the scheduler state machine (not raw `UPDATE ... status`)
- [ ] `CHANGELOG.md` updated for user-facing changes
- [ ] No secrets, tokens, or literal credentials added (secret-scan clean)
