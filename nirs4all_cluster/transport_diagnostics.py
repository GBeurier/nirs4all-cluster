"""Nonsecret HTTP failure diagnostics for owned CLI and provider reports."""

import httpx


def http_failure_diagnostic(error: httpx.HTTPError) -> str:
    """Return status/class only; never inspect message, request, headers or causes."""
    if isinstance(error, httpx.HTTPStatusError):
        return f"HTTP {error.response.status_code}"
    return type(error).__name__
