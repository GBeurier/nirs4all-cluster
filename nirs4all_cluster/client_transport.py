"""Shared HTTP transport for the typed cluster clients.

Both :class:`~nirs4all_cluster.client.ClusterClient` (the submitter / inspection
SDK) and :class:`~nirs4all_cluster.client_worker.WorkerClient` (the executor
control plane) build on this so they share one credential + version-handshake +
rights-respecting error surface.

- :func:`make_http_client` builds an ``httpx.Client`` carrying the bearer token and
  the ``X-N4C-*`` handshake headers, and installs a response hook that warns once on
  a compatible version drift and raises :class:`ClusterVersionError` on an
  incompatible protocol major (HTTP 426).
- :func:`request` performs a call, turning httpx transport failures into
  :class:`ClusterConnectionError` and mapping any 4xx/5xx to the typed error
  (see :mod:`nirs4all_cluster.client_errors`).

Never imports nirs4all (same red-line as the rest of the control plane).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from .client_errors import ClusterConnectionError, raise_for_response
from .versioning import (
    API_VERSION,
    CLUSTER_VERSION,
    H_API,
    H_VERSION,
    ClusterVersionError,
    is_divergent,
    request_headers,
)

logger = logging.getLogger("nirs4all_cluster.client")


def make_http_client(
    base_url: str,
    *,
    token: str | None,
    role: str,
    timeout: float,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """Build an ``httpx.Client`` for ``role`` (``"client"`` / ``"worker"``).

    ``transport`` lets a caller (or a test) inject an in-process ASGI transport; when
    ``None`` the default networking transport is used.
    """
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    headers.update(request_headers(role))
    warned: set[str | None] = set()

    def _on_response(response: httpx.Response) -> None:
        server_version = response.headers.get(H_VERSION)
        if is_divergent(server_version) and server_version not in warned:
            warned.add(server_version)
            logger.warning(
                "server runs nirs4all-cluster %s; %s runs %s (compatible)",
                server_version,
                role,
                CLUSTER_VERSION,
            )
        if response.status_code == 426:
            raise ClusterVersionError(
                f"server rejected {role} as protocol-incompatible "
                f"(server api={response.headers.get(H_API)}, {role} api={API_VERSION})"
            )

    kwargs: dict[str, Any] = {
        "base_url": base_url.rstrip("/"),
        "headers": headers,
        "timeout": timeout,
        "event_hooks": {"response": [_on_response]},
    }
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.Client(**kwargs)


def request(http: httpx.Client, method: str, url: str, **kwargs: Any) -> httpx.Response:
    """Perform a request, translating transport + HTTP-status failures to ``ClusterError``.

    A successful call returns the (already-read) response; a 4xx/5xx raises the typed
    error via :func:`raise_for_response`; an unreachable server raises
    :class:`ClusterConnectionError`.
    """
    try:
        response = http.request(method, url, **kwargs)
    except ClusterVersionError:
        raise  # protocol-incompatible (surfaced from the response hook)
    except httpx.TransportError as exc:
        raise ClusterConnectionError(str(exc), method=method, url=str(url)) from exc
    raise_for_response(response)
    return response
