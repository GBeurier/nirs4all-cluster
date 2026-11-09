"""Authenticated HTTP/WS front door, before parsing bodies or persisting events."""

import re
from urllib.parse import parse_qsl

from nirs4all_access import AccessError, ProjectTokenVerifier, hash_token, parse_bearer
from starlette.datastructures import Headers
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from ..versioning import API_VERSION, CLUSTER_VERSION, H_API, H_VERSION, is_incompatible, parse_api

PUBLIC = {"/", "/healthz", "/version", "/ui", "/ui/"}


def credential_query(pairs) -> bool:
    """Delegate recognition of values to the accepted shared leaf."""
    if {name.lower() for name, _ in pairs} & {"token", "access_token", "project_token", "authorization"}:
        return True
    for _, value in pairs:
        try:
            hash_token(value)
        except ValueError:
            continue
        return True
    return False


class CredentialSafeCORSMiddleware(CORSMiddleware):
    """Keep CORS outermost while routing credential queries through the front door."""

    async def __call__(self, scope, receive, send):
        async def safe_send(message):
            if message["type"] == "http.response.start" and message["status"] >= 400:
                scope["query_string"] = b""
            await send(message)

        pairs = parse_qsl(scope.get("query_string", b"").decode("latin1"), keep_blank_values=True)
        if scope["type"] == "http" and scope.get("method") == "OPTIONS" and credential_query(pairs):
            # Standard simple_response attaches CORS response headers but does not
            # short circuit authentication with the preflight fast path.
            headers = Headers(scope=scope)
            if "origin" in headers:
                await self.simple_response(scope, receive, safe_send, request_headers=headers)
            else:
                # Standard CORS also bypasses response decoration without Origin.
                # Forward to FrontDoor so credential queries still fail closed.
                await self.app(scope, receive, safe_send)
        else:
            await super().__call__(scope, receive, safe_send)


class FrontDoor:
    def __init__(self, app, *, owner, verifier: ProjectTokenVerifier, config):
        self.app, self.owner, self.verifier, self.config = app, owner, verifier, config

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            return await self.app(scope, receive, send)
        websocket = scope["type"] == "websocket"
        headers = {}
        duplicates = False
        for key, value in scope.get("headers", []):
            name = key.decode("latin1").lower()
            if name in headers and name in {"authorization", "x-n4c-worker-secret"}:
                duplicates = True
            headers[name] = value.decode("latin1")
        path = scope["path"]
        version_headers = {H_API: str(API_VERSION), H_VERSION: CLUSTER_VERSION}

        async def deny(status, detail, *, code=None, missing_rights=()):
            scope["query_string"] = b""
            body = {"detail": detail}
            if code:
                body.update(code=code, missing_rights=sorted(missing_rights))
            h = dict(version_headers)
            if status == 401:
                h["WWW-Authenticate"] = "Bearer"
            response = JSONResponse(body, status_code=status, headers=h)
            if websocket:
                # Supported by current uvicorn; don't accept before rejecting.
                await send({"type": "websocket.http.response.start", "status": status, "headers": response.raw_headers})
                await send({"type": "websocket.http.response.body", "body": response.body})
            else:
                await response(scope, receive, send)

        query_pairs = parse_qsl(scope.get("query_string", b"").decode("latin1"), keep_blank_values=True)
        query = dict(query_pairs)
        if credential_query(query_pairs):
            return await deny(401, "query credentials are forbidden", code="invalid_credential")
        public = path in PUBLIC and not websocket
        if not public:
            if duplicates:
                return await deny(401, "invalid or missing credential", code="invalid_credential")
            required = "cluster.read"
            if path.startswith("/v1/tasks/") or path.startswith("/v1/workers/"):
                required = "cluster.execute"
            elif scope.get("method") == "POST":
                required = "cluster.cancel" if path.endswith("/cancel") else "cluster.submit"
            try:
                principal = self.verifier.verify(
                    parse_bearer(headers.get("authorization")), service="cluster", required_rights=[required]
                )
            except AccessError as exc:
                return await deny(exc.status, exc.detail, code=exc.code, missing_rights=exc.missing_rights)
            if any(re.fullmatch(r"x-.*project(?:-id)?", h) for h in headers):
                return await deny(400, "project override headers are forbidden")
            service = self.owner.state.services.get(principal.project_id)
            if service is None:
                return await deny(403, "project service unavailable")
            scope.setdefault("state", {}).update(principal=principal, service=service)
            if is_incompatible(parse_api(headers.get(H_API.lower()))):
                return await deny(426, "incompatible protocol version")
            # Session authentication precedes multipart parsing. IDs alone are public.
            if (path.startswith("/v1/tasks/") or path.startswith("/v1/workers/")) and path != "/v1/workers/register":
                worker_id = query.get("worker_id") if path.startswith("/v1/tasks/") else path.split("/")[3]
                worker = service.db.get_worker(worker_id or "")
                if worker is None:
                    return await deny(404, "unknown worker")
                if worker["principal"] != principal.principal_id or not service.db.verify_worker_secret(
                    worker_id, headers.get("x-n4c-worker-secret")
                ):
                    return await deny(403, "worker session does not belong to caller")
            peer_version = headers.get(H_VERSION.lower(), "")
            if (
                peer_version != CLUSTER_VERSION
                and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", peer_version)
                and len(peer_version) <= 32
            ):
                # Bound storage; never persist arbitrary role/version text or credentials.
                seen = getattr(service, "seen_versions", None)
                if seen is None:
                    seen = service.seen_versions = set()
                if peer_version not in seen and len(seen) < 64:
                    seen.add(peer_version)
                    service.broker.emit(
                        level="warning",
                        type="version_divergence",
                        data={"peer_version": peer_version, "principal_id": principal.principal_id},
                    )
        # Count actual received bytes, including chunked JSON and multipart overhead.
        maximum = int(
            (self.config.max_artifact_mb if path.endswith("/artifacts") else self.config.max_request_mb) * 1024 * 1024
        )
        maximum += 65536 if path.endswith("/artifacts") else 0
        size = 0
        too_large = False

        async def bounded_receive():
            nonlocal size, too_large
            message = await receive()
            if message["type"] == "http.request":
                size += len(message.get("body", b""))
                if size > maximum:
                    too_large = True
                    raise ValueError("request body too large")
            return message

        sent = False

        async def version_send(message):
            nonlocal sent
            if too_large:
                return
            if (
                message["type"] in {"http.response.start", "websocket.http.response.start"}
                and message.get("status", 200) >= 400
            ):
                scope["query_string"] = b""
            if message["type"] in {"http.response.start", "websocket.accept", "websocket.http.response.start"}:
                sent = True
                message.setdefault("headers", []).extend(
                    [(k.lower().encode(), v.encode()) for k, v in version_headers.items()]
                )
            await send(message)

        try:
            length = headers.get("content-length")
            if not websocket and length is not None and (not length.isdecimal() or int(length) > maximum):
                return await deny(413, "request body too large")
            await self.app(scope, bounded_receive, version_send)
        except Exception:
            if not too_large:
                raise
        if too_large and not sent:
            await deny(413, "request body too large")
