"""Serves an app at the paths Holon gives it: ``python -m holon.serve server.py``."""

from __future__ import annotations

import asyncio
import base64
import importlib
import mimetypes
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any

import uvicorn
from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.server.transport_security import TransportSecuritySettings
from opentelemetry import context as tracing
from opentelemetry import propagate
from pydantic import BaseModel
from starlette.authentication import AuthCredentials, UnauthenticatedUser
from starlette.datastructures import Headers
from starlette.responses import FileResponse, PlainTextResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from holon.context import _Request, _request

__all__ = ["App", "load", "serve"]

_MCP = re.compile(r"/mcp(/[^/]+)?")
_CONTEXT = re.compile(r"[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.[A-Za-z0-9_-]+")
_CALLER = re.compile(r"([^/]+)/([^/]+)")
_MALFORMED = PlainTextResponse(
    "Malformed Holon context\n",
    status_code=400,
    headers={"x-content-type-options": "nosniff"},
)


@dataclass(frozen=True)
class App:
    """An app's module: its MCP server, and the directory of its page."""

    server: MCPServer[Any]
    frontend: Path | None = None


def load(entry: str) -> App:
    """Import an app's module, such as ``server.py`` or ``src/server.py``, from the current directory.

    The module defines ``server``, an ``MCPServer``, and may define ``frontend``, the directory of its page.
    """
    sys.path.insert(0, os.getcwd())
    name = ".".join(PurePath(entry.removesuffix(".py")).parts)
    module = importlib.import_module(name)
    server = getattr(module, "server", None)
    if not isinstance(server, MCPServer):
        raise TypeError(f"{entry} must define server, an MCPServer")
    frontend = os.fspath(getattr(module, "frontend", ""))
    return App(server, Path(frontend).absolute() if frontend else None)


async def serve(app: App) -> None:
    """Serve ``app`` on ``HOST`` and ``PORT``, or else on 127.0.0.1:3000.

    The MCP endpoint is at ``/mcp`` and ``/mcp/<app>``, and the frontend's files are everywhere else.
    """
    config = uvicorn.Config(
        _asgi(app),
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "3000")),
        lifespan="on",
        h11_max_incomplete_event_size=65536,
        access_log=False,
        proxy_headers=False,
        ws="none",
        timeout_graceful_shutdown=1,
    )
    await uvicorn.Server(config).serve()


def _asgi(app: App) -> ASGIApp:
    server = app.server
    served = server.streamable_http_app(
        stateless_http=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False
        ),
    )
    mcp = AuthContextMiddleware(server.session_manager.handle_request)
    platform = os.environ.get("HOLON_PLATFORM") == "1"

    async def dispatch(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await served(scope, receive, send)
            return
        headers = scope["headers"]
        scope["headers"] = [
            (name, value)
            for name, value in headers
            if not name.startswith(b"x-holon-") and name != b"authorization"
        ]
        if not _MCP.fullmatch(scope["path"]):
            frontend = app.frontend if scope["method"] in ("GET", "HEAD") else None
            await _page(frontend, scope["path"])(scope, receive, send)
            return
        read = _admit(headers) if platform else ("", None)
        if read is None:
            # A context the waypoint could not have sent is a malformed request (RFC 9110 §15.5.1).
            await _MALFORMED(scope, receive, send)
            return
        token, admitted = read
        scope["user"] = UnauthenticatedUser()
        scope["auth"] = AuthCredentials()
        if admitted is not None:
            scope["user"] = AuthenticatedUser(admitted)
            scope["auth"] = AuthCredentials(admitted.scopes)
        request = _Request(token)
        current = _request.set(request)
        # The SDK's server span parents on params._meta, else on this context: the caller's trace headers.
        trace = tracing.attach(propagate.extract(Headers(scope=scope)))
        try:
            await mcp(scope, receive, send)
        finally:
            request.ended = True
            tracing.detach(trace)
            _request.reset(current)

    return dispatch


class _Principal(BaseModel, strict=True):
    iss: str | None = None
    sub: str | None = None
    client_id: str | None = None
    scope: str | None = None
    claims: dict[str, Any] | None = None
    act: dict[str, Any] | None = None


class _Claims(BaseModel, strict=True):
    principal: _Principal | None = None


def _admit(
    headers: list[tuple[bytes, bytes]],
) -> tuple[str, AccessToken | None] | None:
    """The request's context and who it comes from, or None when the waypoint's headers are malformed.

    The waypoint names the calling app of the session, a lifecycle hook's job as its app.
    """
    contexts = [value for name, value in headers if name == b"x-holon-context"]
    callers = [value for name, value in headers if name == b"x-holon-caller"]
    if len(contexts) > 1 or len(callers) > 1:
        return None
    token, principal, caller = "", None, None
    if contexts:
        token = contexts[0].decode("latin-1")
        if (form := _CONTEXT.fullmatch(token)) is None:
            return None
        payload = form[1] + "=" * (-len(form[1]) % 4)
        try:
            claims = _Claims.model_validate_json(base64.urlsafe_b64decode(payload))
        except ValueError:
            return None
        principal = claims.principal
    if callers and (caller := _CALLER.fullmatch(callers[0].decode("latin-1"))) is None:
        return None
    if principal is None and caller is None:
        return token, None
    extra: dict[str, Any] = {}
    if caller is not None:
        extra["caller"] = {"app": caller[2], "session": caller[1]}
    if principal is not None:
        extra |= {"iss": principal.iss or "", "sub": principal.sub or ""}
        if principal.claims is not None:
            extra["claims"] = principal.claims
        if principal.act is not None:
            extra["act"] = principal.act
    scope = (principal.scope if principal else None) or ""
    return token, AccessToken(
        token=token,
        client_id=(principal.client_id if principal else None) or "",
        scopes=[each for each in scope.split(" ") if each],
        subject=extra.get("sub"),
        claims=extra,
    )


def _page(directory: Path | None, path: str) -> Response:
    if directory is None or "/." in path or "\\" in path:
        return Response(status_code=404)
    try:
        root = directory.resolve()
        name = (root / path.lstrip("/")).resolve()
        if name.is_dir():
            name = (name / "index.html").resolve()
        if name.is_relative_to(root) and name.is_file():
            kind = mimetypes.guess_type(name)[0] or "application/octet-stream"
            return FileResponse(
                name, media_type=kind, headers={"x-content-type-options": "nosniff"}
            )
    except (OSError, ValueError):
        pass
    return Response(status_code=404)


if __name__ == "__main__":
    asyncio.run(serve(load(sys.argv[1])))
