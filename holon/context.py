from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mcp.server.auth.middleware.auth_context import get_access_token


@dataclass(frozen=True)
class Principal:
    """The user a request acts for, as Holon admitted it.

    ``claims`` and ``actor`` are the identity provider's, as issued.
    """

    issuer: str
    subject: str
    client_id: str
    scopes: list[str]
    claims: dict[str, Any]
    actor: Any = None


@dataclass(frozen=True)
class RequestContext:
    principal: Principal | None


def get_context() -> RequestContext:
    """Read the caller of the current MCP request.

    Holon validates the request before it reaches the application and hands the
    caller over as MCP's access token, so this reads ``get_access_token()``.

    Returns:
        A ``RequestContext`` whose ``principal`` is ``None`` for anonymous requests.

    Example:
        ``principal = get_context().principal``
    """
    admitted = get_access_token()
    if admitted is None or admitted.subject is None:
        return RequestContext(principal=None)
    extra = admitted.claims or {}
    return RequestContext(
        Principal(
            issuer=extra["iss"],
            subject=admitted.subject,
            client_id=admitted.client_id,
            scopes=admitted.scopes,
            claims=extra.get("claims", {}),
            actor=extra.get("act"),
        )
    )
