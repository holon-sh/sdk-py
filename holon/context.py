from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from mcp.server.auth.middleware.auth_context import get_access_token


class AddressClaims(TypedDict, total=False):
    formatted: str
    street_address: str
    locality: str
    region: str
    postal_code: str
    country: str


class IdentityClaims(TypedDict, total=False):
    """Standard OpenID Connect claims.

    Holon drops one whose value has the wrong type; other claims pass through.
    """

    name: str
    given_name: str
    family_name: str
    middle_name: str
    nickname: str
    preferred_username: str
    profile: str
    picture: str
    website: str
    email: str
    email_verified: bool
    gender: str
    birthdate: str
    zoneinfo: str
    locale: str
    phone_number: str
    phone_number_verified: bool
    address: AddressClaims
    updated_at: float


@dataclass(frozen=True)
class Principal:
    """The user a request acts for, as Holon admitted it."""

    issuer: str
    subject: str
    client_id: str
    scopes: list[str]
    claims: IdentityClaims
    actor: dict[str, Any] | None = None


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
    extra = (admitted.claims or {}) if admitted else {}
    if admitted is None or admitted.subject is None or "iss" not in extra:
        return RequestContext(principal=None)
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
