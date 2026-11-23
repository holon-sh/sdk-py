from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
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

    https://openid.net/specs/openid-connect-core-1_0.html#StandardClaims
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
    """The user a request acts for."""

    issuer: str
    subject: str
    client_id: str
    scopes: list[str]
    claims: IdentityClaims
    actor: dict[str, Any] | None = None


@dataclass(frozen=True)
class Caller:
    """The app a request comes from. A lifecycle hook's job calls as its app."""

    app: str  # as named in holon.yaml
    session: str  # the namespace of the session both apps run in


@dataclass(frozen=True)
class RequestContext:
    principal: Principal | None  # None when anonymous
    caller: Caller | None  # None when a person calls the app directly


def get_context() -> RequestContext:
    """Who the current MCP request comes from: the person and the calling app."""
    admitted = get_access_token()
    extra = (admitted.claims or {}) if admitted else {}
    # None for a person's direct call, and for an older runtime's raw mesh identity.
    caller: Caller | None = None
    match extra.get("caller"):
        case {"app": str(app), "session": str(session)}:
            caller = Caller(app, session)
    if admitted is None or admitted.subject is None or "iss" not in extra:
        return RequestContext(principal=None, caller=caller)
    return RequestContext(
        Principal(
            issuer=extra["iss"],
            subject=admitted.subject,
            client_id=admitted.client_id,
            scopes=admitted.scopes,
            claims=extra.get("claims", {}),
            actor=extra.get("act"),
        ),
        caller,
    )


@dataclass
class _Request:
    """The MCP request being served. Tasks it spawns share it, and see when it ends."""

    token: str
    ended: bool = False


_request: ContextVar[_Request | None] = ContextVar("holon_request", default=None)


_acting: ContextVar[str | None] = ContextVar("holon_acting", default=None)


@contextmanager
def act_for(subject: str) -> Iterator[None]:
    """Call other apps as ``subject`` in this block, for work no request of theirs waits on.

    Use it on a timer, at startup, or after a request was answered: those calls otherwise carry no user. Holon admits
    them only while the person lets this app act for them, with their current access. The apps called see them as
    ``principal``, and this app as ``principal.actor``. Inside a request, the block acts for ``subject`` instead of the
    request's user, so name only a person whose ``principal.subject`` this app read itself. Dependencies bound
    explicitly never carry the person.

    Args:
        subject: The person's ``principal.subject``.
    """
    if not subject:
        raise ValueError("act_for needs the subject of a person")
    acting = _acting.set(subject)
    try:
        yield
    finally:
        _acting.reset(acting)
