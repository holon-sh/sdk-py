from mcp.server.auth.middleware.auth_context import auth_context_var
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken

from holon.context import Principal, get_context

PROFILE = {"email": "user@example.com", "email_verified": True}


def test_get_context_reads_the_caller_holon_admitted():
    assert get_context().principal is None
    for client_id, scopes, extra, expected in [
        (
            "web",
            ["read", "write"],
            {
                "iss": "https://id.example",
                "sub": "user",
                "claims": PROFILE,
                "act": {"sub": "agent"},
            },
            Principal(
                issuer="https://id.example",
                subject="user",
                client_id="web",
                scopes=["read", "write"],
                claims=PROFILE,
                actor={"sub": "agent"},
            ),
        ),
        (
            "web",
            [],
            {"iss": "https://id.example", "sub": "user"},
            Principal("https://id.example", "user", "web", [], {}),
        ),
        # A hook call carries no user.
        ("", [], {"hook": {"event": "installed"}}, None),
    ]:
        # The Holon host builds the access token this way from what it admitted.
        token = AccessToken(
            token="t",
            client_id=client_id,
            scopes=scopes,
            subject=extra.get("sub"),
            claims=extra,
        )
        admitted = auth_context_var.set(AuthenticatedUser(token))
        try:
            assert get_context().principal == expected
        finally:
            auth_context_var.reset(admitted)
