from mcp.server.auth.middleware.auth_context import auth_context_var
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken

from holon.context import Caller, Principal, RequestContext, get_context

PROFILE = {"email": "user@example.com", "email_verified": True}
REPORTER = {"app": "reporter", "session": "session"}


def test_get_context_reads_the_caller_holon_admitted():
    assert get_context() == RequestContext(principal=None, caller=None)
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
            RequestContext(
                Principal(
                    issuer="https://id.example",
                    subject="user",
                    client_id="web",
                    scopes=["read", "write"],
                    claims=PROFILE,
                    actor={"sub": "agent"},
                ),
                caller=None,
            ),
        ),
        # An app acting for a person, then an app or its lifecycle hook's job with nobody signed in.
        (
            "web",
            [],
            {"iss": "https://id.example", "sub": "user", "caller": REPORTER},
            RequestContext(
                Principal("https://id.example", "user", "web", [], {}),
                Caller("reporter", "session"),
            ),
        ),
        (
            "",
            [],
            {"caller": REPORTER},
            RequestContext(None, Caller("reporter", "session")),
        ),
        # An older runtime passed the raw mesh identity, and a person needs an issuer.
        (
            "",
            [],
            {"caller": "spiffe://cluster.local/ns/session/sa/reporter"},
            RequestContext(None, None),
        ),
        ("web", [], {"sub": "user"}, RequestContext(None, None)),
    ]:
        token = AccessToken(
            token="t",
            client_id=client_id,
            scopes=scopes,
            subject=extra.get("sub"),
            claims=extra,
        )
        admitted = auth_context_var.set(AuthenticatedUser(token))
        try:
            assert get_context() == expected
        finally:
            auth_context_var.reset(admitted)
