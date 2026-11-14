import asyncio

import httpx2
import pytest

from holon.client import Apps, ToolCallError

CONTRACT = {
    "tools": [
        {
            "name": "query",
            "inputSchema": {"type": "object", "properties": {}},
        }
    ]
}


def refusing(status, body, headers=()):
    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"text/plain"), *headers],
            }
        )
        await send({"type": "http.response.body", "body": body})

    bindings = {
        "bigquery": {
            "endpoint": "http://bigquery.test/mcp",
            "http_client": lambda: httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=app),
                headers={"Authorization": "Bearer secret-token"},
            ),
        }
    }
    identity = {"name": "analytics", "version": "1", "requires": {"bigquery": {}}}
    return Apps(identity, {"bigquery": CONTRACT}, bindings).bigquery


def test_an_http_error_names_the_dependency_its_status_and_body():
    bigquery = refusing(401, b"Request had invalid authentication credentials.")

    with pytest.raises(ToolCallError) as typed:
        asyncio.run(bigquery.tools.query())
    with pytest.raises(ToolCallError) as full:
        asyncio.run(bigquery.call_tool("query"))

    expected = "bigquery: HTTP 401 Unauthorized: Request had invalid authentication credentials."
    assert str(typed.value) == str(full.value) == expected
    assert "secret-token" not in expected


def test_an_empty_body_falls_back_to_the_authentication_challenge():
    challenge = b'Bearer error="invalid_token"'
    bigquery = refusing(401, b"", [(b"www-authenticate", challenge)])

    with pytest.raises(
        ToolCallError, match='HTTP 401 Unauthorized: Bearer error="invalid_token"$'
    ):
        asyncio.run(bigquery.tools.query())


def test_a_long_body_is_cut_short():
    bigquery = refusing(502, b"upstream\n\n  failed " + b"x" * 5000)

    with pytest.raises(ToolCallError) as error:
        asyncio.run(bigquery.tools.query())

    message = str(error.value)
    assert message.startswith("bigquery: HTTP 502 Bad Gateway: upstream failed xxx")
    assert message.endswith("…")
    assert len(message) < 400
