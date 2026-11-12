import asyncio
import json
import socket

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer
from mcp.types import CallToolResult, InputRequiredResult, TextContent

from holon.client import Apps, ToolCallError

CONTRACT = {
    "tools": [
        {
            "name": "read",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "limit": {"type": "integer", "default": 5},
                    "name": {"type": "string"},
                    "timeout": {"type": "integer"},
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            "outputSchema": {
                "type": "object",
                "properties": {"total": {"type": "number"}},
                "required": ["total"],
            },
        }
    ]
}


def test_clients_preserve_mcp_results_and_validate(monkeypatch):
    async def exercise():
        server = MCPServer("orders")

        @server.tool()
        async def read(
            id: str, limit: int = 5, name: str = "", timeout: int = 0
        ) -> CallToolResult | InputRequiredResult:
            if id == "error":
                return CallToolResult(
                    is_error=True,
                    content=[TextContent(type="text", text="Unavailable")],
                    _meta={"preserved": True},
                )
            if id == "input":
                return InputRequiredResult(
                    request_state="opaque", _meta={"preserved": True}
                )
            return CallToolResult(
                content=[TextContent(type="text", text="Order")],
                structured_content={"total": "bad" if id == "bad" else limit},
            )

        app = server.streamable_http_app(stateless_http=True)
        requests = []
        clients = []
        headers = []

        async def record(request):
            if request.method == "POST":
                requests.append(json.loads(await request.aread()))

        async def listen(scope, receive, send):
            if scope["type"] == "http":
                headers.append(dict(scope["headers"]).get(b"authorization"))
            await app(scope, receive, send)

        def http_client():
            client = httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=listen),
                headers={"Authorization": "Bearer external"},
                event_hooks={"request": [record]},
            )
            clients.append(client)
            return client

        monkeypatch.delenv("MISSING_MCP_URL", raising=False)
        monkeypatch.setenv("ORDERS_MCP_TOKEN", "secret")
        binding = {"endpoint": "http://127.0.0.1:8000/mcp", "http_client": http_client}
        bindings = {"orders": binding}
        identity = {"name": "checkout", "version": "1", "requires": {"orders": {}}}
        apps = Apps(identity, {"orders": CONTRACT}, bindings)
        other = Apps({**identity, "name": "other"}, {"orders": CONTRACT}, bindings)
        async with app.router.lifespan_context(app):
            await asyncio.gather(
                apps.orders.tools.read(id="good"),
                other.orders.call_tool(
                    "read",
                    {
                        "timeout": 5,
                        "meta": {"trace": "control"},
                    },
                    id="good",
                    limit=2,
                    name="field",
                    timeout=7,
                ),
            )
            request = next(
                request["params"]
                for request in requests
                if request["method"] == "tools/call"
                and request["params"]["arguments"].get("name") == "field"
            )
            assert request["arguments"] == {
                "id": "good",
                "limit": 2,
                "name": "field",
                "timeout": 7,
            }
            assert request["_meta"]["trace"] == "control"
            assert not any(request["method"] == "tools/list" for request in requests)
            callers = [
                request["params"]["_meta"]["io.modelcontextprotocol/clientInfo"]["name"]
                for request in requests
                if request["method"] == "tools/call"
            ]
            assert set(callers) == {"checkout", "other"}
            for value in ["error", "input"]:
                raw = await apps.orders.call_tool("read", id=value)
                with pytest.raises(ToolCallError) as error:
                    await apps.orders.tools.read(id=value)
                assert type(error.value.result) is type(raw)
                assert error.value.result.meta["preserved"] is True
                if value == "error":
                    assert error.value.result == raw
                    assert "Unavailable" in str(error.value)
            with pytest.raises(ValueError):
                await apps.orders.tools.read(id="bad")
            undeclared = {**identity, "requires": {"orders": {}, "missing": {}}}
            untyped = Apps(undeclared, {}, bindings)
            raw = await untyped.orders.call_tool("read", id="good")
            assert raw.structured_content == {"total": 5}
            tool = {
                k: v for k, v in CONTRACT["tools"][0].items() if k != "outputSchema"
            }
            plain = Apps(identity, {"orders": {"tools": [tool]}}, bindings)
            assert (await plain.orders.tools.read(id="good")).content[0].text == "Order"
            with pytest.raises(RuntimeError):
                await Apps(undeclared, {}).missing.call_tool("read")
            script = {**identity, "requires": {}}
            with pytest.raises(ValueError):
                Apps(script, {"orders": CONTRACT}, {"missing": binding})
            bound = Apps(script, {"orders": CONTRACT}, bindings)
            assert await bound.orders.tools.read(id="good") == {"total": 5}
            with pytest.raises(ValueError):
                await Apps(script, {"orders": CONTRACT}).orders.tools.read(id="good")
            assert set(headers) == {b"Bearer external"}
            sock = socket.create_server(("127.0.0.1", 0))
            monkeypatch.setenv(
                "ORDERS_MCP_URL", f"http://127.0.0.1:{sock.getsockname()[1]}/mcp"
            )
            http = uvicorn.Server(
                uvicorn.Config(listen, lifespan="off", log_level="warning")
            )
            serving = asyncio.create_task(http.serve(sockets=[sock]))
            apps = Apps(identity, {"orders": CONTRACT})
            try:
                headers.clear()
                assert await apps.orders.tools.read(id="good") == {"total": 5}
                assert set(headers) == {b"Bearer secret"}
                monkeypatch.delenv("ORDERS_MCP_TOKEN")
                headers.clear()
                await apps.orders.tools.read(id="good")
                assert set(headers) == {None}
            finally:
                http.should_exit = True
                await serving
        assert all(client.is_closed for client in clients)

    asyncio.run(asyncio.wait_for(exercise(), timeout=15))
