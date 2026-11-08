import asyncio
import json
import socket

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer
from mcp.types import CallToolResult, InputRequiredResult, TextContent

import holon.binding
from holon.binding import set_default_binding
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


def test_clients_preserve_mcp_results_validate_and_cancel(monkeypatch):
    async def exercise():
        started = asyncio.Event()
        cancelled = asyncio.Event()
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
            if id == "slow":
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            return CallToolResult(
                content=[TextContent(type="text", text="Order")],
                structured_content={"total": "bad" if id == "bad" else limit},
                _meta={"preserved": True},
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

        monkeypatch.setattr(holon.binding, "_resolver", None)
        monkeypatch.delenv("MISSING_MCP_URL", raising=False)
        monkeypatch.setenv("ORDERS_MCP_TOKEN", "secret")
        set_default_binding(
            lambda app: (
                {"endpoint": "http://127.0.0.1:8000/mcp", "http_client": http_client}
                if app == "orders"
                else None
            )
        )
        identity = {"name": "checkout", "version": "1", "requires": {"orders": {}}}
        apps = Apps(identity, {"orders": CONTRACT})
        other = Apps({**identity, "name": "other"}, {"orders": CONTRACT})
        async with app.router.lifespan_context(app):
            output, raw = await asyncio.gather(
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
            assert output == {"total": 5}
            assert raw.meta["preserved"] is True
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
            before = len(requests)
            with pytest.raises(ValueError, match="arguments"):
                await apps.orders.tools.read(id=1)
            assert len(requests) == before
            with pytest.raises(ValueError, match="structured output"):
                await apps.orders.tools.read(id="bad")
            task = asyncio.create_task(apps.orders.tools.read(id="slow"))
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await cancelled.wait()
            untyped = Apps({**identity, "requires": {"orders": {}, "missing": {}}}, {})
            raw = await untyped.orders.call_tool("read", id="good")
            assert raw.structured_content == {"total": 5}
            assert not hasattr(untyped.orders.tools, "read")
            tool = {
                k: v for k, v in CONTRACT["tools"][0].items() if k != "outputSchema"
            }
            plain = Apps(identity, {"orders": {"tools": [tool]}})
            assert (await plain.orders.tools.read(id="good")).content[0].text == "Order"
            with pytest.raises(RuntimeError, match=r"unavailable \(MISSING_MCP_URL\)"):
                await untyped.missing.call_tool("read")
            set_default_binding(
                lambda _: {"endpoint": "http://unavailable.invalid/mcp"}
            )
            script = {**identity, "requires": {}}
            binding = {
                "endpoint": "http://127.0.0.1:8000/mcp",
                "http_client": http_client,
            }
            with pytest.raises(ValueError, match="Unknown app binding"):
                Apps(script, {"orders": CONTRACT}, {"missing": binding})
            bound = Apps(script, {"orders": CONTRACT}, {"orders": binding})
            assert await bound.orders.tools.read(id="good") == {"total": 5}
            set_default_binding(lambda _: None)
            with pytest.raises(ValueError, match="not declared"):
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
