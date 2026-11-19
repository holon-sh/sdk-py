import asyncio
from importlib.metadata import version

import pytest
from http_server import listening
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import CLIENT_INFO_META_KEY, CallToolResult

from holon.system import control, create_control


def test_control_reads_and_validates_traces(monkeypatch):
    async def exercise():
        server = MCPServer("control")

        @server.tool()
        async def get_traces(ctx: Context, status: str = "invalid") -> CallToolResult:
            peer = ctx.request_context.meta[CLIENT_INFO_META_KEY]
            assert (peer["name"], peer["version"]) == ("holon-sh", version("holon-sh"))
            trace = {
                "traceId": "a1",
                "start": "2026-11-18T12:00:00Z",
                "method": "tools/call",
                "status": status,
                "durationMs": 12,
            }
            return CallToolResult(
                content=[], structured_content={"session": "dev", "traces": [trace]}
            )

        app = server.streamable_http_app(stateless_http=True)
        async with app.router.lifespan_context(app), listening(app) as origin:
            endpoint = origin + "/mcp"
            bound = create_control({"endpoint": endpoint})
            monkeypatch.delenv("CONTROL_MCP_URL", raising=False)
            page = await bound.tools.get_traces(status="error")
            assert page["traces"][0]["status"] == "error"
            monkeypatch.setenv("CONTROL_MCP_URL", endpoint)
            with pytest.raises(
                ValueError, match="structured output from control.get_traces"
            ):
                await control.tools.get_traces()

    asyncio.run(asyncio.wait_for(exercise(), timeout=15))
