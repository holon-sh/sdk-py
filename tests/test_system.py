import asyncio
import json
import socket
from importlib.resources import files
from typing import get_args, get_origin, get_type_hints

import pytest
import uvicorn
from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent

from holon.client import ToolCallError
from holon.system import (
    ControlGetTracesInput,
    ControlGetTracesOutput,
    ControlTools,
    control,
    create_control,
)

CONTRACT = json.loads(files("holon.system").joinpath("control.json").read_bytes())
TRACE = {
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "start": "2026-11-18T12:00:00Z",
    "method": "tools/call",
    "tool": "read",
    "status": "ok",
    "durationMs": 12,
}


def test_types_match_the_contract():
    def matches(typed, schema):
        hints = get_type_hints(typed)
        assert set(hints) == set(schema["properties"]), typed
        assert typed.__required_keys__ == set(schema.get("required", ())), typed
        for key, value in schema["properties"].items():
            if "enum" in value:
                assert get_args(hints[key]) == tuple(value["enum"]), key
            elif value["type"] == "array":
                assert get_origin(hints[key]) is list, key
                matches(get_args(hints[key])[0], value["items"])
            else:
                assert hints[key] is {"string": str, "integer": int}[value["type"]]

    tools = {tool["name"]: tool for tool in CONTRACT["tools"]}
    assert set(tools) == {name for name in vars(ControlTools) if name[0] != "_"}
    matches(ControlGetTracesInput, tools["get_traces"]["inputSchema"])
    matches(ControlGetTracesOutput, tools["get_traces"]["outputSchema"])


def test_control_reads_traces(monkeypatch):
    async def exercise():
        server = MCPServer("holon-control")
        sent = []

        @server.tool()
        async def get_traces(
            since: str | None = None,
            status: str | None = None,
            tool: str | None = None,
            limit: int | None = None,
            before: str | None = None,
        ) -> CallToolResult:
            sent.append({"since": since, "limit": limit})
            if status == "error":
                return CallToolResult(
                    is_error=True, content=[TextContent(type="text", text="no calls")]
                )
            trace = {**TRACE, "status": "failed"} if tool == "bad" else TRACE
            return CallToolResult(
                content=[],
                structured_content={"session": "dev-1", "traces": [trace]},
            )

        app = server.streamable_http_app(stateless_http=True)
        sock = socket.create_server(("127.0.0.1", 0))
        endpoint = f"http://127.0.0.1:{sock.getsockname()[1]}/mcp"
        http = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning"))
        async with app.router.lifespan_context(app):
            serving = asyncio.create_task(http.serve(sockets=[sock]))
            try:
                bound = create_control({"endpoint": endpoint})
                page = await bound.tools.get_traces(since="24h", limit=200)
                assert page == {"session": "dev-1", "traces": [TRACE]}
                assert sent == [{"since": "24h", "limit": 200}]
                with pytest.raises(ToolCallError, match="control.get_traces.*no calls"):
                    await bound.tools.get_traces(status="error")
                with pytest.raises(ValueError, match="structured output"):
                    await bound.tools.get_traces(tool="bad")

                monkeypatch.delenv("CONTROL_MCP_URL", raising=False)
                with pytest.raises(RuntimeError, match="CONTROL_MCP_URL"):
                    await control.tools.get_traces()
                monkeypatch.setenv("CONTROL_MCP_URL", endpoint)
                assert await control.tools.get_traces() == page
            finally:
                http.should_exit = True
                await serving

    asyncio.run(asyncio.wait_for(exercise(), timeout=15))
