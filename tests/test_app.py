import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError, UnexpectedToolError
from mcp.server.subscriptions import InMemorySubscriptionBus

from holon.app import App


def test_native_contract_and_post_success_invalidation():
    async def run():
        bus = InMemorySubscriptionBus()
        events = []
        stop = bus.subscribe(events.append)
        app = App("counter", subscriptions=bus)
        calls = []

        @app.tool(changes=["counter://value"])
        def add(value: int) -> dict[str, int]:
            calls.append(value)
            return {"value": value + 1}

        @app.tool(changes=["counter://value"])
        def fail() -> dict[str, int]:
            raise ValueError("Write rejected")

        catalog = await app.list_tools()
        assert catalog[0].input_schema["properties"]["value"]["type"] == "integer"
        assert catalog[0].meta["sh.holon/changes"] == ["counter://value"]
        result = await app.call_tool("add", {"value": 2})
        assert result.structured_content == {"value": 3}
        assert [event.uri for event in events] == ["counter://value"]
        with pytest.raises(ToolError):
            await app.call_tool("add", {"value": "invalid"})
        with pytest.raises(UnexpectedToolError):
            await app.call_tool("fail", {})
        assert calls == [2]
        assert len(events) == 1
        stop()

    asyncio.run(run())


def test_tool_meta_passes_through_and_notification_failure_keeps_committed_result():
    async def run():
        class UnavailableBus(InMemorySubscriptionBus):
            async def publish(self, event):
                raise RuntimeError("Fan-out unavailable")

        app = App("counter", subscriptions=UnavailableBus())

        @app.tool(meta={"ui": {"resourceUri": "ui://counter/index.html"}})
        def show() -> dict[str, int]:
            return {"value": 0}

        @app.tool(changes=["counter://value"])
        def add() -> dict[str, int]:
            return {"value": 1}

        result = await app.call_tool("add", {})
        assert result.structured_content == {"value": 1}
        assert [tool.meta for tool in await app.list_tools()] == [
            {"ui": {"resourceUri": "ui://counter/index.html"}},
            {"sh.holon/changes": ["counter://value"]},
        ]

    asyncio.run(run())
