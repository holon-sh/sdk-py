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


def test_native_views_and_notification_failure_do_not_change_committed_result(tmp_path):
    async def run():
        class UnavailableBus(InMemorySubscriptionBus):
            async def publish(self, event):
                raise RuntimeError("Fan-out unavailable")

        (tmp_path / "index.html").write_text("<!doctype html><p>Counter</p>")
        (tmp_path / "views.json").write_text(
            '[{"tool":"add","uri":"ui://counter/app.html","file":"index.html"}]'
        )
        app = App(
            "counter", views=tmp_path / "views.json", subscriptions=UnavailableBus()
        )

        @app.tool(changes=["counter://value"])
        def add() -> dict[str, int]:
            return {"value": 1}

        result = await app.call_tool("add", {})
        assert result.structured_content == {"value": 1}
        assert (await app.list_tools())[0].meta["ui"][
            "resourceUri"
        ] == "ui://counter/app.html"
        assert (
            "Counter" in (await app.read_resource("ui://counter/app.html"))[0].content
        )

    asyncio.run(run())
