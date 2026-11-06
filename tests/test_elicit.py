import asyncio
from typing import Annotated

import httpx2
import pytest
from mcp import MCPError
from mcp.server import MCPServer
from mcp.server.mcpserver import Elicit, Resolve
from mcp.types import MISSING_REQUIRED_CLIENT_CAPABILITY, CallToolResult, ElicitResult
from pydantic import BaseModel

from holon.client import Apps, ToolCallError

QUESTION = "Which size should I ship?"
CONTRACT = {
    "tools": [
        {
            "name": "ship",
            "inputSchema": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            },
            "outputSchema": {
                "type": "object",
                "properties": {"shipped": {"type": "string"}},
                "required": ["shipped"],
            },
        }
    ]
}


class Choice(BaseModel):
    size: str


class Shipped(BaseModel):
    shipped: str


def ask_size() -> Elicit[Choice]:
    return Elicit(QUESTION, Choice)


def exercise(body, elicit=None, contract=CONTRACT):
    """Run body against a real server, through a binding that may answer its question."""

    async def run():
        server = MCPServer("orders")

        @server.tool()
        async def ship(
            id: str, choice: Annotated[Choice, Resolve(ask_size)]
        ) -> Shipped:
            return Shipped(shipped=choice.size)

        app = server.streamable_http_app(stateless_http=True)

        def http_client():
            return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app))

        apps = Apps(
            {"name": "checkout", "version": "1", "requires": {}},
            {"orders": contract},
            {
                "orders": {
                    "endpoint": "http://127.0.0.1:8000/mcp",
                    "http_client": http_client,
                    "elicit": elicit,
                }
            },
        )
        async with app.router.lifespan_context(app):
            await body(apps.orders)

    asyncio.run(asyncio.wait_for(run(), timeout=15))


def test_elicit_answers_in_place_and_the_tool_returns_typed_output():
    asked = []

    async def elicit(context, params):
        asked.append(params.message)
        return ElicitResult(action="accept", content={"size": "large"})

    async def body(orders):
        assert await orders.tools.ship(id="A-1") == {"shipped": "large"}
        whole = await orders.call_tool("ship", id="A-1")
        assert isinstance(whole, CallToolResult)
        assert whole.structured_content == {"shipped": "large"}

    exercise(body, elicit)
    assert asked == [QUESTION, QUESTION]


def test_accepted_output_is_still_validated():
    async def elicit(context, params):
        return ElicitResult(action="accept", content={"size": "large"})

    tool = CONTRACT["tools"][0]
    wrong = {"type": "object", "required": ["total"]}

    async def body(orders):
        with pytest.raises(ValueError, match="structured output"):
            await orders.tools.ship(id="A-1")

    exercise(body, elicit, {"tools": [{**tool, "outputSchema": wrong}]})


def test_a_declined_elicitation_reaches_the_tool():
    async def elicit(context, params):
        return ElicitResult(action="decline")

    async def body(orders):
        with pytest.raises(ToolCallError, match="elicitation was decline"):
            await orders.tools.ship(id="A-1")

    exercise(body, elicit)


def test_a_client_without_the_capability_is_told_how_to_declare_it():
    async def body(orders):
        with pytest.raises(MCPError) as error:
            await orders.tools.ship(id="A-1")
        assert error.value.code == MISSING_REQUIRED_CLIENT_CAPABILITY
        assert str(error.value).startswith("orders.ship: ")
        assert str(error.value).endswith("Set elicit on the binding.")
        assert error.value.data["requiredCapabilities"]["elicitation"]["form"] == {}

    exercise(body)
