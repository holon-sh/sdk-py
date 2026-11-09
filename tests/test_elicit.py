import asyncio
from typing import Annotated

import httpx2
import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver import Elicit, Resolve
from mcp.types import (
    CallToolResult,
    ElicitRequest,
    ElicitRequestFormParams,
    ElicitRequestURLParams,
    ElicitResult,
    InputRequiredResult,
)
from pydantic import BaseModel

from holon.client import Apps, ToolCallError

QUESTION = "Which size should I ship?"
ID_ONLY = {
    "type": "object",
    "properties": {"id": {"type": "string"}},
    "required": ["id"],
}
CONTRACT = {
    "tools": [
        {
            "name": "ship",
            "inputSchema": ID_ONLY,
            "outputSchema": {
                "type": "object",
                "properties": {"shipped": {"type": "string"}},
                "required": ["shipped"],
            },
        },
        {"name": "nag", "inputSchema": ID_ONLY},
    ]
}


class Choice(BaseModel):
    size: str


class Shipped(BaseModel):
    shipped: str


def ask_size() -> Elicit[Choice]:
    return Elicit(QUESTION, Choice)


def exercise(body):
    async def run():
        server = MCPServer("orders")

        @server.tool()
        async def ship(
            id: str, choice: Annotated[Choice, Resolve(ask_size)]
        ) -> Shipped:
            return Shipped(shipped=choice.size)

        @server.tool()
        async def nag(id: str) -> CallToolResult | InputRequiredResult:
            params = ElicitRequestFormParams(
                message=QUESTION,
                requested_schema={"type": "object", "properties": {}},
            )
            return InputRequiredResult(
                input_requests={"q": ElicitRequest(params=params)}
            )

        app = server.streamable_http_app(stateless_http=True)

        def http_client():
            return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app))

        apps = Apps(
            {"name": "checkout", "version": "1", "requires": {}},
            {"orders": CONTRACT},
            {
                "orders": {
                    "endpoint": "http://127.0.0.1:8000/mcp",
                    "http_client": http_client,
                }
            },
        )
        async with app.router.lifespan_context(app):
            await body(apps.orders)

    asyncio.run(asyncio.wait_for(run(), timeout=15))


def test_elicit_answers_in_place_and_the_call_returns_typed_output():
    asked = []

    async def elicit(params):
        asked.append(params.message)
        return ElicitResult(action="accept", content={"size": "large"})

    async def body(orders):
        assert await orders.tools.ship({"elicit": elicit}, id="A-1") == {
            "shipped": "large"
        }

    exercise(body)
    assert asked == [QUESTION]


def test_a_server_that_keeps_asking_stops_after_ten_answers():
    answers = []

    async def elicit(params):
        answers.append(params)
        return ElicitResult(action="decline")

    async def body(orders):
        with pytest.raises(ToolCallError, match=QUESTION):
            await orders.tools.nag({"elicit": elicit}, id="A-1")

    exercise(body)
    assert len(answers) == 10


def test_without_elicit_the_question_reaches_the_caller_who_continues():
    async def body(orders):
        with pytest.raises(ToolCallError) as error:
            await orders.tools.ship(id="A-1")
        assert str(error.value) == f"orders.ship: additional input required: {QUESTION}"
        result = error.value.result
        [key] = result.input_requests
        answer = ElicitResult(action="accept", content={"size": "small"})
        continued = await orders.call_tool(
            "ship",
            {"input_responses": {key: answer}, "request_state": result.request_state},
            id="A-1",
        )
        assert continued.structured_content == {"shipped": "small"}

    exercise(body)


def test_an_error_while_answering_reaches_the_caller_unwrapped():
    async def elicit(params):
        raise LookupError("no answer")

    async def body(orders):
        with pytest.raises(LookupError, match="no answer"):
            await orders.tools.ship({"elicit": elicit}, id="A-1")

    exercise(body)


def test_a_url_question_names_its_url():
    params = ElicitRequestURLParams(
        message="Connect your account.",
        url="https://orders.example/connect",
        elicitation_id="link",
    )
    result = InputRequiredResult(input_requests={"link": ElicitRequest(params=params)})
    assert str(ToolCallError("orders", "ship", result)) == (
        "orders.ship: additional input required: "
        "Connect your account. https://orders.example/connect"
    )
