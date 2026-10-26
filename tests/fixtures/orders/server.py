"""Static tool catalog for generated-client typing regressions."""

from mcp.server import Server
from mcp.types import Tool

server = Server("orders")

READ_INPUT = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "limit": {"type": "integer"},
        "name": {"type": "string"},
        "timeout": {"type": "string"},
        "options": {"type": "string"},
        "self": {"type": "string"},
        "arguments": {"type": "string"},
        "class": {"type": "string"},
        "order-id": {"type": "string"},
    },
    "required": ["id"],
    "additionalProperties": False,
}
READ_OUTPUT = {
    "type": "object",
    "properties": {"total": {"type": "number"}},
    "required": ["total"],
}


@server.list_tools()
async def tools() -> list[Tool]:
    return [
        Tool(name="read", input_schema=READ_INPUT, output_schema=READ_OUTPUT),
        Tool(name="get-order", input_schema=READ_INPUT, output_schema=READ_OUTPUT),
        Tool(name="ping", input_schema={"type": "object", "additionalProperties": False}),
        Tool(
            name="counts",
            input_schema={"type": "object", "additionalProperties": {"type": "integer"}},
        ),
        Tool(
            name="find",
            input_schema={
                "type": "object",
                "anyOf": [
                    READ_INPUT,
                    READ_INPUT,
                    {
                        "type": "object",
                        "properties": {"email": {"type": "string"}},
                        "required": ["email"],
                        "additionalProperties": False,
                    },
                ],
            },
        ),
    ]
