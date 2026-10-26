from typing import TypedDict

from mcp.server import MCPServer

server = MCPServer("orders")


class Order(TypedDict):
    total: float


@server.tool()
def read(id: str) -> Order:
    return {"total": 42.0}
