import asyncio

from mcp import Client
from mcp.server import MCPServer

from holon.page import create_page


def test_a_page_is_the_view_a_tool_names_one_html_file_of_mcp_apps_type_with_the_views_meta(
    tmp_path,
):
    file = tmp_path / "index.html"
    file.write_text("<p>page</p>")
    page = create_page(file, {"csp": {"resourceDomains": ["https://cdn.example"]}})
    server = MCPServer("app")

    @server.tool(description="Opens the page", meta=page.meta)
    def open() -> None:
        return None

    page.serve(server)

    async def host():
        # An MCP Apps host is an MCP client.
        async with Client(server) as client:
            [tool] = (await client.list_tools()).tools
            uri = tool.meta["ui"]["resourceUri"]
            read = await client.read_resource(uri)
            return uri, [
                content.model_dump(by_alias=True, exclude_none=True)
                for content in read.contents
            ]

    uri, contents = asyncio.run(asyncio.wait_for(host(), timeout=20))
    assert contents == [
        {
            "uri": uri,
            "mimeType": "text/html;profile=mcp-app",
            "text": "<p>page</p>",
            "_meta": {"ui": {"csp": {"resourceDomains": ["https://cdn.example"]}}},
        }
    ]
