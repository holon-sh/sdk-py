import asyncio

from mcp import Client
from mcp.server import MCPServer
from mcp.server.apps import APP_MIME_TYPE

from holon.view import app_view


def test_app_view_serves_one_build_for_the_life_of_the_process(tmp_path):
    page = tmp_path / "index.html"
    page.write_text("<html>one</html>")
    server = MCPServer("site")
    server.add_resource(
        app_view(
            "ui://site/index.html",
            "Site",
            page,
            connect_domains=["https://api.example"],
        )
    )

    async def exercise():
        async with Client(server, mode="2026-07-28") as client:
            listed = await client.list_resources()
            resource = listed.resources[0]
            assert (str(resource.uri), resource.name, resource.mime_type) == (
                "ui://site/index.html",
                "Site",
                APP_MIME_TYPE,
            )
            assert resource.meta == {
                "ui": {
                    "csp": {
                        "connectDomains": ["https://api.example"],
                        "resourceDomains": [],
                    }
                }
            }
            first = await client.read_resource("ui://site/index.html")
            assert first.contents[0].text == "<html>one</html>"
            assert first.contents[0].mime_type == APP_MIME_TYPE
            page.write_text("<html>two</html>")
            second = await client.read_resource("ui://site/index.html")
            assert second.contents[0].text == "<html>one</html>"

    asyncio.run(exercise())
