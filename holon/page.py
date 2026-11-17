"""An app's page, as the view an MCP Apps host shows with a tool's result."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Final, TypeVar

import anyio
from mcp.server import MCPServer

__all__ = ["Page", "create_page"]

# MCP Apps names a view with a ui:// URI and serves it as this type. An app has one page, so the URI names no app and
# survives renaming one.
# https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx
_URI: Final = "ui://page"
_MIME_TYPE: Final = "text/html;profile=mcp-app"

_Lifespan = TypeVar("_Lifespan")


class Page:
    """An app's page, as the view an MCP Apps host shows with a tool's result."""

    def __init__(self, file: Path, ui: Mapping[str, object]) -> None:
        self._file = anyio.Path(file)
        self._ui = dict(ui)

    @property
    def meta(self) -> dict[str, dict[str, str]]:
        """A tool's ``_meta``, so that hosts show the page with what the tool returns."""
        return {"ui": {"resourceUri": _URI}}

    def serve(self, server: MCPServer[_Lifespan]) -> None:
        """Offers the page to a server's clients as its view."""

        async def read() -> str:
            return await self._file.read_text(encoding="utf-8")

        server.resource(_URI, name="Page", mime_type=_MIME_TYPE, meta={"ui": self._ui})(
            read
        )


def create_page(file: Path, ui: Mapping[str, object] | None = None) -> Page:
    """Makes an app's page a view: ``file`` is the page built as one HTML file, and ``ui`` the view's ``_meta.ui``, such
    as its ``csp``.

    ```python
    page = create_page(Path(__file__).parent / "dist/mcp/index.html")
    server.tool(meta=page.meta)(open)
    page.serve(server)
    ```
    """
    return Page(file, ui or {})
