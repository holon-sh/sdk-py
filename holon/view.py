from __future__ import annotations

from pathlib import Path

from mcp.server.apps import APP_MIME_TYPE
from mcp.server.mcpserver.resources import FunctionResource


def app_view(
    uri: str,
    name: str,
    file: Path | str,
    *,
    connect_domains: list[str] | None = None,
    resource_domains: list[str] | None = None,
) -> FunctionResource:
    """Create a server resource serving a built MCP App HTML file.

    Args:
        uri: Resource URI, normally starting with ``ui://``.
        name: Human-readable resource name.
        file: HTML file to read lazily on the first resource request.
        connect_domains: Domains allowed for network connections by the view.
        resource_domains: Domains allowed for external view assets.

    Returns:
        A ``FunctionResource`` ready for the MCP server's resource manager.
        Successful reads are cached for the lifetime of this resource instance.

    Raises:
        OSError: On a resource read if the HTML file cannot be read.

    Example:
        ``resource = app_view("ui://app/index.html", "App", "dist/index.html")``
    """
    page = Path(file)
    csp = {
        "connectDomains": connect_domains or [],
        "resourceDomains": resource_domains or [],
    }
    built: list[str] = []

    def read() -> str:
        # Concurrent first reads must all return the first cached build.
        if not built:
            built.append(page.read_text())
        return built[0]

    return FunctionResource.from_function(
        read,
        uri,
        name=name,
        mime_type=APP_MIME_TYPE,
        meta={"ui": {"csp": csp}},
    )
