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
    page = Path(file)
    csp = {
        "connectDomains": connect_domains or [],
        "resourceDomains": resource_domains or [],
    }
    built: list[str] = []

    def read() -> str:
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
