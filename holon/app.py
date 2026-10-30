"""Function-based apps using the native MCP server and request context."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, TypeVar

from mcp.server.apps import APP_MIME_TYPE
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.subscriptions import (
    InMemorySubscriptionBus,
    ResourceUpdated,
    SubscriptionBus,
)
from mcp.types import CallToolResult, InputRequiredResult

_F = TypeVar("_F", bound=Callable[..., Any])
_log = logging.getLogger(__name__)


class App(MCPServer):
    """An MCPServer with declared resource invalidation and compiled view assets.

    Native tools, resources, authorization middleware and contexts remain available.
    ``views`` is the manifest from ``holon-ui/build``, not application code.
    """

    def __init__(
        self,
        *args: Any,
        views: Path | str | None = None,
        subscriptions: SubscriptionBus | None = None,
        **kwargs: Any,
    ) -> None:
        self._app_bus = (
            subscriptions if subscriptions is not None else InMemorySubscriptionBus()
        )
        super().__init__(*args, subscriptions=self._app_bus, **kwargs)
        self._changes: dict[str, tuple[str, ...]] = {}
        self._views: dict[str, dict[str, Any]] = {}
        if views is not None:
            manifest = Path(views)
            for item in json.loads(manifest.read_text()):
                if not item["uri"].startswith("ui://") or item["tool"] in self._views:
                    raise ValueError("Invalid or duplicate view manifest entry")
                self._views[item["tool"]] = item
                self._add_view(item, manifest.parent)

    def _add_view(self, item: dict[str, Any], directory: Path) -> None:
        def read() -> str:
            return (directory / str(item["file"])).read_text()

        self.resource(
            item["uri"],
            name=item["tool"],
            mime_type=APP_MIME_TYPE,
            meta={
                "ui": {
                    "csp": {"connectDomains": [], "resourceDomains": []},
                    **item.get("ui", {}),
                }
            },
        )(read)

    def tool(
        self, *args: Any, changes: Iterable[str] = (), **kwargs: Any
    ) -> Callable[[_F], _F]:
        """Register a native typed function, invalidating ``changes`` after success.

        The URI list is metadata, not an authorization or transaction declaration.
        Native tool arguments and structured results retain MCP SDK validation.
        """
        uris = tuple(changes)
        if any(not isinstance(uri, str) or not uri for uri in uris):
            raise ValueError("Changed resources require nonempty URI strings")

        def register(fn: _F) -> _F:
            name = kwargs.get("name") or (args[0] if args else None) or fn.__name__
            metadata = dict(kwargs.get("meta") or {})
            metadata["sh.holon/changes"] = list(uris)
            if view := self._views.get(name):
                metadata["ui"] = {**metadata.get("ui", {}), "resourceUri": view["uri"]}
            options = {**kwargs, "meta": metadata}
            super(App, self).tool(*args, **options)(fn)
            self._changes[name] = uris
            return fn

        return register

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
        context: Context[Any, Any] | None = None,
    ) -> CallToolResult | InputRequiredResult:
        result = await super().call_tool(name, arguments, context)
        if isinstance(result, CallToolResult) and not result.is_error:
            for uri in self._changes.get(name, ()):
                try:
                    await self._app_bus.publish(ResourceUpdated(uri=uri))
                except Exception:  # noqa: BLE001 -- custom buses must not change committed results
                    # An already committed mutation must not appear to fail because fan-out failed.
                    _log.exception("Resource notification failed for %s", uri)
        return result
