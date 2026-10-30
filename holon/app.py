"""Function-based apps using the native MCP server and request context."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.subscriptions import ResourceUpdated
from mcp.types import CallToolResult, InputRequiredResult

_F = TypeVar("_F", bound=Callable[..., Any])
_log = logging.getLogger(__name__)


class App(MCPServer):
    """An MCPServer with declared resource invalidation.

    Native tools, resources, authorization middleware and contexts remain available.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._changes: dict[str, tuple[str, ...]] = {}

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
        if uris:
            kwargs["meta"] = {
                **(kwargs.get("meta") or {}),
                "sh.holon/changes": list(uris),
            }
        decorate = super().tool(*args, **kwargs)

        def register(fn: _F) -> _F:
            decorate(fn)
            name = kwargs.get("name") or (args[0] if args else None) or fn.__name__
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
                    await self._subscriptions.publish(ResourceUpdated(uri=uri))
                except Exception:  # noqa: BLE001 -- custom buses must not change committed results
                    # An already committed mutation must not appear to fail because fan-out failed.
                    _log.exception("Resource notification failed for %s", uri)
        return result
