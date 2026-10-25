from __future__ import annotations

from collections.abc import Callable
from typing import NotRequired, TypedDict

import httpx2


class Binding(TypedDict):
    """Connection settings for an MCP dependency.

    Attributes:
        endpoint: Streamable HTTP MCP URL.
        http_client: Factory returning a fresh ``httpx2.AsyncClient`` for each
            call. Configure authentication headers or a custom transport here.
            The SDK closes the returned client when the call finishes.
        scoped: Compatibility setting. Python always opens and closes a session
            per call, regardless of this value.
    """

    endpoint: str
    http_client: NotRequired[Callable[[], httpx2.AsyncClient]]
    # Python clients open one session per call, so every binding already behaves as scoped.
    scoped: NotRequired[bool]


_resolver: Callable[[str], Binding | None] | None = None


def set_default_binding(resolve: Callable[[str], Binding | None]) -> None:
    """Register the default binding resolver for app clients.

    Args:
        resolve: Process-wide callback receiving the dependency name. Return a
            binding or ``None`` to use the environment fallback. Explicit
            bindings take precedence. Register during application startup.
    """
    global _resolver
    _resolver = resolve


def resolve_default_binding(app: str) -> Binding | None:
    """Return the registered resolver's binding for an app.

    Args:
        app: Dependency name passed unchanged to the resolver.

    Returns:
        The resolved binding, or ``None`` if none is available.
    """
    return _resolver(app) if _resolver is not None else None
