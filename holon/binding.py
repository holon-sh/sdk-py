from __future__ import annotations

from collections.abc import Callable
from typing import NotRequired, TypedDict

import httpx2


class Binding(TypedDict):
    endpoint: str
    http_client: NotRequired[Callable[[], httpx2.AsyncClient]]
    # Python clients open one session per call, so every binding already behaves as scoped.
    scoped: NotRequired[bool]


_resolver: Callable[[str], Binding | None] | None = None


def set_default_binding(resolve: Callable[[str], Binding | None]) -> None:
    """Resolve bindings for app clients that were not bound with ``create_apps``.

    Without a resolver, or when it returns ``None``, a client falls back to its
    ``<NAME>_MCP_URL`` environment variable.
    """
    global _resolver
    _resolver = resolve


def resolve_default_binding(app: str) -> Binding | None:
    return _resolver(app) if _resolver is not None else None
