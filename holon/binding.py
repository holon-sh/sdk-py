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
    """

    endpoint: str
    http_client: NotRequired[Callable[[], httpx2.AsyncClient]]
