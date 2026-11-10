from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import NotRequired, TypedDict

import httpx2


class Binding(TypedDict):
    """Connection settings for an MCP dependency.

    Attributes:
        endpoint: Streamable HTTP MCP URL.
        http_client: Factory returning a fresh ``httpx2.AsyncClient`` for each
            call. Configure authentication headers or a custom transport here.
            The SDK closes the returned client when the call finishes.
        file_origins: Origins (``https://host[:port]``) the server may send
            file transfers to. When set, no other origin is used, and a listed
            origin may also be plain-http loopback or a private address. When
            unset, any https origin on a public address.
    """

    endpoint: str
    http_client: NotRequired[Callable[[], httpx2.AsyncClient]]
    file_origins: NotRequired[Sequence[str]]
