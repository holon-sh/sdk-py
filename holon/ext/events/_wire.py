"""Wire shapes and the codes both sides share, from the draft events extension."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeAlias, TypeVar

EVENTS_EXTENSION = "io.modelcontextprotocol/events"
"""The identifier a server declares the extension under, in ``capabilities.extensions``."""
# The sketch's general-purpose codes, provisional until the specification allocates them.
NOT_FOUND = -32023
FORBIDDEN = -32024
RESOURCE_EXHAUSTED = -32025
UNSUPPORTED = -32026

D = TypeVar("D")
Delivery: TypeAlias = Literal["poll", "push"]


@dataclass(frozen=True)
class Event(Generic[D]):
    """One occurrence of an event type.

    ``match`` and ``transform`` are given these, and a client's subscription yields them.
    """

    data: D
    event_id: str | None = None
    """The source's own id, so that a client drops an event it has seen. Made up when not given."""
    timestamp: str | None = None
    """When it happened, in RFC 3339."""
    cursor: str | None = None
    """The position just after this event, where a client resumes."""
    meta: dict[str, Any] | None = None
    name: str | None = None
    """The event type, as the occurrence on the wire names it."""
