"""Shared by the events server and client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeAlias, TypeVar

EVENTS_EXTENSION = "io.modelcontextprotocol/events"
# https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md#error-codes
NOT_FOUND = -32023
FORBIDDEN = -32024
RESOURCE_EXHAUSTED = -32025
UNSUPPORTED = -32026

D = TypeVar("D")
Delivery: TypeAlias = Literal["poll", "push"]


@dataclass(frozen=True)
class Event(Generic[D]):
    """One occurrence of an event type."""

    data: D
    event_id: str | None = None
    """Identifies the occurrence, so a client drops duplicates."""
    timestamp: str | None = None
    """When it happened, in RFC 3339."""
    cursor: str | None = None
    """Where a client resumes after this event."""
    meta: dict[str, Any] | None = None
    name: str | None = None
    """The event type."""
