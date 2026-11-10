"""MCP Events, the Triggers & Events Working Group's draft extension, in its poll and push delivery modes.

https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md
"""

from __future__ import annotations

import asyncio
import bisect
import hashlib
import inspect
import json
import logging
import math
import secrets
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Generic, Literal, TypeAlias, TypedDict, TypeVar, cast

import anyio
from mcp.server.context import HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.server.lowlevel.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecurityMiddleware
from mcp.shared.exceptions import MCPError
from mcp.types import ErrorData, Notification, RequestParams, ServerNotification
from pydantic import Field, StrictInt, StrictStr, TypeAdapter, ValidationError
from pydantic_core import to_json, to_jsonable_python
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from holon.context import RequestContext, get_context

__all__ = [
    "EVENTS_EXTENSION",
    "CheckResult",
    "Event",
    "EventType",
    "Events",
    "Keep",
]

logger = logging.getLogger(__name__)

EVENTS_EXTENSION = "io.modelcontextprotocol/events"
"""The identifier a server declares the extension under, in ``capabilities.extensions``."""
SUBSCRIPTION_ID = "io.modelcontextprotocol/subscriptionId"
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
# The sketch's general-purpose codes, provisional until the specification allocates them.
NOT_FOUND = -32023
FORBIDDEN = -32024
RESOURCE_EXHAUSTED = -32025
UNSUPPORTED = -32026

A = TypeVar("A")
D = TypeVar("D")
Delivery: TypeAlias = Literal["poll", "push"]
Teardown: TypeAlias = Callable[[], object]


class Keep(TypedDict, total=False):
    """What an event type without a check function keeps of what it emits, for polls and reconnecting streams."""

    events: int
    """How many events. Ten thousand unless given."""
    age: float
    """For how long, in seconds. Ten minutes unless given."""
    bytes: int
    """How many bytes they take as JSON. The newest is kept whatever it takes. 16 MiB unless given."""


@dataclass(frozen=True)
class Event(Generic[D]):
    """One occurrence of an event type.

    A check function returns these, ``match`` and ``transform`` are given them,
    and a client's subscription yields them.
    """

    data: D
    event_id: str | None = None
    """The source's own id, so that a client drops an event it has seen. Made up when not given."""
    timestamp: datetime | str | None = None
    """When it happened. Now when not given."""
    cursor: str | None = None
    """The position just after this event, so that a check's batch can be cut short, and where a client resumes."""
    meta: dict[str, Any] | None = None


@dataclass(frozen=True)
class CheckResult(Generic[D]):
    """What happened after a cursor, as a check function finds it."""

    events: Sequence[Event[D]]
    """Empty when the cursor was None, which asks only for the current position. A source without positions
    answers with a None cursor, is asked with None each time, and answers each time with what is new."""
    cursor: str | None
    """The position after these events, or None when the source cannot be resumed from a position."""
    truncated: bool = False
    """Events between the cursor and these were skipped, for instance because the source no longer keeps them."""
    has_more: bool = False
    """More events are ready at once after this batch."""


Check: TypeAlias = Callable[
    [RequestContext, A, str | None], CheckResult[D] | Awaitable[CheckResult[D]]
]
Match: TypeAlias = Callable[[RequestContext, Event[D], A], bool]
Transform: TypeAlias = Callable[[RequestContext, Event[D], A], object]
Authorize: TypeAlias = Callable[[RequestContext, A], bool | Awaitable[bool]]
OnSubscribe: TypeAlias = Callable[
    [RequestContext, A, str], Teardown | None | Awaitable[Teardown | None]
]


class _ListParams(RequestParams):
    cursor: StrictStr | None = None


class _SubscribeParams(RequestParams):
    name: StrictStr
    arguments: dict[str, Any] | None = None
    cursor: StrictStr | None = None
    max_age_ms: StrictInt | None = Field(default=None, ge=0)
    max_events: StrictInt | None = Field(default=None, ge=1)


_FORBIDDEN = ErrorData(code=FORBIDDEN, message="Forbidden")
_REMOVED = ErrorData(code=NOT_FOUND, message="NotFound", data={"kind": "event"})


def _error(error: ErrorData) -> dict[str, Any]:
    return error.model_dump(by_alias=True, exclude_none=True, mode="json")


# What a client is told of a failure: its own error when it raised an MCP one. Anything else is the app's to log.
def _described(error: BaseException) -> dict[str, Any]:
    if isinstance(error, MCPError):
        return _error(error.error)
    logger.exception("An event source failed", exc_info=error)
    return {"code": INTERNAL_ERROR, "message": "InternalError"}


async def _settle(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _iso(value: datetime | str | None) -> str:
    if isinstance(value, str):
        return value
    moment = (value or datetime.now(UTC)).astimezone(UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _seconds(timestamp: str) -> float:
    try:
        return datetime.fromisoformat(timestamp).timestamp()
    except ValueError:
        return math.inf


# Who asked, as Holon admitted them: the user, the client acting for them, and the agent when one acts for the
# user. A caller without credentials is nobody in particular.
def _caller(context: RequestContext) -> str | None:
    principal = context.principal
    if principal is None:
        return None
    who = [principal.issuer, principal.subject, principal.client_id, principal.actor]
    return json.dumps(who, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class _Request(Generic[A]):
    """A subscription as a request names it. Its id is the same for the same caller, event type and arguments,
    so it survives reconnects and poll leases."""

    id: str
    context: RequestContext
    caller: str | None
    arguments: A


@dataclass(eq=False)
class _Kept:
    event: Event[Any]
    size: int
    target: str | None
    replaces: str | None
    sequence: int = 0
    at: float = 0.0


@dataclass
class _Read:
    events: list[dict[str, Any]]
    start: str | None
    cursor: str | None
    truncated: bool = False
    has_more: bool = False


@dataclass(eq=False)
class _Hold:
    """One delivery mode holding a subscription: a stream, or a poll lease."""

    type: EventType[Any, Any]
    request: _Request[Any]
    end: Callable[[ErrorData], None]
    entry: _Entry | None = None
    closed: bool = False


@dataclass(eq=False)
class _Entry:
    """A subscription held by one or more delivery modes, set up once by the type's on_subscribe."""

    ready: asyncio.Future[Any]
    holds: set[_Hold] = field(default_factory=set)


@dataclass(eq=False)
class _Lease:
    type: EventType[Any, Any]
    opening: asyncio.Future[_Hold] | None = None
    hold: _Hold | None = None
    ended: ErrorData | None = None
    timer: asyncio.TimerHandle | None = None


class EventType(Generic[A, D]):
    """An event type an app declared with :meth:`Events.event`. Emit its occurrences with :meth:`emit`."""

    def __init__(
        self,
        events: Events,
        name: str,
        order: int,
        *,
        input_schema: TypeAdapter[A],
        payload_schema: TypeAdapter[D],
        description: str | None,
        meta: dict[str, Any] | None,
        match: Match[D, A] | None,
        transform: Transform[D, A] | None,
        key: Callable[[D], str] | None,
        check: Check[A, D] | None,
        interval: float,
        delivery: Sequence[Delivery],
        authorize: Authorize[A] | None,
        on_subscribe: OnSubscribe[A] | None,
    ) -> None:
        self.name = name
        self._events = events
        self._order = order
        self._input = input_schema
        self._match = match
        self._transform = transform
        self._key = key
        self._check = check
        self._interval = interval
        self._delivery = list(delivery)
        self._authorize = authorize
        self._on_subscribe = on_subscribe
        self._descriptor: dict[str, Any] = {
            "name": name,
            **({"description": description} if description else {}),
            "delivery": self._delivery,
            "inputSchema": input_schema.json_schema(),
            "payloadSchema": payload_schema.json_schema(),
            **({"_meta": meta} if meta else {}),
        }
        self._sequence = 0
        self._dropped = 0
        self._kept: list[_Kept] = []
        self._bytes = 0
        self._latest: dict[str, _Kept] = {}
        self._sinks: set[Callable[[_Kept], None]] = set()
        self._subscriptions: dict[str, _Entry] = {}

    def emit(
        self,
        data: D,
        *,
        event_id: str | None = None,
        timestamp: datetime | str | None = None,
        subscription: str | None = None,
    ) -> None:
        """Emit an occurrence to the subscriptions it matches.

        Give the source's own ``event_id`` when it has one, so that a client drops an occurrence it has seen. With
        ``subscription``, the occurrence goes to that subscription alone, whatever its arguments; it is kept like any
        other, so a subscription that reconnects or polls again from its cursor still has it, or is told of a gap. A
        type with a check function pushes what it emits to open streams at once and does not keep it: polls and
        reconnecting streams learn of it from the check function.

        Emitting is synchronous; call it on the event loop's thread. Occurrences emitted one after another in the
        same synchronous run reach each subscription in that order, with no other occurrence of this type between
        them, whether polled or pushed. A subscription skips part of such a group only when it is told of a gap.
        """
        event = Event(data, event_id or str(uuid.uuid4()), _iso(timestamp))
        size = len(to_json(_occurrence(self.name, event, data)))
        replaces = json.dumps([subscription, self._key(data)]) if self._key else None
        kept = _Kept(event, size, subscription, replaces)
        if self._check is None:
            self._keep(kept)
        for sink in list(self._sinks):
            sink(kept)

    def cursor(self) -> str:
        """The position after the last occurrence emitted.

        Return it with the state a tool reads, in the same synchronous run, and a client that subscribes from it
        hears every change after that state and none before. A type with a check function has no such position
        here, since its cursor is its source's own, so this raises for it.
        """
        if self._check is not None:
            raise TypeError(
                f"Event {self.name} has a check function, whose cursor is its source's own."
            )
        return self._position()

    def end(
        self, subscription: str | None = None, error: ErrorData | None = None
    ) -> None:
        """End one subscription, or all of this type's. A stream is told ``error`` and closed, and the next poll
        is answered with it. Forbidden unless given."""
        for id in (
            [subscription] if subscription is not None else list(self._subscriptions)
        ):
            for hold in (
                list(entry.holds) if (entry := self._subscriptions.get(id)) else []
            ):
                hold.end(error or _FORBIDDEN)

    def remove(self) -> None:
        """Stop offering this event type: its subscriptions end with NotFound."""
        if self._events._types.get(self.name) is self:
            del self._events._types[self.name]
            self.end(error=_REMOVED)

    def _position(self, sequence: int | None = None) -> str:
        return (
            f"{self._events._epoch}.{self._sequence if sequence is None else sequence}"
        )

    def _keep(self, kept: _Kept) -> None:
        events = self._events
        self._sequence += 1
        kept.sequence, kept.at = self._sequence, time.time()
        # An event with a key replaces the one before it with the same key, for the same subscriptions.
        if kept.replaces is not None:
            if older := self._latest.get(kept.replaces):
                self._kept.pop(
                    bisect.bisect_left(
                        self._kept, older.sequence, key=lambda each: each.sequence
                    )
                )
                self._bytes -= older.size
            self._latest[kept.replaces] = kept
        self._kept.append(kept)
        self._bytes += kept.size
        # The newest is kept whatever it weighs, since streams read it from here.
        while len(self._kept) > 1 and (
            len(self._kept) > events._keep_events
            or self._bytes > events._keep_bytes
            or self._kept[0].at < kept.at - events._keep_age
        ):
            oldest = self._kept.pop(0)
            self._dropped = oldest.sequence
            self._bytes -= oldest.size
            if (
                oldest.replaces is not None
                and self._latest.get(oldest.replaces) is oldest
            ):
                del self._latest[oldest.replaces]

    # The occurrence a subscription receives, or None when it is not for it. An event emitted to one subscription
    # is for that one alone, and its arguments are not asked.
    def _shape(self, kept: _Kept, hold: _Hold) -> dict[str, Any] | None:
        request = hold.request
        if kept.target is not None:
            if kept.target != request.id:
                return None
        elif self._match is not None and not self._match(
            request.context, kept.event, request.arguments
        ):
            return None
        data = (
            self._transform(request.context, kept.event, request.arguments)
            if self._transform
            else kept.event.data
        )
        occurrence = _occurrence(self.name, kept.event, data)
        if kept.sequence:
            occurrence["cursor"] = self._position(kept.sequence)
        return occurrence

    # What a subscription is owed after a cursor from the events kept: the ones for it, in order, where delivery
    # starts, and where it now stands. A None cursor starts from now, and so does one from another process. One
    # past the events still kept starts from the oldest kept, and says that events were skipped.
    def _since(
        self, hold: _Hold, cursor: str | None, max_age_ms: int | None, limit: int
    ) -> _Read:
        head = self._position()
        if cursor is None:
            return _Read([], head, head)
        epoch, _, text = cursor.partition(".")
        if (
            epoch != self._events._epoch
            or not text.isdigit()
            or int(text) > self._sequence
        ):
            return _Read([], head, head, truncated=True)
        sequence = int(text)
        floor = -math.inf if max_age_ms is None else time.time() - max_age_ms / 1000
        later = self._kept[
            bisect.bisect_right(self._kept, sequence, key=lambda each: each.sequence) :
        ]
        recent = [kept for kept in later if kept.at >= floor]
        truncated = sequence < self._dropped or len(recent) < len(later)
        reached = recent[0].sequence - 1 if recent else self._sequence
        start = self._position(reached)
        events: list[dict[str, Any]] = []
        for kept in recent:
            if len(events) == limit:
                return _Read(
                    events, start, self._position(reached), truncated, has_more=True
                )
            reached = kept.sequence
            if (occurrence := self._shape(kept, hold)) is not None:
                events.append(occurrence)
        return _Read(events, start, head, truncated)

    # What a subscription is owed after a cursor from the type's check function, whose cursors are the source's
    # own. Each event is placed at its own cursor when it has one, so a batch can be cut short; otherwise at the
    # cursor before the batch, which is safe to resume from. After a gap, delivery starts where the check reached.
    async def _checked(
        self, hold: _Hold, cursor: str | None, max_age_ms: int | None, limit: int
    ) -> _Read:
        assert self._check is not None
        request = hold.request
        result = await _settle(self._check(request.context, request.arguments, cursor))
        after_all = result.cursor
        # A None cursor asks for the current position and nothing before it, except of a source that has no
        # positions: each check of that is asked with None, and answers with what is new.
        if cursor is None and after_all is not None:
            return _Read([], after_all, after_all)
        floor = (
            -math.inf
            if max_age_ms is None or cursor is None
            else time.time() - max_age_ms / 1000
        )
        # Without a position there is nothing to have skipped past.
        truncated = after_all is not None and result.truncated
        events: list[dict[str, Any]] = []
        reached = cursor
        cuttable = False

        def answer(end: str | None, has_more: bool) -> _Read:
            return _Read(
                events,
                end if truncated or not events else cursor,
                end,
                truncated,
                has_more,
            )

        for index, item in enumerate(result.events):
            if len(events) == limit and cuttable:
                return answer(reached, True)
            after = (
                after_all if index == len(result.events) - 1 else item.cursor or reached
            )
            event = Event(
                item.data,
                item.event_id or str(uuid.uuid4()),
                _iso(item.timestamp),
                meta=item.meta,
            )
            if _seconds(cast(str, event.timestamp)) < floor:
                truncated = after_all is not None
            else:
                events.append(
                    {**_occurrence(self.name, event, event.data), "cursor": after}
                )
            reached = after
            cuttable = item.cursor is not None
        return answer(after_all, result.has_more)

    async def _read(
        self, hold: _Hold, cursor: str | None, max_age_ms: int | None, limit: int
    ) -> _Read:
        if self._check is not None:
            return await self._checked(hold, cursor, max_age_ms, limit)
        return self._since(hold, cursor, max_age_ms, limit)


def _occurrence(name: str, event: Event[Any], data: Any) -> dict[str, Any]:
    occurrence = {
        "eventId": event.event_id,
        "name": name,
        "timestamp": event.timestamp,
        "data": to_jsonable_python(data, by_alias=True),
    }
    if event.meta:
        occurrence["_meta"] = event.meta
    return occurrence


@dataclass
class _Stream:
    """What a push stream is waiting to send."""

    wake: asyncio.Event = field(default_factory=asyncio.Event)
    ending: ErrorData | None = None
    stopped: bool = False
    behind: bool = False
    overflowed: bool = False
    pushed: deque[_Kept] = field(default_factory=deque)
    pushed_bytes: int = 0


Notify: TypeAlias = Callable[[str, dict[str, Any]], Awaitable[None]]


class Events(Extension):
    """The event types an app serves as MCP Events, by poll and push.

    Pass it to ``MCPServer(extensions=[events])``, or call :meth:`serve` on a low-level ``Server``. An endpoint
    whose ``StreamableHTTPSessionManager`` answers in JSON serves streams through :meth:`asgi`.

    An event type either has a check function, whose cursors are its source's and outlive the process, or keeps
    what it emits in memory, as ``keep`` bounds it, whose cursors last as long as the process: after a restart a
    client is told that events were skipped. A type is asked again, and a poll told to come back, every
    ``interval`` seconds unless the type says otherwise. A stream hears from the server every ``heartbeat``
    seconds. A poll subscription lasts ``poll_lease`` seconds after its last poll, a minute or three of the type's
    intervals unless given. A stream holds at most ``max_queued`` events, taking ``max_queued_bytes``, emitted to a
    type with a check function for a reader that has not read them; past that it drops them, and once the reader
    has caught up it is told of a gap and goes on from the newest. ``max_queued`` also caps one answer.
    ``max_subscriptions`` bounds the subscriptions held at once, by every delivery mode, and
    ``max_subscriptions_per_caller`` those of one caller. ``page_size`` is how many types ``events/list`` returns
    at a time.
    """

    identifier = EVENTS_EXTENSION

    def __init__(
        self,
        *,
        keep: Keep | None = None,
        interval: float = 1.0,
        heartbeat: float = 30.0,
        poll_lease: float | None = None,
        max_queued: int = 1000,
        max_queued_bytes: int = 4 * 2**20,
        max_subscriptions: int = 10_000,
        max_subscriptions_per_caller: int = 1000,
        page_size: int = 100,
    ) -> None:
        keep = keep or {}
        self._keep_events = keep.get("events", 10_000)
        self._keep_age = keep.get("age", 600.0)
        self._keep_bytes = keep.get("bytes", 16 * 2**20)
        self._interval = interval
        self._heartbeat = heartbeat
        self._poll_lease = poll_lease
        self._max_queued = max_queued
        self._max_queued_bytes = max_queued_bytes
        self._max_subscriptions = max_subscriptions
        self._max_per_caller = max_subscriptions_per_caller
        self._page_size = page_size
        # Cursors of kept events are positions in this process's memory; one from another process names nothing.
        self._epoch = secrets.token_hex(6)
        self._types: dict[str, EventType[Any, Any]] = {}
        self._declared = 0
        self._held = 0
        self._callers: dict[str, int] = {}
        self._leases: dict[str, _Lease] = {}
        self._tasks: set[asyncio.Future[Any]] = set()

    def event(
        self,
        name: str,
        *,
        input_schema: type[A] | TypeAdapter[A],
        payload_schema: type[D] | TypeAdapter[D],
        description: str | None = None,
        meta: dict[str, Any] | None = None,
        match: Match[D, A] | None = None,
        transform: Transform[D, A] | None = None,
        key: Callable[[D], str] | None = None,
        check: Check[A, D] | None = None,
        interval: float | None = None,
        delivery: Sequence[Delivery] = ("poll", "push"),
        authorize: Authorize[A] | None = None,
        on_subscribe: OnSubscribe[A] | None = None,
    ) -> EventType[A, D]:
        """Declare an event type.

        Args:
            input_schema: The arguments of a subscription, as a type Pydantic validates. A request whose
                arguments do not match is refused; callbacks receive them validated.
            payload_schema: The data of each occurrence, listed as JSON Schema. Use a ``TypeAdapter`` for a union.
            match: ``(context, event, arguments)``: whether a subscription receives an emitted event. Without it,
                every subscription does.
            transform: ``(context, event, arguments)``: the data a subscription receives of an emitted event.
            key: For events that each hold the whole of a record: an emitted event replaces the one before it with
                the same key, among the events kept and those a stream has yet to send. A subscription behind it
                receives only the newest, and is not told of a gap, since nothing it would act on was lost. Do not
                give a key to events that each say what changed.
            check: ``(context, arguments, cursor)``: what happened after a cursor, for a source that keeps its own
                history. Polls are answered by it, and streams ask it every ``interval``. A None cursor asks for the
                current position. It filters and shapes for the arguments itself; ``match`` and ``transform`` apply
                only to what is emitted. Without it, emitted events are kept in memory and served from there.
            interval: Seconds a poll is told to wait, and how often a stream asks the check function.
            delivery: The delivery modes this type offers.
            authorize: ``(context, arguments)``: whether the caller may subscribe. A subscription is refused with
                Forbidden when it may not, and one already held is ended when it no longer may: each poll asks, and
                a stream asks at each heartbeat. A refused poll ends its lease at once.
            on_subscribe: ``(context, arguments, subscription_id)``: called when a subscription begins, to set up
                what its events come from. What it returns is called when the subscription ends: its last stream
                closes, its poll lease lapses because polls stopped, or the app ends it. A subscription held by two
                streams at once is set up once. A server that restarts calls it again with the same id.
        """
        if name in self._types:
            raise ValueError(f"Event {name} is already declared.")
        if not delivery or any(mode not in ("poll", "push") for mode in delivery):
            raise ValueError(
                f"Event {name} must be delivered by poll, push or both, not {list(delivery)}."
            )
        self._declared += 1
        declared = EventType(
            self,
            name,
            self._declared,
            input_schema=_adapter(input_schema),
            payload_schema=_adapter(payload_schema),
            description=description,
            meta=meta,
            match=match,
            transform=transform,
            key=key,
            check=check,
            interval=self._interval if interval is None else interval,
            delivery=delivery,
            authorize=authorize,
            on_subscribe=on_subscribe,
        )
        self._types[name] = declared
        return declared

    def settings(self) -> dict[str, Any]:
        # Event types change only in the process, and the SDK has no channel to tell every client of it.
        return {"listChanged": False}

    def methods(self) -> Sequence[MethodBinding]:
        return [
            MethodBinding("events/list", _ListParams, self._list),
            MethodBinding("events/poll", _SubscribeParams, self._poll),
            MethodBinding("events/stream", _SubscribeParams, self._stream),
        ]

    def serve(self, server: Server[Any]) -> None:
        """Serve ``events/list``, ``events/poll`` and ``events/stream`` on a low-level server, and declare them."""
        for binding in self.methods():
            server.add_request_handler(
                binding.method, binding.params_type, binding.handler
            )
        server.extensions[self.identifier] = self.settings()

    def asgi(self, manager: StreamableHTTPSessionManager) -> ASGIApp:
        """Answer ``events/stream`` as a stream on an endpoint whose session manager answers in JSON.

        Mount the returned app where ``manager.handle_request`` was mounted, inside any authentication. A manager
        set to answer in JSON drops the notifications a request sends, so this answers ``events/stream`` itself
        and passes every other request to the manager.
        """
        security = TransportSecurityMiddleware(manager.security_settings)

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            if (
                scope["type"] != "http"
                or scope["method"] != "POST"
                or not manager.json_response
            ):
                return await manager.handle_request(scope, receive, send)
            body, more = b"", True
            while more:
                message = await receive()
                if message["type"] != "http.request":
                    return
                body += message.get("body", b"")
                more = message.get("more_body", False)
                if len(body) > manager.max_request_body_size:
                    return await Response(status_code=413)(scope, receive, send)
            replayed = False

            async def replay() -> Message:
                nonlocal replayed
                if replayed:
                    return await receive()
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}

            try:
                request = json.loads(body)
            except ValueError:
                request = None
            if not (
                isinstance(request, dict)
                and request.get("method") == "events/stream"
                and "id" in request
            ):
                return await manager.handle_request(scope, replay, send)
            if refused := await security.validate_request(
                Request(scope, replay), is_post=True
            ):
                return await refused(scope, replay, send)
            await self._answer_stream(request, scope, receive, send)

        return app

    # Answers events/stream on an HTTP response of its own: JSON when it is refused, else a stream of server-sent
    # events, ended when the client goes.
    async def _answer_stream(
        self, request: dict[str, Any], scope: Scope, receive: Receive, send: Send
    ) -> None:
        started = False

        async def notify(method: str, params: dict[str, Any]) -> None:
            nonlocal started
            if not started:
                started = True
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": _SSE_HEADERS,
                    }
                )
            await send(
                {
                    "type": "http.response.body",
                    "body": _frame({"method": method, "params": params}),
                    "more_body": True,
                }
            )

        async with anyio.create_task_group() as group:

            async def watch() -> None:
                while (await receive())["type"] != "http.disconnect":
                    pass
                group.cancel_scope.cancel()

            group.start_soon(watch)
            reply: dict[str, Any] = {"jsonrpc": "2.0", "id": request["id"]}
            try:
                params = _SubscribeParams.model_validate(
                    request.get("params") or {}, by_name=False
                )
                reply["result"] = await self._serve_stream(
                    params, request["id"], notify
                )
            except MCPError as error:
                reply["error"] = _error(error.error)
            except ValidationError:
                reply["error"] = {
                    "code": INVALID_PARAMS,
                    "message": "Invalid request parameters",
                }
            except Exception as error:  # noqa: BLE001
                reply["error"] = _described(error)
            if started:
                await send(
                    {
                        "type": "http.response.body",
                        "body": _frame(reply),
                        "more_body": False,
                    }
                )
            else:
                body = json.dumps(reply).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send({"type": "http.response.body", "body": body})
            group.cancel_scope.cancel()

    async def _list(
        self, ctx: ServerRequestContext[Any, Any], params: _ListParams
    ) -> HandlerResult:
        declared = list(self._types.values())
        start = 0
        if params.cursor is not None:
            if not params.cursor.isdigit():
                raise MCPError(INVALID_PARAMS, "Invalid cursor for events/list")
            start = next(
                (
                    index
                    for index, each in enumerate(declared)
                    if each._order > int(params.cursor)
                ),
                len(declared),
            )
        page = declared[start : start + self._page_size]
        result: dict[str, Any] = {"events": [each._descriptor for each in page]}
        if start + self._page_size < len(declared):
            result["nextCursor"] = str(page[-1]._order)
        return result

    async def _poll(
        self, ctx: ServerRequestContext[Any, Any], params: _SubscribeParams
    ) -> HandlerResult:
        declared, request = await self._subscription(params, "poll")
        hold = await self._lease(declared, request)
        read = await declared._read(
            hold,
            params.cursor,
            params.max_age_ms,
            params.max_events or self._max_queued,
        )
        # A poll carries its cursor once, for the whole answer.
        return {
            "events": [
                {key: value for key, value in each.items() if key != "cursor"}
                for each in read.events
            ],
            "cursor": read.cursor,
            "truncated": read.truncated,
            "hasMore": read.has_more,
            "nextPollMs": round(declared._interval * 1000),
        }

    async def _stream(
        self, ctx: ServerRequestContext[Any, Any], params: _SubscribeParams
    ) -> HandlerResult:
        async def notify(method: str, body: dict[str, Any]) -> None:
            notification = Notification[dict[str, Any], str](method=method, params=body)
            await ctx.session.send_notification(
                cast(ServerNotification, notification),
                related_request_id=ctx.request_id,
            )

        assert ctx.request_id is not None
        return await self._serve_stream(params, ctx.request_id, notify)

    # The event type and arguments a request names, checked as the sketch's errors require: the type, the delivery
    # mode, the arguments, and then whether the caller may subscribe. A caller refused loses what it holds of that
    # subscription at once.
    async def _subscription(
        self, params: _SubscribeParams, delivery: Delivery
    ) -> tuple[EventType[Any, Any], _Request[Any]]:
        declared = self._types.get(params.name)
        if declared is None:
            raise MCPError(_REMOVED.code, _REMOVED.message, _REMOVED.data)
        if delivery not in declared._delivery:
            raise MCPError(
                UNSUPPORTED,
                "Unsupported",
                {"feature": "deliveryMode", "value": delivery},
            )
        try:
            arguments = declared._input.validate_python(params.arguments or {})
        except ValidationError as error:
            problems = "; ".join(
                f"{'.'.join(map(str, each['loc'])) or 'arguments'}: {each['msg']}"
                for each in error.errors()
            )
            raise MCPError(
                INVALID_PARAMS, f"Invalid arguments for {params.name}: {problems}"
            ) from None
        context = get_context()
        caller = _caller(context)
        canonical = json.dumps(
            [
                caller,
                declared.name,
                declared._input.dump_python(arguments, mode="json"),
            ],
            sort_keys=True,
        )
        request = _Request(
            f"sub_{hashlib.sha256(canonical.encode()).hexdigest()[:32]}",
            context,
            caller,
            arguments,
        )
        if declared._authorize is not None and not await _settle(
            declared._authorize(context, arguments)
        ):
            declared.end(request.id, _FORBIDDEN)
            if lease := self._leases.get(request.id):
                self._stop(request.id, lease)
            raise MCPError(FORBIDDEN, "Forbidden")
        return declared, request

    # Begins holding a subscription, by a stream or a poll lease: counted against the limits first, so a refused
    # one provisions nothing. The type's on_subscribe runs when the first hold of an id begins, and its teardown
    # when the last ends. `end` is how the delivery mode is told the app ended it; `_close` is how the mode says its
    # hold is over.
    async def _open(
        self,
        declared: EventType[Any, Any],
        request: _Request[Any],
        end: Callable[[ErrorData], None],
    ) -> _Hold:
        if self._types.get(declared.name) is not declared:
            raise MCPError(_REMOVED.code, _REMOVED.message, _REMOVED.data)
        if self._held >= self._max_subscriptions:
            raise MCPError(
                RESOURCE_EXHAUSTED,
                "ResourceExhausted",
                {"limit": "subscriptions", "max": self._max_subscriptions},
            )
        # Callers without credentials cannot be told apart, so only the limit on everyone holds them.
        if (
            request.caller is not None
            and self._callers.get(request.caller, 0) >= self._max_per_caller
        ):
            raise MCPError(
                RESOURCE_EXHAUSTED,
                "ResourceExhausted",
                {"limit": "subscriptionsPerCaller", "max": self._max_per_caller},
            )
        hold = _Hold(declared, request, end)
        self._held += 1
        if request.caller is not None:
            self._callers[request.caller] = self._callers.get(request.caller, 0) + 1
        entry = declared._subscriptions.get(request.id)
        if entry is None:
            # Set up apart from the request that began it, so that a request cancelled meanwhile does not cut it
            # short for another hold of the same subscription.
            entry = _Entry(asyncio.ensure_future(self._subscribed(declared, request)))
            declared._subscriptions[request.id] = entry
        hold.entry = entry
        entry.holds.add(hold)
        try:
            await asyncio.shield(entry.ready)
            if self._types.get(declared.name) is not declared:
                raise MCPError(_REMOVED.code, _REMOVED.message, _REMOVED.data)
        except BaseException:
            self._close(hold)
            raise
        return hold

    async def _subscribed(
        self, declared: EventType[Any, Any], request: _Request[Any]
    ) -> Teardown | None:
        if declared._on_subscribe is None:
            return None
        return cast(
            Teardown | None,
            await _settle(
                declared._on_subscribe(request.context, request.arguments, request.id)
            ),
        )

    def _close(self, hold: _Hold) -> None:
        if hold.closed:
            return
        hold.closed = True
        self._held -= 1
        caller = hold.request.caller
        if caller is not None:
            if self._callers[caller] == 1:
                del self._callers[caller]
            else:
                self._callers[caller] -= 1
        entry = cast(_Entry, hold.entry)
        entry.holds.discard(hold)
        if entry.holds:
            return
        if hold.type._subscriptions.get(hold.request.id) is entry:
            del hold.type._subscriptions[hold.request.id]
        entry.ready.add_done_callback(self._tear_down)

    def _tear_down(self, ready: asyncio.Future[Any]) -> None:
        if (
            ready.cancelled()
            or ready.exception() is not None
            or not callable(teardown := ready.result())
        ):
            return
        self._spawn(_settle(teardown()))

    # A teardown that fails has nobody to tell but the log, and must not take the process down.
    def _spawn(self, work: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(work)
        self._tasks.add(task)

        def done(task: asyncio.Future[Any]) -> None:
            self._tasks.discard(task)
            if not task.cancelled() and (error := task.exception()) is not None:
                logger.error("An event subscription's teardown failed", exc_info=error)

        task.add_done_callback(done)

    # Poll subscriptions are leased: the first poll of a subscription begins one, each poll renews it, and it ends
    # when polls stop for the lease's length.
    async def _lease(
        self, declared: EventType[Any, Any], request: _Request[Any]
    ) -> _Hold:
        lease = self._leases.get(request.id)
        if lease is not None and (
            lease.ended is not None or lease.type is not declared
        ):
            self._stop(request.id, lease)
            # The poll after the app ended a subscription is answered with why, and the one after it begins afresh.
            if lease.ended is not None and lease.type is declared:
                raise MCPError(lease.ended.code, lease.ended.message, lease.ended.data)
            lease = None
        if lease is None:
            lease = self._leases[request.id] = _Lease(declared)
            began = lease

            def end(error: ErrorData) -> None:
                began.ended = error
                if began.hold is not None:
                    self._close(began.hold)

            def opened(opening: asyncio.Future[_Hold]) -> None:
                if opening.cancelled() or opening.exception() is not None:
                    if self._leases.get(request.id) is began:
                        del self._leases[request.id]
                    return
                began.hold = opening.result()
                if began.ended is not None:
                    self._close(began.hold)

            lease.opening = asyncio.ensure_future(self._open(declared, request, end))
            lease.opening.add_done_callback(opened)
        hold = await asyncio.shield(cast(asyncio.Future[_Hold], lease.opening))
        if lease.ended is not None:
            self._stop(request.id, lease)
            raise MCPError(lease.ended.code, lease.ended.message, lease.ended.data)
        if lease.timer is not None:
            lease.timer.cancel()
        length = (
            self._poll_lease
            if self._poll_lease is not None
            else max(60.0, 3 * declared._interval)
        )
        lease.timer = asyncio.get_running_loop().call_later(
            length, self._stop, request.id, lease
        )
        return hold

    def _stop(self, id: str, lease: _Lease) -> None:
        if self._leases.get(id) is lease:
            del self._leases[id]
        if lease.timer is not None:
            lease.timer.cancel()
        if lease.hold is not None:
            self._close(lease.hold)

    # A push stream follows its cursor: emitted events wake it, and a type with a check function is asked again
    # every interval. One task sends everything, in order, so a reader that stops reading holds up only this
    # stream, and nothing is sent before `active`.
    async def _serve_stream(
        self, params: _SubscribeParams, request_id: Any, notify: Notify
    ) -> dict[str, Any]:
        declared, request = await self._subscription(params, "push")
        meta = {SUBSCRIPTION_ID: request_id}
        state = _Stream()
        checked = declared._check is not None

        def end(error: ErrorData) -> None:
            state.ending, state.stopped = error, True
            state.wake.set()

        # What is emitted to a type with a check function waits here for the stream; anything else is read from
        # what the type keeps.
        def sink(kept: _Kept) -> None:
            if not checked:
                state.behind = True
            elif not state.overflowed:
                if kept.replaces is not None:
                    for older in [
                        each for each in state.pushed if each.replaces == kept.replaces
                    ]:
                        state.pushed.remove(older)
                        state.pushed_bytes -= older.size
                state.pushed.append(kept)
                state.pushed_bytes += kept.size
                if (
                    len(state.pushed) > self._max_queued
                    or state.pushed_bytes > self._max_queued_bytes
                ):
                    state.overflowed = True
                    state.pushed.clear()
                    state.pushed_bytes = 0
            state.wake.set()

        hold = await self._open(declared, request, end)
        declared._sinks.add(sink)
        try:

            async def say(method: str, body: dict[str, Any]) -> None:
                await notify(f"notifications/events/{method}", {**body, "_meta": meta})

            async def deliver(read: _Read) -> None:
                for occurrence in read.events:
                    if state.stopped:
                        return
                    await say("event", occurrence)

            # Without a first answer there is no stream to tell, and the request fails.
            read = await declared._read(
                hold, params.cursor, params.max_age_ms, self._max_queued
            )
            await say("active", {"cursor": read.start, "truncated": read.truncated})
            await deliver(read)
            cursor = read.cursor
            # Events emitted while this batch was being sent are still owed.
            state.behind |= read.has_more
            due = time.monotonic() + (0 if read.has_more else declared._interval)
            beat = time.monotonic() + self._heartbeat
            while not state.stopped:
                if state.overflowed:
                    try:
                        cursor = (await declared._read(hold, None, None, 1)).cursor
                    except Exception as error:  # noqa: BLE001
                        await say("error", {"error": _described(error)})
                    state.overflowed = False
                    due = time.monotonic() + declared._interval
                    await say("active", {"cursor": cursor, "truncated": True})
                    continue
                if (checked and time.monotonic() >= due) or (
                    not checked and state.behind
                ):
                    state.behind = False
                    try:
                        read = await declared._read(
                            hold, cursor, None, self._max_queued
                        )
                    except Exception as error:  # noqa: BLE001
                        await say("error", {"error": _described(error)})
                    else:
                        if read.truncated:
                            await say(
                                "active", {"cursor": read.start, "truncated": True}
                            )
                        await deliver(read)
                        cursor = read.cursor
                        state.behind |= read.has_more
                    due = time.monotonic() + (0 if state.behind else declared._interval)
                    continue
                # What a type with a check function emits is sent at once, at the cursor the stream has reached.
                while state.pushed and not state.stopped and not state.overflowed:
                    kept = state.pushed.popleft()
                    state.pushed_bytes -= kept.size
                    if (occurrence := declared._shape(kept, hold)) is not None:
                        await say("event", {**occurrence, "cursor": cursor})
                if time.monotonic() >= beat:
                    # Access is checked again while the subscription lasts, and one that is lost ends it.
                    try:
                        if declared._authorize is not None and not await _settle(
                            declared._authorize(request.context, request.arguments)
                        ):
                            end(_FORBIDDEN)
                            break
                        await say("heartbeat", {"cursor": cursor})
                    except Exception as error:  # noqa: BLE001
                        await say("error", {"error": _described(error)})
                    beat = time.monotonic() + self._heartbeat
                state.wake.clear()
                if state.stopped or state.overflowed or state.pushed or state.behind:
                    continue
                wait = min(beat, due if checked else math.inf) - time.monotonic()
                with anyio.move_on_after(max(0.0, wait)):
                    await state.wake.wait()
            if state.ending is not None:
                await say("terminated", {"error": _error(state.ending)})
        finally:
            declared._sinks.discard(sink)
            self._close(hold)
        # The final frame, sent when the server ends the stream; when the client did, nobody is listening.
        return {"_meta": {}}


def _adapter(schema: type[Any] | TypeAdapter[Any]) -> TypeAdapter[Any]:
    return schema if isinstance(schema, TypeAdapter) else TypeAdapter(schema)


_SSE_HEADERS = [
    (b"content-type", b"text/event-stream"),
    (b"cache-control", b"no-cache, no-transform"),
    (b"x-accel-buffering", b"no"),
]


def _frame(message: dict[str, Any]) -> bytes:
    return (
        b"event: message\r\ndata: "
        + json.dumps({"jsonrpc": "2.0", **message}).encode()
        + b"\r\n\r\n"
    )
