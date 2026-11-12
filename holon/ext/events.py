"""MCP Events, the Triggers & Events Working Group's draft extension, in its poll and push delivery modes.

The server declares and emits event types; every app client lists and subscribes to them. Like the draft, this
interface may change.

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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeAlias,
    TypeVar,
    cast,
)

import anyio
from mcp import Client
from mcp.client import Transport
from mcp.client._transport import TransportStreams
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.server.lowlevel.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import RequestBodyLimitMiddleware
from mcp.shared.exceptions import MCPError
from mcp.shared.inbound import MCP_METHOD_HEADER
from mcp.shared.message import SessionMessage
from mcp.shared.subscriptions import SUBSCRIPTION_ID_META_KEY
from mcp.types import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    ErrorData,
    JSONRPCNotification,
    Notification,
    PaginatedRequestParams,
    Request,
    RequestParams,
    ServerNotification,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
)
from pydantic_core import to_json, to_jsonable_python
from starlette.types import ASGIApp, Receive, Scope, Send

from holon.context import RequestContext, get_context

if TYPE_CHECKING:
    from holon.client import _App

__all__ = [
    "EVENTS_EXTENSION",
    "AppEvents",
    "Event",
    "EventType",
    "Events",
    "NoCompatibleDeliveryMode",
    "Subscription",
]

logger = logging.getLogger(__name__)

EVENTS_EXTENSION = "io.modelcontextprotocol/events"
"""The identifier a server declares the extension under, in ``capabilities.extensions``."""
# The sketch's general-purpose codes, provisional until the specification allocates them.
NOT_FOUND = -32023
FORBIDDEN = -32024
RESOURCE_EXHAUSTED = -32025
UNSUPPORTED = -32026
# What one poll answers at most, unless the request asks for fewer.
_PAGE = 1000
# Marks a request that came through `Events.asgi`, whose answer can be a stream.
_STREAMS = "holon.ext.events.streams"
# What each event type keeps of what it emits, for polls and for streams that are behind or reconnect: the sketch's
# emit buffer. For how long in seconds, how many events, and how many bytes their data takes as JSON. The newest is
# kept whatever it takes.
_KEEP_AGE = 600.0
_KEEP_EVENTS = 10_000
_KEEP_BYTES = 4 * 2**20
# Seconds a poll is told to wait.
_INTERVAL = 1.0
# Seconds between a stream's heartbeats. One whose reader takes nothing for two is closed, to reconnect from its cursor.
_HEARTBEAT = 30.0
# Seconds a poll subscription lasts after its last poll.
_POLL_SUBSCRIPTION_TTL = 60.0
# The bytes of events every stream together is sending at once, of which one caller's streams may send a quarter.
_MAX_SENDING_BYTES = 16 * 2**20
# The subscriptions held at once, streams and poll leases alike, and those of one caller.
_MAX_SUBSCRIPTIONS = 10_000
_MAX_SUBSCRIPTIONS_PER_CALLER = 100
# How many types events/list returns at a time.
_LIST_PAGE_SIZE = 100
# Seconds a type's on_subscribe has to return before the subscription is refused.
_SUBSCRIBE_TIMEOUT = 30.0

A = TypeVar("A")
D = TypeVar("D")
T = TypeVar("T")
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


Match: TypeAlias = Callable[[RequestContext, Event[D], A], bool]
Transform: TypeAlias = Callable[[RequestContext, Event[D], A], object]
Authorize: TypeAlias = Callable[[RequestContext, A], bool | Awaitable[bool]]
SubscriptionHook: TypeAlias = Callable[[RequestContext, A, str], None | Awaitable[None]]


class _SubscribeParams(RequestParams):
    name: StrictStr
    arguments: dict[str, Any] | None = None
    cursor: StrictStr | None = None
    max_age_ms: StrictInt | None = Field(default=None, ge=0)
    max_events: StrictInt | None = Field(default=None, ge=1)


_FORBIDDEN = ErrorData(code=FORBIDDEN, message="Forbidden")
_NOT_FOUND = ErrorData(code=NOT_FOUND, message="NotFound", data={"kind": "event"})


class _Stalled(Exception):
    """A stream's reader took nothing for two heartbeats."""


# What a client is told of a failure: its own error when it raised an MCP one. Anything else is the app's to log.
def _described(error: BaseException) -> dict[str, Any]:
    if isinstance(error, MCPError):
        return error.error.model_dump(by_alias=True, exclude_none=True, mode="json")
    logger.error("An event could not be served", exc_info=error)
    return {"code": INTERNAL_ERROR, "message": "InternalError"}


async def _settle(value: T | Awaitable[T]) -> T:
    return await value if inspect.isawaitable(value) else value


@dataclass(frozen=True, eq=False)
class _Request:
    """A subscription as one request names it, with that request's caller.

    A caller with an identity has one id for the same event type and arguments, so it survives reconnects and poll
    leases. A caller without one is nobody in particular: each of its streams is a subscription of its own, and its
    polls share one lease per arguments, which no targeted event reaches.
    """

    id: str
    context: RequestContext
    caller: str
    arguments: Any
    targetable: bool


@dataclass(eq=False)
class _Kept:
    event: Event[Any]
    size: int
    target: str | None
    sequence: int
    at: float


@dataclass(eq=False)
class _Entry:
    """A subscription held by one or more streams or poll leases, set up once by the type's on_subscribe."""

    ready: asyncio.Future[None]
    request: _Request
    """The request that began it, whose context and arguments on_unsubscribe is given."""
    holds: set[_Hold] = field(default_factory=set)


@dataclass(eq=False)
class _Hold:
    """One stream, or one poll lease, holding a subscription."""

    type: EventType[Any, Any]
    request: _Request
    entry: _Entry
    wake: Callable[[], None]
    """Told when there is something new for it, or it was ended."""
    ended: ErrorData | None = None
    until: int = 0


@dataclass(eq=False)
class _Lease:
    hold: _Hold
    timer: asyncio.TimerHandle | None = None


@dataclass(eq=False, repr=False)
class EventType(Generic[A, D]):
    """An event type an app declared with :meth:`Events.event`. Emit its occurrences with :meth:`emit`."""

    name: str
    _events: Events
    _input: TypeAdapter[A]
    _descriptor: dict[str, Any]
    _match: Match[D, A] | None
    _transform: Transform[D, A] | None
    _delivery: list[Delivery]
    _authorize: Authorize[A] | None
    _on_subscribe: SubscriptionHook[A] | None
    _on_unsubscribe: SubscriptionHook[A] | None
    # What was emitted, oldest first. `_dropped` is the last sequence let go for room or age.
    _kept: list[_Kept] = field(default_factory=list)
    _sequence: int = 0
    _dropped: int = 0
    _bytes: int = 0
    _subscriptions: dict[str, _Entry] = field(default_factory=dict)
    # Subscriptions being told they ended, which one that begins with the same id waits for.
    _closing: dict[str, asyncio.Event] = field(default_factory=dict)

    def emit(
        self,
        data: D,
        *,
        event_id: str | None = None,
        timestamp: datetime | None = None,
        subscription_id: str | None = None,
    ) -> None:
        """Emit an occurrence to the subscriptions it matches.

        Give the source's own ``event_id`` when it has one, so that a client drops an occurrence it has seen. With
        ``subscription_id``, the sketch's targeted emit, the occurrence goes to that subscription alone, whatever its
        arguments. It is kept like any other, so a subscription that reconnects or polls from its cursor still has
        it, or is told of a gap.

        Emitting is synchronous, and happens on the event loop that serves the events, from the first emit on: from
        another thread, call ``loop.call_soon_threadsafe(event_type.emit, data)``. Occurrences emitted one after
        another in the same synchronous run reach each subscription in that order, with no other occurrence of this
        type between them, whether polled or pushed, unless it is told of a gap. Data that cannot be serialised
        raises here, before anything changes.
        """
        self._events._on_loop()
        size = len(to_json(data, by_alias=True))
        moment = (timestamp or datetime.now(UTC)).astimezone(UTC)
        event = Event(
            data,
            event_id or str(uuid.uuid4()),
            moment.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            name=self.name,
        )
        self._keep(event, size, subscription_id)
        for hold in self._holds(subscription_id):
            hold.wake()

    def cursor(self) -> str:
        """The position after the last occurrence emitted.

        Return it with the state a tool reads, in the same synchronous run, and a client that subscribes from it
        hears every change after that state and none before.
        """
        return self._position()

    def terminate(
        self, subscription_id: str | None = None, error: ErrorData | None = None
    ) -> None:
        """Terminate one subscription, or all of this type's: a stream is sent what was emitted to it before, then
        ``notifications/events/terminated`` with ``error``, and closed. The next poll is answered with the error.
        Forbidden unless given, which ends delivery at once, since access is lost.
        """
        error = error or _FORBIDDEN
        until = 0 if error.code == _FORBIDDEN.code else self._sequence
        for hold in self._holds(subscription_id):
            if hold.ended is None:
                hold.ended, hold.until = error, until
                hold.wake()

    def _holds(self, subscription_id: str | None) -> list[_Hold]:
        if subscription_id is None:
            entries = list(self._subscriptions.values())
        else:
            entry = self._subscriptions.get(subscription_id)
            entries = [entry] if entry else []
        return [hold for entry in entries for hold in entry.holds]

    def _position(self, sequence: int | None = None) -> str:
        return (
            f"{self._events._epoch}.{self._sequence if sequence is None else sequence}"
        )

    def _keep(self, event: Event[Any], size: int, target: str | None) -> None:
        self._sequence += 1
        kept = _Kept(event, size, target, self._sequence, time.monotonic())
        self._kept.append(kept)
        self._bytes += kept.size
        # The newest is kept whatever it weighs, since streams read it from here.
        dropped = 0
        while len(self._kept) - dropped > 1 and (
            len(self._kept) - dropped > _KEEP_EVENTS
            or self._bytes > _KEEP_BYTES
            or self._kept[dropped].at < kept.at - _KEEP_AGE
        ):
            self._dropped = self._kept[dropped].sequence
            self._bytes -= self._kept[dropped].size
            dropped += 1
        del self._kept[:dropped]

    def _next(self, sequence: int) -> _Kept | None:
        index = bisect.bisect_right(
            self._kept, sequence, key=lambda each: each.sequence
        )
        return self._kept[index] if index < len(self._kept) else None

    # Where a cursor resumes reading what is kept, and whether events between were let go. A None cursor starts where
    # the subscription was opened, so it has what its on_subscribe emitted, and so does one from another process.
    def _resume(
        self, cursor: str | None, max_age_ms: int | None, opened: int
    ) -> tuple[int, bool]:
        if cursor is None:
            return opened, False
        epoch, _, text = cursor.partition(".")
        if (
            epoch != self._events._epoch
            or not text.isdigit()
            or int(text) > self._sequence
        ):
            return opened, True
        sequence, truncated = max(int(text), self._dropped), int(text) < self._dropped
        if max_age_ms is not None:
            floor = time.monotonic() - max_age_ms / 1000
            start = bisect.bisect_right(
                self._kept, sequence, key=lambda each: each.sequence
            )
            aged = bisect.bisect_left(
                self._kept, floor, lo=start, key=lambda each: each.at
            )
            if aged > start:
                sequence, truncated = self._kept[aged - 1].sequence, True
        return sequence, truncated

    # The occurrence a subscription receives, or None when it is not for it. An event emitted to one subscription
    # is for that one alone, and its arguments are not asked.
    def _shape(self, kept: _Kept, request: _Request) -> dict[str, Any] | None:
        if kept.target is not None:
            if not request.targetable or kept.target != request.id:
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
        return {
            "eventId": kept.event.event_id,
            "name": kept.event.name,
            "timestamp": kept.event.timestamp,
            "data": to_jsonable_python(data, by_alias=True),
        }

    # A poll's answer after a cursor from what is kept. An event that cannot be shaped for this subscription is
    # skipped for it, and the failure logged.
    def _since(
        self,
        request: _Request,
        cursor: str,
        max_age_ms: int | None,
        limit: int,
        opened: int,
    ) -> dict[str, Any]:
        reached, truncated = self._resume(cursor, max_age_ms, opened)
        events: list[dict[str, Any]] = []
        while len(events) < limit and (kept := self._next(reached)) is not None:
            reached = kept.sequence
            try:
                occurrence = self._shape(kept, request)
            except Exception as error:  # noqa: BLE001
                _described(error)
                continue
            if occurrence is not None:
                events.append(occurrence)
        return {
            "events": events,
            "cursor": self._position(reached),
            "truncated": truncated,
            "hasMore": self._next(reached) is not None,
        }

    async def _allows(self, context: RequestContext, arguments: Any) -> bool:
        return self._authorize is None or await _settle(
            self._authorize(context, arguments)
        )

    async def _settled(self, id: str) -> None:
        while (closing := self._closing.get(id)) is not None:
            await closing.wait()


# The manager's own handling of a 2026-07-28 request, answering by stream as it does subscriptions/listen, which is
# the one method the SDK streams on an endpoint that answers in JSON. The path is private to the SDK, so it is looked
# for when an endpoint is mounted, and an endpoint without it offers poll only.
def _streaming(manager: StreamableHTTPSessionManager) -> ASGIApp | None:
    try:
        from mcp.server._streamable_http_modern import handle_modern_request
    except ImportError:
        return None
    expected = ["app", "security_settings", "json_response", "lifespan_state"]
    expected += ["scope", "receive", "send"]
    taken = list(inspect.signature(handle_modern_request).parameters)
    if taken != expected or not hasattr(manager, "_lifespan_state"):
        return None

    async def stream(scope: Scope, receive: Receive, send: Send) -> None:
        await handle_modern_request(
            manager.app,
            manager.security_settings,
            False,
            manager._lifespan_state,
            scope,
            receive,
            send,
        )

    return RequestBodyLimitMiddleware(stream, manager.max_request_body_size)


def _streams(ctx: ServerRequestContext[Any, Any]) -> bool:
    # Over stdio every request can stream. Over HTTP only one that came through `Events.asgi` is known to.
    return ctx.request is None or bool(ctx.request.scope.get(_STREAMS))


class Events(Extension):
    """The event types an app serves as MCP Events, by poll and push.

    Pass it to ``MCPServer(extensions=[events], middleware=[events])``, or call :meth:`serve` on a low-level
    ``Server``. Over Streamable HTTP, push is offered only on an endpoint mounted through :meth:`asgi`; elsewhere
    clients poll.

    Each type keeps what it emits, for ten minutes and up to 10,000 events and 4 MiB, for polls and for streams that
    are behind or reconnect; those cursors last as long as the process, and after a restart a client is told that
    events were skipped. A poll is told to come back every second. A stream hears from the server every 30 seconds,
    and one whose reader has taken nothing for two heartbeats is closed, to reconnect from its cursor. A poll
    subscription lasts a minute after its last poll.

    A stream sends one event at a time and holds nothing else of its own. Every stream together sends at most 16 MiB
    of events at once, and one caller's streams a quarter of it; an event is sent alone when nothing else is, whatever
    it weighs. Waiting for room counts toward the two heartbeats. At most 10,000 subscriptions are held at once,
    streams and poll leases alike, and 100 of one caller; callers without an identity count together as one.
    ``events/list`` returns 100 types at a time. A subscription whose ``on_subscribe`` has not returned after 30
    seconds is refused, and may be tried again. Events thus take about 4 MiB kept for each type and 16 MiB being
    sent, whatever one caller does.
    """

    identifier = EVENTS_EXTENSION

    def __init__(self) -> None:
        # Cursors of kept events are positions in this process's memory; one from another process names nothing.
        self._epoch = secrets.token_hex(6)
        self._types: dict[str, EventType[Any, Any]] = {}
        self._held = 0
        self._callers: dict[str, int] = {}
        self._leases: dict[str, _Lease] = {}
        self._tasks: set[asyncio.Future[Any]] = set()
        self._sending = 0
        self._sending_by: dict[str, int] = {}
        self._sent = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    def event(
        self,
        name: str,
        *,
        input_schema: type[A] | TypeAdapter[A],
        payload_schema: type[D] | TypeAdapter[D],
        description: str | None = None,
        match: Match[D, A] | None = None,
        transform: Transform[D, A] | None = None,
        delivery: Sequence[Delivery] = ("poll", "push"),
        authorize: Authorize[A] | None = None,
        on_subscribe: SubscriptionHook[A] | None = None,
        on_unsubscribe: SubscriptionHook[A] | None = None,
    ) -> EventType[A, D]:
        """Declare an event type.

        Args:
            input_schema: The arguments of a subscription, as a type Pydantic validates. A request whose
                arguments do not match is refused; callbacks receive them validated.
            payload_schema: The data of each occurrence, listed as JSON Schema. Use a ``TypeAdapter`` for a union.
            match: ``(context, event, arguments)``: whether a subscription receives an emitted event. Without it,
                every subscription does. An event it raises for, or that cannot be shaped, is skipped for that
                subscription and logged, and a stream is told with ``notifications/events/error``.
            transform: ``(context, event, arguments)``: the data a subscription receives of an emitted event.
            delivery: The delivery modes this type offers.
            authorize: ``(context, arguments)``: whether the caller may subscribe. A subscription is refused with
                Forbidden when it may not, and one already held is ended when it no longer may: each poll asks, and
                a stream asks at each heartbeat. A refused poll ends its lease at once.
            on_subscribe: ``(context, arguments, subscription_id)``: called when a subscription begins, to set up
                what its events come from. A subscription held by two streams at once is set up once. A server that
                restarts calls it again, with the same id for a caller with an identity.
            on_unsubscribe: ``(context, arguments, subscription_id)``: called when a subscription that was set up
                ends: its last stream closes, its poll lease lapses because polls stopped, or the app terminates it.
                One that begins again with the same id waits for it to return.
        """
        if name in self._types:
            raise ValueError(f"Event {name} is already declared.")
        if not delivery:
            raise ValueError(f"Event {name} must be delivered by poll, push or both.")
        arguments = (
            input_schema
            if isinstance(input_schema, TypeAdapter)
            else TypeAdapter(input_schema)
        )
        payload = (
            payload_schema
            if isinstance(payload_schema, TypeAdapter)
            else TypeAdapter(payload_schema)
        )
        descriptor = {
            "name": name,
            **({"description": description} if description else {}),
            "inputSchema": arguments.json_schema(),
            "payloadSchema": payload.json_schema(),
        }
        declared = EventType(
            name,
            self,
            arguments,
            descriptor,
            match,
            transform,
            list(delivery),
            authorize,
            on_subscribe,
            on_unsubscribe,
        )
        self._types[name] = declared
        return declared

    def settings(self) -> dict[str, Any]:
        # Event types change only in the process, and the SDK has no channel to tell every client of it.
        return {"listChanged": False}

    def methods(self) -> Sequence[MethodBinding]:
        return [
            MethodBinding("events/list", PaginatedRequestParams, self._list),
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
        server.middleware.append(self)

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        """Declare ``capabilities.events`` beside ``capabilities.extensions``, as middleware of the server."""
        result = await call_next(ctx)
        if ctx.method in ("initialize", "server/discover") and isinstance(result, dict):
            capabilities = result.get("capabilities")
            if isinstance(capabilities, dict):
                result = {
                    **result,
                    "capabilities": {**capabilities, "events": self.settings()},
                }
        return result

    def asgi(self, manager: StreamableHTTPSessionManager) -> ASGIApp:
        """The endpoint of ``manager``, on which push is offered.

        Mount it where ``manager.handle_request`` would be, inside any authentication. A manager set to answer in
        JSON drops the notifications a request sends, so there ``events/stream`` is answered by the SDK's own stream
        of server-sent events, for clients of the 2026-07-28 revision, which name the method in a header. Clients of
        earlier revisions poll there, and so does every client if the SDK no longer has that path.
        """
        streaming = _streaming(manager)

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            method = dict(scope.get("headers", [])).get(MCP_METHOD_HEADER.encode())
            if scope["type"] == "http" and not manager.json_response:
                scope = {**scope, _STREAMS: True}
            elif scope["type"] == "http" and streaming and method is not None:
                scope = {**scope, _STREAMS: True}
                if method == b"events/stream":
                    return await streaming(scope, receive, send)
            await manager.handle_request(scope, receive, send)

        return app

    async def _list(
        self, ctx: ServerRequestContext[Any, Any], params: PaginatedRequestParams
    ) -> HandlerResult:
        # Types are only ever added, so a cursor is the index of the next page.
        cursor = params.cursor or "0"
        if not cursor.isdigit():
            raise MCPError(INVALID_PARAMS, "Invalid cursor for events/list")
        start = int(cursor)
        streams = _streams(ctx)
        listed = []
        for each in list(self._types.values())[start : start + _LIST_PAGE_SIZE]:
            if delivery := [
                mode for mode in each._delivery if streams or mode != "push"
            ]:
                listed.append({**each._descriptor, "delivery": delivery})
        result: dict[str, Any] = {"events": listed}
        if start + _LIST_PAGE_SIZE < len(self._types):
            result["nextCursor"] = str(start + _LIST_PAGE_SIZE)
        return result

    async def _poll(
        self, ctx: ServerRequestContext[Any, Any], params: _SubscribeParams
    ) -> HandlerResult:
        declared, request = await self._subscription(params, "poll", _streams(ctx))
        opened = declared._sequence
        await self._lease(declared, request)
        if params.cursor is None:
            # The sketch: a poll from null "returns no events and provides a fresh cursor".
            answer = {
                "events": [],
                "cursor": declared._position(opened),
                "truncated": False,
                "hasMore": False,
            }
        else:
            answer = declared._since(
                request,
                params.cursor,
                params.max_age_ms,
                params.max_events or _PAGE,
                opened,
            )
        return {**answer, "nextPollMs": round(_INTERVAL * 1000)}

    # Emitting must happen on the loop that serves the events, since it changes what they keep and wakes their
    # streams without a lock: the first emit fixes the loop, and every later one must run on it.
    def _on_loop(self) -> None:
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        self._loop = self._loop or running
        if running is None or running is not self._loop:
            raise RuntimeError(
                "Events are emitted on the event loop that serves them. From another thread, call "
                "loop.call_soon_threadsafe(event_type.emit, data)."
            )

    # The event type and arguments a request names, checked as the sketch's errors require: the type, the delivery
    # mode, the arguments, and then whether the caller may subscribe. A caller refused loses what it holds of that
    # subscription at once.
    async def _subscription(
        self, params: _SubscribeParams, delivery: Delivery, streams: bool
    ) -> tuple[EventType[Any, Any], _Request]:
        self._loop = self._loop or asyncio.get_running_loop()
        declared = self._types.get(params.name)
        if declared is None:
            raise MCPError.from_error_data(_NOT_FOUND)
        if delivery not in declared._delivery or (delivery == "push" and not streams):
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
        principal = context.principal
        caller = ""
        if principal is not None:
            who = [
                principal.issuer,
                principal.subject,
                principal.client_id,
                sorted(principal.scopes),
                principal.actor,
            ]
            caller = json.dumps(who, sort_keys=True)
        if principal is None and delivery == "push":
            id = f"sub_{uuid.uuid4().hex}"
        else:
            canonical = json.dumps(
                [
                    caller,
                    declared.name,
                    declared._input.dump_python(arguments, mode="json"),
                ],
                sort_keys=True,
            )
            id = f"sub_{hashlib.sha256(canonical.encode()).hexdigest()[:32]}"
        request = _Request(
            id,
            context,
            caller,
            arguments,
            targetable=principal is not None or delivery == "push",
        )
        if not await declared._allows(context, arguments):
            declared.terminate(id, _FORBIDDEN)
            if lease := self._leases.get(id):
                self._stop(id, lease)
            raise MCPError.from_error_data(_FORBIDDEN)
        return declared, request

    # Begins holding a subscription, by a stream or a poll lease: counted against the limits first, so a refused one
    # provisions nothing. The type's on_subscribe runs, apart from the request, when the first hold of an id begins,
    # and its on_unsubscribe when the last ends. Await `_ready` before serving the hold.
    def _open(
        self,
        declared: EventType[Any, Any],
        request: _Request,
        wake: Callable[[], None],
    ) -> _Hold:
        if self._held >= _MAX_SUBSCRIPTIONS:
            raise MCPError(
                RESOURCE_EXHAUSTED,
                "ResourceExhausted",
                {"limit": "subscriptions", "max": _MAX_SUBSCRIPTIONS},
            )
        if self._callers.get(request.caller, 0) >= _MAX_SUBSCRIPTIONS_PER_CALLER:
            raise MCPError(
                RESOURCE_EXHAUSTED,
                "ResourceExhausted",
                {
                    "limit": "subscriptionsPerCaller",
                    "max": _MAX_SUBSCRIPTIONS_PER_CALLER,
                },
            )
        self._held += 1
        self._callers[request.caller] = self._callers.get(request.caller, 0) + 1
        entry = declared._subscriptions.get(request.id)
        if entry is None:
            entry = _Entry(
                asyncio.ensure_future(self._set_up(declared, request)), request
            )
            declared._subscriptions[request.id] = entry
        hold = _Hold(declared, request, entry, wake)
        entry.holds.add(hold)
        return hold

    async def _set_up(self, declared: EventType[Any, Any], request: _Request) -> None:
        if declared._on_subscribe is None:
            return
        # A hook that never returns would hold its id for good: past the deadline the subscription is refused.
        async with asyncio.timeout(_SUBSCRIBE_TIMEOUT):
            await _settle(
                declared._on_subscribe(request.context, request.arguments, request.id)
            )

    async def _ready(self, hold: _Hold) -> None:
        await asyncio.shield(hold.entry.ready)
        # Ended while it was being set up.
        if hold.ended is not None:
            raise MCPError.from_error_data(hold.ended)

    def _close(self, hold: _Hold) -> None:
        entry = hold.entry
        if hold not in entry.holds:
            return
        entry.holds.discard(hold)
        self._held -= 1
        caller = hold.request.caller
        if self._callers[caller] == 1:
            del self._callers[caller]
        else:
            self._callers[caller] -= 1
        if entry.holds:
            return
        declared, id = hold.type, hold.request.id
        if declared._subscriptions.get(id) is entry:
            del declared._subscriptions[id]
        closing = declared._closing[id] = asyncio.Event()
        task = asyncio.ensure_future(self._tear_down(declared, id, entry, closing))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # Only a subscription that was set up is told it ended. A hook that fails has nobody to tell but the log, and must
    # not take the process down.
    async def _tear_down(
        self,
        declared: EventType[Any, Any],
        id: str,
        entry: _Entry,
        closing: asyncio.Event,
    ) -> None:
        try:
            await entry.ready
            if declared._on_unsubscribe is not None:
                request = entry.request
                await _settle(
                    declared._on_unsubscribe(request.context, request.arguments, id)
                )
        except Exception:
            logger.exception(
                "An event subscription's on_subscribe or on_unsubscribe failed"
            )
        finally:
            if declared._closing.get(id) is closing:
                del declared._closing[id]
            closing.set()

    # Poll subscriptions are leased: the first poll of a subscription begins one, each poll renews it, and it ends
    # when polls stop for the lease's length. The poll after the app ended it is answered with why, and the one after
    # that begins afresh.
    async def _lease(self, declared: EventType[Any, Any], request: _Request) -> _Hold:
        await declared._settled(request.id)
        lease = self._leases.get(request.id)
        if lease is None:
            hold = self._open(declared, request, lambda: None)
            # Nothing waits on a lease for new events; ending one tears it down at once.
            hold.wake = lambda: self._close(hold) if hold.ended is not None else None
            lease = self._leases[request.id] = _Lease(hold)
        # Renewed before waiting, so that a poll which gives up during setup still lets the lease lapse.
        if lease.timer is not None:
            lease.timer.cancel()
        lease.timer = asyncio.get_running_loop().call_later(
            _POLL_SUBSCRIPTION_TTL, self._stop, request.id, lease
        )
        try:
            await self._ready(lease.hold)
        except Exception:
            self._stop(request.id, lease)
            raise
        return lease.hold

    def _stop(self, id: str, lease: _Lease) -> None:
        if self._leases.get(id) is lease:
            del self._leases[id]
        if lease.timer is not None:
            lease.timer.cancel()
        self._close(lease.hold)

    # Waits until every stream together, and the caller's own streams, are sending few enough bytes for one more
    # event of `size`. A caller may send a quarter of the whole, so that one cannot hold up the rest.
    @asynccontextmanager
    async def _send_budget(self, size: int, caller: str) -> AsyncIterator[None]:
        while (self._sending and self._sending + size > _MAX_SENDING_BYTES) or (
            (mine := self._sending_by.get(caller, 0))
            and mine + size > _MAX_SENDING_BYTES // 4
        ):
            await self._sent.wait()
        self._sending += size
        self._sending_by[caller] = self._sending_by.get(caller, 0) + size
        try:
            yield
        finally:
            self._sending -= size
            if self._sending_by[caller] == size:
                del self._sending_by[caller]
            else:
                self._sending_by[caller] -= size
            self._sent.set()
            self._sent = asyncio.Event()

    # A push stream follows what the type keeps from its position. One task sends everything, in order and one event
    # at a time, and nothing before `active`.
    async def _stream(
        self, ctx: ServerRequestContext[Any, Any], params: _SubscribeParams
    ) -> HandlerResult:
        declared, request = await self._subscription(params, "push", _streams(ctx))
        await declared._settled(request.id)
        wake = asyncio.Event()
        hold = self._open(declared, request, wake.set)
        # Events emitted from here on reach this stream.
        seen = declared._sequence
        try:
            await self._ready(hold)

            async def say(method: str, body: dict[str, Any], size: int = 0) -> None:
                body["_meta"] = {SUBSCRIPTION_ID_META_KEY: ctx.request_id}
                notification = Notification[dict[str, Any], str](
                    method=f"notifications/events/{method}", params=body
                )
                with anyio.move_on_after(2 * _HEARTBEAT) as waited:
                    async with self._send_budget(size, request.caller):
                        await ctx.session.send_notification(
                            cast(ServerNotification, notification),
                            related_request_id=ctx.request_id,
                        )
                if waited.cancelled_caught:
                    raise _Stalled

            async def send(kept: _Kept) -> None:
                try:
                    shaped = declared._shape(kept, request)
                except Exception as error:  # noqa: BLE001
                    await say("error", {"error": _described(error)})
                    return
                if shaped is not None:
                    cursor = declared._position(seen)
                    await say("event", {**shaped, "cursor": cursor}, kept.size)

            seen, truncated = declared._resume(params.cursor, params.max_age_ms, seen)
            await say(
                "active", {"cursor": declared._position(seen), "truncated": truncated}
            )
            beat = time.monotonic() + _HEARTBEAT
            while hold.ended is None:
                wake.clear()
                if time.monotonic() >= beat:
                    beat = time.monotonic() + _HEARTBEAT
                    # Access is checked again while the subscription lasts, and one that is lost ends it.
                    try:
                        allowed = await declared._allows(
                            request.context, request.arguments
                        )
                    except Exception as error:  # noqa: BLE001
                        await say("error", {"error": _described(error)})
                        continue
                    if not allowed:
                        hold.ended = _FORBIDDEN
                        break
                    await say("heartbeat", {"cursor": declared._position(seen)})
                elif seen < declared._dropped:
                    # What the stream had yet to send was let go: a gap, from the oldest still kept.
                    seen = declared._dropped
                    await say(
                        "active",
                        {"cursor": declared._position(seen), "truncated": True},
                    )
                elif (kept := declared._next(seen)) is not None:
                    seen = kept.sequence
                    await send(kept)
                else:
                    with anyio.move_on_after(beat - time.monotonic()):
                        await wake.wait()
            while (
                kept := declared._next(seen)
            ) is not None and kept.sequence <= hold.until:
                seen = kept.sequence
                await send(kept)
            if hold.ended is not None:
                await say(
                    "terminated",
                    {
                        "error": hold.ended.model_dump(
                            by_alias=True, exclude_none=True, mode="json"
                        )
                    },
                )
        except _Stalled:
            logger.info(
                "Closed a stream of %s whose reader took nothing for two heartbeats",
                declared.name,
            )
        finally:
            self._close(hold)
        # The final frame, sent when the server ends the stream; when the client did, nobody is listening.
        return {"_meta": {}}


# How long a poll or a list waits for its answer, and a connection takes to open, in seconds.
_TIMEOUT = 30.0
# A stream that has said nothing for twice the longest heartbeat interval is dead: https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md#lifecycle
_QUIET = 60.0


class _Answer(BaseModel):
    model_config = ConfigDict(extra="allow")


@asynccontextmanager
async def _ordered(
    transport: Transport, notified: Callable[[str, dict[str, Any]], Awaitable[None]]
) -> AsyncIterator[TransportStreams]:
    """The transport, with MCP Events' notifications handed to ``notified`` as they arrive.

    The MCP client gives each notification a task of its own, which loses their order, so these are taken out
    before it sees them. A response comes after its request's notifications, and waits until they are handled.
    """
    async with transport as (read, write):
        forward, forwarded = anyio.create_memory_object_stream[
            SessionMessage | Exception
        ]()

        async def pump() -> None:
            async with forward:
                async for message in read:
                    if (
                        isinstance(message, SessionMessage)
                        and isinstance(message.message, JSONRPCNotification)
                        and message.message.method.startswith("notifications/events/")
                    ):
                        await notified(
                            message.message.method, message.message.params or {}
                        )
                    else:
                        await forward.send(message)

        async with anyio.create_task_group() as group:
            group.start_soon(pump)
            yield forwarded, write
            group.cancel_scope.cancel()


async def _request(
    client: Client,
    method: str,
    params: dict[str, Any],
    timeout: float | None = _TIMEOUT,
) -> dict[str, Any]:
    answer = await client.session.send_request(
        Request[dict[str, Any], str](method=method, params=params),
        _Answer,
        request_read_timeout_seconds=timeout,
    )
    return answer.model_extra or {}


# A server's refusal ends a subscription: the event type is gone, access was lost, the request is not valid, or the
# delivery mode is not offered. Anything else is tried again from the cursor.
def _refused(error: Exception) -> bool:
    return isinstance(error, MCPError) and error.code in (
        INVALID_PARAMS,
        NOT_FOUND,
        FORBIDDEN,
        UNSUPPORTED,
    )


class NoCompatibleDeliveryMode(Exception):
    """The sketch's: none of the delivery modes the app offers for an event type is one this subscription uses."""


class AppEvents:
    """The events of one app, as its client lists and subscribes to them: ``app.ext.events``."""

    def __init__(self, app: _App, contract: dict[str, Any] | None) -> None:
        self._app = app
        # The client checks an event's data against the contract it was built against, as it does a tool's output.
        self._payloads: dict[str, dict[str, Any]] = {
            event["name"]: event["payloadSchema"]
            for event in (contract or {}).get("events", [])
            if "payloadSchema" in event
        }

    async def list(self) -> list[dict[str, Any]]:
        """The event types the app offers, as ``events/list`` describes them."""
        events: list[dict[str, Any]] = []
        async with self._app._session(_TIMEOUT) as client:
            cursor = None
            while True:
                page = await _request(
                    client, "events/list", {} if cursor is None else {"cursor": cursor}
                )
                events += page.get("events", [])
                if not (cursor := page.get("nextCursor")):
                    break
        return events

    def subscribe(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        /,
        *,
        cursor: str | None = None,
        max_age: float | None = None,
        delivery: Delivery | None = None,
        on_gap: Callable[[], object] | None = None,
    ) -> Subscription[Any]:
        """Subscribe to an event type of the app.

        Args:
            name: The event type.
            arguments: The subscription's arguments, as the type's ``inputSchema`` describes them.
            cursor: Where to resume from, as a subscription's ``cursor`` was. From now unless given.
            max_age: Seconds: events older than this are not replayed from ``cursor``.
            delivery: ``"push"`` or ``"poll"``. Push when the app offers it, else poll, unless given. One the app
                does not offer raises :class:`NoCompatibleDeliveryMode`.
            on_gap: Called when events were skipped, before the events after the gap.

        Returns:
            A :class:`Subscription` to enter with ``async with`` and iterate.
        """
        return Subscription(
            self, name, dict(arguments or {}), cursor, max_age, delivery, on_gap
        )


@dataclass(frozen=True)
class _Gap:
    pass


@dataclass(frozen=True)
class _Reached:
    cursor: str | None


class Subscription(Generic[D]):
    """A subscription to one event type, from ``app.ext.events.subscribe``.

    Entering it with ``async with`` subscribes, and returns once the app has confirmed the subscription; a refusal
    raises ``MCPError``. Iterating it yields each :class:`Event` once, in order, dropping one whose
    ``event_id`` was seen. A dropped connection is reconnected, and resumes from the cursor. :meth:`cancel`, leaving
    the block, or cancelling its task ends the subscription: breaking out of ``async for`` alone does not. Iteration
    raises ``MCPError`` when the app terminates it.

    ``cursor`` is the position after the last event iterated, to persist and resume from. ``delivery`` is how
    events arrive, once entered.

    Example:
        ``async with app.ext.events.subscribe("board.changed", {"board": "b1"}) as events:``
        ``    async for event in events: ...``
    """

    def __init__(
        self,
        events: AppEvents,
        name: str,
        arguments: dict[str, Any],
        cursor: str | None,
        max_age: float | None,
        delivery: Delivery | None,
        on_gap: Callable[[], object] | None,
    ) -> None:
        self.cursor = cursor
        self.delivery = delivery
        self._forced = delivery is not None
        self._events = events
        self._app = events._app
        self._name = name
        self._request: dict[str, Any] = {"name": name, "arguments": arguments}
        if max_age is not None:
            self._request["maxAgeMs"] = round(max_age * 1000)
        self._on_gap = on_gap
        # Where the next request resumes from, ahead of `cursor` by what waits to be iterated.
        self._reached = cursor
        # Set once the app has confirmed the subscription, or it has failed first.
        self._settled = anyio.Event()
        self._active = False
        self._failure: Exception | None = None
        self._seen: dict[str, None] = {}
        # Nothing waits between the server and the iterator, so a reader that stops reading stops the stream.
        self._send, self._receive = anyio.create_memory_object_stream[
            Event[Any] | _Gap | _Reached
        ]()
        self._group: anyio.abc.TaskGroup | None = None
        self._running = anyio.CancelScope()
        # The open stream request, which a notification ends when the app ended the subscription.
        self._stream: anyio.CancelScope | None = None
        self._ended: Exception | None = None

    async def __aenter__(self) -> Subscription[D]:
        listed = [
            each for each in await self._events.list() if each["name"] == self._name
        ]
        # A type the app does not list is refused by the app, with NotFound.
        offered: list[str] = listed[0].get("delivery", []) if listed else ["poll"]
        wanted: list[Delivery] = [self.delivery] if self.delivery else ["push", "poll"]
        if not (usable := [mode for mode in wanted if mode in offered]):
            raise NoCompatibleDeliveryMode(
                f"{self._app._name} does not deliver {self._name} by {' or '.join(wanted)}."
            )
        self.delivery = usable[0]
        self._group = anyio.create_task_group()
        await self._group.__aenter__()
        self._group.start_soon(self._run)
        try:
            await self._settled.wait()
        except BaseException:
            await self._close()
            raise
        if not self._active:
            await self._close()
            assert self._failure is not None
            raise self._failure
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._close()

    async def cancel(self) -> None:
        """The sketch's ``sub.cancel()``: ends the subscription, and iteration stops. Leaving the block does too."""
        # The block's own code runs inside the task group, so only the work behind the iterator is cancelled here.
        self._running.cancel()
        self._receive.close()

    async def _close(self) -> None:
        if self._group is not None:
            group, self._group = self._group, None
            group.cancel_scope.cancel()
            await group.__aexit__(None, None, None)
        self._receive.close()

    def __aiter__(self) -> Subscription[D]:
        return self

    async def __anext__(self) -> Event[D]:
        while True:
            try:
                item = await self._receive.receive()
            except (anyio.EndOfStream, anyio.ClosedResourceError):
                if self._failure is not None:
                    raise self._failure from None
                raise StopAsyncIteration from None
            if isinstance(item, _Gap):
                if self._on_gap is not None:
                    self._on_gap()
            elif isinstance(item, _Reached):
                self.cursor = item.cursor
            else:
                if item.cursor is not None:
                    self.cursor = item.cursor
                return item

    async def _run(self) -> None:
        with self._running:
            try:
                async with self._app._session(
                    _TIMEOUT, partial(_ordered, notified=self._notified)
                ) as client:
                    if self.delivery == "push":
                        try:
                            await self._push(client)
                        except MCPError as error:
                            # An endpoint that cannot stream refuses push, and the subscription polls instead.
                            if (
                                self._forced
                                or self._active
                                or error.code != UNSUPPORTED
                            ):
                                raise
                            self.delivery = "poll"
                    await self._poll(client)
            except Exception as error:  # noqa: BLE001
                self._failure = error
            finally:
                self._settled.set()
                self._send.close()

    def _confirm(self) -> None:
        self._active = True
        self._settled.set()

    async def _reach(self, cursor: str | None) -> None:
        self._reached = cursor
        await self._send.send(_Reached(cursor))

    async def _deliver(self, occurrence: dict[str, Any]) -> None:
        cursor = occurrence.get("cursor")
        if cursor is not None:
            self._reached = cursor
        event_id = occurrence.get("eventId")
        if event_id in self._seen:
            if cursor is not None:
                await self._send.send(_Reached(cursor))
            return
        if event_id is not None:
            self._seen[event_id] = None
            if len(self._seen) > 1024:
                del self._seen[next(iter(self._seen))]
        data = occurrence.get("data")
        if (schema := self._events._payloads.get(self._name)) is not None:
            self._app._validate(
                "events/" + self._name,
                schema,
                data,
                f"data of {self._app._name} {self._name}",
            )
        # A pushed event's `_meta` also routes it to its stream, which is not the event's own.
        meta = {
            key: value
            for key, value in (occurrence.get("_meta") or {}).items()
            if key != SUBSCRIPTION_ID_META_KEY
        }
        await self._send.send(
            Event(
                data,
                event_id,
                occurrence.get("timestamp"),
                cursor,
                meta or None,
                occurrence.get("name"),
            )
        )

    async def _poll(self, client: Client) -> None:
        while True:
            wait = 1.0
            try:
                result = await _request(
                    client, "events/poll", {**self._request, "cursor": self._reached}
                )
            except Exception as error:
                if not self._active or _refused(error):
                    raise
            else:
                self._confirm()
                # The gap comes before the events after it.
                if result.get("truncated"):
                    await self._send.send(_Gap())
                for occurrence in result.get("events", []):
                    await self._deliver(occurrence)
                await self._reach(result.get("cursor"))
                # A server that asks for a tight loop is not obeyed.
                next_poll = (result.get("nextPollMs") or 0) / 1000
                wait = 0 if result.get("hasMore") else max(1.0, next_poll)
            await anyio.sleep(wait)

    async def _push(self, client: Client) -> None:
        while True:
            with anyio.CancelScope() as self._stream:
                self._stream.deadline = anyio.current_time() + _QUIET
                try:
                    await _request(
                        client,
                        "events/stream",
                        {**self._request, "cursor": self._reached},
                        None,
                    )
                except Exception as error:
                    if not self._active or _refused(error):
                        raise
            if self._ended is not None:
                raise self._ended
            if self._stream.cancelled_caught and not self._active:
                raise TimeoutError(
                    f"{self._app._name} did not confirm the subscription to {self._name}."
                )
            await anyio.sleep(1)

    # Told each notification of the open stream, in order. The stream is not quiet while one is being handled,
    # since a reader that has not taken the last event holds it up.
    async def _notified(self, method: str, params: dict[str, Any]) -> None:
        stream = self._stream
        if stream is not None:
            stream.deadline = math.inf
        try:
            if method == "notifications/events/active":
                self._confirm()
                if params.get("truncated"):
                    await self._send.send(_Gap())
                await self._reach(params.get("cursor"))
            elif method == "notifications/events/event":
                await self._deliver(params)
            elif method == "notifications/events/heartbeat":
                await self._reach(params.get("cursor"))
            elif method == "notifications/events/terminated":
                error = ErrorData.model_validate(params.get("error") or {})
                raise MCPError.from_error_data(error)
            # notifications/events/error: the server recovers by itself, and the stream stays open.
        except Exception as error:  # noqa: BLE001
            self._ended = error
            if stream is not None:
                stream.cancel()
        else:
            if stream is not None:
                stream.deadline = anyio.current_time() + _QUIET
