"""Client side: list an app's event types and subscribe to them."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic

import anyio
from mcp import Client
from mcp.client import Transport
from mcp.client._transport import TransportStreams
from mcp.shared.exceptions import MCPError
from mcp.shared.message import SessionMessage
from mcp.shared.subscriptions import SUBSCRIPTION_ID_META_KEY
from mcp.types import INVALID_PARAMS, ErrorData, JSONRPCNotification, Request
from pydantic import BaseModel, ConfigDict

from holon.ext.events._wire import FORBIDDEN, NOT_FOUND, UNSUPPORTED, D, Delivery, Event

if TYPE_CHECKING:
    from holon.client import _App

_TIMEOUT = 30.0
# Twice the longest heartbeat interval: https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md#lifecycle
_QUIET = 60.0


class _Answer(BaseModel):
    model_config = ConfigDict(extra="allow")


@asynccontextmanager
async def _ordered(
    transport: Transport, notified: Callable[[str, dict[str, Any]], Awaitable[None]]
) -> AsyncIterator[TransportStreams]:
    """The transport, handing Events notifications to ``notified`` in the order they arrive.

    The MCP client handles each notification in a task of its own, which loses their order.
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


async def _list(client: Client) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    cursor = None
    while True:
        page = await _request(
            client, "events/list", {} if cursor is None else {"cursor": cursor}
        )
        events += page.get("events", [])
        if not (cursor := page.get("nextCursor")):
            return events


# Errors that end a subscription; anything else is retried from the cursor.
def _refused(error: Exception) -> bool:
    return isinstance(error, MCPError) and error.code in (
        INVALID_PARAMS,
        NOT_FOUND,
        FORBIDDEN,
        UNSUPPORTED,
    )


class NoCompatibleDeliveryMode(Exception):
    """The app offers no delivery mode this subscription can use."""


class AppEvents:
    """An app's events, as ``app.ext.events``."""

    def __init__(self, app: _App, contract: dict[str, Any] | None) -> None:
        self._app = app
        self._payloads: dict[str, dict[str, Any]] = {
            event["name"]: event["payloadSchema"]
            for event in (contract or {}).get("events", [])
            if "payloadSchema" in event
        }

    async def list(self) -> list[dict[str, Any]]:
        """The event types the app offers."""
        async with self._app._session(_TIMEOUT) as client:
            return await _list(client)

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
        """Subscribe to an event type.

        Args:
            name: The event type.
            arguments: The subscription's arguments.
            cursor: Where to resume from. Defaults to now.
            max_age: Seconds; older events are not replayed.
            delivery: ``"push"`` or ``"poll"``, which the app refuses when it does not offer it.
                Defaults to push when offered.
            on_gap: Called when events were skipped.

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
    """A subscription to one event type.

    Entering it subscribes. Iterating it yields each :class:`Event` once, in order, reconnecting as needed.
    Leaving the block or calling :meth:`cancel` ends it. ``cursor`` is the position after the last event
    iterated.

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
        self._app = events._app
        self._name = name
        self._payload = events._payloads.get(name)
        self._request: dict[str, Any] = {"name": name, "arguments": arguments}
        if max_age is not None:
            self._request["maxAgeMs"] = round(max_age * 1000)
        self._on_gap = on_gap
        # Where the next request resumes, ahead of `cursor`.
        self._reached = cursor
        self._settled = anyio.Event()
        self._active = False
        self._failure: Exception | None = None
        self._seen: dict[str, None] = {}
        # Unbuffered, so a reader that stops reading stops the stream.
        self._send, self._receive = anyio.create_memory_object_stream[
            Event[Any] | _Gap | _Reached
        ]()
        self._group: anyio.abc.TaskGroup | None = None
        self._running = anyio.CancelScope()
        self._stream: anyio.CancelScope | None = None
        self._ended: Exception | None = None

    async def __aenter__(self) -> Subscription[D]:
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
        """End the subscription."""
        # The caller's block runs in the task group, so cancel only the background work.
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
                    await self._follow(client)
            except Exception as error:  # noqa: BLE001
                self._failure = error
            finally:
                self._settled.set()
                self._send.close()

    async def _follow(self, client: Client) -> None:
        wanted: list[Delivery] = [self.delivery] if self.delivery else ["push", "poll"]
        # A named mode, like an unlisted type, is left for the app to refuse.
        offered: list[str] = list(wanted)
        if not self.delivery:
            listed = [
                each for each in await _list(client) if each["name"] == self._name
            ]
            offered = listed[0].get("delivery", []) if listed else ["poll"]
        modes = [mode for mode in wanted if mode in offered]
        if not modes:
            raise NoCompatibleDeliveryMode(
                f"{self._app._name} does not deliver {self._name} by {' or '.join(wanted)}."
            )
        self.delivery = modes[0]
        if self.delivery == "push":
            try:
                await self._push(client)
            except MCPError as error:
                # An endpoint that cannot stream refuses push, and the subscription polls instead.
                if "poll" not in modes or self._active or error.code != UNSUPPORTED:
                    raise
                self.delivery = "poll"
        await self._poll(client)

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
        if self._payload is not None:
            self._app._validate(
                "events/" + self._name,
                self._payload,
                data,
                f"data of {self._app._name} {self._name}",
            )
        # Drop the stream's routing key from the event's `_meta`.
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
                if result.get("truncated"):
                    await self._send.send(_Gap())
                for occurrence in result.get("events", []):
                    await self._deliver(occurrence)
                await self._reach(result.get("cursor"))
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

    # The stream is not quiet while a notification waits for the reader.
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
            # notifications/events/error leaves the stream open.
        except Exception as error:  # noqa: BLE001
            self._ended = error
            if stream is not None:
                stream.cancel()
        else:
            if stream is not None:
                stream.deadline = anyio.current_time() + _QUIET
