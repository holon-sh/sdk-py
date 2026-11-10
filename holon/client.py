from __future__ import annotations

import math
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from types import SimpleNamespace, TracebackType
from typing import Any, Generic, Literal, Protocol, TypeAlias, TypedDict, TypeVar

import anyio
import httpx2
import jsonschema
from mcp import Client
from mcp.client import Transport
from mcp.client._transport import TransportStreams
from mcp.client.session import ClientRequestContext
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError
from mcp.shared.message import SessionMessage
from mcp.shared.subscriptions import SUBSCRIPTION_ID_META_KEY
from mcp.types import (
    INVALID_PARAMS,
    CallToolRequest,
    CallToolRequestParams,
    CallToolResult,
    ContentBlock,
    ElicitRequest,
    ElicitRequestParams,
    ElicitResult,
    ErrorData,
    Implementation,
    InputRequiredResult,
    InputResponses,
    JSONRPCNotification,
    Request,
    RequestParamsMeta,
    TextContent,
)
from pydantic import BaseModel, ConfigDict, TypeAdapter
from referencing import Registry

from holon.binding import Binding
from holon.events import FORBIDDEN, NOT_FOUND, UNSUPPORTED, Event

__all__ = [
    "Apps",
    "Binding",
    "CallOptions",
    "CompleteResult",
    "Subscription",
    "ToolCallError",
    "ToolResult",
]

Output = TypeVar("Output")
Data = TypeVar("Data")
_VERSION = "2026-07-28"


class CompleteResult(Protocol, Generic[Output]):
    """Successful tool result with contract-typed structured content."""

    result_type: Literal["complete"]
    is_error: Literal[False]
    structured_content: Output
    content: list[ContentBlock]
    meta: dict[str, object] | None


class ErrorResult(Protocol):
    """Completed tool result reporting an application error."""

    result_type: Literal["complete"]
    is_error: Literal[True]
    structured_content: object
    content: list[ContentBlock]
    meta: dict[str, object] | None


ToolResult: TypeAlias = CompleteResult[Output] | ErrorResult | InputRequiredResult

_result = TypeAdapter(CallToolResult | InputRequiredResult)


class CallOptions(TypedDict, total=False):
    """Request controls, passed positionally to keep tool keywords available."""

    timeout: float | None
    input_responses: InputResponses | None
    request_state: str | None
    meta: RequestParamsMeta | None
    elicit: Callable[[ElicitRequestParams], Awaitable[ElicitResult]]


class ToolCallError(RuntimeError):
    """Raised by ``tools.<name>`` for tool errors or additional input requests.

    Attributes:
        result: Original MCP error or input-required result. Pass ``elicit`` to
            answer questions in place, or continue later through ``call_tool``.
    """

    def __init__(
        self, app: str, tool: str, result: CallToolResult | InputRequiredResult
    ) -> None:
        self.result = result
        if isinstance(result, InputRequiredResult):
            message = "additional input required"
            detail = " ".join(
                f"{request.params.message} {getattr(request.params, 'url', '')}".strip()
                for request in (result.input_requests or {}).values()
                if isinstance(request, ElicitRequest)
            )
        else:
            message = "tool failed"
            detail = next(
                (part.text for part in result.content if isinstance(part, TextContent)),
                "",
            )
        if detail:
            message = f"{message}: {detail}"
        super().__init__(f"{app}.{tool}: {message}")


async def _declare_elicitation(
    context: ClientRequestContext, params: ElicitRequestParams
) -> ElicitResult:
    return ElicitResult(action="decline")


async def _send(
    client: Client, name: str, arguments: dict[str, Any], options: CallOptions
) -> CallToolResult | InputRequiredResult:
    elicit = options.get("elicit")
    responses, state = options.get("input_responses"), options.get("request_state")
    answered = 0
    while True:
        result = await client.session.send_request(
            CallToolRequest(
                params=CallToolRequestParams(
                    name=name,
                    arguments=arguments,
                    input_responses=responses,
                    request_state=state,
                    _meta=options.get("meta"),
                )
            ),
            _result,
            request_read_timeout_seconds=options.get("timeout"),
        )
        if not (elicit and answered < 10 and isinstance(result, InputRequiredResult)):
            return result
        answered += 1
        responses = {
            key: await elicit(request.params)
            for key, request in (result.input_requests or {}).items()
            if isinstance(request, ElicitRequest)
        }
        state = result.request_state


class _App:
    def __init__(
        self,
        name: str,
        identity: dict[str, Any],
        contract: dict[str, Any] | None,
        binding: Binding | None = None,
    ) -> None:
        self._name = name
        self._identity = identity
        self._binding = binding
        self._validators: dict[str, Any] = {}
        self._definitions = (
            None
            if contract is None
            else {tool["name"]: tool for tool in contract["tools"]}
        )
        self._payloads = {
            event["name"]: event["payloadSchema"]
            for event in (contract or {}).get("events", [])
            if "payloadSchema" in event
        }
        self.tools = SimpleNamespace(
            **{
                name: partial(self._call_tool, tool)
                for name, tool in (self._definitions or {}).items()
                if not name.startswith("_")
            }
        )
        self.events = _Events(self)

    async def call_tool(
        self,
        name: str,
        options: CallOptions | None = None,
        /,
        **arguments: object,
    ) -> CallToolResult | InputRequiredResult:
        """Call a dependency tool and return its full MCP result.

        Args:
            name: Tool name. Contracted dependencies reject unknown tools.
            options: Request timeout in seconds, input responses, continuation
                state, metadata, and ``elicit`` to answer the server's questions in
                place. Passed positionally, separate from tool input.
            **arguments: Tool input, which the server validates.

        Returns:
            A complete result, or an input-required result for a question left
            unanswered. Tool error results are returned unchanged; successful
            structured output is contract-validated.

        Raises:
            ValueError: Unknown tool, invalid output, or undeclared dependency.
            RuntimeError: Endpoint resolution failed.

        Transport and protocol exceptions propagate. Each call closes its HTTP
        client and MCP session, including when an exception occurs.
        """
        options = options or {}
        definition = None
        if self._definitions is not None:
            definition = self._definitions.get(name)
            if definition is None:
                raise ValueError(f"Unknown tool {self._name}.{name}.")
        failure: Exception | None = None
        async with self._session(options.get("timeout")) as client:
            try:
                result = await _send(client, name, arguments, options)
            except Exception as error:  # noqa: BLE001
                failure = error
        if failure is not None:
            raise failure
        if (
            definition is not None
            and isinstance(result, CallToolResult)
            and not result.is_error
            and "outputSchema" in definition
        ):
            self._validate(
                name,
                definition["outputSchema"],
                result.structured_content,
                f"structured output from {self._name}.{name}",
            )
        return result

    def _connection(
        self, timeout: float | None
    ) -> tuple[str, Callable[[], httpx2.AsyncClient]]:
        http_client = partial(
            httpx2.AsyncClient,
            trust_env=False,
            timeout=httpx2.Timeout(timeout, read=None),
        )
        if self._binding is not None:
            return self._binding["endpoint"], self._binding.get(
                "http_client", http_client
            )
        if self._name not in self._identity["requires"]:
            raise ValueError(
                f"Dependency {self._name} is not declared by this application."
            )
        prefix = self._name.upper().replace("-", "_") + "_MCP_"
        endpoint = os.environ.get(prefix + "URL")
        if not endpoint:
            raise RuntimeError(
                f"Dependency {self._name} is unavailable ({prefix}URL). Set the endpoint or supply a binding."
            )
        token = os.environ.get(prefix + "TOKEN")
        headers = {"Authorization": f"Bearer {token}"} if token else None
        return endpoint, partial(http_client, headers=headers)

    @asynccontextmanager
    async def _session(
        self, timeout: float | None, notified: _Notified | None = None
    ) -> AsyncIterator[Client]:
        endpoint, http_client = self._connection(timeout)
        async with http_client() as http:
            transport: Transport = streamable_http_client(
                endpoint, http_client=http, max_sse_event_size=None
            )
            if notified is not None:
                transport = _ordered(transport, notified)
            async with Client(
                transport,
                client_info=Implementation(
                    name=self._identity["name"], version=self._identity["version"]
                ),
                mode=_VERSION,
                elicitation_callback=_declare_elicitation,
            ) as client:
                yield client

    def _validate(
        self, name: str, schema: dict[str, Any], value: Any, label: str
    ) -> None:
        validator = self._validators.get(name)
        if validator is None or validator.schema is not schema:
            cls = jsonschema.validators.validator_for(schema)
            cls.check_schema(schema)
            validator = self._validators[name] = cls(
                schema, registry=Registry(), format_checker=jsonschema.FormatChecker()
            )
        if (
            error := jsonschema.exceptions.best_match(validator.iter_errors(value))
        ) is not None:
            raise ValueError(f"Invalid {label}: {error.message}") from error

    async def _call_tool(
        self,
        tool: dict[str, Any],
        options: CallOptions | None = None,
        /,
        **arguments: object,
    ) -> object:
        result = await self.call_tool(tool["name"], options, **arguments)
        if isinstance(result, InputRequiredResult) or result.is_error:
            raise ToolCallError(self._name, tool["name"], result)
        return result.structured_content if "outputSchema" in tool else result


_Notified: TypeAlias = Callable[[str, dict[str, Any]], Awaitable[None]]
# How long a poll or a list waits for its answer, and a connection takes to open, in seconds.
_TIMEOUT = 30.0
# A stream that has said nothing for twice the longest heartbeat interval is dead: https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md#lifecycle
_QUIET = 60.0


class _Answer(BaseModel):
    model_config = ConfigDict(extra="allow")


@asynccontextmanager
async def _ordered(
    transport: Transport, notified: _Notified
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


class _Events:
    """MCP Events of one app, the draft extension of the Triggers & Events Working Group."""

    def __init__(self, app: _App) -> None:
        self._app = app

    async def list(self) -> list[dict[str, Any]]:
        """The event types the app offers, as ``events/list`` describes them."""
        events: list[dict[str, Any]] = []
        failure: Exception | None = None
        async with self._app._session(_TIMEOUT) as client:
            try:
                cursor = None
                while True:
                    page = await _request(
                        client,
                        "events/list",
                        {} if cursor is None else {"cursor": cursor},
                    )
                    events += page.get("events", [])
                    if not (cursor := page.get("nextCursor")):
                        break
            except Exception as error:  # noqa: BLE001
                failure = error
        if failure is not None:
            raise failure
        return events

    def subscribe(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        /,
        *,
        cursor: str | None = None,
        max_age: float | None = None,
        delivery: Literal["poll", "push"] | None = None,
        on_gap: Callable[[], object] | None = None,
    ) -> Subscription[Any]:
        """Subscribe to an event type of the app.

        Args:
            name: The event type.
            arguments: The subscription's arguments, as the type's ``inputSchema`` describes them.
            cursor: Where to resume from, as a subscription's ``cursor`` was. From now unless given.
            max_age: Seconds: events older than this are not replayed from ``cursor``.
            delivery: ``"push"`` or ``"poll"``. Push when the app offers it, else poll, unless given.
            on_gap: Called when events were skipped, before the events after the gap.

        Returns:
            A :class:`Subscription` to enter with ``async with`` and iterate.
        """
        return Subscription(
            self._app, name, dict(arguments or {}), cursor, max_age, delivery, on_gap
        )


@dataclass(frozen=True)
class _Gap:
    pass


@dataclass(frozen=True)
class _Reached:
    cursor: str | None


class Subscription(Generic[Data]):
    """A subscription to one event type, from ``app.events.subscribe``.

    Entering it with ``async with`` subscribes, and returns once the app has confirmed the subscription; a refusal
    raises ``MCPError``. Iterating it yields each :class:`~holon.events.Event` once, in order, dropping one whose
    ``event_id`` was seen. A dropped connection is reconnected, and resumes from the cursor. Leaving the block, or
    cancelling its task, is what ends the subscription: breaking out of ``async for`` alone does not. Iteration
    raises ``MCPError`` when the app ends it.

    ``cursor`` is the position after the last event iterated, to persist and resume from. ``delivery`` is how
    events arrive, once entered.

    Example:
        ``async with app.events.subscribe("board.changed", {"board": "b1"}) as events:``
        ``    async for event in events: ...``
    """

    def __init__(
        self,
        app: _App,
        name: str,
        arguments: dict[str, Any],
        cursor: str | None,
        max_age: float | None,
        delivery: Literal["poll", "push"] | None,
        on_gap: Callable[[], object] | None,
    ) -> None:
        self.cursor = cursor
        self.delivery = delivery
        self._forced = delivery is not None
        self._app = app
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
        # The open stream request, which a notification ends when the app ended the subscription.
        self._stream: anyio.CancelScope | None = None
        self._ended: Exception | None = None

    async def __aenter__(self) -> Subscription[Data]:
        if self.delivery is None:
            offered: list[str] = next(
                (
                    each.get("delivery", [])
                    for each in await self._app.events.list()
                    if each["name"] == self._name
                ),
                [],
            )
            self.delivery = "push" if "push" in offered else "poll"
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

    async def _close(self) -> None:
        if self._group is not None:
            group, self._group = self._group, None
            group.cancel_scope.cancel()
            await group.__aexit__(None, None, None)
        self._receive.close()

    def __aiter__(self) -> Subscription[Data]:
        return self

    async def __anext__(self) -> Event[Data]:
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
        try:
            async with self._app._session(_TIMEOUT, self._notified) as client:
                try:
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
        except Exception as error:  # noqa: BLE001
            self._failure = self._failure or error
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
        if (schema := self._app._payloads.get(self._name)) is not None:
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


class Apps:
    """Create the dependency collection used by generated Python clients.

    Applications typically use ``apps`` or ``create_apps`` from ``holon.apps``.

    Args:
        identity: Application name, version, and declared ``requires`` mapping.
        contracts: Dependency contracts with a ``tools`` list, or ``None`` for
            untyped dependencies.
        bindings: Explicit bindings. When supplied, only these dependencies are
            included; an empty mapping selects none. Otherwise all declared
            dependencies and contracts are available.

    Raises:
        ValueError: A binding names an unknown dependency.

    Access a client with ``clients[name]`` or ``clients.name``. Its
    ``tools.<name>(**arguments)`` methods return structured output, or the
    complete result for a tool without an output schema, and raise
    :class:`ToolCallError` for tool errors or questions left unanswered.
    Use its ``call_tool`` method for the full result envelope.
    """

    def __init__(
        self,
        identity: dict[str, Any],
        contracts: dict[str, Any],
        bindings: Mapping[str, Binding] | None = None,
    ) -> None:
        available = dict.fromkeys(identity["requires"]) | contracts
        self._apps = {}
        for name in available if bindings is None else bindings:
            if name not in available:
                raise ValueError(f"Unknown app binding: {name}")
            self._apps[name] = _App(
                name, identity, available[name], bindings[name] if bindings else None
            )

    def __getitem__(self, name: str) -> _App:
        return self._apps[name]

    def __getattr__(self, name: str) -> _App:
        if name.startswith("_") or name not in self._apps:
            raise AttributeError(name)
        return self._apps[name]
