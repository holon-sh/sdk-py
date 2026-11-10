from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from types import SimpleNamespace, TracebackType
from typing import Any, Generic, Literal, Protocol, TypeAlias, TypedDict, TypeVar

import anyio
import httpx2
import jsonschema
from mcp import Client
from mcp.client.session import ClientRequestContext
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError
from mcp.shared.inbound import MCP_METHOD_HEADER, MCP_PROTOCOL_VERSION_HEADER
from mcp.types import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    PROTOCOL_VERSION_META_KEY,
    CallToolRequest,
    CallToolRequestParams,
    CallToolResult,
    ContentBlock,
    ElicitRequest,
    ElicitRequestParams,
    ElicitResult,
    Implementation,
    InputRequiredResult,
    InputResponses,
    RequestParamsMeta,
    TextContent,
)
from pydantic import TypeAdapter
from referencing import Registry

from holon.binding import Binding
from holon.events import SUBSCRIPTION_ID, Event

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
        endpoint, http_client = self._connection(options.get("timeout"))
        failure: Exception | None = None
        async with http_client() as http:
            transport = streamable_http_client(endpoint, http_client=http)
            async with Client(
                transport,
                client_info=Implementation(
                    name=self._identity["name"], version=self._identity["version"]
                ),
                mode=_VERSION,
                elicitation_callback=_declare_elicitation,
            ) as client:
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

    # MCP Events are requests outside the core protocol, sent here rather than through the MCP client, which hands
    # a request's notifications over one task each and so out of order. Notifications are passed to `notified` in
    # the order they arrive; the stream fails after `quiet` seconds with no message.
    async def _exchange(
        self,
        method: str,
        params: dict[str, Any],
        notified: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        quiet: float | None = None,
    ) -> dict[str, Any]:
        endpoint, http_client = self._connection(None)
        meta = {
            PROTOCOL_VERSION_META_KEY: _VERSION,
            CLIENT_INFO_META_KEY: {
                "name": self._identity["name"],
                "version": self._identity["version"],
            },
            CLIENT_CAPABILITIES_META_KEY: {},
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": {**params, "_meta": meta},
        }
        headers = {
            "accept": "application/json, text/event-stream",
            MCP_PROTOCOL_VERSION_HEADER: _VERSION,
            MCP_METHOD_HEADER: method,
        }
        async with (
            http_client() as http,
            http.stream("POST", endpoint, json=body, headers=headers) as response,
        ):
            if response.headers.get("content-type", "").startswith("text/event-stream"):
                frames = aiter(httpx2.EventSource(response, max_event_size=None))
                while True:
                    with anyio.fail_after(quiet):
                        frame = await anext(frames, None)
                    if frame is None:
                        raise ConnectionError(f"{method} ended without an answer")
                    message = json.loads(frame.data) if frame.data else {}
                    if "result" in message or "error" in message:
                        return _answer(message)
                    if notified is not None and "method" in message:
                        await notified(message["method"], message.get("params") or {})
            await response.aread()
            if not response.headers.get("content-type", "").startswith(
                "application/json"
            ):
                response.raise_for_status()
            return _answer(response.json())

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


def _answer(message: dict[str, Any]) -> dict[str, Any]:
    if (error := message.get("error")) is not None:
        raise MCPError(
            error.get("code", -32603), error.get("message", ""), error.get("data")
        )
    return message.get("result") or {}


# A server's refusal ends a subscription: the event type is gone, access was lost, the request is not valid, or the
# delivery mode is not offered. Anything else is tried again from the cursor.
def _refused(error: Exception) -> bool:
    if isinstance(error, httpx2.HTTPStatusError):
        return 400 <= error.response.status_code < 500
    return isinstance(error, MCPError) and error.code in (
        -32602,
        -32023,
        -32024,
        -32026,
    )


class _Terminated(Exception):
    def __init__(self, error: dict[str, Any]) -> None:
        super().__init__()
        self.error = error


# A stream that has said nothing for twice the longest heartbeat interval is dead: https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md#lifecycle
_QUIET = 60.0


class _Events:
    """MCP Events of one app, the draft extension of the Triggers & Events Working Group."""

    def __init__(self, app: _App) -> None:
        self._app = app

    async def list(self) -> list[dict[str, Any]]:
        """The event types the app offers, as ``events/list`` describes them."""
        events: list[dict[str, Any]] = []
        cursor = None
        while True:
            page = await self._app._exchange(
                "events/list", {} if cursor is None else {"cursor": cursor}
            )
            events += page.get("events", [])
            if not (cursor := page.get("nextCursor")):
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


class Subscription(Generic[Data]):
    """A subscription to one event type, from ``app.events.subscribe``.

    Entering it with ``async with`` subscribes, and returns once the app has confirmed the subscription; a refusal
    raises ``MCPError``. Iterating it yields each :class:`~holon.events.Event` once, in order, dropping one whose
    ``event_id`` was seen. A dropped connection is reconnected, and resumes from the cursor. The subscription ends
    when the block is left or its task cancelled; iteration raises ``MCPError`` when the app ends it.

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
            tuple[Event[Any] | None, str | None, bool]
        ]()
        self._group: anyio.abc.TaskGroup | None = None

    async def __aenter__(self) -> Subscription[Data]:
        offered = next(
            (
                each.get("delivery", [])
                for each in await self._app.events.list()
                if each["name"] == self._name
            ),
            None,
        )
        if offered is None:
            raise ValueError(f"{self._app._name} has no event {self._name}.")
        mode = self.delivery or next(
            (each for each in ("push", "poll") if each in offered), None
        )
        if mode not in offered:
            raise ValueError(
                f"{self._app._name} does not deliver {self._name} by {mode or 'poll or push'}."
            )
        self.delivery = mode
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
                event, cursor, gap = await self._receive.receive()
            except (anyio.EndOfStream, anyio.ClosedResourceError):
                if self._failure is not None:
                    raise self._failure from None
                raise StopAsyncIteration from None
            if gap and self._on_gap is not None:
                self._on_gap()
            if cursor is not None or event is None:
                self.cursor = cursor
            if event is not None:
                return event

    async def _run(self) -> None:
        try:
            await (self._push() if self.delivery == "push" else self._poll())
        except Exception as error:  # noqa: BLE001
            self._failure = error
        finally:
            self._settled.set()
            self._send.close()

    def _confirm(self) -> None:
        self._active = True
        self._settled.set()

    # The cursor of a batch is the position after it, given once the batch has been iterated.
    async def _reach(self, cursor: str | None, gap: bool = False) -> None:
        self._reached = cursor
        await self._send.send((None, cursor, gap))

    async def _deliver(self, occurrence: dict[str, Any], cursor: str | None) -> None:
        event_id = occurrence.get("eventId")
        if event_id in self._seen:
            if cursor is not None:
                await self._reach(cursor)
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
        if cursor is not None:
            self._reached = cursor
        # A pushed event's `_meta` also routes it to its stream, which is not the event's own.
        meta = {
            key: value
            for key, value in (occurrence.get("_meta") or {}).items()
            if key != SUBSCRIPTION_ID
        }
        event = Event(data, event_id, occurrence.get("timestamp"), cursor, meta or None)
        await self._send.send((event, cursor, False))

    async def _poll(self) -> None:
        while True:
            wait = 1.0
            try:
                result = await self._app._exchange(
                    "events/poll", {**self._request, "cursor": self._reached}
                )
            except Exception as error:
                if not self._active or _refused(error):
                    raise
            else:
                self._confirm()
                for occurrence in result.get("events", []):
                    await self._deliver(occurrence, None)
                await self._reach(result.get("cursor"), bool(result.get("truncated")))
                # A server that asks for a tight loop is not obeyed.
                next_poll = (result.get("nextPollMs") or 0) / 1000
                wait = 0 if result.get("hasMore") else max(1.0, next_poll)
            await anyio.sleep(wait)

    async def _push(self) -> None:
        async def notified(method: str, params: dict[str, Any]) -> None:
            if method == "notifications/events/active":
                self._confirm()
                await self._reach(params.get("cursor"), bool(params.get("truncated")))
            elif method == "notifications/events/event":
                await self._deliver(params, params.get("cursor"))
            elif method == "notifications/events/heartbeat":
                await self._reach(params.get("cursor"))
            elif method == "notifications/events/terminated":
                raise _Terminated(params.get("error") or {})
            # notifications/events/error: the server recovers by itself, and the stream stays open.

        while True:
            try:
                await self._app._exchange(
                    "events/stream",
                    {**self._request, "cursor": self._reached},
                    notified,
                    _QUIET,
                )
            except _Terminated as ended:
                error = ended.error
                raise MCPError(
                    error.get("code", -32603),
                    error.get("message", ""),
                    error.get("data"),
                ) from None
            except Exception as error:
                if not self._active or _refused(error):
                    raise
            await anyio.sleep(1)


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
