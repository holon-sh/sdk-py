from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from types import SimpleNamespace
from typing import Any, Generic, Literal, Protocol, TypeAlias, TypedDict, TypeVar

import httpx2
import jsonschema  # type: ignore[import-untyped,unused-ignore]
from mcp import Client
from mcp.client.session import ClientRequestContext
from mcp.client.streamable_http import streamable_http_client
from mcp.types import (
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

from holon.binding import Binding, resolve_default_binding

__all__ = [
    "Apps",
    "Binding",
    "CallOptions",
    "CompleteResult",
    "ToolCallError",
    "ToolResult",
]

Output = TypeVar("Output")


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
# MCP's own client timeouts: a server may hold the response stream open while a tool runs.
_http_client = partial(
    httpx2.AsyncClient, trust_env=False, timeout=httpx2.Timeout(30.0, read=300.0)
)


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


def validate(schema: dict[str, Any], value: object, label: str) -> None:
    try:
        jsonschema.validate(
            value,
            schema,
            registry=Registry(),
            format_checker=jsonschema.FormatChecker(),
        )
    except jsonschema.ValidationError as error:
        raise ValueError(f"Invalid {label}: {error.message}") from error


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
        self._definitions = (
            None
            if contract is None
            else {tool["name"]: tool for tool in contract["tools"]}
        )
        self.tools = SimpleNamespace(
            **{
                name: partial(self._call_tool, tool)
                for name, tool in (self._definitions or {}).items()
                if not name.startswith("_")
            }
        )

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
        binding = self._binding or resolve_default_binding(self._name)
        if binding is None:
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
            binding = {
                "endpoint": endpoint,
                "http_client": partial(_http_client, headers=headers),
            }
        failure: Exception | None = None
        async with binding.get("http_client", _http_client)() as http:
            transport = streamable_http_client(binding["endpoint"], http_client=http)
            async with Client(
                transport,
                client_info=Implementation(
                    name=self._identity["name"], version=self._identity["version"]
                ),
                mode="2026-07-28",
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
            validate(
                definition["outputSchema"],
                result.structured_content,
                f"structured output from {self._name}.{name}",
            )
        return result

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
