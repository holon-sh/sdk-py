from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from typing import (
    Any,
    Generic,
    Literal,
    NotRequired,
    Protocol,
    TypeAlias,
    TypedDict,
    TypeVar,
)

import httpx2
import jsonschema
from jsonschema.protocols import Validator
from mcp import Client
from mcp.client import Transport
from mcp.client.session import ClientRequestContext
from mcp.client.streamable_http import streamable_http_client
from mcp.types import (
    LATEST_PROTOCOL_VERSION,
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
from referencing import Registry

from holon.ext.events import AppEvents
from holon.ext.files._client import (
    FILES,
    OutcomeUnknownError,
    Transfers,
    holds_file,
    remember,
)
from holon.ext.files._wire import CALL_RESULT, UploadRequiredResult

__all__ = [
    "Apps",
    "Binding",
    "CallOptions",
    "CompleteResult",
    "ToolCallError",
    "ToolResult",
]

Output = TypeVar("Output")


class Binding(TypedDict):
    """Connection settings for an MCP dependency.

    Attributes:
        endpoint: Streamable HTTP MCP URL.
        http_client: Returns a new ``httpx2.AsyncClient`` for each call, e.g. to authenticate.
        file_origins: The only origins file transfers may use. Any public https origin when unset.
    """

    endpoint: str
    http_client: NotRequired[Callable[[], httpx2.AsyncClient]]
    file_origins: NotRequired[Sequence[str]]


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


class CallOptions(TypedDict, total=False):
    """Options for one call, passed positionally before the tool's arguments."""

    timeout: float | None
    input_responses: InputResponses | None
    request_state: str | None
    meta: RequestParamsMeta | None
    elicit: Callable[[ElicitRequestParams], Awaitable[ElicitResult]]


class ToolCallError(RuntimeError):
    """Raised by ``tools.<name>`` when a tool fails or needs input.

    Attributes:
        result: The error or input-required result.
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
    client: Client,
    name: str,
    arguments: dict[str, Any],
    options: CallOptions,
    transfers: Transfers,
) -> CallToolResult | InputRequiredResult:
    elicit = options.get("elicit")
    responses, state = options.get("input_responses"), options.get("request_state")
    answered = 0
    while True:
        try:
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
                CALL_RESULT,
                request_read_timeout_seconds=options.get("timeout"),
            )
        except Exception as error:
            if transfers.uploaded:
                raise OutcomeUnknownError(
                    f"{transfers.label}: the outcome is unknown; the call failed after its files were sent"
                ) from error
            raise
        if isinstance(result, UploadRequiredResult):
            responses, state = None, await transfers.send(result)
            continue
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
        binding: Binding | None,
    ) -> None:
        self._name = name
        self._identity = identity
        self._binding = binding
        self._validators: dict[str, Validator] = {}
        self._definitions: dict[str, dict[str, Any]] = (
            {}
            if contract is None
            else {tool["name"]: tool for tool in contract["tools"]}
        )
        self.tools = SimpleNamespace(
            **{
                name: partial(self._call_tool, tool)
                for name, tool in self._definitions.items()
                if not name.startswith("_")
            }
        )
        self.ext = SimpleNamespace(events=AppEvents(self, contract))

    async def call_tool(
        self,
        name: str,
        options: CallOptions | None = None,
        /,
        **arguments: object,
    ) -> CallToolResult | InputRequiredResult:
        """Call a tool and return its full MCP result.

        Args:
            name: Tool name.
            options: Timeout, input responses, request state, metadata, and ``elicit``.
            **arguments: Tool input. Pass a ``holon.ext.files.File`` where the tool takes a file.

        Returns:
            The complete result, or an input-required result for questions left unanswered.

        Raises:
            ValueError: The output does not match the contract, or the dependency is undeclared.
            RuntimeError: The dependency has no endpoint.
            holon.ext.files.FileTransferError: A file was not sent; the tool did not run.
            holon.ext.files.OutcomeUnknownError: The call failed after its files were sent.
        """
        options = options or {}
        definition = self._definitions.get(name, {})
        transfers = Transfers(
            f"{self._name}.{name}",
            self._binding.get("file_origins") if self._binding else None,
        )
        async with self._session(options.get("timeout"), transfers=transfers) as client:
            if holds_file(arguments):
                schema = definition.get("inputSchema")
                arguments = await transfers.prepare(client, name, schema, arguments)
            result = await _send(client, name, arguments, options, transfers)
        if isinstance(result, CallToolResult):
            remember(result, transfers)
            if not result.is_error and "outputSchema" in definition:
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

    # Raises an error from the block as itself, not in the session's ExceptionGroup.
    @asynccontextmanager
    async def _session(
        self,
        timeout: float | None,
        wrap: Callable[[Transport], Transport] | None = None,
        transfers: Transfers | None = None,
    ) -> AsyncIterator[Client]:
        endpoint, http_client = self._connection(timeout)
        failure: Exception | None = None
        try:
            async with http_client() as http:
                if transfers is not None:
                    transfers.bind(endpoint, http_client, http)
                transport: Transport = streamable_http_client(
                    endpoint, http_client=http, max_sse_event_size=None
                )
                if wrap is not None:
                    transport = wrap(transport)
                async with Client(
                    transport,
                    client_info=Implementation(
                        name=self._identity["name"], version=self._identity["version"]
                    ),
                    mode=LATEST_PROTOCOL_VERSION,
                    elicitation_callback=_declare_elicitation,
                    extensions=[FILES],
                ) as client:
                    try:
                        yield client
                    except Exception as error:  # noqa: BLE001
                        failure = error
        except Exception:
            if failure is None:
                raise
        if failure is not None:
            raise failure

    def _validate(
        self, name: str, schema: dict[str, Any], value: Any, label: str
    ) -> None:
        validator = self._validators.get(name)
        if validator is None:
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


class Apps:
    """An application's dependencies, as generated clients use them.

    Applications use ``apps`` or ``create_apps`` from ``holon.apps``. Each dependency is ``apps.<name>``
    or ``apps[name]``: its ``tools.<name>`` return structured output or raise :class:`ToolCallError`,
    and its ``call_tool`` returns the full result.

    Args:
        identity: Application name, version, and ``requires``.
        contracts: Dependency contracts by name.
        bindings: Connection settings by name. When given, only these dependencies are available.

    Raises:
        ValueError: A binding names an unknown dependency.
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
