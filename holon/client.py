from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Generic, Literal, Protocol, TypeAlias, TypeVar

import httpx2
import jsonschema  # type: ignore[import-untyped,unused-ignore]
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import (
    CallToolRequest,
    CallToolRequestParams,
    CallToolResult,
    ContentBlock,
    Implementation,
    InputRequiredResult,
    InputResponses,
    RequestParamsMeta,
)
from pydantic import TypeAdapter
from referencing import Registry

from holon.binding import Binding, resolve_default_binding

# Binding is re-exported for the generated holon.apps module.
__all__ = ["Apps", "Binding", "ToolCallError", "ToolResult"]

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


class ToolCallError(RuntimeError):
    """Raised by ``tools.<name>`` for tool errors or additional input requests.

    Attributes:
        app: Dependency name.
        tool: Requested tool name.
        result: Original MCP error or input-required result.
        code: ``tool_error`` or ``input_required``. Use ``call_tool`` when you
            need to inspect these results and submit additional input.
    """

    def __init__(
        self, app: str, tool: str, result: CallToolResult | InputRequiredResult
    ) -> None:
        self.app = app
        self.tool = tool
        self.result = result
        self.code: Literal["tool_error", "input_required"]
        if isinstance(result, InputRequiredResult):
            self.code, message = "input_required", "additional input required"
        else:
            self.code, message = "tool_error", "tool failed"
        super().__init__(f"{app}.{tool}: {message}.")


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
        # None for a dependency without a contract: any tool, unvalidated.
        self._definitions = (
            None
            if contract is None
            else {tool["name"]: tool for tool in contract["tools"]}
        )
        self.tools = _Tools(self)

    async def call_tool(
        self,
        name: str,
        arguments: Mapping[str, object],
        *,
        timeout: float | None = None,
        input_responses: InputResponses | None = None,
        request_state: str | None = None,
        meta: RequestParamsMeta | None = None,
    ) -> CallToolResult | InputRequiredResult:
        """Call a dependency tool and return its full MCP result.

        Args:
            name: Tool name. Contracted dependencies reject unknown tools.
            arguments: Tool arguments, validated against the contract if present.
            timeout: Request read timeout in seconds, or the MCP client default.
            input_responses: Responses to a previous input-required result.
            request_state: Continuation state returned by the server.
            meta: Optional MCP request metadata.

        Returns:
            A complete or input-required result. Tool error results are returned
            unchanged; successful structured output is contract-validated.

        Raises:
            ValueError: Unknown tool, invalid input/output, or undeclared dependency.
            RuntimeError: Endpoint resolution failed.

        Transport and protocol exceptions propagate. Each call closes its HTTP
        client and MCP session, including when an exception occurs.
        """
        definition = None
        if self._definitions is not None:
            definition = self._definitions.get(name)
            if definition is None:
                raise ValueError(f"Unknown tool {self._name}.{name}.")
            validate(
                definition["inputSchema"],
                arguments,
                f"arguments for {self._name}.{name}",
            )
        binding = self._binding or resolve_default_binding(self._name)
        if binding is None and self._name not in self._identity["requires"]:
            raise ValueError(
                f"Dependency {self._name} is not declared by this application."
            )
        variable = self._name.upper().replace("-", "_") + "_MCP_URL"
        endpoint = binding["endpoint"] if binding else os.environ.get(variable)
        if not endpoint:
            raise RuntimeError(
                f"Dependency {self._name} is unavailable ({variable}). Set the endpoint or supply a binding."
            )
        http = (
            binding["http_client"]()
            if binding and "http_client" in binding
            else httpx2.AsyncClient(trust_env=False)
        )
        async with http:
            transport = streamable_http_client(endpoint, http_client=http)
            async with Client(
                transport,
                client_info=Implementation(
                    name=self._identity["name"], version=self._identity["version"]
                ),
                mode="2026-07-28",
            ) as client:
                result = await client.session.send_request(
                    CallToolRequest(
                        params=CallToolRequestParams(
                            name=name,
                            arguments=dict(arguments),
                            input_responses=input_responses,
                            request_state=request_state,
                            _meta=meta,
                        )
                    ),
                    _result,
                    request_read_timeout_seconds=timeout,
                )
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


class _Tools:
    def __init__(self, client: _App) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        definitions = self._client._definitions
        if name.startswith("_") or definitions is None or name not in definitions:
            raise AttributeError(name)

        async def call(
            arguments: Mapping[str, object], *, timeout: float | None = None
        ) -> object:
            result = await self._client.call_tool(name, arguments, timeout=timeout)
            if isinstance(result, InputRequiredResult) or result.is_error:
                raise ToolCallError(self._client._name, name, result)
            return result.structured_content

        return call


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
    ``tools.<name>(arguments, timeout=seconds)`` methods return structured output
    and raise :class:`ToolCallError` for tool errors or additional input requests.
    Use its ``call_tool`` method for the full result envelope.
    """

    def __init__(
        self,
        identity: dict[str, Any],
        contracts: dict[str, Any],
        bindings: Mapping[str, Binding] | None = None,
    ) -> None:
        # Every declared dependency gets a client; those without a contract are untyped.
        available: dict[str, dict[str, Any] | None] = {
            name: None for name in identity["requires"]
        }
        available.update(contracts)
        selected = available if bindings is None else {}
        for name in bindings or ():
            if name not in available:
                raise ValueError(f"Unknown app binding: {name}")
            selected[name] = available[name]
        self._apps = {
            name: _App(name, identity, contract, bindings[name] if bindings else None)
            for name, contract in selected.items()
        }

    def __getitem__(self, name: str) -> _App:
        return self._apps[name]

    def __getattr__(self, name: str) -> _App:
        if name.startswith("_") or name not in self._apps:
            raise AttributeError(name)
        return self._apps[name]
