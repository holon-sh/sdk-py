# holon-generated-apps-v1 app=caller
# Input alternatives share a result type; redundant overloads are harmless.
# mypy: disable-error-code=overload-cannot-match
from __future__ import annotations

import json as _json

from holon.client import Apps as _Apps
from holon.client import Binding as _Binding
from holon.client import CallOptions as _CallOptions
from holon.client import ToolResult as _ToolResult

import builtins as _builtins
import typing as _typing

OrdersEchoInput = _typing.TypedDict("OrdersEchoInput", {"arguments": _builtins.str, "class": _builtins.str, "name": _builtins.str, "options": _builtins.str, "order-id": _builtins.str, "self": _builtins.str, "timeout": _builtins.str})

OrdersFindInput = _typing.TypedDict("OrdersFindInput", {"id": _builtins.str})

OrdersFindInput2 = _typing.TypedDict("OrdersFindInput2", {"id": _builtins.str})

OrdersFindInput3 = _typing.TypedDict("OrdersFindInput3", {"email": _builtins.str})

OrdersGetOrderInput = _typing.TypedDict("OrdersGetOrderInput", {"id": _builtins.str, "limit": _typing.NotRequired[_builtins.int]})

OrdersGetOrderOutput = _typing.TypedDict("OrdersGetOrderOutput", {"total": _builtins.float})

OrdersPingInput = _typing.TypedDict("OrdersPingInput", {})

OrdersReadInput = _typing.TypedDict("OrdersReadInput", {"id": _builtins.str, "limit": _typing.NotRequired[_builtins.int]})

OrdersReadOutput = _typing.TypedDict("OrdersReadOutput", {"total": _builtins.float})

class Inputs:
    class orders:
        counts: _typing.TypeAlias = _builtins.dict[_builtins.str, _builtins.int]
        echo: _typing.TypeAlias = OrdersEchoInput
        find: _typing.TypeAlias = OrdersFindInput | OrdersFindInput2 | OrdersFindInput3
        ping: _typing.TypeAlias = OrdersPingInput
        read: _typing.TypeAlias = OrdersReadInput

class Outputs:
    class orders:
        counts: _typing.TypeAlias = _builtins.object
        echo: _typing.TypeAlias = _builtins.object
        find: _typing.TypeAlias = _builtins.object
        ping: _typing.TypeAlias = _builtins.object
        read: _typing.TypeAlias = OrdersReadOutput

class InputTypes:
    class orders:
        class counts:
            pass
        class echo:
            pass
        class find:
            pass
        class ping:
            pass
        class read:
            pass

class OutputTypes:
    class orders:
        class counts:
            pass
        class echo:
            pass
        class find:
            pass
        class ping:
            pass
        class read:
            pass

class OrdersTools(_typing.Protocol):
    async def counts(self, options: _CallOptions | None = ..., /, **arguments: _builtins.int) -> _builtins.object: ...
    async def echo(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersEchoInput]) -> _builtins.object: ...
    @_typing.overload
    async def find(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput]) -> _builtins.object: ...
    @_typing.overload
    async def find(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput2]) -> _builtins.object: ...
    @_typing.overload
    async def find(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput3]) -> _builtins.object: ...
    async def ping(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersPingInput]) -> _builtins.object: ...
    async def read(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersReadInput]) -> OrdersReadOutput: ...

class OrdersApp(_typing.Protocol):
    tools: OrdersTools
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["counts"], options: _CallOptions | None = ..., /, **arguments: _builtins.int) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["echo"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersEchoInput]) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["find"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput]) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["find"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput2]) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["find"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersFindInput3]) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["get-order"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersGetOrderInput]) -> _ToolResult[OrdersGetOrderOutput]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["ping"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersPingInput]) -> _ToolResult[_builtins.object]: ...
    @_typing.overload
    async def call_tool(self, name: _typing.Literal["read"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[OrdersReadInput]) -> _ToolResult[OrdersReadOutput]: ...

class _Clients(_typing.Protocol):
    orders: OrdersApp
    def __getitem__(self, name: _typing.Literal["orders"]) -> OrdersApp: ...


_identity, _contracts = _json.loads('[{"name":"fixtures/caller","requires":{"orders":{"name":"fixtures/orders","version":"1"}},"version":"1"},{"orders":{"tools":[{"name":"counts","inputSchema":{"additionalProperties":{"type":"integer"},"type":"object"}},{"name":"echo","inputSchema":{"additionalProperties":false,"properties":{"arguments":{"type":"string"},"class":{"type":"string"},"name":{"type":"string"},"options":{"type":"string"},"order-id":{"type":"string"},"self":{"type":"string"},"timeout":{"type":"string"}},"required":["name","timeout","options","self","arguments","class","order-id"],"type":"object"}},{"name":"find","inputSchema":{"anyOf":[{"additionalProperties":false,"properties":{"id":{"type":"string"}},"required":["id"],"type":"object"},{"additionalProperties":false,"properties":{"id":{"type":"string"}},"required":["id"],"type":"object"},{"additionalProperties":false,"properties":{"email":{"type":"string"}},"required":["email"],"type":"object"}],"type":"object"}},{"name":"get-order","inputSchema":{"additionalProperties":false,"properties":{"id":{"type":"string"},"limit":{"type":"integer"}},"required":["id"],"type":"object"},"outputSchema":{"properties":{"total":{"type":"number"}},"required":["total"],"type":"object"}},{"name":"ping","inputSchema":{"additionalProperties":false,"type":"object"}},{"name":"read","inputSchema":{"additionalProperties":false,"properties":{"id":{"type":"string"},"limit":{"type":"integer"}},"required":["id"],"type":"object"},"outputSchema":{"properties":{"total":{"type":"number"}},"required":["total"],"type":"object"}}]}}]')

apps = _typing.cast(_Clients, _Apps(_identity, _contracts))

def create_apps(bindings: _typing.Mapping[str, _Binding]) -> _Clients:
    return _typing.cast(_Clients, _Apps(_identity, _contracts, bindings))
