# holon-generated-apps-v1 app=caller
from __future__ import annotations

import json as _json

from mcp.types import InputResponses as _InputResponses
from mcp.types import RequestParamsMeta as _RequestParamsMeta

from holon.client import Apps as _Apps
from holon.client import Binding as _Binding
from holon.client import ToolResult as _ToolResult

import builtins as _builtins
import typing as _typing

_Shape0 = _typing.TypedDict("_Shape0", {"id": _builtins.str})

_Shape1 = _typing.TypedDict("_Shape1", {"total": _builtins.float})

class Inputs:
    class orders:
        read: _typing.TypeAlias = _Shape0

class Outputs:
    class orders:
        read: _typing.TypeAlias = _Shape1

class InputTypes:
    class orders:
        class read:
            pass

class OutputTypes:
    class orders:
        class read:
            pass

class _Tools0(_typing.Protocol):
    async def read(self, arguments: _Shape0, *, timeout: _builtins.float | None = ...) -> _Shape1: ...

class _App0(_typing.Protocol):
    tools: _Tools0
    async def call_tool(self, name: _typing.Literal["read"], arguments: _Shape0, *, timeout: _builtins.float | None = ..., input_responses: _InputResponses | None = ..., request_state: _builtins.str | None = ..., meta: _RequestParamsMeta | None = ...) -> _ToolResult[_Shape1]: ...

class _Clients(_typing.Protocol):
    orders: _App0
    def __getitem__(self, name: _typing.Literal["orders"]) -> _App0: ...


_identity, _contracts = _json.loads('[{"name":"fixtures/caller","requires":{"orders":{"name":"fixtures/orders","version":"1"}},"version":"1"},{"orders":{"tools":[{"name":"read","inputSchema":{"properties":{"id":{"type":"string"}},"required":["id"],"type":"object"},"outputSchema":{"properties":{"total":{"type":"number"}},"required":["total"],"type":"object"}}]}}]')

apps = _typing.cast(_Clients, _Apps(_identity, _contracts))

def create_apps(bindings: _typing.Mapping[str, _Binding]) -> _Clients:
    return _typing.cast(_Clients, _Apps(_identity, _contracts, bindings))
