# holon-generated-apps-v1 app=system
# Input alternatives share a result type; redundant overloads are harmless.
# mypy: disable-error-code=overload-cannot-match
from __future__ import annotations

import json as _json

from holon.client import Apps as _Apps
from holon.client import Binding as _Binding
from holon.client import CallOptions as _CallOptions
from holon.client import CompleteResult as _CompleteResult
from holon.client import ToolResult as _ToolResult

import builtins as _builtins
import typing as _typing

ControlGetTracesInput = _typing.TypedDict("ControlGetTracesInput", {"before": _typing.NotRequired[_builtins.str], "limit": _typing.NotRequired[_builtins.int], "since": _typing.NotRequired[_builtins.str], "status": _typing.NotRequired[_typing.Literal["ok"] | _typing.Literal["error"]], "tool": _typing.NotRequired[_builtins.str]})

ControlGetTracesOutputTracesItem = _typing.TypedDict("ControlGetTracesOutputTracesItem", {"app": _typing.NotRequired[_builtins.str], "durationMs": _builtins.int, "errorCode": _typing.NotRequired[_builtins.int], "method": _builtins.str, "start": _builtins.str, "status": _typing.Literal["ok"] | _typing.Literal["error"], "tool": _typing.NotRequired[_builtins.str], "traceId": _builtins.str})

ControlGetTracesOutput = _typing.TypedDict("ControlGetTracesOutput", {"app": _typing.NotRequired[_builtins.str], "session": _builtins.str, "traces": _builtins.list[ControlGetTracesOutputTracesItem]})

class Inputs:
    class control:
        get_traces: _typing.TypeAlias = ControlGetTracesInput

class Outputs:
    class control:
        get_traces: _typing.TypeAlias = ControlGetTracesOutput

class InputTypes:
    class control:
        class get_traces:
            pass

class OutputTypes:
    class control:
        class get_traces:
            pass

class ControlTools(_typing.Protocol):
    async def get_traces(self, options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[ControlGetTracesInput]) -> ControlGetTracesOutput: ...

class ControlEvents(_typing.Protocol):
    async def list(self) -> _builtins.list[_builtins.dict[_builtins.str, _typing.Any]]: ...

class ControlExt(_typing.Protocol):
    events: ControlEvents

class ControlApp(_typing.Protocol):
    tools: ControlTools
    ext: ControlExt
    async def call_tool(self, name: _typing.Literal["get_traces"], options: _CallOptions | None = ..., /, **arguments: _typing.Unpack[ControlGetTracesInput]) -> _ToolResult[ControlGetTracesOutput]: ...

class _Clients(_typing.Protocol):
    control: ControlApp
    def __getitem__(self, name: _typing.Literal["control"]) -> ControlApp: ...


_identity, _contracts, _methods = _json.loads("[{\"name\":\"holon-sh\",\"requires\":{\"control\":{\"name\":\"holon-control\",\"version\":\"1\"}},\"version\":\"1\"},{\"control\":{\"tools\":[{\"inputSchema\":{\"additionalProperties\":false,\"properties\":{\"before\":{\"description\":\"RFC 3339 time, exclusive: pass the oldest start returned to read the page before it.\",\"type\":\"string\"},\"limit\":{\"description\":\"The most calls to return. Defaults to 50, at most 200.\",\"type\":\"integer\"},\"since\":{\"description\":\"How far back to read: a duration such as 24h, or an RFC 3339 time. Defaults to 15m.\",\"type\":\"string\"},\"status\":{\"description\":\"Only the calls that ended this way.\",\"enum\":[\"ok\",\"error\"],\"type\":\"string\"},\"tool\":{\"description\":\"Only the calls to this tool.\",\"type\":\"string\"}},\"type\":\"object\"},\"name\":\"get_traces\",\"outputSchema\":{\"additionalProperties\":false,\"properties\":{\"app\":{\"type\":\"string\"},\"session\":{\"type\":\"string\"},\"traces\":{\"items\":{\"additionalProperties\":false,\"properties\":{\"app\":{\"type\":\"string\"},\"durationMs\":{\"type\":\"integer\"},\"errorCode\":{\"type\":\"integer\"},\"method\":{\"type\":\"string\"},\"start\":{\"type\":\"string\"},\"status\":{\"enum\":[\"ok\",\"error\"],\"type\":\"string\"},\"tool\":{\"type\":\"string\"},\"traceId\":{\"type\":\"string\"}},\"required\":[\"traceId\",\"start\",\"method\",\"status\",\"durationMs\"],\"type\":\"object\"},\"type\":\"array\"}},\"required\":[\"session\",\"traces\"],\"type\":\"object\"}}]}},{}]")

def _clients(bindings: _typing.Mapping[str, _Binding] | None = None) -> _Clients:
    apps = _Apps(_identity, _contracts, bindings)
    for binding in _methods if bindings is None else _methods.keys() & bindings:
        tools = apps[binding].tools
        for tool, method in _methods[binding].items():
            setattr(tools, method, getattr(tools, tool))
    return _typing.cast(_Clients, apps)

from importlib.metadata import version as _version
_identity["version"] = _version("holon-sh")
apps = _clients()

def create_apps(bindings: _typing.Mapping[str, _Binding]) -> _Clients:
    return _clients(bindings)
