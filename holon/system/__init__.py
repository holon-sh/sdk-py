"""What Holon gives every app, with nothing to declare.

``control`` reads the MCP calls made to this app::

    from holon.system import control

    page = await control.tools.get_traces(since="24h", limit=200)
"""

import json
from importlib.metadata import version
from importlib.resources import files
from typing import Literal, NotRequired, Protocol, TypedDict, Unpack, cast

from holon.client import Apps, Binding, CallOptions, ToolResult

__all__ = [
    "ControlApp",
    "ControlGetTracesInput",
    "ControlGetTracesOutput",
    "ControlGetTracesOutputTracesItem",
    "ControlTools",
    "control",
    "create_control",
]


class ControlGetTracesInput(TypedDict, total=False):
    since: str
    status: Literal["ok", "error"]
    tool: str
    limit: int
    before: str


class ControlGetTracesOutputTracesItem(TypedDict):
    traceId: str
    start: str
    app: NotRequired[str]
    method: str
    tool: NotRequired[str]
    status: Literal["ok", "error"]
    errorCode: NotRequired[int]
    durationMs: int


class ControlGetTracesOutput(TypedDict):
    session: str
    app: NotRequired[str]
    traces: list[ControlGetTracesOutputTracesItem]


class ControlTools(Protocol):
    async def get_traces(
        self,
        options: CallOptions | None = ...,
        /,
        **arguments: Unpack[ControlGetTracesInput],
    ) -> ControlGetTracesOutput: ...


class ControlApp(Protocol):
    tools: ControlTools

    async def call_tool(
        self,
        name: Literal["get_traces"],
        options: CallOptions | None = ...,
        /,
        **arguments: Unpack[ControlGetTracesInput],
    ) -> ToolResult[ControlGetTracesOutput]: ...


# control.json is schema/control.json from holon-sh/holon, copied verbatim. A tool's
# result never changes there (new behavior is a new tool), so the SDK can carry it.
_contracts = {
    "control": json.loads(files(__name__).joinpath("control.json").read_bytes())
}
_identity = {
    "name": "holon-sh",
    "version": version("holon-sh"),
    "requires": {"control": {}},
}


def create_control(binding: Binding) -> ControlApp:
    """Control at ``binding`` instead of ``CONTROL_MCP_URL``, as in ``create_apps``."""
    return cast(
        ControlApp, Apps(_identity, _contracts, {"control": binding})["control"]
    )


# Holon gives every app control at CONTROL_MCP_URL.
control = cast(ControlApp, Apps(_identity, _contracts)["control"])
