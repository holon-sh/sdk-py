"""What Holon gives every app, with nothing to declare.

``control`` reads the MCP calls made to this app::

    from holon.system import control

    page = await control.tools.get_traces(since="24h", limit=200)
"""

from holon.client import Binding

from . import _generated
from ._generated import (
    ControlApp,
    ControlGetTracesInput,
    ControlGetTracesOutput,
    ControlGetTracesOutputTracesItem,
    ControlTools,
)

__all__ = [
    "ControlApp",
    "ControlGetTracesInput",
    "ControlGetTracesOutput",
    "ControlGetTracesOutputTracesItem",
    "ControlTools",
    "control",
    "create_control",
]

control = _generated.apps.control


def create_control(binding: Binding) -> ControlApp:
    """Control at ``binding`` instead of ``CONTROL_MCP_URL``, as in ``create_apps``."""
    return _generated.create_apps({"control": binding}).control
