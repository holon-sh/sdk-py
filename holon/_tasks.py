from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from mcp.client.extension import advertise
from mcp.types import (
    CallToolResult,
    ErrorData,
    InputRequests,
    InputRequiredResult,
    InputResponses,
    Request,
    RequestParams,
    Result,
    TaskStatus,
)
from pydantic import Field, TypeAdapter

from holon.ext.files._wire import UploadRequiredResult

# https://github.com/modelcontextprotocol/ext-tasks/blob/main/specification/2026-07-28/tasks.md#extension-identifier
TASKS = advertise("io.modelcontextprotocol/tasks")


class _Task(Result):
    task_id: str
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl_ms: int | None
    poll_interval_ms: int | None = None


class CreateTaskResult(_Task):
    """A task the server made in place of the tool result."""

    result_type: Literal["task"]
    status: TaskStatus


class WorkingTask(_Task):
    status: Literal["working"]


class InputRequiredTask(_Task):
    """A task waiting for ``input_requests`` to be answered with ``update_task``."""

    status: Literal["input_required"]
    input_requests: InputRequests


class CompletedTask(_Task):
    """A task that ended with the tool result."""

    status: Literal["completed"]
    result: CallToolResult


class FailedTask(_Task):
    """A task that ended with a JSON-RPC error."""

    status: Literal["failed"]
    error: ErrorData


class CancelledTask(_Task):
    status: Literal["cancelled"]


STARTED: TypeAdapter[
    UploadRequiredResult | CreateTaskResult | CallToolResult | InputRequiredResult
]
STARTED = TypeAdapter(
    Annotated[
        UploadRequiredResult | CreateTaskResult | CallToolResult | InputRequiredResult,
        Field(union_mode="left_to_right"),
    ]
)


DetailedTask: TypeAlias = (
    WorkingTask | InputRequiredTask | CompletedTask | FailedTask | CancelledTask
)
DETAILED: TypeAdapter[DetailedTask] = TypeAdapter(
    Annotated[DetailedTask, Field(discriminator="status")]
)


class TaskParams(RequestParams):
    task_id: str


class UpdateParams(TaskParams):
    input_responses: InputResponses


# https://github.com/modelcontextprotocol/ext-tasks/blob/main/specification/2026-07-28/tasks.md#streamable-http-routing-headers
class TaskRequest(
    Request[
        TaskParams | UpdateParams,
        Literal["tasks/get", "tasks/update", "tasks/cancel"],
    ]
):
    name_param = "taskId"
