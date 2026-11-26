import asyncio

import httpx2
import pytest
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import MCPError
from mcp.types import CLIENT_CAPABILITIES_META_KEY, INVALID_PARAMS
from starlette.responses import Response
from starlette.routing import Route

from holon.client import Apps
from holon.ext.files import EXTENSION, downloads

TASKS = "io.modelcontextprotocol/tasks"
ID = "786512e2-9e0d-44bd-8f29-789f320fe840"
SHIPPED = {
    "content": [{"type": "text", "text": "Shipped large."}],
    "structuredContent": {"shipped": "large"},
}
RETURNED = {"resultType": "complete", "isError": False, **SHIPPED}
ASKED = {
    "q": {
        "method": "elicitation/create",
        "params": {
            "mode": "form",
            "message": "Which size?",
            "requestedSchema": {"type": "object", "properties": {}},
        },
    }
}
ANSWER = {"q": {"action": "accept", "content": {"size": "large"}}}
UNKNOWN = {"code": INVALID_PARAMS, "message": "Failed to retrieve task: Task not found"}
CALLED = {"name": "ship", "arguments": {"id": "A-1"}}
NAMED = {"taskId": ID}
LABEL = b"%PDF-1.7 label"
LABELLED = {
    **SHIPPED,
    "_meta": {
        EXTENSION: {
            "downloads": [
                {
                    "path": "/files/label.pdf",
                    "name": "label.pdf",
                    "mimeType": "application/pdf",
                    "size": len(LABEL),
                    "credentials": "omit",
                }
            ]
        }
    },
}


def task(status, **fields):
    return {
        "taskId": ID,
        "status": status,
        "createdAt": "2026-11-26T15:04:05Z",
        "lastUpdatedAt": "2026-11-26T15:11:42Z",
        "ttlMs": 7200000,
        "pollIntervalMs": 2000,
        **fields,
    }


def serve(reply, log):
    async def tasks(ctx, call_next):
        params = dict(ctx.params)
        meta = params.pop("_meta")[CLIENT_CAPABILITIES_META_KEY]
        declared = TASKS in meta.get("extensions", {})
        log.append((ctx.method, ctx.request.headers.get("mcp-name"), declared, params))
        if "code" in reply:
            raise MCPError(**reply)
        return reply

    async def label(request):
        return Response(LABEL)

    server = Server("orders")
    server.middleware.insert(0, tasks)
    return server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        custom_starlette_routes=[Route("/mcp/files/label.pdf", label)],
    )


def run(act, reply):
    log = []
    app = serve(reply, log)
    orders = Apps(
        {"name": "checkout", "version": "1", "requires": {}},
        {"orders": {"tools": [{"name": "ship", "inputSchema": {"type": "object"}}]}},
        {
            "orders": {
                "endpoint": "http://127.0.0.1:8000/mcp",
                "http_client": lambda: httpx2.AsyncClient(
                    transport=httpx2.ASGITransport(app=app)
                ),
            }
        },
    ).orders

    async def main():
        async with app.router.lifespan_context(app):
            try:
                return await act(orders)
            except MCPError as error:
                return error.error

    return asyncio.run(main()), log


def shape(outcome):
    return outcome and (
        type(outcome).__name__,
        outcome.model_dump(by_alias=True, exclude_none=True),
    )


def get(orders):
    return orders.get_task(ID)


def start(orders):
    return orders.start_task("ship", id="A-1")


CASES = {
    "an ordinary call declares nothing and gets the direct answer": (
        lambda orders: orders.call_tool("ship", id="A-1"),
        {"resultType": "complete", **SHIPPED},
        ("CallToolResult", RETURNED),
        ("tools/call", "ship", False, CALLED),
    ),
    "start_task declares tasks and returns the task unchanged": (
        start,
        {**task("working"), "resultType": "task"},
        ("CreateTaskResult", {**task("working"), "resultType": "task"}),
        ("tools/call", "ship", True, CALLED),
    ),
    "start_task returns a direct answer unchanged": (
        start,
        {"resultType": "complete", **SHIPPED},
        ("CallToolResult", RETURNED),
        ("tools/call", "ship", True, CALLED),
    ),
    "get_task returns a working task": (
        get,
        {"resultType": "complete", **task("working", statusMessage="Round 1")},
        ("WorkingTask", task("working", statusMessage="Round 1")),
        ("tasks/get", ID, True, NAMED),
    ),
    "get_task returns a task's questions": (
        get,
        {"resultType": "complete", **task("input_required", inputRequests=ASKED)},
        ("InputRequiredTask", task("input_required", inputRequests=ASKED)),
        ("tasks/get", ID, True, NAMED),
    ),
    "get_task returns a completed task's tool result": (
        get,
        {"resultType": "complete", **task("completed", result=SHIPPED)},
        ("CompletedTask", task("completed", result=RETURNED)),
        ("tasks/get", ID, True, NAMED),
    ),
    "get_task returns a failed task's error": (
        get,
        {"resultType": "complete", **task("failed", error=UNKNOWN)},
        ("FailedTask", task("failed", error=UNKNOWN)),
        ("tasks/get", ID, True, NAMED),
    ),
    "get_task returns a cancelled task": (
        get,
        {"resultType": "complete", **task("cancelled")},
        ("CancelledTask", task("cancelled")),
        ("tasks/get", ID, True, NAMED),
    ),
    "update_task sends the answers": (
        lambda orders: orders.update_task(ID, ANSWER),
        {"resultType": "complete"},
        None,
        ("tasks/update", ID, True, {**NAMED, "inputResponses": ANSWER}),
    ),
    "cancel_task sends the cancel": (
        lambda orders: orders.cancel_task(ID),
        {"resultType": "complete"},
        None,
        ("tasks/cancel", ID, True, NAMED),
    ),
    "an unknown task is the server's error": (
        get,
        UNKNOWN,
        ("ErrorData", UNKNOWN),
        ("tasks/get", ID, True, NAMED),
    ),
}


@pytest.mark.parametrize("act, reply, expected, sent", CASES.values(), ids=CASES.keys())
def test_each_call_sends_one_request_and_returns_the_answer_unchanged(
    act, reply, expected, sent
):
    outcome, log = run(act, reply)

    assert shape(outcome) == expected
    assert log == [sent]


def test_the_file_a_completed_task_returns_is_listed_and_fetched():
    async def fetch(orders):
        completed = await orders.get_task(ID)
        [download] = downloads(completed.result)
        return download.name, await download.read()

    fetched, _ = run(
        fetch, {"resultType": "complete", **task("completed", result=LABELLED)}
    )

    assert fetched == ("label.pdf", LABEL)
