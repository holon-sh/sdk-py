import asyncio
import json
import socket
from typing import Annotated, Literal

import httpx2
import uvicorn
from mcp.server import MCPServer, Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import CallToolResult
from pydantic import BaseModel, Field, TypeAdapter
from starlette.applications import Starlette
from starlette.routing import Mount

from holon.client import Apps
from holon.events import Events

IDENTITY = {"name": "watcher", "version": "1", "requires": {"boards": {}}}


class Board(BaseModel):
    board: str


class Change(BaseModel):
    board: str
    text: str


async def serve(app, port=0):
    sock = socket.create_server(("127.0.0.1", port))
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", timeout_graceful_shutdown=0)
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    while not server.started:
        await asyncio.sleep(0.01)

    async def stop():
        server.should_exit = True
        await task

    return sock.getsockname()[1], stop


def boards(port):
    endpoint = {"endpoint": f"http://127.0.0.1:{port}/mcp"}
    return Apps(IDENTITY, {}, {"boards": endpoint}).boards


def declare(events, **hooks):
    return events.event(
        "board.changed", input_schema=Board, payload_schema=Change, **hooks
    )


async def until(condition):
    while not condition():
        await asyncio.sleep(0.02)


def test_push_delivers_what_matches_as_shaped_and_resumes_after_the_connection_drops():
    async def exercise():
        events = Events()
        ended = []
        changed = declare(
            events,
            match=lambda context, event, arguments: event.data.board == arguments.board,
            transform=lambda context, event, arguments: {
                "board": event.data.board,
                "text": event.data.text.upper(),
            },
            on_subscribe=lambda context, arguments, id: lambda: ended.append(id),
        )
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(server.streamable_http_app(stateless_http=True))
        app = boards(port)
        async with app.events.subscribe("board.changed", {"board": "b1"}) as changes:
            assert changes.delivery == "push"
            changed.emit(Change(board="b2", text="elsewhere"))
            changed.emit(Change(board="b1", text="first"))
            first = await anext(changes)
            assert first.data == {"board": "b1", "text": "FIRST"}
            assert changes.cursor == first.cursor == changed.cursor()
            await stop()
            await until(lambda: len(ended) == 1)
            # Emitted while the client is away, and owed to it from its cursor.
            changed.emit(Change(board="b1", text="second"))
            port, stop = await serve(
                server.streamable_http_app(stateless_http=True), port
            )
            second = await anext(changes)
            assert second.data == {"board": "b1", "text": "SECOND"}
            assert second.cursor == changed.cursor()
        await until(lambda: len(ended) == 2)
        # The same caller and arguments are the same subscription across connections.
        assert ended[0] == ended[1]
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


def test_a_reader_that_falls_behind_what_is_kept_is_told_of_a_gap_and_goes_on():
    async def exercise():
        events = Events(keep={"events": 50})
        changed = declare(events)
        port, stop = await serve(
            MCPServer("boards", extensions=[events]).streamable_http_app(
                stateless_http=True
            )
        )
        received: list[str] = []
        gaps = []
        app = boards(port)
        async with app.events.subscribe(
            "board.changed", {"board": "b1"}, on_gap=lambda: gaps.append(len(received))
        ) as changes:
            # Far more than the connection holds while nobody reads.
            for index in range(300):
                changed.emit(
                    Change(board="b1", text="x" * 200_000), event_id=str(index)
                )
                await asyncio.sleep(0.002)
            while not received or received[-1] != "299":
                received.append((await anext(changes)).event_id)
        await stop()
        [gap] = gaps
        assert received[:gap] == [str(index) for index in range(gap)]
        assert received[gap:] == [str(index) for index in range(250, 300)]

    asyncio.run(asyncio.wait_for(exercise(), timeout=30))


def test_poll_reports_a_gap_when_the_server_restarted_and_carries_on():
    async def exercise():
        before = Events()
        changed = declare(before)
        port, stop = await serve(
            MCPServer("boards", extensions=[before]).streamable_http_app(
                stateless_http=True
            )
        )
        gaps = []
        app = boards(port)
        async with app.events.subscribe(
            "board.changed",
            {"board": "b1"},
            delivery="poll",
            on_gap=lambda: gaps.append(changes.cursor),
        ) as changes:
            changed.emit(Change(board="b1", text="first"))
            assert (await anext(changes)).data["text"] == "first"
            await stop()
            after = Events()
            changed = declare(after)
            port, stop = await serve(
                MCPServer("boards", extensions=[after]).streamable_http_app(
                    stateless_http=True
                ),
                port,
            )
            following = asyncio.ensure_future(anext(changes))
            await until(lambda: gaps)
            # The gap is reported with the cursor before it, and delivery goes on from the new process's.
            assert gaps[0] is not None and changes.cursor == changed.cursor()
            changed.emit(Change(board="b1", text="second"))
            assert (await following).data["text"] == "second"
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


ENVELOPE = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {},
}


POLL = ["events", "cursor", "truncated", "hasMore", "nextPollMs"]


def request(method, params, id=1):
    return {
        "json": {
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": {**params, "_meta": ENVELOPE},
        },
        "headers": {
            "accept": "application/json, text/event-stream",
            "mcp-protocol-version": "2026-07-28",
            "mcp-method": method,
        },
    }


def test_the_messages_on_the_wire_are_the_sketchs():
    async def exercise():
        events = Events(heartbeat=0.2, max_subscriptions=3)
        changed = declare(events, description="A board changed")
        events.event(
            "board.locked",
            input_schema=Board,
            payload_schema=Change,
            delivery=["poll"],
            authorize=lambda context, arguments: arguments.board != "secret",
        )
        port, stop = await serve(
            MCPServer("boards", extensions=[events]).streamable_http_app(
                stateless_http=True
            )
        )
        url = f"http://127.0.0.1:{port}/mcp"
        b1 = {"name": "board.changed", "arguments": {"board": "b1"}}

        async def answer(http, method, params):
            response = await http.post(url, **request(method, params))
            return response.json()

        async with httpx2.AsyncClient(timeout=10) as http:
            listed = (await answer(http, "events/list", {}))["result"]["events"]
            assert listed[0] == {
                "name": "board.changed",
                "description": "A board changed",
                "delivery": ["poll", "push"],
                "inputSchema": Board.model_json_schema(),
                "payloadSchema": Change.model_json_schema(),
            }
            now = (await answer(http, "events/poll", {**b1, "cursor": None}))["result"]
            # The core protocol stamps its own fields on every result besides these.
            assert {key: now[key] for key in POLL} == {
                "events": [],
                "cursor": changed.cursor(),
                "truncated": False,
                "hasMore": False,
                "nextPollMs": 1000,
            }
            for text in ["one", "two", "three"]:
                changed.emit(Change(board="b1", text=text), event_id=text)
            page = (
                await answer(
                    http,
                    "events/poll",
                    {**b1, "cursor": now["cursor"], "maxEvents": 2},
                )
            )["result"]
            assert [event["eventId"] for event in page["events"]] == ["one", "two"]
            assert set(page["events"][0]) == {"eventId", "name", "timestamp", "data"}
            assert page["hasMore"] is True
            rest = (
                await answer(http, "events/poll", {**b1, "cursor": page["cursor"]})
            )["result"]
            assert [event["eventId"] for event in rest["events"]] == ["three"]
            assert rest["hasMore"] is False
            stale = (await answer(http, "events/poll", {**b1, "cursor": "gone.1"}))[
                "result"
            ]
            assert stale["truncated"] is True and stale["cursor"] == changed.cursor()

            refusals = [
                ("events/poll", {"name": "board.moved"}, -32023, {"kind": "event"}),
                (
                    "events/poll",
                    {"name": "board.changed", "arguments": {}},
                    -32602,
                    None,
                ),
                ("events/poll", {**b1, "cursor": 1}, -32602, None),
                (
                    "events/stream",
                    {"name": "board.locked", "arguments": {"board": "b1"}},
                    -32026,
                    {"feature": "deliveryMode", "value": "push"},
                ),
                (
                    "events/poll",
                    {"name": "board.locked", "arguments": {"board": "secret"}},
                    -32024,
                    None,
                ),
            ]
            for method, params, code, data in refusals:
                response = await http.post(url, **request(method, params))
                # Refused at once, with no stream opened.
                assert response.headers["content-type"] == "application/json"
                error = response.json()["error"]
                assert error["code"] == code
                assert data is None or error["data"] == data
            # Two poll leases are held; a third subscription is the limit, and a fourth is refused.
            await answer(
                http,
                "events/poll",
                {"name": "board.locked", "arguments": {"board": "b1"}},
            )
            async with http.stream(
                "POST", url, **request("events/stream", b1, id=7)
            ) as stream:
                frames = aiter(httpx2.EventSource(stream))
                assert stream.headers["content-type"] == "text/event-stream"
                active = json.loads((await anext(frames)).data)
                assert active == {
                    "jsonrpc": "2.0",
                    "method": "notifications/events/active",
                    "params": {
                        "cursor": changed.cursor(),
                        "truncated": False,
                        "_meta": {"io.modelcontextprotocol/subscriptionId": 7},
                    },
                }
                exhausted = await answer(
                    http,
                    "events/poll",
                    {"name": "board.locked", "arguments": {"board": "b2"}},
                )
                assert exhausted["error"]["code"] == -32025
                assert exhausted["error"]["data"] == {
                    "limit": "subscriptions",
                    "max": 3,
                }
                changed.emit(Change(board="b1", text="four"), event_id="four")
                event = json.loads((await anext(frames)).data)["params"]
                assert event == {
                    "eventId": "four",
                    "name": "board.changed",
                    "timestamp": event["timestamp"],
                    "data": {"board": "b1", "text": "four"},
                    "cursor": changed.cursor(),
                    "_meta": {"io.modelcontextprotocol/subscriptionId": 7},
                }
                heartbeat = json.loads((await anext(frames)).data)
                assert heartbeat["method"] == "notifications/events/heartbeat"
                assert heartbeat["params"]["cursor"] == changed.cursor()
                changed.end()
                terminated = json.loads((await anext(frames)).data)
                assert terminated["method"] == "notifications/events/terminated"
                assert terminated["params"]["error"] == {
                    "code": -32024,
                    "message": "Forbidden",
                }
                final = json.loads((await anext(frames)).data)
                assert final["id"] == 7 and "result" in final
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


class CellAdded(BaseModel):
    kind: Literal["added"] = "added"
    cell: str


class CellRemoved(BaseModel):
    kind: Literal["removed"] = "removed"
    cell: str


def test_a_stream_is_served_on_a_stateless_endpoint_that_answers_in_json():
    async def exercise():
        events = Events()
        cells = ["a"]
        changed = events.event(
            "notebook.changed",
            input_schema=dict,
            payload_schema=TypeAdapter(
                Annotated[CellAdded | CellRemoved, Field(discriminator="kind")]
            ),
        )

        # A tool reads the state together with the cursor it is at.
        async def snapshot(context, params):
            state = {"cells": list(cells), "cursor": changed.cursor()}
            return CallToolResult(content=[], structured_content=state)

        server = Server("notebook", on_call_tool=snapshot)
        events.serve(server)
        manager = StreamableHTTPSessionManager(
            server, stateless=True, json_response=True
        )
        app = Starlette(
            routes=[Mount("/mcp", app=events.asgi(manager))],
            lifespan=lambda app: manager.run(),
        )
        changed.emit(CellAdded(cell="a"))
        port, stop = await serve(app)
        notebook = Apps(
            {**IDENTITY, "requires": {"notebook": {}}},
            {},
            {"notebook": {"endpoint": f"http://127.0.0.1:{port}/mcp/"}},
        ).notebook
        state = (await notebook.call_tool("snapshot")).structured_content
        assert state["cells"] == ["a"]
        # Changed after the snapshot, in one synchronous run.
        changed.emit(CellAdded(cell="b"))
        changed.emit(CellRemoved(cell="a"))
        async with notebook.events.subscribe(
            "notebook.changed", {}, cursor=state["cursor"]
        ) as changes:
            assert changes.delivery == "push"
            received = [(await anext(changes)).data for _ in range(2)]
        assert received == [
            {"kind": "added", "cell": "b"},
            {"kind": "removed", "cell": "a"},
        ]
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))
