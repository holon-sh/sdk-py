import asyncio
import json
import socket
import sys
from typing import Annotated, Literal

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer, Server
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import CallToolResult
from pydantic import BaseModel, Field, TypeAdapter
from starlette.applications import Starlette
from starlette.routing import Mount

from holon.client import Apps
from holon.ext import events as events_module
from holon.ext.events import Events, NoCompatibleDeliveryMode

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


# Holon admits a caller before the app sees the request; here an `x-user` header stands in for it.
def mounted(server, events):
    server.streamable_http_app(stateless_http=True)
    manager = server.session_manager
    app = AuthContextMiddleware(
        Starlette(
            routes=[Mount("/mcp", app=events.asgi(manager))],
            lifespan=lambda app: manager.run(),
        )
    )

    async def admitted(scope, receive, send):
        if user := dict(scope.get("headers", [])).get(b"x-user"):
            claims = {"iss": "https://id.example", "sub": user.decode()}
            token = AccessToken(
                token="t",
                client_id="c",
                scopes=[],
                subject=user.decode(),
                claims=claims,
            )
            scope = {**scope, "user": AuthenticatedUser(token)}
        await app(scope, receive, send)

    return admitted


def boards(port, path="/mcp/", user=None):
    binding = {"endpoint": f"http://127.0.0.1:{port}{path}"}
    if user:
        binding["http_client"] = lambda: httpx2.AsyncClient(headers={"x-user": user})
    return Apps(IDENTITY, {}, {"boards": binding}).boards


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
        began, ended = [], []

        changed = declare(
            events,
            match=lambda context, event, arguments: event.data.board == arguments.board,
            transform=lambda context, event, arguments: {
                "board": event.data.board,
                "text": event.data.text.upper(),
            },
            on_subscribe=lambda context, arguments, id: began.append(id),
            on_unsubscribe=lambda context, arguments, id: ended.append(id),
        )
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(mounted(server, events))
        app = boards(port, user="ann")
        async with app.ext.events.subscribe(
            "board.changed", {"board": "b1"}
        ) as changes:
            assert changes.delivery == "push"
            changed.emit(Change(board="b2", text="elsewhere"))
            changed.emit(Change(board="b1", text="first"))
            first = await anext(changes)
            assert (first.name, first.data) == (
                "board.changed",
                {"board": "b1", "text": "FIRST"},
            )
            assert changes.cursor == first.cursor == changed.cursor()
            await stop()
            await until(lambda: len(ended) == 1)
            # Emitted while the client is away, and owed to it from its cursor.
            changed.emit(Change(board="b1", text="second"))
            port, stop = await serve(mounted(server, events), port)
            second = await anext(changes)
            assert second.data == {"board": "b1", "text": "SECOND"}
            assert second.cursor == changed.cursor()
        await until(lambda: len(ended) == 2)
        # The same caller and arguments are the same subscription across connections.
        assert began[0] == began[1] == ended[0] == ended[1]
        # A caller without an identity is nobody in particular: each stream is a subscription of its own.
        anonymous = boards(port)
        async with (
            anonymous.ext.events.subscribe("board.changed", {"board": "b1"}),
            anonymous.ext.events.subscribe("board.changed", {"board": "b1"}),
        ):
            assert len(set(began[2:])) == 2
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


def test_a_reader_that_falls_behind_what_is_kept_is_told_of_a_gap_and_goes_on(
    monkeypatch,
):
    monkeypatch.setattr(events_module, "_KEEP_EVENTS", 50)
    monkeypatch.setattr(events_module, "_KEEP_BYTES", 2**30)

    async def exercise():
        events = Events()
        changed = declare(events)
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(mounted(server, events))
        received: list[str] = []
        gaps = []
        app = boards(port)
        async with app.ext.events.subscribe(
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


def test_poll_reports_a_gap_before_the_events_after_it_and_after_a_restart(
    monkeypatch,
):
    monkeypatch.setattr(events_module, "_KEEP_EVENTS", 2)

    async def exercise():
        before = Events()
        changed = declare(before)
        port, stop = await serve(
            mounted(MCPServer("boards", extensions=[before]), before)
        )
        order = []
        app = boards(port)
        async with app.ext.events.subscribe(
            "board.changed",
            {"board": "b1"},
            delivery="poll",
            on_gap=lambda: order.append("gap"),
        ) as changes:
            changed.emit(Change(board="b1", text="one"))
            order.append((await anext(changes)).data["text"])
            # More than is kept, between two polls.
            for text in ["a", "b", "c", "d"]:
                changed.emit(Change(board="b1", text=text))
            order += [(await anext(changes)).data["text"] for _ in range(2)]
            assert order == ["one", "gap", "c", "d"]
            await stop()
            after = Events()
            changed = declare(after)
            port, stop = await serve(
                mounted(MCPServer("boards", extensions=[after]), after), port
            )
            following = asyncio.ensure_future(anext(changes))
            await until(lambda: len(order) == 5)
            # The new process's cursors start a gap, and delivery goes on from them.
            assert order[4] == "gap" and changes.cursor == changed.cursor()
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


def test_the_messages_on_the_wire_are_the_sketchs(monkeypatch):
    monkeypatch.setattr(events_module, "_HEARTBEAT", 0.2)
    monkeypatch.setattr(events_module, "_MAX_SUBSCRIPTIONS", 3)

    async def exercise():
        events = Events()

        def shaped(context, event, arguments):
            if event.data.text == "bad":
                raise ValueError("cannot be shaped")
            return event.data

        changed = declare(events, description="A board changed", transform=shaped)
        events.event(
            "board.locked",
            input_schema=Board,
            payload_schema=Change,
            delivery=["poll"],
            authorize=lambda context, arguments: arguments.board != "secret",
        )
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(mounted(server, events))
        url = f"http://127.0.0.1:{port}/mcp/"
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
            # One that cannot be shaped is skipped, and the cursor moves past it.
            for text in ["one", "bad", "two", "three"]:
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
                changed.emit(Change(board="b1", text="bad"))
                changed.emit(Change(board="b1", text="four"), event_id="four")
                # The stream is told once of the one it skipped, and goes on.
                error = json.loads((await anext(frames)).data)
                assert error["method"] == "notifications/events/error"
                assert error["params"]["error"]["code"] == -32603
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
                changed.terminate()
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


def test_a_poll_that_gives_up_while_it_is_set_up_still_lets_its_lease_lapse(
    monkeypatch,
):
    monkeypatch.setattr(events_module, "_POLL_SUBSCRIPTION_TTL", 0.3)
    monkeypatch.setattr(events_module, "_MAX_SUBSCRIPTIONS", 1)

    async def exercise():
        events = Events()
        ended = []

        async def on_subscribe(context, arguments, id):
            await asyncio.sleep(0.3)

        declare(
            events,
            on_subscribe=on_subscribe,
            on_unsubscribe=lambda context, arguments, id: ended.append(id),
        )
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(mounted(server, events))
        url = f"http://127.0.0.1:{port}/mcp/"
        params = {"name": "board.changed", "arguments": {"board": "b1"}}
        async with httpx2.AsyncClient(timeout=0.1) as http:
            with pytest.raises(httpx2.ReadTimeout):
                await http.post(url, **request("events/poll", params))
        await until(lambda: ended)
        # Its place is free again.
        async with httpx2.AsyncClient(timeout=10) as http:
            params["arguments"] = {"board": "b2"}
            answer = (await http.post(url, **request("events/poll", params))).json()
            assert "result" in answer
        await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


def test_where_push_cannot_be_served_it_is_not_offered_and_a_subscription_polls(
    monkeypatch,
):
    async def exercise():
        events = Events()
        changed = declare(events)
        bare = MCPServer("boards", extensions=[events])
        # An SDK release without the private stream path the wrapper borrows on a JSON endpoint.
        monkeypatch.setitem(sys.modules, "mcp.server._streamable_http_modern", None)
        wrapped = MCPServer("boards", extensions=[events])
        wrapped.streamable_http_app(stateless_http=True, json_response=True)
        manager = wrapped.session_manager
        endpoints = [
            bare.streamable_http_app(stateless_http=True, json_response=True),
            Starlette(
                routes=[Mount("/mcp", app=events.asgi(manager))],
                lifespan=lambda app: manager.run(),
            ),
        ]
        params = {"name": "board.changed", "arguments": {"board": "b1"}}
        for app, path in zip(endpoints, ["/mcp", "/mcp/"], strict=True):
            port, stop = await serve(app)
            url = f"http://127.0.0.1:{port}{path}"
            async with httpx2.AsyncClient(timeout=10) as http:
                listed = (await http.post(url, **request("events/list", {}))).json()
                assert listed["result"]["events"][0]["delivery"] == ["poll"]
                stream = await http.post(url, **request("events/stream", params))
                assert stream.json()["error"]["code"] == -32026
            async with boards(port, path).ext.events.subscribe(
                "board.changed", {"board": "b1"}
            ) as changes:
                assert changes.delivery == "poll"
                changed.emit(Change(board="b1", text="polled"))
                assert (await anext(changes)).data["text"] == "polled"
                await changes.cancel()
                assert await anext(changes, None) is None
            with pytest.raises(NoCompatibleDeliveryMode):
                async with boards(port, path).ext.events.subscribe(
                    "board.changed", {"board": "b1"}, delivery="push"
                ):
                    pass
            await stop()

    asyncio.run(asyncio.wait_for(exercise(), timeout=20))


def test_a_subscribe_hook_that_does_not_return_in_time_refuses_and_releases_the_id(
    monkeypatch,
):
    monkeypatch.setattr(events_module, "_SUBSCRIBE_TIMEOUT", 0.2)

    async def exercise():
        events = Events()
        calls = []

        async def on_subscribe(context, arguments, id):
            calls.append(id)
            if len(calls) == 1:
                await asyncio.Event().wait()

        declare(events, on_subscribe=on_subscribe)
        server = MCPServer("boards", extensions=[events])
        port, stop = await serve(mounted(server, events))
        url = f"http://127.0.0.1:{port}/mcp/"
        params = {"name": "board.changed", "arguments": {"board": "b1"}}
        async with httpx2.AsyncClient(timeout=10) as http:
            first = (await http.post(url, **request("events/poll", params))).json()
            assert first["error"]["code"] == -32603
            second = (await http.post(url, **request("events/poll", params))).json()
            assert "result" in second and calls[0] == calls[1]
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
        async with notebook.ext.events.subscribe(
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
