import asyncio
import base64
import json
import socket
from contextlib import asynccontextmanager
from functools import partial, wraps

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer, Server
from mcp.server.streamable_http import CONTENT_TYPE_JSON, CONTENT_TYPE_SSE
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.shared.exceptions import MCPError
from mcp.shared.inbound import MCP_METHOD_HEADER, MCP_PROTOCOL_VERSION_HEADER
from mcp.shared.subscriptions import SUBSCRIPTION_ID_META_KEY
from mcp.types import (
    CLIENT_CAPABILITIES_META_KEY,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    JSONRPC_VERSION,
    LATEST_PROTOCOL_VERSION,
    PROTOCOL_VERSION_META_KEY,
    ErrorData,
)
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.routing import Mount

from holon.client import Apps
from holon.ext.events import Events, EventType
from holon.serve import App, _asgi

IDENTITY = {"name": "watcher", "version": "1", "requires": {"boards": {}}}


class Board(BaseModel):
    board: str


class Change(BaseModel):
    board: str
    text: str


def asynchronous(test):
    @wraps(test)
    def run(*args, **kwargs):
        asyncio.run(asyncio.wait_for(test(*args, **kwargs), timeout=20))

    return run


def declare(events=None, **hooks) -> tuple[Events, EventType[dict[str, str], Change]]:
    events = events or Events()
    return events, events.event(
        "board.changed",
        input_schema=Board.model_json_schema(),
        payload_schema=Change.model_json_schema(),
        **hooks,
    )


# What is said is its own event id.
def say(changed, board, text, to=None):
    changed.emit(Change(board=board, text=text), event_id=text, subscription_id=to)


async def take(changes, count):
    return [(await anext(changes)).event_id for _ in range(count)]


async def until(condition):
    while not condition():
        await asyncio.sleep(0.01)


async def serve(app, port=0, *, timeout_graceful_shutdown=0):
    sock = socket.create_server(("127.0.0.1", port))
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="warning",
            timeout_graceful_shutdown=timeout_graceful_shutdown,
        )
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    await until(lambda: server.started)

    async def stop():
        server.should_exit = True
        await task

    return sock.getsockname()[1], stop


@pytest.fixture(autouse=True)
def inside_holon(monkeypatch):
    monkeypatch.setenv("HOLON_PLATFORM", "1")


def mounted(events):
    return _asgi(App(MCPServer("boards", extensions=[events], middleware=[events])))


def signed_in(user):
    principal = {"iss": "https://id.example", "sub": user, "client_id": "c"}
    claims = {
        "exp": 1900000000,
        "aud": "https://boards.example",
        "principal": principal,
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    return f"e30.{payload.decode()}.signature"


@asynccontextmanager
async def served(events):
    port, stop = await serve(mounted(events))
    try:
        yield port
    finally:
        await stop()


def client(port, user=None, path="/mcp"):
    binding = {"endpoint": f"http://127.0.0.1:{port}{path}"}
    if user:
        headers = {"x-holon-context": signed_in(user)}
        binding["http_client"] = lambda: httpx2.AsyncClient(headers=headers)
    return Apps(IDENTITY, {}, {"boards": binding}).boards.ext.events


def request(method, params, id=1):
    return {
        "json": {
            "jsonrpc": JSONRPC_VERSION,
            "id": id,
            "method": method,
            "params": {
                **params,
                "_meta": {
                    PROTOCOL_VERSION_META_KEY: LATEST_PROTOCOL_VERSION,
                    CLIENT_CAPABILITIES_META_KEY: {},
                },
            },
        },
        "headers": {
            "accept": f"{CONTENT_TYPE_JSON}, {CONTENT_TYPE_SSE}",
            MCP_PROTOCOL_VERSION_HEADER: LATEST_PROTOCOL_VERSION,
            MCP_METHOD_HEADER: method,
        },
    }


async def call(http, port, method, params):
    url = f"http://127.0.0.1:{port}/mcp"
    return (await http.post(url, **request(method, params))).json()


def changed_on(board, cursor=None):
    params = {"name": "board.changed", "arguments": {"board": board}}
    return params if cursor is None else {**params, "cursor": cursor}


@pytest.mark.parametrize("delivery", ["push", "poll"])
@asynchronous
async def test_a_subscription_hears_what_its_arguments_match_as_shaped_across_a_dropped_connection(
    delivery,
):
    events, changed = declare(
        match=lambda context, event, arguments: event.data.board == arguments["board"],
        transform=lambda context, event, arguments: {
            "board": event.data.board,
            "text": event.data.text.upper(),
        },
    )
    port, stop = await serve(mounted(events))
    async with client(port).subscribe(
        "board.changed", {"board": "b1"}, delivery=delivery
    ) as changes:
        say(changed, "b2", "elsewhere")
        say(changed, "b1", "one")
        assert (await anext(changes)).data == {"board": "b1", "text": "ONE"}
        await stop()
        say(changed, "b1", "two")
        port, stop = await serve(mounted(events), port)
        assert await take(changes, 1) == ["two"]
    await stop()


@pytest.mark.parametrize("user", ["ann", None])
@asynchronous
async def test_targeted_events_follow_shared_identity_but_isolate_anonymous_readers(
    user,
):
    subscribed = asyncio.Queue()
    events, changed = declare(
        on_subscribe=lambda context, arguments, id: subscribed.put_nowait(id)
    )
    async with served(events) as port:
        reader = client(port, user=user)
        async with reader.subscribe("board.changed", {"board": "b1", "n": 1}) as first:
            subscription_id = await subscribed.get()
            async with reader.subscribe(
                "board.changed", {"n": 1, "board": "b1"}
            ) as second:
                say(changed, "b1", "private", to=subscription_id)
                say(changed, "b1", "public")
                assert await take(first, 2) == ["private", "public"]
                for expected in ["private", "public"] if user else ["public"]:
                    assert (await anext(second)).event_id == expected


@asynchronous
async def test_a_cursor_from_before_a_restart_is_a_gap():
    events, changed = declare()
    restarted, gaps = declare()[1].cursor(), []
    async with served(events) as port:
        for delivery in ["push", "poll"]:
            async with client(port, user="ann").subscribe(
                "board.changed",
                {"board": delivery},
                delivery=delivery,
                cursor=restarted,
                on_gap=partial(gaps.append, delivery),
            ) as changes:
                say(changed, delivery, "hello")
                assert await take(changes, 1) == ["hello"]
    assert gaps == ["push", "poll"]


@asynchronous
async def test_a_subscription_the_app_ends_is_sent_what_was_emitted_to_it_unless_it_lost_access():
    ids = {}

    def on_subscribe(context, arguments, id):
        ids[arguments["board"]] = id

    events, changed = declare(on_subscribe=on_subscribe)
    async with served(events) as port:
        boards = client(port, user="ann")
        async with (
            boards.subscribe("board.changed", {"board": "closed"}) as closed,
            boards.subscribe("board.changed", {"board": "revoked"}) as revoked,
            boards.subscribe(
                "board.changed", {"board": "polled"}, delivery="poll"
            ) as polled,
        ):
            for board, id in ids.items():
                say(changed, board, f"last words to {board}", to=id)
            changed.terminate(ids["closed"], ErrorData(code=-32000, message="Closed"))
            changed.terminate(ids["revoked"])
            changed.terminate(ids["polled"])
            for board in ids:
                say(changed, board, "too late")
            assert await take(closed, 1) == ["last words to closed"]
            for changes, code in [
                (closed, -32000),
                (revoked, -32024),
                (polled, -32024),
            ]:
                with pytest.raises(MCPError) as ended:
                    await anext(changes)
                assert ended.value.error.code == code


@asynchronous
async def test_the_messages_on_the_wire_are_the_sketchs():
    def shaped(context, event, arguments):
        if event.data.text == "bad":
            raise ValueError("cannot be shaped")
        return event.data

    events, changed = declare(
        description="A board changed",
        transform=shaped,
        on_subscribe=lambda context, arguments, id: say(changed, "b1", "welcome"),
    )
    events.event(
        "board.locked",
        input_schema=Board.model_json_schema(),
        payload_schema=Change.model_json_schema(),
        delivery=["poll"],
        authorize=lambda context, arguments: arguments["board"] != "secret",
    )
    async with served(events) as port, httpx2.AsyncClient(timeout=10) as http:
        discovered = await call(http, port, "server/discover", {})
        assert discovered["result"]["capabilities"]["events"]["listChanged"] is False
        listed = {
            event["name"]: event
            for event in (await call(http, port, "events/list", {}))["result"]["events"]
        }
        assert set(listed["board.changed"]["delivery"]) == {"poll", "push"}
        expected = {
            "description": "A board changed",
            "inputSchema": Board.model_json_schema(),
            "payloadSchema": Change.model_json_schema(),
        }
        assert {key: listed["board.changed"][key] for key in expected} == expected
        # A poll from null has no events, and a cursor from before its setup's welcome.
        now = (await call(http, port, "events/poll", changed_on("b1")))["result"]
        assert (now["events"], now["truncated"], now["hasMore"]) == (
            [],
            False,
            False,
        )
        for text in ["one", "bad", "two", "three"]:
            say(changed, "b1", text)
        page = await call(
            http,
            port,
            "events/poll",
            {**changed_on("b1", now["cursor"]), "maxEvents": 2},
        )
        [welcome, one] = page["result"]["events"]
        assert (welcome["eventId"], one["eventId"], page["result"]["hasMore"]) == (
            "welcome",
            "one",
            True,
        )
        rest = await call(
            http, port, "events/poll", changed_on("b1", page["result"]["cursor"])
        )
        assert [event["eventId"] for event in rest["result"]["events"]] == [
            "two",
            "three",
        ]

        for method, params, code in [
            ("events/poll", {"name": "board.moved"}, -32023),
            ("events/poll", {"name": "board.changed", "arguments": {}}, INVALID_PARAMS),
            ("events/poll", changed_on("b1", 1), INVALID_PARAMS),
            (
                "events/poll",
                {"name": "board.locked", "arguments": {"board": "secret"}},
                -32024,
            ),
        ]:
            assert (await call(http, port, method, params))["error"]["code"] == code
        with pytest.raises(MCPError) as refused:
            async with client(port).subscribe(
                "board.locked", {"board": "b1"}, delivery="push"
            ):
                pass
        assert refused.value.error.code == -32026

        url, opened = f"http://127.0.0.1:{port}/mcp", changed.cursor()
        async with http.stream(
            "POST", url, **request("events/stream", changed_on("b1"), id=7)
        ) as stream:
            frames = (
                json.loads(frame.data) async for frame in httpx2.EventSource(stream)
            )
            active = await anext(frames)
            assert (active["params"]["cursor"], active["params"]["truncated"]) == (
                opened,
                False,
            )
            assert (await anext(frames))["params"]["eventId"] == "welcome"
            say(changed, "b1", "bad")
            say(changed, "b1", "four")
            error = await anext(frames)
            assert (error["method"], error["params"]["error"]["code"]) == (
                "notifications/events/error",
                INTERNAL_ERROR,
            )
            event = (await anext(frames))["params"]
            expected = {
                "eventId": "four",
                "name": "board.changed",
                "data": {"board": "b1", "text": "four"},
                "cursor": changed.cursor(),
            }
            assert {key: event[key] for key in expected} == expected
            assert event["_meta"][SUBSCRIPTION_ID_META_KEY] == 7
            changed.terminate()
            terminated = await anext(frames)
            assert (terminated["method"], terminated["params"]["error"]["code"]) == (
                "notifications/events/terminated",
                -32024,
            )
            assert "result" in await anext(frames)


@asynchronous
async def test_a_low_level_server_serves_the_events():
    events, changed = declare()
    server = Server("boards")
    events.serve(server)
    manager = StreamableHTTPSessionManager(server, stateless=True)
    port, stop = await serve(
        Starlette(
            routes=[Mount("/mcp", app=manager.handle_request)],
            lifespan=lambda app: manager.run(),
        )
    )
    async with client(port, path="/mcp/").subscribe(
        "board.changed", {"board": "b1"}
    ) as changes:
        say(changed, "b1", "hello")
        assert await take(changes, 1) == ["hello"]
    await stop()
