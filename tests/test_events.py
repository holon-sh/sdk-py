import asyncio
import json
import socket
import sys
from contextlib import asynccontextmanager
from functools import partial, wraps

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer, Server
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
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
    CallToolResult,
    ErrorData,
)
from pydantic import BaseModel, TypeAdapter
from starlette.applications import Starlette
from starlette.routing import Mount

from holon.client import Apps
from holon.ext.events import Events, NoCompatibleDeliveryMode, _server

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


def declare(events=None, **hooks):
    events = events or Events()
    return events, events.event(
        "board.changed", input_schema=Board, payload_schema=Change, **hooks
    )


# What is said is its own event id.
def say(changed, board, text, to=None):
    changed.emit(Change(board=board, text=text), event_id=text, subscription_id=to)


async def take(changes, count):
    return [(await anext(changes)).event_id for _ in range(count)]


async def until(condition):
    while not condition():
        await asyncio.sleep(0.01)


async def serve(app, port=0):
    sock = socket.create_server(("127.0.0.1", port))
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", timeout_graceful_shutdown=0)
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    await until(lambda: server.started)

    async def stop():
        server.should_exit = True
        await task

    return sock.getsockname()[1], stop


# An `x-user` header stands in for the caller Holon admits.
def mounted(events):
    server = MCPServer("boards", extensions=[events], middleware=[events])
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


@asynccontextmanager
async def served(events):
    port, stop = await serve(mounted(events))
    try:
        yield port
    finally:
        await stop()


def client(port, user=None, path="/mcp/"):
    binding = {"endpoint": f"http://127.0.0.1:{port}{path}"}
    if user:
        binding["http_client"] = lambda: httpx2.AsyncClient(headers={"x-user": user})
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
    url = f"http://127.0.0.1:{port}/mcp/"
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
        match=lambda context, event, arguments: event.data.board == arguments.board,
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


@asynchronous
async def test_a_signed_in_caller_has_one_subscription_per_arguments_and_a_caller_without_an_identity_its_own():
    began, ended = [], []
    events, _ = declare(
        on_subscribe=lambda context, arguments, id: began.append(id),
        on_unsubscribe=lambda context, arguments, id: ended.append(id),
    )
    async with served(events) as port:
        ann, anonymous = client(port, user="ann"), client(port)
        async with (
            ann.subscribe("board.changed", {"board": "b1"}),
            ann.subscribe("board.changed", {"board": "b1"}),
            anonymous.subscribe("board.changed", {"board": "b1"}),
            anonymous.subscribe("board.changed", {"board": "b1"}),
        ):
            assert len(set(began)) == len(began) == 3
        await until(lambda: len(ended) == 3)
        async with ann.subscribe("board.changed", {"board": "b1"}):
            assert began[3] == began[0]


@pytest.mark.parametrize("delivery", ["push", "poll"])
@asynchronous
async def test_a_subscription_behind_what_is_kept_is_told_of_the_gap_and_carries_on(
    monkeypatch, delivery
):
    monkeypatch.setattr(_server, "_KEEP_EVENTS", 2)
    events, changed = declare()
    heard = []
    async with (
        served(events) as port,
        client(port).subscribe(
            "board.changed",
            {"board": "b1"},
            delivery=delivery,
            on_gap=lambda: heard.append("gap"),
        ) as changes,
    ):
        for text in ["a", "b", "c", "d"]:
            say(changed, "b1", text)
        heard.extend(await take(changes, 2))
    assert heard == ["gap", "c", "d"]


@asynchronous
async def test_a_stream_is_sent_everything_emitted_to_its_subscription_past_what_is_kept(
    monkeypatch,
):
    monkeypatch.setattr(_server, "_KEEP_BYTES", 1)
    ids, heard = [], []
    events, changed = declare(
        on_subscribe=lambda context, arguments, id: ids.append(id)
    )
    async with (
        served(events) as port,
        client(port).subscribe(
            "board.changed", {"board": "b1"}, on_gap=lambda: heard.append("gap")
        ) as changes,
    ):
        for text in ["a", "b", "c", "d"]:
            say(changed, "b1", text, to=ids[0])
        heard.extend(await take(changes, 4))
    assert heard == ["a", "b", "c", "d"]


@asynchronous
async def test_what_a_subscribe_hook_emits_reaches_its_subscription_first_and_a_cursor_from_before_a_restart_is_a_gap():
    events, changed = declare(
        on_subscribe=lambda context, arguments, id: say(
            changed, arguments.board, "welcome", to=id
        )
    )
    restarted = declare()[1].cursor()
    gaps = []
    async with served(events) as port:
        for cursor in [None, restarted]:
            for delivery in ["push", "poll"]:
                board = f"{delivery} {'resumed' if cursor else 'fresh'}"
                async with client(port, user="ann").subscribe(
                    "board.changed",
                    {"board": board},
                    delivery=delivery,
                    cursor=cursor,
                    on_gap=partial(gaps.append, board),
                ) as changes:
                    say(changed, board, "hello")
                    assert await take(changes, 2) == ["welcome", "hello"]
    assert gaps == ["push resumed", "poll resumed"]


@asynchronous
async def test_a_subscription_the_app_ends_is_sent_what_was_emitted_to_it_unless_it_lost_access():
    ids = {}
    events, changed = declare(
        on_subscribe=lambda context, arguments, id: ids.setdefault(arguments.board, id)
    )
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
        input_schema=Board,
        payload_schema=Change,
        delivery=["poll"],
        authorize=lambda context, arguments: arguments.board != "secret",
    )
    async with served(events) as port, httpx2.AsyncClient(timeout=10) as http:
        discovered = await call(http, port, "server/discover", {})
        assert discovered["result"]["capabilities"]["events"] == {"listChanged": False}
        [listed, _] = (await call(http, port, "events/list", {}))["result"]["events"]
        assert listed == {
            "name": "board.changed",
            "description": "A board changed",
            "delivery": ["poll", "push"],
            "inputSchema": Board.model_json_schema(),
            "payloadSchema": Change.model_json_schema(),
        }
        # A poll from null has no events, and a cursor from before its setup's welcome.
        now = (await call(http, port, "events/poll", changed_on("b1")))["result"]
        assert (now["events"], now["truncated"], now["hasMore"], now["nextPollMs"]) == (
            [],
            False,
            False,
            1000,
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
        assert set(one) == {"eventId", "name", "timestamp", "data"}
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
                "events/stream",
                {"name": "board.locked", "arguments": {"board": "b1"}},
                -32026,
            ),
            (
                "events/poll",
                {"name": "board.locked", "arguments": {"board": "secret"}},
                -32024,
            ),
        ]:
            assert (await call(http, port, method, params))["error"]["code"] == code

        url, opened = f"http://127.0.0.1:{port}/mcp/", changed.cursor()
        async with http.stream(
            "POST", url, **request("events/stream", changed_on("b1"), id=7)
        ) as stream:
            frames = (
                json.loads(frame.data) async for frame in httpx2.EventSource(stream)
            )
            meta = {"_meta": {SUBSCRIPTION_ID_META_KEY: 7}}
            active = await anext(frames)
            assert active["params"] == {"cursor": opened, "truncated": False, **meta}
            assert (await anext(frames))["params"]["eventId"] == "welcome"
            say(changed, "b1", "bad")
            say(changed, "b1", "four")
            error = await anext(frames)
            assert (error["method"], error["params"]["error"]["code"]) == (
                "notifications/events/error",
                INTERNAL_ERROR,
            )
            event = (await anext(frames))["params"]
            assert event == {
                "eventId": "four",
                "name": "board.changed",
                "timestamp": event["timestamp"],
                "data": {"board": "b1", "text": "four"},
                "cursor": changed.cursor(),
                **meta,
            }
            changed.terminate()
            terminated = await anext(frames)
            assert (terminated["method"], terminated["params"]["error"]) == (
                "notifications/events/terminated",
                {"code": -32024, "message": "Forbidden"},
            )
            assert "result" in await anext(frames)


@asynchronous
async def test_a_poll_that_gives_up_while_it_is_set_up_holds_its_place_until_its_lease_lapses(
    monkeypatch,
):
    monkeypatch.setattr(_server, "_POLL_SUBSCRIPTION_TTL", 0.1)
    monkeypatch.setattr(_server, "_MAX_SUBSCRIPTIONS", 1)
    setting_up, release, ended = asyncio.Event(), asyncio.Event(), []

    async def on_subscribe(context, arguments, id):
        setting_up.set()
        await release.wait()

    events, _ = declare(
        on_subscribe=on_subscribe,
        on_unsubscribe=lambda context, arguments, id: ended.append(id),
    )
    app, left = mounted(events), asyncio.Event()

    # Tells when the server sees the poll that gives up disconnect.
    async def noticing(scope, receive, send):
        async def receiving():
            message = await receive()
            if message["type"] == "http.disconnect":
                left.set()
            return message

        leaving = (b"x-leaving", b"1") in scope.get("headers", [])
        await app(scope, receiving if leaving else receive, send)

    async def give_up():
        async with httpx2.AsyncClient(headers={"x-leaving": "1"}) as http:
            await call(http, port, "events/poll", changed_on("b1"))

    port, stop = await serve(noticing)
    giving_up = asyncio.ensure_future(give_up())
    await setting_up.wait()
    async with httpx2.AsyncClient(timeout=10) as http:
        exhausted = await call(http, port, "events/poll", changed_on("b2"))
        assert exhausted["error"]["data"] == {"limit": "subscriptions", "max": 1}
        giving_up.cancel()
        await left.wait()
        release.set()
        await until(lambda: ended)
        assert "result" in await call(http, port, "events/poll", changed_on("b2"))
    await stop()


@asynchronous
async def test_a_subscribe_hook_that_does_not_return_in_time_refuses_and_releases_the_id(
    monkeypatch,
):
    monkeypatch.setattr(_server, "_SUBSCRIBE_TIMEOUT", 0.1)
    calls = []

    async def on_subscribe(context, arguments, id):
        calls.append(id)
        if len(calls) == 1:
            await asyncio.Event().wait()

    events, _ = declare(on_subscribe=on_subscribe)
    async with served(events) as port, httpx2.AsyncClient(timeout=10) as http:
        refused = await call(http, port, "events/poll", changed_on("b1"))
        assert refused["error"]["code"] == INTERNAL_ERROR
        assert "result" in await call(http, port, "events/poll", changed_on("b1"))
    assert calls[0] == calls[1]


@asynchronous
async def test_where_push_cannot_be_served_a_subscription_polls(monkeypatch):
    events, changed = declare()
    bare = MCPServer("boards", extensions=[events])
    # An SDK without the private stream path `Events.asgi` uses on a JSON endpoint.
    monkeypatch.setitem(sys.modules, "mcp.server._streamable_http_modern", None)
    wrapped = MCPServer("boards", extensions=[events])
    wrapped.streamable_http_app(stateless_http=True, json_response=True)
    manager = wrapped.session_manager
    for app, path in [
        (bare.streamable_http_app(stateless_http=True, json_response=True), "/mcp"),
        (
            Starlette(
                routes=[Mount("/mcp", app=events.asgi(manager))],
                lifespan=lambda app: manager.run(),
            ),
            "/mcp/",
        ),
    ]:
        port, stop = await serve(app)
        boards = client(port, path=path)
        async with boards.subscribe("board.changed", {"board": "b1"}) as changes:
            assert changes.delivery == "poll"
            say(changed, "b1", "polled")
            assert await take(changes, 1) == ["polled"]
            await changes.cancel()
            assert await anext(changes, None) is None
        with pytest.raises(NoCompatibleDeliveryMode):
            async with boards.subscribe(
                "board.changed", {"board": "b1"}, delivery="push"
            ):
                pass
        await stop()


@asynchronous
async def test_a_subscription_from_the_cursor_read_with_a_snapshot_hears_every_change_after_it_on_a_json_endpoint():
    events, said = Events(), []
    changed = events.event(
        "board.changed", input_schema=Board, payload_schema=TypeAdapter(Change)
    )

    def tell(text):
        said.append(text)
        say(changed, "b1", text)

    async def snapshot(context, params):
        state = {"said": list(said), "cursor": changed.cursor()}
        return CallToolResult(content=[], structured_content=state)

    server = Server("boards", on_call_tool=snapshot)
    events.serve(server)
    manager = StreamableHTTPSessionManager(server, stateless=True, json_response=True)
    port, stop = await serve(
        Starlette(
            routes=[Mount("/mcp", app=events.asgi(manager))],
            lifespan=lambda app: manager.run(),
        )
    )
    tell("in the snapshot")
    binding = {"endpoint": f"http://127.0.0.1:{port}/mcp/"}
    boards = Apps(IDENTITY, {}, {"boards": binding}).boards
    state = (await boards.call_tool("snapshot")).structured_content
    tell("after the snapshot")
    async with client(port).subscribe(
        "board.changed", {"board": "b1"}, cursor=state["cursor"]
    ) as changes:
        assert changes.delivery == "push"
        tell("once subscribed")
        assert state["said"] + await take(changes, 2) == said
    await stop()
