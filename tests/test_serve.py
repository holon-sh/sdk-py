import asyncio
import base64
import json
import os
import signal
import socket
import subprocess
import sys
from contextlib import asynccontextmanager
from functools import wraps
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import ClassVar

import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.mcpserver import Context
from mcp.shared.exceptions import MCPError
from mcp.types import (
    LATEST_PROTOCOL_VERSION,
)
from opentelemetry import propagate
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from starlette.datastructures import Headers
from starlette.responses import RedirectResponse, Response
from test_events import request

from holon.client import Apps
from holon.context import Caller, Principal, RequestContext, _request, get_context
from holon.serve import App, _asgi

PERSON = {"iss": "https://id.example", "sub": "ann", "client_id": "web"}
PROFILE = {"email": "ann@example.com", "email_verified": True}
REPORTER = "production/reporter"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
IDENTITY = {"name": "caller", "version": "1", "requires": {"peer": {}}}


def asynchronous(test):
    @wraps(test)
    def run(*args, **kwargs):
        asyncio.run(asyncio.wait_for(test(*args, **kwargs), timeout=20))

    return run


def context(**payload):
    claims = {"exp": 1900000060.25, "aud": ["https://holon.example"], **payload}
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    return f"e30.{encoded.decode()}.signature"


async def until(condition):
    while not condition():
        await asyncio.sleep(0.01)


@asynccontextmanager
async def served(app):
    sock = socket.create_server(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    await until(lambda: server.started)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        await task


async def call(url, tool, headers):
    binding = {
        "endpoint": url,
        "http_client": lambda: httpx2.AsyncClient(headers=headers),
    }
    result = await Apps(IDENTITY, {}, {"peer": binding}).peer.call_tool(tool)
    assert not result.is_error, result
    return result


def inspector(seen):
    server = MCPServer("inspector")

    @server.tool()
    async def inspect(ctx: Context) -> str:
        trace = {}
        propagate.inject(trace)
        seen.append(
            (
                get_context(),
                get_access_token(),
                set(ctx.headers),
                trace.get("traceparent"),
            )
        )
        return "seen"

    return server


@asynchronous
async def test_inside_holon_a_tool_sees_whom_the_waypoint_admitted_and_not_the_headers_that_said_so(
    monkeypatch,
):
    monkeypatch.setenv("HOLON_PLATFORM", "1")
    signed_in = context(
        principal={
            **PERSON,
            "scope": " read  write ",
            "claims": PROFILE,
            "act": {"sub": "agent"},
        },
        flowId="flow",
    )
    ann = Principal(
        "https://id.example", "ann", "web", ["read", "write"], PROFILE, {"sub": "agent"}
    )
    seen = []
    async with served(_asgi(App(inspector(seen)))) as origin:
        for path, headers, expected in [
            ("/mcp", {"x-holon-context": signed_in}, RequestContext(ann, None)),
            (
                "/mcp/inspector",
                {
                    "x-holon-context": context(principal=PERSON),
                    "x-holon-caller": REPORTER,
                },
                RequestContext(
                    Principal("https://id.example", "ann", "web", [], {}),
                    Caller("reporter", "production"),
                ),
            ),
            (
                "/mcp",
                {"x-holon-caller": REPORTER},
                RequestContext(None, Caller("reporter", "production")),
            ),
            ("/mcp", {"x-holon-context": context()}, RequestContext(None, None)),
        ]:
            forged = {"x-holon-other": "forged", "authorization": "Bearer forged"}
            headers = {**headers, **forged, "traceparent": TRACEPARENT}
            await call(origin + path, "inspect", headers)
            admitted, token, names, traceparent = seen.pop()
            assert admitted == expected
            assert (token is None) == (expected == RequestContext(None, None))
            assert not {name for name in names if name.startswith("x-holon-")}
            assert "authorization" not in names
            assert traceparent == TRACEPARENT


@asynchronous
async def test_a_malformed_context_is_a_bad_request(monkeypatch):
    monkeypatch.setenv("HOLON_PLATFORM", "1")
    async with served(_asgi(App(MCPServer("strict")))) as origin:
        for headers in [
            [("x-holon-context", context()), ("x-holon-context", context())],
            [("x-holon-caller", REPORTER), ("x-holon-caller", REPORTER)],
            [("x-holon-caller", "reporter")],
            [("x-holon-context", context(principal="user"))],
        ]:
            async with httpx2.AsyncClient() as client:
                response = await client.post(
                    origin + "/mcp", headers=headers, content="{}"
                )
            assert (response.status_code, response.text) == (
                400,
                "Malformed Holon context\n",
            ), headers


@asynchronous
async def test_outside_holon_the_same_headers_make_an_anonymous_request(monkeypatch):
    monkeypatch.delenv("HOLON_PLATFORM", raising=False)
    seen = []
    async with served(_asgi(App(inspector(seen)))) as origin:
        headers = {
            "x-holon-context": context(principal=PERSON),
            "x-holon-caller": REPORTER,
            "authorization": "Bearer forged",
        }
        await call(origin + "/mcp", "inspect", headers)
    [(admitted, _, _, _)] = seen
    assert admitted == RequestContext(None, None)


@asynchronous
async def test_a_peer_call_carries_the_context_until_its_request_ends_and_the_apps_token_always(
    monkeypatch,
):
    monkeypatch.setenv("HOLON_PLATFORM", "1")
    monkeypatch.setenv("PEER_MCP_TOKEN", "secret")
    peer, received = MCPServer("peer"), []

    @peer.tool()
    def ping() -> str:
        return "pong"

    answer = peer.streamable_http_app(stateless_http=True)

    async def recording(scope, receive, send):
        if scope["type"] == "http":
            headers = Headers(scope=scope)
            received.append(
                (headers.get("x-holon-context"), headers.get("authorization"))
            )
        await answer(scope, receive, send)

    server, apps, phases, later = MCPServer("caller"), Apps(IDENTITY, {}), [], []

    @server.tool()
    async def relay() -> str:
        await apps.peer.call_tool("ping")
        phases.append(len(received))
        bound = {"endpoint": os.environ["PEER_MCP_URL"]}
        await Apps(IDENTITY, {}, {"peer": bound}).peer.call_tool("ping")
        phases.append(len(received))
        request = _request.get()

        async def afterwards():
            await until(lambda: request.ended)
            await apps.peer.call_tool("ping")

        later.append(asyncio.create_task(afterwards()))
        return "relayed"

    token = context(principal=PERSON)
    async with served(recording) as peer_origin, served(_asgi(App(server))) as origin:
        monkeypatch.setenv("PEER_MCP_URL", peer_origin + "/mcp")
        await call(origin + "/mcp", "relay", {"x-holon-context": token})
        await later[0]
    during, bound = received[: phases[0]], received[phases[0] : phases[1]]
    assert set(during) == {(token, "Bearer secret")}
    assert set(bound) == {(None, None)}
    assert set(received[phases[1] :]) == {(None, "Bearer secret")}


@asynchronous
async def test_a_peer_that_redirects_is_not_followed(monkeypatch):
    reached = []

    async def elsewhere(scope, receive, send):
        if scope["type"] == "http":
            reached.append(scope["path"])
            await Response("followed")(scope, receive, send)

    async with served(elsewhere) as target:

        async def moved(scope, receive, send):
            if scope["type"] == "http":
                await RedirectResponse(target + "/mcp", 307)(scope, receive, send)

        async with served(moved) as origin:
            monkeypatch.setenv("PEER_MCP_URL", origin + "/mcp")
            with pytest.raises(MCPError):
                await Apps(IDENTITY, {}).peer.call_tool("ping")
    assert reached == []


@asynchronous
async def test_the_frontend_serves_its_files_and_nothing_hidden_or_outside_it(
    tmp_path,
):
    web = tmp_path / "web"
    web.mkdir()
    (web / "index.html").write_text("<p>Home</p>")
    (web / "style.css").write_text("p {}")
    (web / ".private").write_text("Hidden")
    (tmp_path / "secret").write_text("Outside")
    (web / "escape").symlink_to(tmp_path / "secret")
    app = _asgi(App(MCPServer("site"), web))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://app"
    ) as http:
        for path, body, kind in [
            ("/", "<p>Home</p>", "text/html"),
            ("/style.css", "p {}", "text/css"),
        ]:
            response = await http.get(path)
            assert (response.status_code, response.text) == (200, body), path
            assert response.headers["content-type"].startswith(kind)
            assert response.headers["x-content-type-options"] == "nosniff"
        assert (await http.head("/style.css")).status_code == 200
        for path in ["/.private", "/escape", "/missing"]:
            assert (await http.get(path)).status_code == 404, path
        assert (await http.post("/")).status_code == 404


class _Collector(BaseHTTPRequestHandler):
    exports: ClassVar[list[bytes]] = []

    def do_POST(self):
        _Collector.exports.append(self.rfile.read(int(self.headers["content-length"])))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


def test_python_m_holon_serve_runs_an_entry_with_a_16_kib_context_exports_its_spans_and_on_sigterm_closes_its_streams_and_stops(
    tmp_path,
):
    (tmp_path / "src").mkdir()
    (tmp_path / "web").mkdir()
    (tmp_path / "web/index.html").write_text("<p>Home</p>")
    (tmp_path / "src/server.py").write_text(
        """from mcp.server import MCPServer
from holon.ext.events import Events

events = Events()
events.event("ticked", input_schema={"type": "object"}, payload_schema={"type": "object"})
server = MCPServer("large", extensions=[events], middleware=[events])
frontend = "web"
"""
    )
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    collector = HTTPServer(("127.0.0.1", 0), _Collector)
    Thread(target=collector.serve_forever, daemon=True).start()
    process = subprocess.Popen(
        [sys.executable, "-m", "holon.serve", "src/server.py"],
        cwd=tmp_path,
        env={
            **os.environ,
            "PORT": str(port),
            "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{collector.server_port}",
            "OTEL_SERVICE_NAME": "large",
            "OTEL_RESOURCE_ATTRIBUTES": "holon.session=production",
        },
    )

    async def exercise():
        origin = f"http://127.0.0.1:{port}"
        async with httpx2.AsyncClient() as http:
            while True:
                try:
                    home = await http.get(origin)
                    break
                except httpx2.ConnectError:
                    await asyncio.sleep(0.05)
        assert home.text == "<p>Home</p>"
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        head = f"GET / HTTP/1.1\r\nhost: app\r\nx-holon-context: {'a' * 16384}\r\n"
        writer.write(head.encode())
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.write(b"\r\n")
        assert (await reader.readline()).startswith(b"HTTP/1.1 200")
        writer.close()
        ticked = request("events/stream", {"name": "ticked", "arguments": {}}, id=7)
        async with (
            Client(origin + "/mcp", mode=LATEST_PROTOCOL_VERSION) as client,
            client.listen(tools_list_changed=True) as subscription,
            httpx2.AsyncClient() as http,
            http.stream("POST", origin + "/mcp", **ticked) as sent,
        ):
            frames = sent.aiter_lines()
            while not (await anext(frames)).startswith("data: "):
                pass
            process.send_signal(signal.SIGTERM)
            last = [frame async for frame in frames if frame.startswith("data: ")][-1]
            async for _ in subscription:
                pass
        assert "result" in json.loads(last.removeprefix("data: "))

    try:
        asyncio.run(asyncio.wait_for(exercise(), timeout=20))
        process.wait(timeout=10)
    finally:
        process.kill()
        collector.shutdown()
    exported = {}
    for body in _Collector.exports:
        for spans in ExportTraceServiceRequest.FromString(body).resource_spans:
            resource = {
                each.key: each.value.string_value for each in spans.resource.attributes
            }
            for scoped in spans.scope_spans:
                exported |= {span.name: resource for span in scoped.spans}
    resource = {"service.name": "large", "holon.session": "production"}
    assert resource.items() <= exported["events/stream ticked"].items()
