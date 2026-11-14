"""The files extension end to end: a low-level server behind Starlette, with
its storage under the MCP address, called by holon's client."""

import asyncio
import base64
import hashlib
import json
import os
import socket
from contextlib import asynccontextmanager
from pathlib import Path

import httpx2
import pytest
import uvicorn
from mcp.server import MCPServer
from mcp.server.lowlevel import Server
from mcp.server.mcpserver import Context
from mcp.server.streamable_http import CONTENT_TYPE_JSON, CONTENT_TYPE_SSE
from mcp.shared.inbound import (
    MCP_METHOD_HEADER,
    MCP_NAME_HEADER,
    MCP_PROTOCOL_VERSION_HEADER,
)
from mcp.types import (
    CLIENT_CAPABILITIES_META_KEY,
    INVALID_PARAMS,
    JSONRPC_VERSION,
    LATEST_PROTOCOL_VERSION,
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    PROTOCOL_VERSION_META_KEY,
    CallToolResult,
    ElicitRequest,
    ElicitRequestFormParams,
    ElicitResult,
    InputRequiredResult,
    ListToolsResult,
    TextContent,
    Tool,
)
from starlette import status
from starlette.responses import Response
from starlette.routing import Mount, Route, Router

from holon.client import Apps
from holon.ext.files import (
    EXTENSION,
    File,
    FileServer,
    FileTransferError,
    LocalStorage,
    OutcomeUnknownError,
    ReceivedFile,
    downloads,
    file_input_schema,
)

ENDPOINT = "http://127.0.0.1:8000/mcp"
CSV = b"a,b\n1,2\n"
TOOLS = [
    {
        "name": "save",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file": file_input_schema(max_size=4096),
                "cover": {"anyOf": [file_input_schema(), {"type": "null"}]},
                "note": {"type": "string"},
            },
            "required": ["file"],
        },
    },
    {
        "name": "check",
        "inputSchema": {
            "type": "object",
            "properties": {
                "files": {
                    "type": "array",
                    "items": file_input_schema(accept=["text/plain"], digest=True),
                }
            },
        },
        "annotations": {"idempotentHint": True},
    },
    {"name": "export", "inputSchema": {"type": "object"}},
]


def serve(tmp_path):
    """A server whose tools save, check and export files; returns (app, ran)."""
    storage = LocalStorage(tmp_path / "storage")
    files = FileServer(
        storage, tools={t["name"]: t for t in TOOLS}.get, max_inline_bytes=16
    )
    ran = []
    (tmp_path / "saved").mkdir()

    async def list_tools(ctx, params):
        return ListToolsResult(tools=[Tool.model_validate(tool) for tool in TOOLS])

    async def call_tool(ctx, params):
        ran.append(params.name)
        arguments = params.arguments or {}
        if params.name == "export":
            return await files.reply(ctx, CSV, name="report.csv", text="Exported.")
        if arguments.get("note") == "ask" and not params.input_responses:
            question = ElicitRequestFormParams(
                message="Why?", requested_schema={"type": "object", "properties": {}}
            )
            return InputRequiredResult(
                input_requests={"why": ElicitRequest(params=question)},
                request_state="asked",
            )
        for value in arguments.get("files") or [arguments["file"]]:
            received = ReceivedFile(value)
            if params.name == "save":
                await received.save(tmp_path / "saved" / Path(received.name).name)
        return CallToolResult(content=[TextContent(type="text", text="ok")])

    server = Server("files", on_list_tools=list_tools, on_call_tool=call_tool)
    server.extensions[EXTENSION] = files.settings()
    server.middleware.insert(0, files)
    app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        custom_starlette_routes=[Mount("/mcp/files", app=storage.app)],
    )
    return app, ran


def recording(app, seen):
    async def asgi(scope, receive, send):
        if scope["type"] != "http":
            return await app(scope, receive, send)
        entry = {
            "method": scope["method"],
            "path": scope["path"],
            "headers": {k.decode(): v.decode() for k, v in scope["headers"]},
            "body": b"",
        }
        seen.append(entry)

        async def tap():
            message = await receive()
            entry["body"] += message.get("body", b"")
            return message

        await app(scope, tap, send)

    return asgi


def client(app, seen, tools=TOOLS, **binding):
    def http_client():
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=recording(app, seen)),
            headers={"Authorization": "Bearer mcp"},
            cookies={"session": "secret"},
        )

    return Apps(
        {"name": "caller", "version": "1", "requires": {}},
        {"files": {"tools": tools}},
        {"files": {"endpoint": ENDPOINT, "http_client": http_client, **binding}},
    ).files


def tool_calls(seen):
    bodies = [json.loads(e["body"]) for e in seen if e["path"] == "/mcp"]
    return [body for body in bodies if body["method"] == "tools/call"]


def run(app, body):
    async def main():
        async with app.router.lifespan_context(app):
            await body()

    asyncio.run(asyncio.wait_for(main(), timeout=20))


async def answer(params):
    return ElicitResult(action="accept", content={})


def test_files_reach_the_tool_inline_and_out_of_band(tmp_path):
    (tmp_path / "notes.txt").write_bytes(b"x" * 100)
    app, ran = serve(tmp_path)
    seen = []
    files = client(app, seen)

    async def body():
        await files.tools.save(file=File(b"tiny", name="a.txt"), cover=None)
        [inline] = tool_calls(seen)
        assert inline["params"]["arguments"]["file"]["data"] == "dGlueQ=="
        seen.clear()

        await files.tools.save(
            {"elicit": answer}, file=File(tmp_path / "notes.txt"), note="ask"
        )
        first, upload_retry, _ = tool_calls(seen)
        assert "data" not in first["params"]["arguments"]["file"]
        assert upload_retry["params"]["arguments"] == first["params"]["arguments"]
        assert "inputResponses" not in upload_retry["params"]
        [put] = [e for e in seen if e["method"] == "PUT"]
        assert put["path"].startswith("/mcp/files/") and put["body"] == b"x" * 100
        # A path target goes through the binding's client, without its credentials.
        assert not {"authorization", "cookie", "content-type"} & put["headers"].keys()

        # The retry that answered the upload request is spent.
        [replay] = [
            e
            for e in seen
            if e["path"] == "/mcp" and json.loads(e["body"]) == upload_retry
        ]
        headers = {k: v for k, v in replay["headers"].items() if k != "content-length"}
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as raw:
            response = await raw.post(ENDPOINT, content=replay["body"], headers=headers)
        assert response.json()["error"]["code"] == INVALID_PARAMS

    run(app, body)
    assert (tmp_path / "saved/a.txt").read_bytes() == b"tiny"
    assert (tmp_path / "saved/notes.txt").read_bytes() == b"x" * 100
    assert ran == ["save"] * 3


def test_a_declared_file_argument_reaches_the_tool_after_an_input_request(tmp_path):
    (tmp_path / "notes.txt").write_bytes(b"x" * 100)
    storage = LocalStorage(tmp_path / "storage")
    server = MCPServer("files")
    read = []

    async def save(file: ReceivedFile, ctx: Context) -> str:
        if not ctx.input_responses:
            question = ElicitRequestFormParams(
                message="Why?", requested_schema={"type": "object", "properties": {}}
            )
            return InputRequiredResult(
                input_requests={"why": ElicitRequest(params=question)},
                request_state="asked",
            )
        read.append(await file.read())
        return "ok"

    server.add_tool(save)
    tools = [
        tool.model_dump(by_alias=True, exclude_none=True)
        for tool in asyncio.run(server.list_tools())
    ]
    files = FileServer(
        storage, tools={t["name"]: t for t in tools}.get, max_inline_bytes=16
    )
    server._lowlevel_server.extensions[EXTENSION] = files.settings()
    server.middleware.insert(0, files)
    app = server.streamable_http_app(stateless_http=True, json_response=True)
    app.mount("/mcp/files", storage.app)

    async def body():
        for file in [File(b"tiny"), File(tmp_path / "notes.txt")]:
            await client(app, [], tools).tools.save({"elicit": answer}, file=file)

    run(app, body)
    assert read == [b"tiny", b"x" * 100]


def sink(attempts, served=None):
    """Where transfers land: records each (method, authorization), serves
    ``served`` by name, and answers 503 for ``busy``."""

    async def transfer(request):
        attempts.append((request.method, request.headers.get("authorization")))
        name = request.path_params["name"]
        if request.method == "GET":
            return Response(served[name])
        return Response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
            if name == "busy"
            else status.HTTP_201_CREATED
        )

    return Router(routes=[Route("/{name}", transfer, methods=["GET", "PUT", "POST"])])


def scripted(replies, transfers):
    """A server that answers tools/call from ``replies``, with ``transfers``
    under its MCP address."""

    async def call_tool(ctx, params):
        return replies.pop(0)(params.arguments)

    server = Server("scripted", on_call_tool=call_tool)
    server.extensions[EXTENSION] = {"upload": {}}
    return server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        custom_starlette_routes=[Mount("/mcp/files", app=transfers)],
    )


@asynccontextmanager
async def listening(app):
    """Serves ``app`` on a loopback port, for url targets; yields its origin."""
    sock = socket.create_server(("127.0.0.1", 0))
    http = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning"))
    serving = asyncio.create_task(http.serve(sockets=[sock]))
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        http.should_exit = True
        await serving


def upload(requestState="s", **target):
    def reply(arguments):
        file_id = arguments["file"]["id"]
        uploads = [{"fileId": file_id, "credentials": "omit", **target}]
        return {
            "resultType": "upload_required",
            "uploads": uploads,
            "requestState": requestState,
        }

    return reply


def done(arguments):
    return CallToolResult(content=[TextContent(type="text", text="ok")])


def broken(arguments):
    raise RuntimeError("lost")


def test_the_client_checks_every_upload_request_and_retries_only_after_success():
    replies, attempts, seen = [], [], []
    transfers = sink(attempts)
    app = scripted(replies, transfers)
    form = {"fields": [], "fileField": "file"}

    async def body():
        async with listening(transfers) as origin:
            listed = client(app, seen, file_origins=[origin])

            async def call(files=listed):
                return await files.tools.save(file=File(b"0123456789"))

            # Every target is reachable, so only the rule it breaks keeps the
            # bytes from it.
            for files, target in [
                (listed, {"path": "/files/ok", "headers": {"Authorization": "x"}}),
                (listed, {"path": "/files/ok", "headers": {"X-Forwarded-Host": "e"}}),
                (listed, {"path": "/files/../files/ok"}),
                (listed, {"path": "/files/ok", "url": f"{origin}/ok"}),
                (listed, {"path": "/files/ok", "size": 3}),
                (listed, {"path": "/files/ok", "form": form}),
                (listed, {"url": origin.replace("//", "//user@") + "/ok"}),
                (listed, {"url": f"{origin}/ok", "credentials": "include"}),
                (client(app, seen), {"url": f"{origin}/ok"}),  # plain http, unlisted
                (listed, {"path": "/files/ok", "fileId": "another"}),
                (listed, {"path": "/files/ok", "requestState": None}),  # on the result
            ]:
                replies.append(upload(**target))
                with pytest.raises(FileTransferError):
                    await call(files)
            assert attempts == [] and len(tool_calls(seen)) == 11

            # A failed PUT is repeated a bounded number of times, a POST never; no retry.
            seen.clear()
            replies.append(upload(path="/files/busy"))
            with pytest.raises(FileTransferError):
                await call()
            replies.append(upload(path="/files/busy", method="POST", form=form))
            with pytest.raises(FileTransferError):
                await call()
            assert attempts == [("PUT", None)] * 3 + [("POST", None)]
            assert len(tool_calls(seen)) == 2

            # Once the files are sent, failures leave the outcome unknown.
            attempts.clear()
            seen.clear()
            replies.extend([upload(path="/files/ok"), upload(path="/files/ok")])
            with pytest.raises(OutcomeUnknownError):
                await call()
            replies.extend([upload(path="/files/ok"), broken])
            with pytest.raises(OutcomeUnknownError):
                await call()
            assert len(attempts) == 2 and len(tool_calls(seen)) == 4

            # include attaches the credential the client sends to the MCP address.
            attempts.clear()
            replies.extend([upload(path="/files/ok", credentials="include"), done])
            await call()
            assert attempts == [("PUT", "Bearer mcp")]

    run(app, body)


def test_a_form_target_on_an_allowed_origin_gets_a_multipart_post():
    received = []

    async def receive(request):
        received.append((await request.body(), request.headers))
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    replies = []
    app = scripted(replies, Router())
    fields = [{"name": "key", "value": "k"}, {"name": "policy", "value": "signed"}]
    pdf = File(b"%PDF-1.7 report", name='q3/Q3 "final".pdf')

    async def body():
        storage = Router(routes=[Route("/upload", receive, methods=["POST"])])
        async with listening(storage) as origin:
            form = {"fields": fields, "fileField": "file"}
            url = f"{origin}/upload"
            replies.extend([upload(url=url, method="POST", form=form), done])
            await client(app, [], file_origins=[origin]).tools.save(file=pdf)

    run(app, body)
    [(raw, headers)] = received
    boundary = headers["content-type"].split("boundary=")[1].encode()
    assert len(boundary) >= 32 and headers["content-length"] == str(len(raw))
    assert raw.split(b"--" + boundary)[1:] == [
        b'\r\nContent-Disposition: form-data; name="key"\r\n\r\nk\r\n',
        b'\r\nContent-Disposition: form-data; name="policy"\r\n\r\nsigned\r\n',
        (
            b'\r\nContent-Disposition: form-data; name="file"; filename="Q3 %22final%22.pdf"'
            b"\r\nContent-Type: application/pdf\r\n\r\n%PDF-1.7 report\r\n"
        ),
        b"--\r\n",
    ]
    assert "authorization" not in headers and "cookie" not in headers


def rpc(http, method, params=None, *, declare=True):
    capabilities = {"extensions": {EXTENSION: {"upload": {}, "download": {}}}}
    meta = {
        PROTOCOL_VERSION_META_KEY: LATEST_PROTOCOL_VERSION,
        CLIENT_CAPABILITIES_META_KEY: capabilities if declare else {},
    }
    params = {**(params or {}), "_meta": meta}
    headers = {
        "accept": f"{CONTENT_TYPE_JSON}, {CONTENT_TYPE_SSE}",
        MCP_PROTOCOL_VERSION_HEADER: LATEST_PROTOCOL_VERSION,
        MCP_METHOD_HEADER: method,
    }
    if "name" in params:
        headers[MCP_NAME_HEADER] = params["name"]
    body = {"jsonrpc": JSONRPC_VERSION, "id": 1, "method": method, "params": params}
    return http.post(ENDPOINT, json=body, headers=headers)


def text_file(file_id="f", data=None, **fields):
    entry = {"id": file_id, "name": "a.txt", "mimeType": "text/plain", "size": 10}
    if data is not None:
        entry |= {"data": base64.b64encode(data).decode(), "size": len(data)}
    return entry | fields


def digest(data):
    value = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode()
    return {"algorithm": "sha-256", "value": value.rstrip("=")}


def test_the_server_checks_metadata_bytes_and_state_before_the_tool_runs(tmp_path):
    app, ran = serve(tmp_path)
    good = b"0123456789"

    async def body():
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as http:

            async def call(arguments, name="save", declare=True, **extra):
                params = {"name": name, "arguments": arguments, **extra}
                return (await rpc(http, "tools/call", params, declare=declare)).json()

            listed = (await rpc(http, "tools/list", declare=False)).json()
            assert [tool["name"] for tool in listed["result"]["tools"]] == ["export"]

            missing = await call({"file": text_file()}, declare=False)
            assert missing["error"]["code"] == MISSING_REQUIRED_CLIENT_CAPABILITY
            required = missing["error"]["data"]["requiredCapabilities"]
            assert required == {"extensions": {EXTENSION: {"upload": {}}}}

            for name, arguments in [
                ("save", {"file": text_file(size=5000)}),
                ("save", {"file": text_file(data=b"0123456789abcdefg")}),
                ("save", {"file": text_file(data=good) | {"size": 9}}),
                ("save", {"file": text_file(data=good) | {"data": "not base64!"}}),
                ("save", {"file": text_file(data=good) | {"data": "é" * 16}}),
                ("save", {"file": text_file(data=good) | {"data": None}}),
                (
                    "check",
                    {
                        "files": [
                            text_file(
                                data=good, mimeType="image/png", digest=digest(good)
                            )
                        ]
                    },
                ),
                ("check", {"files": [text_file(data=good)]}),
                ("check", {"files": [text_file(data=good, digest=digest(b"x"))]}),
                ("check", {"files": [text_file(size=0, digest=digest(b"x"))]}),
                (
                    "check",
                    {"files": [text_file("same", good, digest=digest(good))] * 2},
                ),
            ]:
                result = await call(arguments, name=name)
                assert result["result"]["isError"] is True, arguments
            assert ran == []

            # Bytes that do not match the declared size never reach the tool.
            arguments = {"file": text_file()}
            requested = (await call(arguments))["result"]
            [target] = requested["uploads"]
            assert target["fileId"] == "f" and target["credentials"] == "omit"
            put = await http.put(f"{ENDPOINT}{target['path']}", content=good[:9])
            assert put.status_code == status.HTTP_400_BAD_REQUEST
            state = requested["requestState"]
            result = await call(arguments, requestState=state)
            assert result["result"]["isError"] is True and ran == []
            changed = await call({**arguments, "note": "x"}, requestState=state)
            forged = await call(arguments, requestState="files.forged")
            assert changed["error"]["code"] == forged["error"]["code"] == INVALID_PARAMS

            exported = (await call({}, name="export", declare=False))["result"]
            [_, resource] = exported["content"]
            assert base64.b64decode(resource["resource"]["blob"]) == CSV
            assert EXTENSION not in exported.get("_meta", {})

    run(app, body)


def test_a_returned_file_is_fetched_checked_and_saved(tmp_path):
    app, ran = serve(tmp_path)
    seen = []
    folder = tmp_path / "downloads"
    folder.mkdir()

    async def body():
        [download] = downloads(await client(app, seen).call_tool("export"))
        assert (download.name, download.mime_type, download.size) == (
            "report.csv",
            "text/csv",
            8,
        )
        assert await download.read() == CSV
        path = await download.save(folder)
        assert path.read_bytes() == CSV and not path.stat().st_mode & 0o111
        with pytest.raises(FileExistsError):
            await download.save(folder)
        fetches = [e for e in seen if e["method"] == "GET"]
        assert len(fetches) == 2
        assert not any("authorization" in e["headers"] for e in fetches)

    run(app, body)
    assert ran == ["export"]


def offer(name, **target):
    """A tool result offering the sink's ``name`` as an 8-byte report.csv."""

    def reply(arguments):
        offered = {
            "path": f"/files/{name}",
            "name": "report.csv",
            "mimeType": "text/csv",
            "size": len(CSV),
            "credentials": "omit",
            **target,
        }
        return CallToolResult(content=[], _meta={EXTENSION: {"downloads": [offered]}})

    return reply


def test_a_fetch_whose_bytes_break_what_was_declared_keeps_nothing(tmp_path):
    served = {"tampered": b"a,b\n1,3\n", "longer": CSV + b"3,4\n", "shorter": CSV[:-1]}
    replies = [offer("tampered", digest=digest(CSV)), offer("longer"), offer("shorter")]
    app = scripted(replies, sink([], served))
    (tmp_path / "report.csv").write_bytes(b"kept")

    async def body():
        files = client(app, [])
        for _ in served:
            [download] = downloads(await files.call_tool("export"))
            with pytest.raises(FileTransferError):
                await download.save(tmp_path, overwrite=True)

    run(app, body)
    assert os.listdir(tmp_path) == ["report.csv"]
    assert (tmp_path / "report.csv").read_bytes() == b"kept"
