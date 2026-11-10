"""The files extension end to end: a low-level server behind Starlette, with
its storage under the MCP address, called by holon's client."""

import asyncio
import base64
import hashlib
import json
import os
import socket
from pathlib import Path

import httpx2
import pytest
import uvicorn
from mcp.server.lowlevel import Server
from mcp.types import (
    CallToolResult,
    ElicitRequest,
    ElicitRequestFormParams,
    ElicitResult,
    InputRequiredResult,
    ListToolsResult,
    TextContent,
    Tool,
)
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Mount, Route, Router

from holon.client import Apps
from holon.files import (
    EXTENSION,
    File,
    FileServer,
    FileTransferError,
    LocalStorage,
    OutcomeUnknownError,
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


def serve(tmp_path, storage=None, max_inline_bytes=16):
    """A server whose tools save, check and export files; returns (app, ran)."""
    storage = storage or LocalStorage(tmp_path / "storage")
    files = FileServer(
        storage,
        tools={tool["name"]: tool for tool in TOOLS}.get,
        max_inline_bytes=max_inline_bytes,
    )
    saved = tmp_path / "saved"
    saved.mkdir(exist_ok=True)
    ran = []

    async def list_tools(ctx, params):
        return ListToolsResult(tools=[Tool.model_validate(tool) for tool in TOOLS])

    async def call_tool(ctx, params):
        ran.append(params.name)
        arguments = params.arguments or {}
        if params.name == "export":
            return await files.reply(ctx, CSV, name="report.csv", text="Exported.")
        if arguments.get("note") == "ask":
            if not params.input_responses:
                question = ElicitRequestFormParams(
                    message="Why?",
                    requested_schema={"type": "object", "properties": {}},
                )
                return InputRequiredResult(
                    input_requests={"why": ElicitRequest(params=question)},
                    request_state="asked",
                )
            assert params.request_state == "asked"
        names = []
        for value in arguments.get("files") or [arguments["file"]]:
            received = files.received(value)
            names.append(received.name)
            if params.name == "save":
                await received.save(saved / Path(received.name).name)
        return CallToolResult(
            content=[TextContent(type="text", text="ok")],
            structured_content={"names": names},
        )

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


def client(app, seen, **binding):
    def http_client():
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=recording(app, seen)),
            headers={"Authorization": "Bearer mcp"},
            cookies={"session": "secret"},
        )

    return Apps(
        {"name": "caller", "version": "1", "requires": {}},
        {"files": {"tools": TOOLS}},
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


def test_files_reach_the_tool_inline_and_out_of_band(tmp_path):
    notes = tmp_path / "notes.txt"
    notes.write_bytes(b"x" * 100)
    app, ran = serve(tmp_path)
    seen = []
    files = client(app, seen)

    async def answer(params):
        return ElicitResult(action="accept", content={})

    async def body():
        await files.tools.save(file=File(b"tiny", name="a.txt"))
        [inline] = tool_calls(seen)
        assert inline["params"]["arguments"]["file"]["data"] == "dGlueQ=="
        assert not any(e["method"] == "PUT" for e in seen)
        seen.clear()

        result = await files.tools.save(
            {"elicit": answer}, file=File(notes), note="ask"
        )
        assert result.structured_content == {"names": ["notes.txt"]}
        first, upload_retry, answer_retry = tool_calls(seen)
        sent = first["params"]["arguments"]
        assert "data" not in sent["file"] and sent["file"]["size"] == 100
        assert upload_retry["params"]["arguments"] == sent
        assert "inputResponses" not in upload_retry["params"]
        assert answer_retry["params"]["inputResponses"]
        [put] = [e for e in seen if e["method"] == "PUT"]
        assert put["path"].startswith("/mcp/files/u/") and put["body"] == b"x" * 100
        # A path target travels through the binding's client, without its credentials.
        assert "authorization" not in put["headers"] and "cookie" not in put["headers"]
        assert "content-type" not in put["headers"]

        # The retry that answered the upload request is spent.
        [replay] = [
            e
            for e in seen
            if e["path"] == "/mcp" and json.loads(e["body"]) == upload_retry
        ]
        headers = {k: v for k, v in replay["headers"].items() if k != "content-length"}
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as raw:
            response = await raw.post(ENDPOINT, content=replay["body"], headers=headers)
        assert response.json()["error"]["code"] == -32602

    run(app, body)
    assert (tmp_path / "saved/a.txt").read_bytes() == b"tiny"
    assert (tmp_path / "saved/notes.txt").read_bytes() == b"x" * 100
    assert ran == ["save", "save", "save"]


class FormStorage:
    """Staging behind a multipart form endpoint at a URL, as object stores do."""

    def __init__(self, root, url):
        self.root, self.url = root, url
        root.mkdir()
        self.app = Router()

    async def issue(self, key, size, mime_type, expires_at):
        fields = [{"name": "key", "value": key}, {"name": "policy", "value": "signed"}]
        return {
            "url": self.url,
            "method": "POST",
            "form": {"fields": fields, "fileField": "file"},
            "credentials": "omit",
        }

    async def confirm(self, key, size, digest):
        path = self.root / key
        return path if path.is_file() and path.stat().st_size == size else None

    async def consume(self, nonce, expires_at):
        return True


def test_a_form_target_on_an_allowed_origin_gets_a_multipart_post(tmp_path):
    received = []

    async def upload(request):
        raw = await request.body()
        form = await request.form()
        received.append((raw, request.headers, [key for key, _ in form.multi_items()]))
        (tmp_path / "staged" / form["key"]).write_bytes(await form["file"].read())
        return Response(status_code=204)

    sock = socket.create_server(("127.0.0.1", 0))
    origin = f"http://127.0.0.1:{sock.getsockname()[1]}"
    app, ran = serve(tmp_path, FormStorage(tmp_path / "staged", f"{origin}/upload"), 0)
    seen = []
    pdf = File(b"%PDF-1.7 report", name='q3/Q3 "final".pdf')

    async def body():
        http = uvicorn.Server(
            uvicorn.Config(
                Starlette(routes=[Route("/upload", upload, methods=["POST"])]),
                lifespan="off",
                log_level="warning",
            )
        )
        serving = asyncio.create_task(http.serve(sockets=[sock]))
        try:
            # Without an allowlist, plain http is refused before anything is sent.
            with pytest.raises(FileTransferError, match="did not run.*not https"):
                await client(app, seen).tools.save(file=pdf)
            assert not received and len(tool_calls(seen)) == 1
            allowed = client(app, seen, file_origins=[origin])
            result = await allowed.tools.save(file=pdf)
            assert result.structured_content == {"names": ['q3/Q3 "final".pdf']}
        finally:
            http.should_exit = True
            await serving

    run(app, body)
    [(raw, headers, order)] = received
    assert order == ["key", "policy", "file"]
    assert (
        b'name="file"; filename="Q3 %22final%22.pdf"\r\nContent-Type: application/pdf'
        in raw
    )
    assert headers["content-length"] == str(len(raw))
    assert "authorization" not in headers and "cookie" not in headers
    assert (tmp_path / "saved" / 'Q3 "final".pdf').read_bytes() == b"%PDF-1.7 report"
    assert ran == ["save"]


def scripted(replies):
    """A server that answers tools/call from ``replies``; returns (app, attempts)."""
    attempts = []

    async def sink(request):
        attempts.append((request.method, request.headers.get("authorization")))
        return Response(
            status_code=503 if request.path_params["name"] == "busy" else 201
        )

    async def call_tool(ctx, params):
        return replies.pop(0)(params.arguments["file"]["id"])

    server = Server("scripted", on_call_tool=call_tool)
    server.extensions[EXTENSION] = {"upload": {}}
    sink_app = Router(routes=[Route("/{name}", sink, methods=["PUT", "POST"])])
    app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        custom_starlette_routes=[Mount("/mcp/files", app=sink_app)],
    )
    return app, attempts


def upload(**target):
    def reply(file_id):
        entry = {"fileId": file_id, "credentials": "omit", **target}
        return {
            "resultType": "upload_required",
            "uploads": [entry],
            "requestState": "s",
        }

    return reply


def done(file_id):
    return CallToolResult(content=[TextContent(type="text", text="ok")])


def test_the_client_checks_every_upload_request_and_retries_only_after_success(
    tmp_path,
):
    replies = []
    app, attempts = scripted(replies)
    seen = []
    files = client(app, seen)
    form = {"fields": [], "fileField": "file"}
    rejected = [
        {"path": "/files/ok", "headers": {"Authorization": "Bearer x"}},
        {"path": "/files/ok", "headers": {"X-Forwarded-Host": "elsewhere"}},
        {"path": "/files/../ok"},
        {"path": "/files/ok", "url": "https://files.example/u"},
        {"path": "/files/ok", "size": 3},
        {"path": "/files/ok", "form": form},
        {"url": "https://user@files.example/u"},
        {"url": "https://files.example/u", "credentials": "include"},
        {"path": "/files/ok", "fileId": "another"},
    ]

    async def call():
        return await files.tools.save(file=File(b"0123456789"))

    async def body():
        for target in rejected:
            replies.append(upload(**target))
            with pytest.raises(FileTransferError, match="did not run"):
                await call()
        replies.append(
            lambda file_id: {**upload(path="/files/ok")(file_id), "requestState": None}
        )
        with pytest.raises(FileTransferError, match="no requestState"):
            await call()
        assert attempts == [] and len(tool_calls(seen)) == len(rejected) + 1

        # A failed PUT is repeated a bounded number of times, a POST never; no retry.
        seen.clear()
        replies.append(upload(path="/files/busy"))
        with pytest.raises(FileTransferError, match="did not run.*HTTP 503"):
            await call()
        replies.append(upload(path="/files/busy", method="POST", form=form))
        with pytest.raises(FileTransferError, match="HTTP 503"):
            await call()
        assert attempts == [("PUT", None)] * 3 + [("POST", None)]
        assert len(tool_calls(seen)) == 2

        # Once the files are sent, failures leave the outcome unknown.
        attempts.clear()
        seen.clear()
        replies.extend([upload(path="/files/ok"), upload(path="/files/ok")])
        with pytest.raises(OutcomeUnknownError, match="second upload"):
            await call()

        def broken(file_id):
            raise RuntimeError("lost")

        replies.extend([upload(path="/files/ok"), broken])
        with pytest.raises(OutcomeUnknownError, match="outcome is unknown"):
            await call()
        assert len(attempts) == 2 and len(tool_calls(seen)) == 4

        # include attaches the credential the client sends to the MCP address.
        attempts.clear()
        replies.extend([upload(path="/files/ok", credentials="include"), done])
        await call()
        assert attempts == [("PUT", "Bearer mcp")]

    run(app, body)


def rpc(http, method, params=None, *, declare=True):
    capabilities = {"extensions": {EXTENSION: {"upload": {}, "download": {}}}}
    params = {
        **(params or {}),
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": capabilities
            if declare
            else {},
        },
    }
    headers = {
        "accept": "application/json, text/event-stream",
        "mcp-protocol-version": "2026-07-28",
        "mcp-method": method,
    }
    if "name" in params:
        headers["mcp-name"] = params["name"]
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    return http.post(ENDPOINT, json=body, headers=headers)


def text_file(file_id="f", data=None, **fields):
    entry = {"id": file_id, "name": "a.txt", "mimeType": "text/plain", "size": 10}
    if data is not None:
        entry |= {"data": base64.b64encode(data).decode(), "size": len(data)}
    return entry | fields


def digest(data):
    value = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
    return {"algorithm": "sha-256", "value": value}


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
            assert missing["error"]["code"] == -32021
            required = missing["error"]["data"]["requiredCapabilities"]
            assert required == {"extensions": {EXTENSION: {"upload": {}}}}

            refused = [
                {"file": text_file(size=5000)},
                {"file": text_file(data=b"0123456789abcdefg")},
                {"file": text_file(data=good) | {"size": 9}},
                {"file": text_file(data=good) | {"data": "not base64!"}},
                {"file": text_file(data=good) | {"data": None}},
            ]
            checked = [
                {
                    "files": [
                        text_file(data=good, digest=digest(good), mimeType="image/png")
                    ]
                },
                {"files": [text_file(data=good)]},
                {"files": [text_file(data=good, digest=digest(b"other"))]},
                {"files": [text_file(size=0, digest=digest(b"x"))]},
                {"files": [text_file("same", good, digest=digest(good))] * 2},
            ]
            for arguments in refused:
                assert (await call(arguments))["result"]["isError"] is True, arguments
            for arguments in checked:
                result = await call(arguments, name="check")
                assert result["result"]["isError"] is True, arguments
            assert ran == []

            # Bytes that do not match the declared size never reach the tool.
            arguments = {"file": text_file()}
            requested = (await call(arguments))["result"]
            [target] = requested["uploads"]
            assert target["fileId"] == "f" and target["credentials"] == "omit"
            put = await http.put(f"{ENDPOINT}{target['path']}", content=good[:9])
            assert put.status_code == 400
            state = requested["requestState"]
            result = await call(arguments, requestState=state)
            assert "did not arrive" in result["result"]["content"][0]["text"]
            assert ran == []
            changed = await call({**arguments, "note": "x"}, requestState=state)
            forged = await call(arguments, requestState="files.forged")
            assert changed["error"]["code"] == forged["error"]["code"] == -32602

            exported = (await call({}, name="export", declare=False))["result"]
            [_, resource] = exported["content"]
            assert base64.b64decode(resource["resource"]["blob"]) == CSV
            assert EXTENSION not in exported.get("_meta", {})

    run(app, body)


def test_a_returned_file_is_fetched_checked_and_saved(tmp_path):
    app, ran = serve(tmp_path)
    seen = []
    files = client(app, seen)
    folder = tmp_path / "downloads"
    folder.mkdir()

    async def body():
        result = await files.call_tool("export")
        assert result.content[0].text == "Exported."
        [download] = downloads(result)
        assert (download.name, download.mime_type, download.size) == (
            "report.csv",
            "text/csv",
            8,
        )
        assert await download.read() == CSV
        path = await download.save(folder)
        assert path == folder / "report.csv" and path.read_bytes() == CSV
        assert not path.stat().st_mode & 0o111
        with pytest.raises(FileExistsError):
            await download.save(folder)
        [get, *_] = [e for e in seen if e["method"] == "GET"]
        assert "authorization" not in get["headers"]

        [published] = (tmp_path / "storage" / "d").iterdir()
        published.write_bytes(b"a,b\n1,3\n")
        with pytest.raises(FileTransferError, match="digest"):
            await download.save(folder, overwrite=True)
        published.write_bytes(CSV + b"3,4\n")
        with pytest.raises(FileTransferError, match="larger"):
            await download.read()
        assert os.listdir(folder) == ["report.csv"] and path.read_bytes() == CSV

    run(app, body)
    assert ran == ["export"]
