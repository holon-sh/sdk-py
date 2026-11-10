"""Server side: accept file arguments in tools/call and return files."""

from __future__ import annotations

import atexit
import base64
import binascii
import contextvars
import hashlib
import json
import mimetypes
import os
import secrets
import shutil
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import anyio
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.request_state import (
    AESGCMRequestStateCodec,
    InvalidRequestState,
    RequestStateCodec,
    authenticated_principal,
)
from mcp.shared.exceptions import MCPError
from mcp.types import (
    INVALID_PARAMS,
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    BlobResourceContents,
    CallToolResult,
    ClientCapabilities,
    ContentBlock,
    EmbeddedResource,
    MissingRequiredClientCapabilityErrorData,
    TextContent,
)
from starlette.requests import ClientDisconnect, Request
from starlette.responses import FileResponse, Response
from starlette.routing import Route, Router

from holon.files._wire import (
    EXTENSION,
    DownloadTarget,
    UploadTarget,
    b64url,
    declared,
    descriptor,
    digest_required,
    sha256,
    walk_files,
)

_STATE = "files."


@dataclass(frozen=True)
class ReceivedFile:
    """A file argument whose bytes the server checked.

    Attributes:
        name: Name the client gave. Untrusted.
        mime_type: Media type the client claimed. A tool that restricts type
            inspects the bytes.
        size: Size in bytes, checked against the bytes.
        digest: Base64url SHA-256, only when the server verified it.
    """

    name: str | None
    mime_type: str
    size: int
    digest: str | None
    _path: Path | None = None
    _data: bytes = b""

    async def read(self) -> bytes:
        if self._path is None:
            return self._data
        return await anyio.Path(self._path).read_bytes()

    async def save(
        self, destination: str | os.PathLike[str], *, overwrite: bool = False
    ) -> Path:
        """Store the file at ``destination``.

        Raises:
            FileExistsError: ``destination`` exists and ``overwrite`` is False.
        """
        target = Path(destination)
        partial = target.with_name(f".{target.name}.{secrets.token_hex(8)}.part")
        try:
            if self._path is None:
                await anyio.Path(partial).write_bytes(self._data)
            else:
                try:
                    os.link(self._path, partial)
                except OSError:
                    await anyio.to_thread.run_sync(shutil.copyfile, self._path, partial)
            if overwrite:
                os.replace(partial, target)
            else:
                os.link(partial, target)
        finally:
            partial.unlink(missing_ok=True)
        return target


class Storage(Protocol):
    """Where out-of-band files are staged and returned files are served.

    Times are POSIX seconds.
    """

    async def issue(
        self, key: str, size: int, mime_type: str, expires_at: float
    ) -> UploadTarget:
        """Return a target that accepts one write of ``size`` bytes for ``key``.

        It must stop accepting writes by ``expires_at`` and keep the staged
        bytes until then, for every retry the upload's requestState allows.
        """
        ...

    async def confirm(self, key: str, size: int, digest: str | None) -> Path | None:
        """Return the staged bytes if exactly ``size`` arrived (and hash to ``digest``)."""
        ...

    async def consume(self, nonce: str, expires_at: float) -> bool:
        """Record ``nonce`` until ``expires_at``; False if it was already recorded."""
        ...

    async def publish(
        self, source: Path | bytes, name: str | None, mime_type: str, expires_at: float
    ) -> DownloadTarget:
        """Return a target that serves a copy of ``source`` until ``expires_at``."""
        ...


_received: contextvars.ContextVar[dict[str, ReceivedFile] | None] = (
    contextvars.ContextVar("holon_files_received", default=None)
)


class _Refused(Exception):
    """A file argument fails a check; the tool does not run."""


class FileServer:
    """Accept file arguments in ``tools/call``, as low-level server middleware.

    Insert it first, so it wraps any ``RequestStateBoundary``::

        server.extensions[EXTENSION] = files.settings()
        server.middleware.insert(0, files)

    Args:
        storage: Where out-of-band files land and returned files are served.
        tools: Returns a tool's definition (``inputSchema``, ``annotations``)
            by name, or None. A tool runs at most once per upload unless its
            annotations set ``idempotentHint``.
        max_inline_bytes: Largest total of inline file bytes in one request;
            0 takes every file out of band.
        principal: Who a request acts for; defaults to the authenticated user.
        codec: Seals ``requestState``. Defaults to a key held by this process.
        ttl: Seconds an upload stays valid, through every retry.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        tools: Callable[[str], Mapping[str, Any] | None],
        max_inline_bytes: int = 0,
        principal: Callable[
            [ServerRequestContext[Any, Any]], str | None
        ] = authenticated_principal,
        codec: RequestStateCodec | None = None,
        ttl: float = 900.0,
    ) -> None:
        self._storage = storage
        self._tools = tools
        self._max_inline_bytes = max_inline_bytes
        self._principal = principal
        self._codec = codec or AESGCMRequestStateCodec([os.urandom(32)])
        self._ttl = ttl

    def settings(self) -> dict[str, Any]:
        """The extension's settings for ``server/discover``."""
        if self._max_inline_bytes:
            return {"upload": {}, "maxInlineBytes": self._max_inline_bytes}
        return {"upload": {}}

    def received(self, file_input: Mapping[str, Any]) -> ReceivedFile:
        """The checked bytes of a file argument of the current call.

        Raises:
            LookupError: The argument is not a file of this call.
        """
        files = _received.get() or {}
        file_id = file_input.get("id")
        found = files.get(file_id) if isinstance(file_id, str) else None
        if found is None:
            raise LookupError("not a file argument of this call")
        return found

    async def reply(
        self,
        ctx: ServerRequestContext[Any, Any],
        source: os.PathLike[str] | bytes,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        text: str,
    ) -> CallToolResult:
        """Return a file: a download target if the request accepts one, else embedded bytes.

        Args:
            source: Path or bytes of the file.
            name: Name for the client. Defaults to the path's base name.
            mime_type: Media type. Guessed from ``name`` when omitted.
            text: What the result says, for the model.
        """
        if name is None and not isinstance(source, bytes):
            name = Path(source).name
        mime_type = (
            mime_type
            or (name and mimetypes.guess_type(name)[0])
            or "application/octet-stream"
        )
        content: list[ContentBlock] = [TextContent(type="text", text=text)]
        if "download" in (declared(ctx.session.client_capabilities) or {}):
            target = await self._storage.publish(
                source if isinstance(source, bytes) else Path(source),
                name,
                mime_type,
                time.time() + self._ttl,
            )
            return CallToolResult(
                content=content, _meta={EXTENSION: {"downloads": [target]}}
            )
        data = (
            source
            if isinstance(source, bytes)
            else await anyio.Path(source).read_bytes()
        )
        resource = BlobResourceContents(
            uri=f"file:///{quote(name or 'file')}",
            mime_type=mime_type,
            blob=base64.b64encode(data).decode(),
        )
        return CallToolResult(
            content=[*content, EmbeddedResource(type="resource", resource=resource)]
        )

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        if ctx.method == "tools/list":
            return self._hide_file_tools(ctx, await call_next(ctx))
        params = ctx.params
        if ctx.method != "tools/call" or params is None:
            return await call_next(ctx)
        name, arguments = params.get("name"), params.get("arguments")
        definition = self._tools(name) if isinstance(name, str) else None
        schema = definition.get("inputSchema") if definition else None
        if not (isinstance(arguments, dict) and isinstance(schema, Mapping)):
            return await call_next(ctx)
        found: list[tuple[Any, Mapping[str, Any]]] = []

        def collect(value: Any, marked: Mapping[str, Any]) -> Any:
            found.append((value, marked))
            return value

        walk_files(schema, arguments, collect)
        if not found:
            return await call_next(ctx)
        assert definition is not None and isinstance(name, str)
        state = params.get("requestState")
        if isinstance(state, str) and state.startswith(_STATE):
            return await self._retry(
                ctx, call_next, name, arguments, definition, found, state
            )
        try:
            received, owed = _inspect(found, self._max_inline_bytes)
        except _Refused as refusal:
            return _error(str(refusal))
        if not owed:
            return await self._run(ctx, call_next, received)
        if "upload" not in (declared(ctx.session.client_capabilities) or {}):
            required = ClientCapabilities(extensions={EXTENSION: {"upload": {}}})
            raise MCPError(
                code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                message="This file must be uploaded, and the request did not declare upload.",
                data=MissingRequiredClientCapabilityErrorData(
                    required_capabilities=required
                ).model_dump(by_alias=True, mode="json", exclude_none=True),
            )
        now = time.time()
        record: dict[str, Any] = {
            "t": name,
            "a": _arguments_digest(arguments),
            "p": self._principal_digest(ctx),
            "e": now + self._ttl,
            "f": {},
        }
        uploads = []
        for file_input in owed:
            key = secrets.token_hex(16)
            digest = (
                file_input["digest"]["value"] if file_input.get("_verify") else None
            )
            record["f"][file_input["id"]] = [
                key,
                file_input["size"],
                file_input["mimeType"],
                digest,
                file_input.get("name"),
            ]
            target = await self._storage.issue(
                key, file_input["size"], file_input["mimeType"], record["e"]
            )
            uploads.append({**target, "fileId": file_input["id"]})
        return {
            "resultType": "upload_required",
            "uploads": uploads,
            "requestState": self._seal(record, None),
        }

    async def _retry(
        self,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
        name: str,
        arguments: dict[str, Any],
        definition: Mapping[str, Any],
        found: list[tuple[Any, Mapping[str, Any]]],
        state: str,
    ) -> HandlerResult:
        record = self._unseal(state)
        if (
            record["t"] != name
            or record["a"] != _arguments_digest(arguments)
            or record["p"] != self._principal_digest(ctx)
        ):
            raise _invalid_state()
        try:
            received, _ = _inspect(found, self._max_inline_bytes)
        except _Refused as refusal:
            return _error(str(refusal))
        for file_id, (key, size, mime_type, digest, file_name) in record["f"].items():
            path = await self._storage.confirm(key, size, digest)
            if path is None:
                return _error(f"The bytes of file {file_id!r} did not arrive intact.")
            received[file_id] = ReceivedFile(file_name, mime_type, size, digest, path)
        annotations = definition.get("annotations")
        idempotent = (
            isinstance(annotations, Mapping)
            and annotations.get("idempotentHint") is True
        )
        if not idempotent and not await self._storage.consume(record["n"], record["e"]):
            raise _invalid_state()
        params = dict(ctx.params or {})
        if record["i"] is None:
            del params["requestState"]
        else:
            params["requestState"] = record["i"]
        result = await self._run(replace(ctx, params=params), call_next, received)
        if isinstance(result, dict) and result.get("resultType") == "input_required":
            # Later rounds keep the files bound; the inner state rides inside ours.
            return {
                **result,
                "requestState": self._seal(record, result.get("requestState")),
            }
        return result

    async def _run(
        self,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
        received: dict[str, ReceivedFile],
    ) -> HandlerResult:
        token = _received.set(received)
        try:
            return await call_next(ctx)
        finally:
            _received.reset(token)

    def _seal(self, record: dict[str, Any], inner: object) -> str:
        # A fresh nonce per result: each requestState is accepted at most once.
        sealed = {**record, "n": secrets.token_hex(16), "i": inner}
        return _STATE + self._codec.seal(json.dumps(sealed).encode())

    def _unseal(self, state: str) -> dict[str, Any]:
        try:
            record = json.loads(self._codec.unseal(state[len(_STATE) :]))
        except (InvalidRequestState, ValueError):
            raise _invalid_state() from None
        if not (
            isinstance(record, dict)
            and isinstance(record.get("e"), float)
            and time.time() < record["e"]
        ):
            raise _invalid_state()
        return record

    def _principal_digest(self, ctx: ServerRequestContext[Any, Any]) -> str | None:
        principal = self._principal(ctx)
        return (
            None
            if principal is None
            else sha256(principal.encode("utf-8", "surrogatepass"))
        )

    def _hide_file_tools(
        self, ctx: ServerRequestContext[Any, Any], result: HandlerResult
    ) -> HandlerResult:
        # Clients without the extension cannot fill a file argument.
        if declared(ctx.session.client_capabilities) is not None or not isinstance(
            result, dict
        ):
            return result
        tools = result.get("tools")
        if not isinstance(tools, list):
            return result
        return {
            **result,
            "tools": [tool for tool in tools if not _has_file(tool.get("inputSchema"))],
        }


def _has_file(schema: object) -> bool:
    if isinstance(schema, dict):
        return "x-mcp-file" in schema or any(
            _has_file(value) for value in schema.values()
        )
    if isinstance(schema, list):
        return any(_has_file(value) for value in schema)
    return False


def _invalid_state() -> MCPError:
    return MCPError(code=INVALID_PARAMS, message="Invalid or expired requestState")


def _error(message: str) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=message)], is_error=True
    )


def _arguments_digest(arguments: Any) -> str:
    """Hash arguments so equal JSON Schema instances hash alike."""

    def canonical(value: Any) -> Any:
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, list):
            return [canonical(item) for item in value]
        if isinstance(value, dict):
            return {key: canonical(item) for key, item in value.items()}
        return value

    return sha256(
        json.dumps(canonical(arguments), sort_keys=True, separators=(",", ":")).encode()
    )


def _inspect(
    found: list[tuple[Any, Mapping[str, Any]]], max_inline_bytes: int
) -> tuple[dict[str, ReceivedFile], list[dict[str, Any]]]:
    """Check file metadata and inline bytes; return the received files and those owed."""
    received: dict[str, ReceivedFile] = {}
    owed: list[dict[str, Any]] = []
    seen: set[str] = set()
    inline = 0
    for value, marked in found:
        file_input = _file_input(value)
        file_id, size, mime_type = (
            file_input["id"],
            file_input["size"],
            file_input["mimeType"],
        )
        label = repr(file_input.get("name") or file_id)
        if file_id in seen:
            raise _Refused(f"Two file arguments share the id {file_id!r}.")
        seen.add(file_id)
        rules = descriptor(marked)
        max_size = rules.get("maxSize")
        if isinstance(max_size, int) and size > max_size:
            raise _Refused(
                f"File {label} is larger than this tool accepts ({max_size} bytes)."
            )
        accept = rules.get("accept")
        if isinstance(accept, list) and not _accepted(
            accept, mime_type, file_input.get("name")
        ):
            raise _Refused(f"This tool does not accept the type of file {label}.")
        verify = digest_required(marked)
        if verify and "digest" not in file_input:
            raise _Refused(f"File {label} needs a SHA-256 digest.")
        if "data" in file_input or size == 0:
            data = file_input.get("data", "")
            try:
                raw = (
                    base64.b64decode(data, validate=True)
                    if isinstance(data, str)
                    else None
                )
            except binascii.Error:
                raw = None
            if raw is None or len(raw) != size:
                raise _Refused(
                    f"The inline data of file {label} is invalid or does not match its size."
                )
            if verify and sha256(raw) != file_input["digest"]["value"]:
                raise _Refused(f"File {label} does not match its digest.")
            inline += size
            received[file_id] = ReceivedFile(
                file_input.get("name"),
                mime_type,
                size,
                sha256(raw) if verify else None,
                None,
                raw,
            )
        else:
            owed.append({**file_input, "_verify": verify})
    if inline > max_inline_bytes:
        raise _Refused("The inline file data is larger than this server accepts.")
    return received, owed


def _file_input(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _Refused("A file argument is not an object.")
    file_id, size = value.get("id"), value.get("size")
    digest = value.get("digest")
    if not (
        isinstance(file_id, str)
        and 1 <= len(file_id) <= 128
        and isinstance(value.get("mimeType"), str)
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size >= 0
        and isinstance(value.get("name", ""), str)
        and (
            digest is None
            or (
                isinstance(digest, dict)
                and digest.get("algorithm") == "sha-256"
                and isinstance(digest.get("value"), str)
            )
        )
    ):
        raise _Refused("A file argument is not a valid FileInput.")
    return {
        key: item
        for key, item in value.items()
        if not (key == "digest" and item is None)
    }


def _accepted(accept: list[Any], mime_type: str, name: object) -> bool:
    media = mime_type.split(";")[0].strip().lower()
    for entry in accept:
        if not isinstance(entry, str):
            continue
        entry = entry.strip().lower()
        if entry.startswith("."):
            if isinstance(name, str) and name.lower().endswith(entry):
                return True
        elif entry == media or (entry.endswith("/*") and media.startswith(entry[:-1])):
            return True
    return False


def _rfc3339(when: float) -> str:
    return (
        datetime.fromtimestamp(when, UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


@dataclass
class _Slot:
    key: str
    size: int
    start_by: float
    busy: bool = False


@dataclass
class _Published:
    path: Path
    name: str | None
    mime_type: str
    expires_at: float


class LocalStorage:
    """``Storage`` on a local directory, for one server process.

    Mount ``app`` at ``{MCP address}{path}``. Its targets are ``path`` targets
    authorized by an unguessable token alone. Tokens, one-write records and
    consumed nonces live in this process, and files left by a restart are not
    cleaned up.

    Args:
        root: Directory for staged uploads and published downloads. Defaults
            to a temporary directory made on first use and removed at exit.
        path: Where ``app`` is mounted, relative to the MCP address.
    """

    def __init__(
        self, root: str | os.PathLike[str] | None = None, *, path: str = "/files"
    ) -> None:
        self._root = None if root is None else Path(root)
        self._path = path.rstrip("/")
        self._slots: dict[str, _Slot] = {}
        self._staged: dict[str, float] = {}
        self._published: dict[str, _Published] = {}
        self._nonces: dict[str, float] = {}
        self.app = Router(
            routes=[
                Route("/u/{token}", self._put, methods=["PUT"]),
                Route("/d/{token}", self._get, methods=["GET"]),
            ]
        )

    async def issue(
        self, key: str, size: int, mime_type: str, expires_at: float
    ) -> UploadTarget:
        self._sweep()
        now = time.time()
        # Half the lifetime to start the transfer leaves the rest for it and the retry.
        start_by = now + (expires_at - now) / 2
        token = secrets.token_urlsafe(32)
        self._slots[token] = _Slot(key, size, start_by)
        self._staged[key] = expires_at
        return {
            "path": f"{self._path}/u/{token}",
            "method": "PUT",
            "credentials": "omit",
            "expiresAt": _rfc3339(start_by),
        }

    async def confirm(self, key: str, size: int, digest: str | None) -> Path | None:
        path = self._dir("u") / key
        if key not in self._staged or not path.is_file() or path.stat().st_size != size:
            return None
        if (
            digest is not None
            and await anyio.to_thread.run_sync(_hash_file, path) != digest
        ):
            return None
        return path

    async def consume(self, nonce: str, expires_at: float) -> bool:
        self._sweep()
        if nonce in self._nonces:
            return False
        self._nonces[nonce] = expires_at
        return True

    async def publish(
        self, source: Path | bytes, name: str | None, mime_type: str, expires_at: float
    ) -> DownloadTarget:
        self._sweep()
        token = secrets.token_urlsafe(32)
        path = self._dir("d") / token
        if isinstance(source, bytes):
            await anyio.Path(path).write_bytes(source)
        else:
            await anyio.to_thread.run_sync(shutil.copyfile, source, path)
        self._published[token] = _Published(path, name, mime_type, expires_at)
        target: DownloadTarget = {
            "mimeType": mime_type,
            "size": path.stat().st_size,
            "digest": {
                "algorithm": "sha-256",
                "value": await anyio.to_thread.run_sync(_hash_file, path),
            },
            "path": f"{self._path}/d/{token}",
            "credentials": "omit",
            "expiresAt": _rfc3339(expires_at),
        }
        if name is not None:
            target["name"] = name
        return target

    def _dir(self, kind: str) -> Path:
        if self._root is None:
            # Made on first use, not when constructed, so a server can be built
            # where nothing may be written, such as while its contract is read.
            self._root = Path(tempfile.mkdtemp(prefix="holon-files-"))
            atexit.register(shutil.rmtree, self._root, ignore_errors=True)
        directory = self._root / kind
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return directory

    def _sweep(self) -> None:
        now = time.time()
        self._slots = {
            token: slot for token, slot in self._slots.items() if slot.start_by > now
        }
        for key in [key for key, until in self._staged.items() if until <= now]:
            del self._staged[key]
            (self._dir("u") / key).unlink(missing_ok=True)
        for token in [
            token for token, item in self._published.items() if item.expires_at <= now
        ]:
            self._published.pop(token).path.unlink(missing_ok=True)
        self._nonces = {
            nonce: until for nonce, until in self._nonces.items() if until > now
        }

    async def _put(self, request: Request) -> Response:
        slot = self._slots.get(request.path_params["token"])
        if slot is None or slot.start_by <= time.time() or slot.key not in self._staged:
            return Response(status_code=404)
        if slot.busy:
            return Response(status_code=409)
        length = request.headers.get("content-length", "")
        if not length.isdigit() or int(length) != slot.size:
            return Response(status_code=400)
        slot.busy = True
        staged = self._dir("u") / slot.key
        partial = staged.with_name(f"{slot.key}.part")
        try:
            written = 0
            async with await anyio.open_file(partial, "xb") as out:
                async for chunk in request.stream():
                    written += len(chunk)
                    if written > slot.size:
                        return Response(status_code=413)
                    await out.write(chunk)
            if written != slot.size:
                return Response(status_code=400)
            os.link(partial, staged)
        except FileExistsError:
            return Response(status_code=409)
        except ClientDisconnect:
            return Response(status_code=400)
        finally:
            slot.busy = False
            partial.unlink(missing_ok=True)
        # One write: the target is spent.
        self._slots.pop(request.path_params["token"], None)
        return Response(status_code=201)

    async def _get(self, request: Request) -> Response:
        item = self._published.get(request.path_params["token"])
        if item is None or item.expires_at <= time.time():
            return Response(status_code=404)
        return FileResponse(item.path, media_type=item.mime_type, filename=item.name)


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1 << 16):
            hasher.update(chunk)
    return b64url(hasher.digest())
