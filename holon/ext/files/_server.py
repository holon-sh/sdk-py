"""Server side: accept file arguments in tools/call and return files."""

from __future__ import annotations

import atexit
import base64
import contextvars
import hashlib
import json
import os
import secrets
import shutil
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol, final
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
    EmbeddedResource,
    MissingRequiredClientCapabilityErrorData,
    TextContent,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_core import CoreSchema, core_schema
from starlette import status
from starlette.requests import ClientDisconnect, Request
from starlette.responses import FileResponse, Response
from starlette.routing import Route, Router

from holon.ext.files._wire import (
    EXTENSION,
    DownloadTarget,
    UploadTarget,
    b64url,
    declared,
    digest_required,
    file_input_schema,
    media_type,
    sha256,
    walk_files,
    writing,
)

_STATE = "files."
# Seconds an upload or a returned file stays valid.
_TTL = 900.0


@final
class ReceivedFile:
    """A file argument of the current call, its bytes checked against its size and any digest.

    A tool takes one by declaring an argument ``ReceivedFile``. ``ReceivedFile(value)`` decodes
    an argument's value where Pydantic does not, such as in a msgspec ``dec_hook``.

    Attributes:
        name: Client-given name. Untrusted.
        mime_type: Client-claimed media type. Untrusted.
        size: Size in bytes.
        digest: Base64url SHA-256, when verified.

    Raises:
        ValueError: ``value`` is not a file argument of the current call.
    """

    __slots__ = ("_source", "digest", "mime_type", "name", "size")
    name: str | None
    mime_type: str
    size: int
    digest: str | None
    _source: Path | bytes

    def __new__(cls, value: object) -> ReceivedFile:
        file_id = value.get("id") if isinstance(value, dict) else None
        found = (
            (_received.get() or {}).get(file_id) if isinstance(file_id, str) else None
        )
        if found is None:
            raise ValueError("not a file argument of this call")
        return found

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"ReceivedFile.{name} is read-only")

    @classmethod
    def __get_pydantic_core_schema__(cls, *_: object) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(cls)

    @classmethod
    def __get_pydantic_json_schema__(cls, *_: object) -> dict[str, Any]:
        return file_input_schema()

    async def read(self) -> bytes:
        if isinstance(self._source, bytes):
            return self._source
        return await anyio.Path(self._source).read_bytes()

    async def save(
        self, destination: str | os.PathLike[str], *, overwrite: bool = False
    ) -> Path:
        """Store the file at ``destination``.

        Raises:
            FileExistsError: ``destination`` exists and ``overwrite`` is False.
        """
        target = Path(destination)
        async with writing(target, overwrite) as partial:
            if isinstance(self._source, bytes):
                await anyio.Path(partial).write_bytes(self._source)
            else:
                try:
                    os.link(self._source, partial)
                except OSError:
                    await anyio.to_thread.run_sync(
                        shutil.copyfile, self._source, partial
                    )
        return target


class Storage(Protocol):
    """Where out-of-band files are staged and returned files are served.

    Times are POSIX seconds.
    """

    async def issue(
        self, key: str, size: int, mime_type: str, expires_at: float
    ) -> UploadTarget:
        """Return a target accepting one write of ``size`` bytes for ``key``, kept until ``expires_at``."""
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


class _Digest(BaseModel):
    model_config = ConfigDict(strict=True)

    algorithm: Literal["sha-256"]
    value: str


class _FileInput(BaseModel):
    model_config = ConfigDict(strict=True)

    id: Annotated[str, Field(min_length=1, max_length=128)]
    name: str | None = None
    mimeType: str
    size: Annotated[int, Field(ge=0)]
    digest: _Digest | None = None
    data: str = ""


class _Refused(Exception): ...


def _verified(
    file: _FileInput, digest: str | None, source: Path | bytes
) -> ReceivedFile:
    made = object.__new__(ReceivedFile)
    values = (source, digest, file.mimeType, file.name, file.size)
    for slot, value in zip(ReceivedFile.__slots__, values, strict=True):
        object.__setattr__(made, slot, value)
    return made


_received: contextvars.ContextVar[dict[str, ReceivedFile] | None] = (
    contextvars.ContextVar("holon_files_received", default=None)
)


class FileServer:
    """Low-level server middleware that accepts file arguments in ``tools/call``.

    Tools declare each file argument as a ``ReceivedFile``. Insert it first::

        server.extensions[EXTENSION] = files.settings()
        server.middleware.insert(0, files)

    Args:
        storage: Where uploads are staged and returned files are served.
        tools: Returns a tool's definition by name, or None.
        max_inline_bytes: Largest total of inline file bytes per request.
        codec: Seals ``requestState``. Defaults to a per-process key.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        tools: Callable[[str], Mapping[str, Any] | None],
        max_inline_bytes: int = 0,
        codec: RequestStateCodec | None = None,
    ) -> None:
        self._storage = storage
        self._tools = tools
        self._max_inline_bytes = max_inline_bytes
        self._codec = codec or AESGCMRequestStateCodec([os.urandom(32)])

    def settings(self) -> dict[str, Any]:
        """The extension's settings for ``server/discover``."""
        if self._max_inline_bytes:
            return {"upload": {}, "maxInlineBytes": self._max_inline_bytes}
        return {"upload": {}}

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
        if not isinstance(source, bytes):
            source = Path(source)
            name = source.name if name is None else name
        mime_type = media_type(name, mime_type)
        said = TextContent(type="text", text=text)
        if "download" in (declared(ctx.session.client_capabilities) or {}):
            target = await self._storage.publish(
                source, name, mime_type, time.time() + _TTL
            )
            return CallToolResult(
                content=[said], _meta={EXTENSION: {"downloads": [target]}}
            )
        if isinstance(source, Path):
            source = await anyio.Path(source).read_bytes()
        resource = BlobResourceContents(
            uri=f"file:///{quote(name or 'file')}",
            mime_type=mime_type,
            blob=base64.b64encode(source).decode(),
        )
        return CallToolResult(
            content=[said, EmbeddedResource(type="resource", resource=resource)]
        )

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        settings = declared(ctx.session.client_capabilities)
        if ctx.method == "tools/list":
            result = await call_next(ctx)
            if settings is None and isinstance(result, dict):
                tools = [t for t in result["tools"] if not _has_file(t["inputSchema"])]
                result = {**result, "tools": tools}
            return result
        params = ctx.params or {}
        name, arguments = params.get("name"), params.get("arguments")
        if not (
            ctx.method == "tools/call"
            and isinstance(name, str)
            and isinstance(arguments, dict)
        ):
            return await call_next(ctx)
        definition = self._tools(name) or {}
        found: list[tuple[Any, Mapping[str, Any]]] = []

        def collect(value: Any, marked: Mapping[str, Any]) -> Any:
            if not isinstance(value, dict):
                return None
            found.append((value, marked))
            return value

        if isinstance(schema := definition.get("inputSchema"), Mapping):
            walk_files(schema, arguments, collect)
        if not found:
            return await call_next(ctx)
        state = params.get("requestState")
        record = None
        if isinstance(state, str) and state.startswith(_STATE):
            # Retry steps 1 and 2: the state verifies and binds this call.
            try:
                record = json.loads(self._codec.unseal(state[len(_STATE) :]))
                valid = time.time() < record["e"] and record["b"] == self._binding(
                    ctx, name, arguments
                )
            except (InvalidRequestState, ValueError, KeyError, TypeError):
                valid = False
            if not valid:
                raise _invalid_state()
        try:
            received, owed = _inspect(found, self._max_inline_bytes)
        except _Refused as refusal:
            return _error(str(refusal))
        if record is None and not owed:
            return await self._run(ctx, call_next, received)
        if record is None:
            if "upload" not in (settings or {}):
                required = ClientCapabilities(extensions={EXTENSION: {"upload": {}}})
                data = MissingRequiredClientCapabilityErrorData(
                    required_capabilities=required
                )
                raise MCPError(
                    code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                    message="This file must be uploaded; the request did not declare upload.",
                    data=data.model_dump(by_alias=True, mode="json", exclude_none=True),
                )
            record = {
                "b": self._binding(ctx, name, arguments),
                "e": time.time() + _TTL,
                "f": {},
            }
            uploads = []
            for file, _ in owed:
                record["f"][file.id] = key = secrets.token_hex(16)
                target = await self._storage.issue(
                    key, file.size, file.mimeType, record["e"]
                )
                uploads.append({**target, "fileId": file.id})
            return {
                "resultType": "upload_required",
                "uploads": uploads,
                "requestState": self._seal(record, None),
            }
        for file, digest in owed:  # Step 3: confirm the bytes.
            key = record["f"].get(file.id)
            path = key and await self._storage.confirm(key, file.size, digest)
            if not path:
                return _error(
                    f"The bytes of file {file.name or file.id!r} did not arrive intact."
                )
            received[file.id] = _verified(file, digest, path)
        annotations = definition.get("annotations") or {}
        if annotations.get("idempotentHint") is not True and not (
            await self._storage.consume(record["n"], record["e"])
        ):
            raise _invalid_state()  # Step 4: another retry used this state.
        inner = {key: value for key, value in params.items() if key != "requestState"}
        if record["i"] is not None:
            inner["requestState"] = record["i"]
        result = await self._run(replace(ctx, params=inner), call_next, received)
        if isinstance(result, dict) and result.get("resultType") == "input_required":
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

    def _binding(
        self, ctx: ServerRequestContext[Any, Any], name: str, arguments: Any
    ) -> str:
        bound = [name, _canonical(arguments), authenticated_principal(ctx)]
        return sha256(json.dumps(bound, sort_keys=True, separators=(",", ":")).encode())

    def _seal(self, record: dict[str, Any], inner: object) -> str:
        sealed = {**record, "n": secrets.token_hex(16), "i": inner}
        return _STATE + self._codec.seal(json.dumps(sealed).encode())


def _has_file(schema: object) -> bool:
    if isinstance(schema, dict):
        return "x-mcp-file" in schema or _has_file(list(schema.values()))
    return isinstance(schema, list) and any(map(_has_file, schema))


def _invalid_state() -> MCPError:
    return MCPError(code=INVALID_PARAMS, message="Invalid or expired requestState")


def _error(message: str) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=message)], is_error=True
    )


def _canonical(value: Any) -> Any:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items()}
    return value


def _inspect(
    found: list[tuple[Any, Mapping[str, Any]]], max_inline_bytes: int
) -> tuple[dict[str, ReceivedFile], list[tuple[_FileInput, str | None]]]:
    received: dict[str, ReceivedFile] = {}
    owed: list[tuple[_FileInput, str | None]] = []
    seen: set[str] = set()
    inline = 0
    for value, marked in found:
        try:
            file = _FileInput.model_validate(value)
        except ValidationError:
            raise _Refused("A file argument is not a valid FileInput.") from None
        label = repr(file.name or file.id)
        rules = marked["x-mcp-file"]
        if file.id in seen:
            raise _Refused(f"Two file arguments share the id {file.id!r}.")
        seen.add(file.id)
        max_size, accept = rules.get("maxSize"), rules.get("accept")
        if isinstance(max_size, int) and file.size > max_size:
            raise _Refused(
                f"File {label} is larger than this tool accepts ({max_size} bytes)."
            )
        if isinstance(accept, list) and not _accepted(accept, file.mimeType, file.name):
            raise _Refused(f"This tool does not accept the type of file {label}.")
        verify = digest_required(marked)
        digest = file.digest.value if verify and file.digest else None
        if verify and digest is None:
            raise _Refused(f"File {label} needs a SHA-256 digest.")
        if file.size and "data" not in file.model_fields_set:
            owed.append((file, digest))
            continue
        try:
            raw = base64.b64decode(file.data, validate=True)
        except ValueError:
            raw = None
        if raw is None or len(raw) != file.size:
            raise _Refused(
                f"The inline data of file {label} is invalid or does not match its size."
            )
        if digest is not None and sha256(raw) != digest:
            raise _Refused(f"File {label} does not match its digest.")
        inline += file.size
        received[file.id] = _verified(file, digest, raw)
    if inline > max_inline_bytes:
        raise _Refused("The inline file data is larger than this server accepts.")
    return received, owed


def _accepted(accept: list[Any], mime_type: str, name: str | None) -> bool:
    media = mime_type.split(";")[0].strip().lower()
    return any(
        (name or "").lower().endswith(entry)
        if entry.startswith(".")
        else entry == media or (entry.endswith("/*") and media.startswith(entry[:-1]))
        for entry in (item.strip().lower() for item in accept if isinstance(item, str))
    )


def _rfc3339(when: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(when))


def _hash_file(path: Path) -> str:
    with path.open("rb") as source:
        return b64url(hashlib.file_digest(source, "sha256").digest())


class LocalStorage:
    """``Storage`` in a local directory, for a single server process.

    Mount ``app`` at ``{MCP address}{path}``.

    Args:
        root: Directory for files. Defaults to a temporary directory.
        path: Where ``app`` is mounted, relative to the MCP address.
    """

    def __init__(
        self, root: str | os.PathLike[str] | None = None, *, path: str = "/files"
    ) -> None:
        self._root = None if root is None else Path(root)
        self._path = path.rstrip("/")
        self._codec = AESGCMRequestStateCodec([os.urandom(32)])
        self._files: dict[Path, float] = {}
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
        start_by = now + (expires_at - now) / 2
        return {
            "path": f"{self._path}/u/{self._seal('u', start_by, key, size, expires_at)}",
            "method": "PUT",
            "credentials": "omit",
            "expiresAt": _rfc3339(start_by),
        }

    async def confirm(self, key: str, size: int, digest: str | None) -> Path | None:
        path = self._dir("u") / key
        if not path.is_file() or path.stat().st_size != size:
            return None
        if digest is not None and digest != await anyio.to_thread.run_sync(
            _hash_file, path
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
        path = self._dir("d") / secrets.token_hex(16)
        if isinstance(source, bytes):
            await anyio.Path(path).write_bytes(source)
        else:
            await anyio.to_thread.run_sync(shutil.copyfile, source, path)
        self._files[path] = expires_at
        token = self._seal("d", expires_at, path.name, name, mime_type)
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
            self._root = Path(tempfile.mkdtemp(prefix="holon-files-"))
            atexit.register(shutil.rmtree, self._root, ignore_errors=True)
        directory = self._root / kind
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return directory

    def _sweep(self) -> None:
        now = time.time()
        for path in [path for path, until in self._files.items() if until <= now]:
            del self._files[path]
            path.unlink(missing_ok=True)
        self._nonces = {n: until for n, until in self._nonces.items() if until > now}

    def _seal(self, kind: str, until: float, *claims: object) -> str:
        return self._codec.seal(json.dumps([kind, until, *claims]).encode())

    def _unseal(self, request: Request, kind: str) -> list[Any] | None:
        try:
            claims = json.loads(self._codec.unseal(request.path_params["token"]))
        except (InvalidRequestState, ValueError):
            return None
        if claims[0] != kind or claims[1] <= time.time():
            return None
        return list(claims[2:])

    async def _put(self, request: Request) -> Response:
        claims = self._unseal(request, "u")
        if claims is None:
            return Response(status_code=status.HTTP_404_NOT_FOUND)
        key, size, keep_until = claims
        staged = self._dir("u") / key
        if staged.exists():  # One write: the target is spent.
            return Response(status_code=status.HTTP_409_CONFLICT)
        written = 0
        try:
            if request.headers.get("content-length") != str(size):
                raise ValueError("not the declared size")
            async with (
                writing(staged, overwrite=False) as partial,
                await anyio.open_file(partial, "xb") as out,
            ):
                async for chunk in request.stream():
                    written += len(chunk)
                    if written > size:
                        raise ValueError("more than the declared size")
                    await out.write(chunk)
                if written != size:
                    raise ValueError("less than the declared size")
        except (ValueError, OSError, ClientDisconnect):
            return Response(status_code=status.HTTP_400_BAD_REQUEST)
        self._files[staged] = keep_until
        return Response(status_code=status.HTTP_201_CREATED)

    async def _get(self, request: Request) -> Response:
        claims = self._unseal(request, "d")
        if claims is None:
            return Response(status_code=status.HTTP_404_NOT_FOUND)
        file, name, mime_type = claims
        return FileResponse(self._dir("d") / file, media_type=mime_type, filename=name)
