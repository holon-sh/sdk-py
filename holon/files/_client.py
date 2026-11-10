"""Client side: send File arguments and fetch the files a tool returns."""

from __future__ import annotations

import base64
import contextvars
import hashlib
import ipaddress
import logging
import mimetypes
import os
import re
import socket
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import httpx2
from httpx2._multipart import DataField, FileField
from mcp import Client
from mcp.client.extension import ClaimContext, ClientExtension, ResultClaim
from mcp.types import CallToolResult, DiscoverResult, PaginatedRequestParams

from holon.files._wire import (
    EXTENSION,
    UNCHANGED,
    Rejected,
    UploadRequiredResult,
    b64url,
    check_target,
    declared,
    descriptor,
    digest_required,
    expires_at,
    valid_media_type,
    walk_files,
)

_CHUNK = 1 << 16
_PUT_ATTEMPTS = 3
_TIMEOUT = httpx2.Timeout(60.0, connect=10.0)


class FileTransferError(RuntimeError):
    """A file could not be sent or fetched.

    Raised for a call means the tool did not run. Some bytes may already be at
    the target.
    """


class OutcomeUnknownError(RuntimeError):
    """The call failed after its files were sent; the tool may have run.

    Do not repeat the call automatically.
    """


class File:
    """A file argument: a local path or bytes.

    Args:
        source: Path of a local file, or the file's bytes.
        name: Name sent to the server. Defaults to the path's base name.
        mime_type: Media type. Guessed from ``name`` when omitted.
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | bytes,
        *,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> None:
        self._data = source if isinstance(source, bytes) else None
        self._path = None if isinstance(source, bytes) else Path(source)
        self.name = name if name is not None or self._path is None else self._path.name
        self.mime_type = (
            mime_type
            or (self.name and mimetypes.guess_type(self.name)[0])
            or "application/octet-stream"
        )

    def __repr__(self) -> str:
        return f"File({self.name!r}, mime_type={self.mime_type!r})"

    async def _size(self) -> int:
        if self._data is not None:
            return len(self._data)
        assert self._path is not None
        return (await anyio.Path(self._path).stat()).st_size

    async def _chunks(self, size: int) -> AsyncIterator[bytes]:
        """Yield exactly ``size`` bytes, never more, or fail."""
        if self._data is not None:
            if len(self._data) != size:
                raise FileTransferError("the file changed while it was sent")
            for start in range(0, size, _CHUNK):
                yield self._data[start : start + _CHUNK]
            return
        assert self._path is not None
        left = size
        async with await anyio.open_file(self._path, "rb") as source:
            while left:
                chunk = await source.read(min(_CHUNK, left))
                if not chunk:
                    raise FileTransferError("the file changed while it was sent")
                left -= len(chunk)
                yield chunk

    async def _read(self, size: int) -> bytes:
        return b"".join([chunk async for chunk in self._chunks(size)])


def _file_name(name: str | None) -> str:
    base = (name or "").replace("\\", "/").rsplit("/", 1)[-1]
    return "file" if base in ("", ".", "..") else base


@dataclass
class _Sent:
    file: File
    entry: dict[str, Any]


class _Files(ClientExtension):
    identifier = EXTENSION

    def settings(self) -> dict[str, Any]:
        return {"upload": {}, "download": {}}

    def claims(self) -> Sequence[ResultClaim[Any]]:
        return [
            ResultClaim(
                result_type="upload_required",
                model=UploadRequiredResult,
                resolve=_unresolvable,
            )
        ]


async def _unresolvable(
    result: UploadRequiredResult, ctx: ClaimContext
) -> CallToolResult:
    # mcp's claim context has no tool arguments, so holon drives uploads itself.
    raise RuntimeError("holon sends file calls itself; use holon.client.Apps")


FILES = _Files()


# Transfers go through this module; httpx2 logs each request URL at INFO.
_quiet: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "holon_files_quiet", default=False
)


class _RedactTargets(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _quiet.get() and isinstance(record.args, tuple):
            record.args = tuple(
                "[target]" if isinstance(arg, httpx2.URL) else arg
                for arg in record.args
            )
        return True


logging.getLogger("httpx2").addFilter(_RedactTargets())


Origin = tuple[str, str, int]


def _origin(url: httpx2.URL) -> Origin:
    return (url.scheme, url.host, url.port or (443 if url.scheme == "https" else 80))


def _loopback(host: str) -> bool:
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass
class Peer:
    """The MCP address a call went to, and how transfers for it are sent."""

    endpoint: httpx2.URL
    http_client: Callable[[], httpx2.AsyncClient]
    origins: frozenset[Origin] | None
    authorization: str | None = None

    @classmethod
    def of(
        cls,
        endpoint: str,
        http_client: Callable[[], httpx2.AsyncClient],
        file_origins: Sequence[str] | None,
    ) -> Peer:
        origins = None
        if file_origins is not None:
            origins = frozenset(_origin(httpx2.URL(origin)) for origin in file_origins)
        return cls(httpx2.URL(endpoint), http_client, origins)

    def watch(self, http: httpx2.AsyncClient) -> None:
        """Remember the credential this client sends to the MCP address, for ``include``."""

        async def capture(request: httpx2.Request) -> None:
            if request.url == self.endpoint:
                self.authorization = request.headers.get("authorization")

        hooks = http.event_hooks
        hooks["request"] = [*hooks.get("request", []), capture]
        http.event_hooks = hooks

    def check(self, target: Mapping[str, Any]) -> None:
        """The Credentials rules and this binding's origin policy."""
        if "path" in target:
            if not (
                self.endpoint.scheme == "https"
                or _loopback(self.endpoint.host)
                or self._listed(self.endpoint)
            ):
                raise Rejected("its path is under a plain-http MCP address")
            return
        url = httpx2.URL(target["url"])
        if target["credentials"] == "include" and _origin(url) != _origin(
            self.endpoint
        ):
            raise Rejected("it asks for credentials on another origin")
        if self.origins is not None and not self._listed(url):
            raise Rejected("its origin is not in file_origins")
        if url.scheme != "https" and not self._listed(url):
            raise Rejected("its url is not https")

    def _listed(self, url: httpx2.URL) -> bool:
        return self.origins is not None and _origin(url) in self.origins

    @asynccontextmanager
    async def exchange(
        self,
        target: Mapping[str, Any],
        method: str,
        headers: dict[str, str],
        content: AsyncIterator[bytes] | None = None,
        http: httpx2.AsyncClient | None = None,
    ) -> AsyncIterator[httpx2.Response]:
        """Send one transfer request and yield its streamed response.

        A ``path`` target goes through the binding's own client, which reaches
        the MCP address; a ``url`` target through a fresh client that connects
        only to an address this checked.
        """
        if target["credentials"] == "include" and self.authorization:
            headers = {**headers, "Authorization": self.authorization}
        quiet = _quiet.set(True)
        try:
            if "path" in target:
                url = self.endpoint.copy_with(
                    path=self.endpoint.path.rstrip("/") + target["path"]
                )
                async with self._client(http) as client:
                    request = client.build_request(
                        method, url, headers=headers, content=content
                    )
                    # Only the target's own headers and the MCP credential, if asked for.
                    request.headers.pop("cookie", None)
                    if "Authorization" not in headers:
                        request.headers.pop("authorization", None)
                    if "content-type" not in {name.lower() for name in headers}:
                        request.headers.pop("content-type", None)
                    response = await client.send(
                        request, stream=True, auth=None, follow_redirects=False
                    )
                    try:
                        yield response
                    finally:
                        await response.aclose()
                return
            url = httpx2.URL(target["url"])
            address = await self._address(url)
            extensions = (
                {} if address == url.host else {"sni_hostname": url.raw_host.decode()}
            )
            async with httpx2.AsyncClient(
                transport=httpx2.AsyncHTTPTransport(),
                trust_env=False,
                follow_redirects=False,
                timeout=_TIMEOUT,
            ) as client:
                request = client.build_request(
                    method,
                    url.copy_with(host=address),
                    headers={**headers, "Host": url.netloc.decode()},
                    content=content,
                    extensions=extensions,
                )
                response = await client.send(request, stream=True)
                try:
                    yield response
                finally:
                    await response.aclose()
        finally:
            _quiet.reset(quiet)

    @asynccontextmanager
    async def _client(
        self, http: httpx2.AsyncClient | None
    ) -> AsyncIterator[httpx2.AsyncClient]:
        if http is not None:
            yield http
            return
        async with self.http_client() as client:
            yield client

    async def _address(self, url: httpx2.URL) -> str:
        """The Addresses rule: connect only to a globally reachable address."""
        listed = self._listed(url)
        port = url.port or (443 if url.scheme == "https" else 80)
        try:
            found = await anyio.getaddrinfo(
                url.raw_host.decode(), port, type=socket.SOCK_STREAM
            )
        except OSError:
            raise Rejected("its host does not resolve") from None
        for *_, sockaddr in found:
            address = ipaddress.ip_address(str(sockaddr[0]).split("%")[0])
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            if url.scheme == "http":
                usable = address.is_loopback
            else:
                usable = listed or (address.is_global and not address.is_multicast)
            if usable:
                return str(address)
        raise Rejected("its address is not globally reachable")


# Uploads.


class Upload:
    """The file arguments of one logical call, as sent and as still owed."""

    def __init__(self, label: str, peer: Peer, http: httpx2.AsyncClient) -> None:
        self.label = label
        self.peer = peer
        self.http = http
        self.owed: dict[str, _Sent] = {}
        self.uploaded = False

    def fail(self, reason: str) -> FileTransferError:
        return FileTransferError(f"{self.label} did not run: {reason}")

    async def prepare(
        self,
        client: Client,
        name: str,
        schema: Mapping[str, Any] | None,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Replace each File with a FileInput, inline or owed out of band."""
        if schema is None:
            schema = await _listed_schema(client, name)
        sent: list[tuple[_Sent, Mapping[str, Any]]] = []

        def place(value: Any, marked: Mapping[str, Any]) -> Any:
            if not isinstance(value, File):
                return UNCHANGED
            item = _Sent(value, {"id": uuid4().hex, "mimeType": value.mime_type})
            if value.name is not None:
                item.entry["name"] = value.name
            sent.append((item, marked))
            return item.entry

        replaced: dict[str, Any] = walk_files(schema or {}, arguments, place)
        if holds_file(replaced):
            raise ValueError(
                f"{self.label}: a File was passed where the tool takes no file"
            )
        raw = await client.session.send_discover(client.protocol_version)
        settings = declared(DiscoverResult.model_validate(raw).capabilities)
        if settings is None:
            raise self.fail("the server does not accept file arguments")
        limit = settings.get("maxInlineBytes")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            limit = 0
        inline = 0
        for item, marked in sent:
            size = await item.file._size()
            item.entry["size"] = size
            max_size = descriptor(marked).get("maxSize")
            if isinstance(max_size, int) and size > max_size:
                raise self.fail(
                    f"{_file_name(item.file.name)!r} is larger than the tool accepts"
                )
            if digest_required(marked):
                item.entry["digest"] = {
                    "algorithm": "sha-256",
                    "value": await _digest(item.file, size),
                }
        for item, _ in sorted(sent, key=lambda pair: pair[0].entry["size"]):
            size = item.entry["size"]
            if size and inline + size <= limit:
                inline += size
                item.entry["data"] = base64.b64encode(
                    await item.file._read(size)
                ).decode()
            elif size:
                self.owed[item.entry["id"]] = item
        if self.owed and not isinstance(settings.get("upload"), Mapping):
            raise self.fail("the server takes no file this large")
        return replaced

    async def send(self, result: UploadRequiredResult) -> str:
        """Check an upload request, transfer every file, and return its requestState."""
        if self.uploaded:
            raise OutcomeUnknownError(
                f"{self.label}: the server asked for a second upload after the call was retried"
            )
        if not self.owed:
            raise self.fail("the server asked for an upload of no file")
        state = result.request_state
        if not isinstance(state, str):
            raise self.fail("the upload request has no requestState")
        planned: list[tuple[dict[str, Any], _Sent]] = []
        for number, raw in enumerate(result.uploads, 1):
            try:
                target = check_target(raw, upload=True)
                self.peer.check(target)
            except Rejected as error:
                raise self.fail(
                    f"upload target {number} was rejected: {error}"
                ) from None
            item = self.owed.get(target["fileId"])
            if item is None or any(
                target["fileId"] == done["fileId"] for done, _ in planned
            ):
                raise self.fail("the upload request names a file it should not")
            planned.append((target, item))
        if len(planned) != len(self.owed):
            raise self.fail("the upload request leaves out a file")
        failures: list[str] = []
        async with anyio.create_task_group() as group:
            for target, item in planned:
                group.start_soon(
                    self._transfer, target, item, failures, group.cancel_scope
                )
        if failures:
            raise self.fail(failures[0])
        self.uploaded = True
        return state

    async def _transfer(
        self,
        target: dict[str, Any],
        item: _Sent,
        failures: list[str],
        group: anyio.CancelScope,
    ) -> None:
        label = repr(_file_name(item.file.name))
        method = target.get("method", "PUT")
        attempts = _PUT_ATTEMPTS if method == "PUT" else 1
        reason = "it was not sent"
        try:
            for _ in range(attempts):
                deadline = expires_at(target)
                if deadline is not None and datetime.now(UTC) >= deadline:
                    reason = "its target expired before the transfer started"
                    break
                try:
                    status = await self._attempt(target, method, item)
                except FileTransferError as error:
                    reason = str(error)
                    break
                except (httpx2.HTTPError, OSError) as error:
                    reason = f"the transfer failed ({type(error).__name__})"
                    continue
                if 200 <= status < 300:
                    return
                reason = f"the target answered HTTP {status}"
                if status not in (408, 429) and status < 500:
                    break
        except Rejected as error:
            reason = f"its target was rejected: {error}"
        failures.append(f"the upload of {label} failed: {reason}")
        group.cancel()

    async def _attempt(
        self, target: Mapping[str, Any], method: str, item: _Sent
    ) -> int:
        size: int = item.entry["size"]
        headers = dict(target.get("headers", {}))
        form = target.get("form")
        if form is None:
            body = item.file._chunks(size)
            length = size
        else:
            boundary = os.urandom(16).hex().encode()
            mime = item.entry["mimeType"]
            head = b"".join(
                b"--%s\r\n%s%s\r\n"
                % (boundary, part.render_headers(), part.render_data())
                for part in (DataField(f["name"], f["value"]) for f in form["fields"])
            ) + b"--%s\r\n%s" % (
                boundary,
                FileField(
                    form["fileField"],
                    (
                        _file_name(item.entry.get("name")),
                        b"",
                        mime if valid_media_type(mime) else "application/octet-stream",
                    ),
                ).render_headers(),
            )
            tail = b"\r\n--%s--\r\n" % boundary
            body = _framed(head, item.file._chunks(size), tail)
            length = len(head) + size + len(tail)
            headers["Content-Type"] = (
                f"multipart/form-data; boundary={boundary.decode()}"
            )
        headers["Content-Length"] = str(length)
        async with self.peer.exchange(
            target, method, headers, body, self.http
        ) as response:
            return response.status_code


async def _framed(
    head: bytes, chunks: AsyncIterator[bytes], tail: bytes
) -> AsyncIterator[bytes]:
    yield head
    async for chunk in chunks:
        yield chunk
    yield tail


async def _digest(file: File, size: int) -> str:
    hasher = hashlib.sha256()
    async for chunk in file._chunks(size):
        hasher.update(chunk)
    return b64url(hasher.digest())


def holds_file(value: Any) -> bool:
    if isinstance(value, File):
        return True
    if isinstance(value, dict):
        return any(holds_file(item) for item in value.values())
    if isinstance(value, list | tuple):
        return any(holds_file(item) for item in value)
    return False


async def _listed_schema(client: Client, name: str) -> Mapping[str, Any] | None:
    cursor = None
    while True:
        page = await client.session.list_tools(
            params=PaginatedRequestParams(cursor=cursor)
        )
        for tool in page.tools:
            if tool.name == name:
                return tool.input_schema
        cursor = page.next_cursor
        if cursor is None:
            return None


# Downloads.

_peers: dict[int, Peer] = {}


def remember(result: CallToolResult, peer: Peer) -> None:
    """Keep the MCP address a result came from, for its download targets."""
    if EXTENSION in (result.meta or {}):
        key = id(result)
        _peers[key] = peer
        weakref.finalize(result, _peers.pop, key, None)


@dataclass(frozen=True)
class Download:
    """A file a tool returned. Fetching it is the caller's choice.

    Attributes:
        name: Name the server suggests. Untrusted.
        mime_type: Media type the server claims. Untrusted.
        size: Size in bytes; a fetch checks it.
        digest: Base64url SHA-256 the fetch checks, if the server gave one.
        expires_at: Time after which the server may refuse the fetch.
    """

    name: str | None
    mime_type: str
    size: int
    digest: str | None
    expires_at: datetime | None
    _target: dict[str, Any] = field(repr=False)
    _peer: Peer = field(repr=False)

    async def read(self) -> bytes:
        """Fetch the file into memory.

        Raises:
            FileTransferError: The fetch failed or the bytes failed a check.
        """
        parts: list[bytes] = []

        async def keep(chunk: bytes) -> None:
            parts.append(chunk)

        await self._fetch(keep)
        return b"".join(parts)

    async def save(
        self, directory: str | os.PathLike[str], *, overwrite: bool = False
    ) -> Path:
        """Fetch the file into ``directory`` under a safe form of its name.

        Args:
            directory: A directory you control, which nothing runs or loads
                configuration from.
            overwrite: Replace a file of the same name.

        Returns:
            The saved file's path. It is never executable.

        Raises:
            FileExistsError: The file exists and ``overwrite`` is False.
            FileTransferError: The fetch failed or the bytes failed a check;
                nothing is kept.
        """
        destination = Path(directory) / _file_name(
            re.sub(r"[\x00-\x1f\x7f]", "", self.name or "")
        )
        if not overwrite and destination.exists():
            raise FileExistsError(destination)
        partial = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        try:
            async with await anyio.open_file(partial, "xb") as out:
                await self._fetch(out.write)
            if overwrite:
                os.replace(partial, destination)
            else:
                os.link(partial, destination)
        finally:
            partial.unlink(missing_ok=True)
        return destination

    async def _fetch(self, sink: Callable[[bytes], Awaitable[object]]) -> None:
        label = repr(_file_name(self.name))
        if self.expires_at is not None and datetime.now(UTC) >= self.expires_at:
            raise FileTransferError(f"the download of {label} expired")
        hasher = hashlib.sha256()
        received = 0
        try:
            async with self._peer.exchange(
                self._target, "GET", dict(self._target.get("headers", {}))
            ) as response:
                if response.status_code != 200:
                    raise FileTransferError(
                        f"the download of {label} failed: HTTP {response.status_code}"
                    )
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > self.size:
                        raise FileTransferError(
                            f"the download of {label} is larger than declared"
                        )
                    hasher.update(chunk)
                    await sink(chunk)
        except Rejected as error:
            raise FileTransferError(
                f"the download of {label} was rejected: {error}"
            ) from None
        except (httpx2.HTTPError, OSError) as error:
            raise FileTransferError(
                f"the download of {label} failed ({type(error).__name__})"
            ) from None
        if received != self.size:
            raise FileTransferError(f"the download of {label} is smaller than declared")
        if self.digest is not None and b64url(hasher.digest()) != self.digest:
            raise FileTransferError(
                f"the download of {label} does not match its digest"
            )


def downloads(result: CallToolResult) -> list[Download]:
    """List the files a tool result offers for download.

    Raises:
        ValueError: A target breaks the extension's rules, or the result did
            not come from a holon client.
    """
    meta = (result.meta or {}).get(EXTENSION)
    if meta is None:
        return []
    try:
        listed = list(meta["downloads"])
    except (TypeError, KeyError):
        raise ValueError("the result's file metadata is malformed") from None
    peer = _peers.get(id(result))
    if peer is None:
        raise ValueError("the result did not come from a holon client")
    found = []
    for number, raw in enumerate(listed, 1):
        try:
            target = check_target(raw, upload=False)
            peer.check(target)
            deadline = expires_at(target)
        except Rejected as error:
            raise ValueError(f"download {number} was rejected: {error}") from None
        found.append(
            Download(
                name=target.get("name"),
                mime_type=target["mimeType"],
                size=target["size"],
                digest=target["digest"]["value"] if "digest" in target else None,
                expires_at=deadline,
                _target=target,
                _peer=peer,
            )
        )
    return found
