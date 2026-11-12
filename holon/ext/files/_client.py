"""Client side: send File arguments and fetch the files a tool returns."""

from __future__ import annotations

import base64
import contextvars
import hashlib
import io
import ipaddress
import logging
import os
import re
import secrets
import socket
import weakref
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import httpx2
from mcp import Client
from mcp.client.extension import advertise
from mcp.types import CallToolResult, DiscoverResult, PaginatedRequestParams

from holon.ext.files._wire import (
    DOWNLOAD,
    EXTENSION,
    MEDIA_TYPE,
    UPLOAD,
    DownloadTarget,
    Rejected,
    UploadRequiredResult,
    UploadTarget,
    b64url,
    check_target,
    declared,
    digest_required,
    expires_at,
    media_type,
    walk_files,
    writing,
)

FILES = advertise(EXTENSION, {"upload": {}, "download": {}})
_CHUNK = 1 << 16
_TIMEOUT = httpx2.Timeout(60.0, connect=10.0)
# The WHATWG multipart/form-data escapes for a filename, and other controls.
_QUOTE = {ord('"'): "%22"} | {c: f"%{c:02X}" for c in range(32)}


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
        self._source = source
        if name is None and not isinstance(source, bytes):
            name = Path(source).name
        self.name = name
        self.mime_type = media_type(name, mime_type)

    def __repr__(self) -> str:
        return f"File({self.name!r}, mime_type={self.mime_type!r})"

    async def _size(self) -> int:
        if isinstance(self._source, bytes):
            return len(self._source)
        return (await anyio.Path(self._source).stat()).st_size

    async def _chunks(self, size: int) -> AsyncIterator[bytes]:
        source = self._source
        async with (
            anyio.wrap_file(io.BytesIO(source))
            if isinstance(source, bytes)
            else await anyio.open_file(source, "rb")
        ) as reader:
            while size:
                chunk = await reader.read(min(_CHUNK, size))
                if not chunk:
                    raise FileTransferError("the file changed while it was sent")
                size -= len(chunk)
                yield chunk


def _file_name(name: str | None) -> str:
    base = (name or "").replace("\\", "/").rsplit("/", 1)[-1]
    return "file" if base in ("", ".", "..") else base


_quiet = contextvars.ContextVar("holon_files_quiet", default=False)


class _RedactTargets(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _quiet.get() and isinstance(record.args, tuple):
            record.args = tuple(
                "[target]" if isinstance(arg, httpx2.URL) else arg
                for arg in record.args
            )
        return True


logging.getLogger("httpx2").addFilter(_RedactTargets())


def _origin(url: httpx2.URL) -> tuple[str, str, int]:
    return url.scheme, url.host, url.port or (443 if url.scheme == "https" else 80)


def _loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost" or host.endswith(".localhost")


class Transfers:
    endpoint: httpx2.URL
    http_client: Callable[[], httpx2.AsyncClient]

    def __init__(self, label: str, file_origins: Sequence[str] | None) -> None:
        self.label = label
        self.origins = (
            None
            if file_origins is None
            else {_origin(httpx2.URL(origin)) for origin in file_origins}
        )
        self.authorization: str | None = None
        self.owed: dict[str, tuple[File, dict[str, Any]]] = {}
        self.uploaded = False

    def bind(
        self,
        endpoint: str,
        http_client: Callable[[], httpx2.AsyncClient],
        http: httpx2.AsyncClient,
    ) -> None:
        self.endpoint = httpx2.URL(endpoint)
        self.http_client = http_client

        async def capture(request: httpx2.Request) -> None:
            if request.url == self.endpoint:
                self.authorization = request.headers.get("authorization")

        hooks = http.event_hooks
        http.event_hooks = {**hooks, "request": [*hooks.get("request", []), capture]}

    def fail(self, reason: str) -> FileTransferError:
        return FileTransferError(f"{self.label} did not run: {reason}")

    def _listed(self, url: httpx2.URL) -> bool:
        return self.origins is not None and _origin(url) in self.origins

    def check(self, target: UploadTarget | DownloadTarget) -> None:
        """The Credentials rules and this binding's origin policy."""
        if "url" not in target:
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

    @asynccontextmanager
    async def exchange(
        self,
        target: UploadTarget | DownloadTarget,
        method: str,
        headers: dict[str, str],
        content: AsyncIterator[bytes] | None = None,
    ) -> AsyncIterator[httpx2.Response]:
        if target["credentials"] == "include" and self.authorization:
            headers = {**headers, "Authorization": self.authorization}
        quiet = _quiet.set(True)
        try:
            async with AsyncExitStack() as stack:
                if "url" not in target:
                    client = await stack.enter_async_context(self.http_client())
                    path = self.endpoint.path.rstrip("/") + target["path"]
                    request = client.build_request(
                        method,
                        self.endpoint.copy_with(path=path),
                        headers=headers,
                        content=content,
                    )
                    listed = {name.lower() for name in headers}
                    for name in ("authorization", "cookie", "content-type"):
                        if name not in listed:
                            request.headers.pop(name, None)
                else:  # A fresh client, connected to an address _address allows.
                    url = httpx2.URL(target["url"])
                    client = await stack.enter_async_context(
                        httpx2.AsyncClient(trust_env=False, timeout=_TIMEOUT)
                    )
                    request = client.build_request(
                        method,
                        url.copy_with(host=await self._address(url)),
                        headers={**headers, "Host": url.netloc.decode()},
                        content=content,
                        extensions={"sni_hostname": url.raw_host.decode()},
                    )
                response = await client.send(
                    request, stream=True, auth=None, follow_redirects=False
                )
                stack.push_async_callback(response.aclose)
                yield response
        finally:
            _quiet.reset(quiet)

    async def _address(self, url: httpx2.URL) -> str:
        """The Addresses rule: connect only to a globally reachable address."""
        try:
            found = await anyio.getaddrinfo(
                url.raw_host.decode(), None, type=socket.SOCK_STREAM
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
                usable = self._listed(url) or (
                    address.is_global and not address.is_multicast
                )
            if usable:
                return str(address)
        raise Rejected("its address is not globally reachable")

    async def prepare(
        self,
        client: Client,
        name: str,
        schema: Mapping[str, Any] | None,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        cursor = None
        while schema is None:  # An untyped dependency: the schema it lists.
            page = await client.session.list_tools(
                params=PaginatedRequestParams(cursor=cursor)
            )
            schema = next((t.input_schema for t in page.tools if t.name == name), None)
            if schema is None and (cursor := page.next_cursor) is None:
                schema = {}
        sent: list[tuple[File, dict[str, Any], Mapping[str, Any]]] = []

        def place(value: Any, marked: Mapping[str, Any]) -> Any:
            if not isinstance(value, File):
                return None
            entry = {"id": uuid4().hex, "mimeType": value.mime_type}
            if value.name is not None:
                entry["name"] = value.name
            sent.append((value, entry, marked))
            return entry

        replaced: dict[str, Any] = walk_files(schema, arguments, place)
        if holds_file(replaced):
            raise ValueError(
                f"{self.label}: a File was passed where the tool takes no file"
            )
        raw = await client.session.send_discover(client.protocol_version)
        settings = declared(DiscoverResult.model_validate(raw).capabilities)
        if settings is None:
            raise self.fail("the server does not accept file arguments")
        for file, entry, marked in sent:
            entry["size"] = size = await file._size()
            max_size = marked["x-mcp-file"].get("maxSize")
            if isinstance(max_size, int) and size > max_size:
                raise self.fail(
                    f"{_file_name(file.name)!r} is larger than the tool accepts"
                )
            if digest_required(marked):
                hasher = hashlib.sha256()
                async for chunk in file._chunks(size):
                    hasher.update(chunk)
                entry["digest"] = {
                    "algorithm": "sha-256",
                    "value": b64url(hasher.digest()),
                }
        budget = settings.get("maxInlineBytes")
        budget = budget if isinstance(budget, int) else 0
        for file, entry, _ in sorted(sent, key=lambda item: item[1]["size"]):
            if 0 < entry["size"] <= budget:
                budget -= entry["size"]
                data = b"".join([chunk async for chunk in file._chunks(entry["size"])])
                entry["data"] = base64.b64encode(data).decode()
            elif entry["size"]:
                self.owed[entry["id"]] = (file, entry)
        if self.owed and not isinstance(settings.get("upload"), dict):
            raise self.fail("the server takes no file this large")
        return replaced

    async def send(self, result: UploadRequiredResult) -> str:
        if self.uploaded:
            raise OutcomeUnknownError(
                f"{self.label}: the server asked for a second upload after the call was retried"
            )
        state = result.request_state
        if not isinstance(state, str):
            raise self.fail("the upload request has no requestState")
        uploads = result.uploads if isinstance(result.uploads, list) else []
        targets: dict[str | None, UploadTarget] = {}
        for number, raw in enumerate(uploads, 1):
            try:
                target = check_target(raw, UPLOAD)
                self.check(target)
            except Rejected as error:
                raise self.fail(
                    f"upload target {number} was rejected: {error}"
                ) from None
            targets[target.get("fileId")] = target
        if (
            not self.owed
            or len(uploads) != len(targets)
            or targets.keys() != self.owed.keys()
        ):
            raise self.fail(
                "the upload request does not list each file sent without data exactly once"
            )
        try:
            async with anyio.create_task_group() as group:
                for file, entry in self.owed.values():
                    group.start_soon(self._transfer, targets[entry["id"]], file, entry)
        except* FileTransferError as failed:
            raise failed.exceptions[0] from None
        self.uploaded = True
        return state

    async def _transfer(
        self, target: UploadTarget, file: File, entry: dict[str, Any]
    ) -> None:
        method = target.get("method", "PUT")
        deadline = expires_at(target)
        reason = "it was not sent"
        for _ in range(3 if method == "PUT" else 1):
            if deadline is not None and datetime.now(UTC) >= deadline:
                reason = "its target expired before the transfer started"
                break
            try:
                async with self.exchange(
                    target, method, *_body(target, file, entry)
                ) as response:
                    status = response.status_code
            except (httpx2.HTTPError, OSError) as error:
                reason = f"the transfer failed ({type(error).__name__})"
                continue
            except (FileTransferError, Rejected) as error:
                reason = str(error)
                break
            if httpx2.codes.is_success(status):
                return
            reason = f"the target answered HTTP {status}"
            if status < httpx2.codes.INTERNAL_SERVER_ERROR and status not in (
                httpx2.codes.REQUEST_TIMEOUT,
                httpx2.codes.TOO_MANY_REQUESTS,
            ):
                break
        raise self.fail(f"the upload of {_file_name(file.name)!r} failed: {reason}")


def _body(
    target: UploadTarget, file: File, entry: Mapping[str, Any]
) -> tuple[dict[str, str], AsyncIterator[bytes]]:
    size: int = entry["size"]
    headers = dict(target.get("headers", {}))
    head = tail = b""
    if "form" in target:
        form, boundary, mime = target["form"], secrets.token_hex(16), entry["mimeType"]
        parts = [
            f'--{boundary}\r\nContent-Disposition: form-data; name="{field["name"]}"'
            f"\r\n\r\n{field['value']}\r\n"
            for field in form["fields"]
        ]
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{form["fileField"]}";'
            f' filename="{_file_name(entry.get("name")).translate(_QUOTE)}"\r\n'
            f"Content-Type: {mime if MEDIA_TYPE.fullmatch(mime) else 'application/octet-stream'}"
            "\r\n\r\n"
        )
        head, tail = "".join(parts).encode(), f"\r\n--{boundary}--\r\n".encode()
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["Content-Length"] = str(len(head) + size + len(tail))

    async def content() -> AsyncIterator[bytes]:
        yield head
        async for chunk in file._chunks(size):
            yield chunk
        yield tail

    return headers, content()


def holds_file(value: Any) -> bool:
    if isinstance(value, dict):
        value = list(value.values())
    return isinstance(value, File) or (
        isinstance(value, list | tuple) and any(map(holds_file, value))
    )


_calls: dict[int, Transfers] = {}


def remember(result: CallToolResult, transfers: Transfers) -> None:
    if EXTENSION in (result.meta or {}):
        _calls[id(result)] = transfers
        weakref.finalize(result, _calls.pop, id(result), None)


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
    _target: DownloadTarget = field(repr=False)
    _call: Transfers = field(repr=False)

    async def read(self) -> bytes:
        """Fetch the file into memory.

        Raises:
            FileTransferError: The fetch failed or the bytes failed a check.
        """
        return b"".join([chunk async for chunk in self._fetch()])

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
        async with (
            writing(destination, overwrite) as partial,
            await anyio.open_file(partial, "xb") as out,
            aclosing(self._fetch()) as chunks,
        ):
            async for chunk in chunks:
                await out.write(chunk)
        return destination

    async def _fetch(self) -> AsyncGenerator[bytes]:
        label = f"the download of {_file_name(self.name)!r}"
        if self.expires_at is not None and datetime.now(UTC) >= self.expires_at:
            raise FileTransferError(f"{label} expired")
        hasher = hashlib.sha256()
        received = 0
        try:
            async with self._call.exchange(
                self._target, "GET", dict(self._target.get("headers", {}))
            ) as response:
                if response.status_code != httpx2.codes.OK:
                    raise FileTransferError(
                        f"{label} failed: HTTP {response.status_code}"
                    )
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > self.size:
                        raise FileTransferError(f"{label} is larger than declared")
                    hasher.update(chunk)
                    yield chunk
        except Rejected as error:
            raise FileTransferError(f"{label} was rejected: {error}") from None
        except (httpx2.HTTPError, OSError) as error:
            raise FileTransferError(
                f"{label} failed ({type(error).__name__})"
            ) from None
        if received != self.size:
            raise FileTransferError(f"{label} is smaller than declared")
        if self.digest is not None and b64url(hasher.digest()) != self.digest:
            raise FileTransferError(f"{label} does not match its digest")


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
    call = _calls.get(id(result))
    if call is None:
        raise ValueError("the result did not come from a holon client")
    found = []
    for number, raw in enumerate(listed, 1):
        try:
            target = check_target(raw, DOWNLOAD)
            call.check(target)
        except Rejected as error:
            raise ValueError(f"download {number} was rejected: {error}") from None
        digest = target["digest"]["value"] if "digest" in target else None
        found.append(
            Download(
                target.get("name"),
                target["mimeType"],
                target["size"],
                digest,
                expires_at(target),
                target,
                call,
            )
        )
    return found
