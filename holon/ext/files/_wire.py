"""Shared by the files server and client."""

from __future__ import annotations

import base64
import hashlib
import mimetypes
import os
import re
import secrets
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, NotRequired, TypeVar

import httpx2
from mcp.types import (
    CallToolResult,
    ClientCapabilities,
    InputRequiredResult,
    Result,
    ServerCapabilities,
)
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError, with_config
from referencing import Registry
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012
from typing_extensions import TypedDict  # pydantic validates it on Python 3.11

EXTENSION = "io.modelcontextprotocol/files"

# Targets rule 1: no unknown members or values.
_SHAPE = ConfigDict(extra="forbid", strict=True)


@with_config(_SHAPE)
class Digest(TypedDict):
    algorithm: Literal["sha-256"]
    value: str


@with_config(_SHAPE)
class FormField(TypedDict):
    name: str
    value: str


@with_config(_SHAPE)
class UploadForm(TypedDict):
    fields: list[FormField]
    fileField: str


class _Target(TypedDict):
    url: NotRequired[str]
    path: NotRequired[str]
    headers: NotRequired[dict[str, str]]
    credentials: Literal["omit", "include"]
    expiresAt: NotRequired[str]
    _meta: NotRequired[dict[str, Any]]


@with_config(_SHAPE)
class UploadTarget(_Target):
    """Where a client sends one file. ``FileServer`` fills in ``fileId``."""

    fileId: NotRequired[str]
    method: NotRequired[Literal["PUT", "POST"]]
    form: NotRequired[UploadForm]


@with_config(_SHAPE)
class DownloadTarget(_Target):
    """Where a client fetches one file a tool returned."""

    name: NotRequired[str]
    mimeType: str
    size: int
    digest: NotRequired[Digest]


UPLOAD, DOWNLOAD = TypeAdapter(UploadTarget), TypeAdapter(DownloadTarget)


class UploadRequiredResult(Result):
    result_type: Literal["upload_required"]
    uploads: object = None
    request_state: object = None


CALL_RESULT: TypeAdapter[UploadRequiredResult | CallToolResult | InputRequiredResult]
CALL_RESULT = TypeAdapter(
    Annotated[
        UploadRequiredResult | CallToolResult | InputRequiredResult,
        Field(union_mode="left_to_right"),
    ]
)


def file_input_schema(
    *,
    accept: Sequence[str] | None = None,
    max_size: int | None = None,
    digest: bool = False,
) -> dict[str, Any]:
    """Return the JSON Schema of a file argument.

    Args:
        accept: Media types or dot-prefixed extensions the tool takes.
        max_size: Largest file in bytes.
        digest: Require a SHA-256 digest.
    """
    descriptor: dict[str, Any] = {}
    if accept is not None:
        descriptor["accept"] = list(accept)
    if max_size is not None:
        descriptor["maxSize"] = max_size
    return {
        "type": "object",
        "x-mcp-file": descriptor,
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 128},
            "name": {"type": "string"},
            "mimeType": {"type": "string"},
            "size": {"type": "integer", "minimum": 0},
            "digest": {
                "type": "object",
                "properties": {
                    "algorithm": {"const": "sha-256"},
                    "value": {"type": "string"},
                },
                "required": ["algorithm", "value"],
            },
            "data": {"type": "string"},
        },
        "required": ["id", "mimeType", "size", *(["digest"] if digest else [])],
    }


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def sha256(data: bytes) -> str:
    return b64url(hashlib.sha256(data).digest())


def media_type(name: str | None, given: str | None) -> str:
    guessed = mimetypes.guess_type(name)[0] if name else None
    return given or guessed or "application/octet-stream"


def digest_required(schema: Mapping[str, Any]) -> bool:
    required = schema.get("required")
    return isinstance(required, list) and "digest" in required


def declared(
    capabilities: ClientCapabilities | ServerCapabilities | None,
) -> Mapping[str, Any] | None:
    settings = ((capabilities and capabilities.extensions) or {}).get(EXTENSION)
    return None if settings is None or "revision" in settings else settings


@asynccontextmanager
async def writing(destination: Path, overwrite: bool) -> AsyncIterator[Path]:
    partial = destination.with_name(f".{destination.name}.{secrets.token_hex(8)}.part")
    try:
        yield partial
        if overwrite:
            os.replace(partial, destination)
        else:
            os.link(partial, destination)
    finally:
        partial.unlink(missing_ok=True)


def walk_files(
    schema: Mapping[str, Any],
    value: Any,
    visit: Callable[[Any, Mapping[str, Any]], Any],
) -> Any:
    resolver = Registry().resolver_with_root(DRAFT202012.create_resource(schema))

    def expand(node: Any, seen: set[int]) -> list[Mapping[str, Any]]:
        """``node`` and the schemas its local ``$ref``, ``anyOf`` and ``oneOf`` add."""
        if not isinstance(node, Mapping) or id(node) in seen:
            return []
        seen.add(id(node))
        nested = [*node.get("anyOf", ()), *node.get("oneOf", ())]
        if isinstance(ref := node.get("$ref"), str) and ref.startswith("#"):
            try:
                nested.insert(0, resolver.lookup(ref).contents)
            except (Unresolvable, LookupError, TypeError):
                pass
        return [node, *(found for child in nested for found in expand(child, seen))]

    def walk(schemas: list[Any], value: Any) -> Any:
        seen: set[int] = set()
        applicable = [found for schema in schemas for found in expand(schema, seen)]
        if not applicable:
            return value
        for marked in applicable:
            if isinstance(marked.get("x-mcp-file"), Mapping):
                replaced = visit(value, marked)
                if replaced is not None:
                    return replaced
        if isinstance(value, dict):
            properties = [s.get("properties") for s in applicable]
            return {
                key: walk(
                    [p.get(key) for p in properties if isinstance(p, Mapping)], item
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                walk([_item_schema(schema, index) for schema in applicable], item)
                for index, item in enumerate(value)
            ]
        return value

    return walk([schema], value)


def _item_schema(schema: Mapping[str, Any], index: int) -> Any:
    prefix, items = schema.get("prefixItems"), schema.get("items")
    if isinstance(items, list):  # draft 2019-09 tuple form
        prefix, items = items, schema.get("additionalItems")
    return prefix[index] if isinstance(prefix, list) and index < len(prefix) else items


class Rejected(Exception): ...


_TOKEN = r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+"
_HEADER_NAME = re.compile(_TOKEN)
_HEADER_VALUE = re.compile(
    r"(?:[\x21-\x7e\x80-\xff](?:[ \t\x21-\x7e\x80-\xff]*[\x21-\x7e\x80-\xff])?)?"
)
MEDIA_TYPE = re.compile(
    rf"{_TOKEN}/{_TOKEN}(?:[ \t]*;[ \t]*{_TOKEN}="
    rf'(?:{_TOKEN}|"(?:[\t \x21\x23-\x5b\x5d-\x7e\x80-\xff]|\\[\t \x21-\x7e\x80-\xff])*"))*'
)
_PATH = re.compile(r"(?:/(?!\.\.?(?:/|$))[A-Za-z0-9\-_~.]+)+")
_DIGEST = re.compile(r"[A-Za-z0-9_-]{43}")
_FORM_NAME = re.compile(r'[^\x00-\x1f\x7f"\\]+')
# https://fetch.spec.whatwg.org/#forbidden-request-header
_FORBIDDEN = frozenset(
    {"accept-charset", "accept-encoding", "authorization", "connection", "cookie"}
    | {"access-control-request-headers", "access-control-request-method"}
    | {"content-length", "cookie2", "date", "dnt", "expect", "host", "keep-alive"}
    | {"origin", "referer", "set-cookie", "te", "trailer", "transfer-encoding"}
    | {"upgrade", "via"}
)
_OVERRIDES = frozenset({"x-http-method", "x-http-method-override", "x-method-override"})
_ROUTING = _OVERRIDES | {"forwarded", "x-original-url", "x-rewrite-url"}
_CONTENT = ("type", "encoding", "range", "digest")
_FORM_HEADERS = {f"content-{name}" for name in _CONTENT} | {"repr-digest"}

_Kind = TypeVar("_Kind", UploadTarget, DownloadTarget)


def _absent(value: object, key: object = None) -> object:
    if isinstance(value, dict) and key != "headers":
        return {
            name: _absent(item, name)
            for name, item in value.items()
            if item is not None
        }
    return [_absent(item) for item in value] if isinstance(value, list) else value


def check_target(raw: object, kind: TypeAdapter[_Kind]) -> _Kind:
    """Return the target, or raise Rejected if it breaks Targets rules 1-5."""
    try:
        checked = kind.validate_python(_absent(raw))
    except ValidationError as error:
        first = error.errors()[0]
        where = ".".join(str(part) for part in first["loc"][:1]) or "it"
        raise Rejected(f"{where}: {first['msg']}") from None
    target: Mapping[str, Any] = checked
    if ("url" in target) == ("path" in target):
        raise Rejected("it needs exactly one of url and path")
    expires_at(target)
    if "url" in target:
        try:
            url = httpx2.URL(target["url"])
        except httpx2.InvalidURL:
            raise Rejected("its url is not an absolute https URL") from None
        if url.scheme not in ("https", "http") or not url.host:
            raise Rejected("its url is not an absolute https URL")
        if url.userinfo:
            raise Rejected("its url carries userinfo")
    elif not _PATH.fullmatch(target["path"]):
        raise Rejected("its path is not a plain path")
    if target.get("size", 0) < 0:
        raise Rejected("its size is negative")
    if "digest" in target and not _DIGEST.fullmatch(target["digest"]["value"]):
        raise Rejected("its digest is not a SHA-256 digest")
    names = []
    for name, value in target.get("headers", {}).items():
        lower = name.lower()
        if not (_HEADER_NAME.fullmatch(name) and _HEADER_VALUE.fullmatch(value)):
            raise Rejected("it has an invalid header")
        methods = {method.strip().upper() for method in value.split(",")}
        if (
            lower in _FORBIDDEN
            or lower.startswith(("proxy-", "sec-"))
            or (lower in _OVERRIDES and {"CONNECT", "TRACE", "TRACK"} & methods)
        ):
            raise Rejected(f"it sets the forbidden header {name}")
        if ("path" in target or target["credentials"] == "include") and (
            lower in _ROUTING or lower.startswith("x-forwarded-")
        ):
            raise Rejected("it sets a routing header")
        names.append(lower)
    if len(set(names)) != len(names):
        raise Rejected("it repeats a header")
    if form := target.get("form"):
        fields: list[str] = [field["name"] for field in form["fields"]]
        if target.get("method") != "POST":
            raise Rejected("its form needs method POST")
        if _FORM_HEADERS.intersection(names):
            raise Rejected("its form sets a content header")
        if form["fileField"] in fields or not all(
            _FORM_NAME.fullmatch(name) for name in [form["fileField"], *fields]
        ):
            raise Rejected("a form field name is invalid")
        if any(c in field["value"] for field in form["fields"] for c in "\r\n\0"):
            raise Rejected("a form field value is invalid")
    return checked


def expires_at(target: Mapping[str, Any]) -> datetime | None:
    if "expiresAt" not in target:
        return None
    try:
        parsed = datetime.fromisoformat(target["expiresAt"])
        if parsed.tzinfo is not None:
            return parsed
    except ValueError:
        pass
    raise Rejected("its expiresAt is not an RFC 3339 time")
