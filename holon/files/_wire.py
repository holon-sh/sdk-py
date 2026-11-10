"""Wire shapes and the checks both sides share, from the draft files extension."""

from __future__ import annotations

import base64
import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal, NotRequired, TypedDict
from urllib.parse import unquote

import httpx2
from mcp.types import Result
from pydantic import ConfigDict

EXTENSION = "io.modelcontextprotocol/files"


class FileInputDescriptor(TypedDict, total=False):
    """The value of ``x-mcp-file`` on a file argument's schema."""

    accept: list[str]
    maxSize: int


class Digest(TypedDict):
    algorithm: Literal["sha-256"]
    value: str


class FileInput(TypedDict):
    """A file argument as it travels in ``tools/call``."""

    id: str
    name: NotRequired[str]
    mimeType: str
    size: int
    digest: NotRequired[Digest]
    data: NotRequired[str]


class FormField(TypedDict):
    name: str
    value: str


class UploadForm(TypedDict):
    fields: list[FormField]
    fileField: str


class UploadTarget(TypedDict):
    """Where a client sends one file. ``FileServer`` fills in ``fileId``."""

    fileId: NotRequired[str]
    url: NotRequired[str]
    path: NotRequired[str]
    method: NotRequired[Literal["PUT", "POST"]]
    form: NotRequired[UploadForm]
    headers: NotRequired[dict[str, str]]
    credentials: Literal["omit", "include"]
    expiresAt: NotRequired[str]
    _meta: NotRequired[dict[str, Any]]


class DownloadTarget(TypedDict):
    """Where a client fetches one file a tool returned."""

    name: NotRequired[str]
    mimeType: str
    size: int
    digest: NotRequired[Digest]
    url: NotRequired[str]
    path: NotRequired[str]
    headers: NotRequired[dict[str, str]]
    credentials: Literal["omit", "include"]
    expiresAt: NotRequired[str]
    _meta: NotRequired[dict[str, Any]]


class ResultFilesMeta(TypedDict):
    """``CallToolResult._meta[EXTENSION]``."""

    downloads: list[DownloadTarget]


class UploadRequiredResult(Result):
    """The ``upload_required`` result of ``tools/call``.

    ``requestState`` stays an extra field: mcp 2.3 refuses a claim model that
    types it, and the client checks every member of ``uploads`` itself.
    """

    model_config = ConfigDict(extra="allow")

    result_type: Literal["upload_required"]
    uploads: list[Any]

    @property
    def request_state(self) -> object:
        return (self.model_extra or {}).get("requestState")


def file_input_schema(
    *,
    accept: Sequence[str] | None = None,
    max_size: int | None = None,
    digest: bool = False,
) -> dict[str, Any]:
    """Return the JSON Schema of a file argument.

    Args:
        accept: Media types or dot-prefixed extensions the tool takes.
        max_size: Largest file in bytes, however it travels.
        digest: Require a SHA-256 digest, which the server then verifies.
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


def sha256(data: bytes) -> str:
    return b64url(hashlib.sha256(data).digest())


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def digest_required(schema: Mapping[str, Any]) -> bool:
    required = schema.get("required")
    return isinstance(required, list) and "digest" in required


def descriptor(schema: Mapping[str, Any]) -> Mapping[str, Any]:
    value = schema.get("x-mcp-file")
    return value if isinstance(value, Mapping) else {}


def declared(capabilities: Any) -> Mapping[str, Any] | None:
    """The extension's settings a peer declared, or None if it did not."""
    extensions = getattr(capabilities, "extensions", None) or {}
    settings = extensions.get(EXTENSION) if isinstance(extensions, Mapping) else None
    # A revision we do not know is a different, incompatible extension.
    if not isinstance(settings, Mapping) or "revision" in settings:
        return None
    return settings


# File arguments: a value at a schema position marked x-mcp-file.

UNCHANGED: Any = object()
_DEPTH = 32


def walk_files(
    schema: Mapping[str, Any],
    value: Any,
    visit: Callable[[Any, Mapping[str, Any]], Any],
) -> Any:
    """Return ``value`` with ``visit(value, marked_schema)`` applied at file positions.

    ``visit`` returns its replacement, or ``UNCHANGED`` when the value there is
    not a file (an ``anyOf`` branch can allow ``null`` beside a file).
    """
    return _walk([schema], value, schema, visit, 0)


def _walk(
    schemas: list[Any],
    value: Any,
    root: Mapping[str, Any],
    visit: Callable[[Any, Mapping[str, Any]], Any],
    depth: int,
) -> Any:
    applicable = [found for schema in schemas for found in _expand(schema, root, depth)]
    for schema in applicable:
        if isinstance(schema.get("x-mcp-file"), Mapping):
            replaced = visit(value, schema)
            if replaced is not UNCHANGED:
                return replaced
    if depth >= _DEPTH:
        return value
    if isinstance(value, dict):
        return {
            key: _walk(
                [
                    properties[key]
                    for schema in applicable
                    if isinstance(properties := schema.get("properties"), Mapping)
                    and key in properties
                ],
                item,
                root,
                visit,
                depth + 1,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _walk(
                [
                    found
                    for schema in applicable
                    if (found := _item_schema(schema, index)) is not None
                ],
                item,
                root,
                visit,
                depth + 1,
            )
            for index, item in enumerate(value)
        ]
    return value


def _item_schema(schema: Mapping[str, Any], index: int) -> Any:
    prefix = schema.get("prefixItems")
    items = schema.get("items")
    if isinstance(items, list):  # draft 2019-09 tuple form
        prefix, items = items, schema.get("additionalItems")
    if isinstance(prefix, list) and index < len(prefix):
        return prefix[index]
    return items if isinstance(items, Mapping) else None


def _expand(
    schema: Any, root: Mapping[str, Any], depth: int
) -> list[Mapping[str, Any]]:
    if not isinstance(schema, Mapping) or depth >= _DEPTH:
        return []
    found = [schema]
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith("#"):
        found += _expand(_pointer(root, ref[1:]), root, depth + 1)
    for key in ("anyOf", "oneOf"):
        branches = schema.get(key)
        if isinstance(branches, list):
            for branch in branches:
                found += _expand(branch, root, depth + 1)
    return found


def _pointer(root: Any, pointer: str) -> Any:
    node = root
    for part in unquote(pointer).split("/")[1:]:
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(node, Mapping):
            node = node.get(part)
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            return None
    return node


# Targets: the rules every upload and download target must pass.


class Rejected(Exception):
    """A target or upload request broke a rule. The reason names no secret."""


_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_FIELD_VALUE = re.compile(
    r"(?:[\x21-\x7e\x80-\xff](?:[ \t\x21-\x7e\x80-\xff]*[\x21-\x7e\x80-\xff])?)?"
)
_PATH = re.compile(r"(?:/(?!\.\.?(?:/|$))[A-Za-z0-9\-_~.]+)+")
_DIGEST = re.compile(r"[A-Za-z0-9_-]{43}")
_MEDIA_TYPE = re.compile(
    r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+/[!#$%&'*+\-.^_`|~0-9A-Za-z]+"
    r"(?:[ \t]*;[ \t]*[!#$%&'*+\-.^_`|~0-9A-Za-z]+="
    r"(?:[!#$%&'*+\-.^_`|~0-9A-Za-z]+|\"(?:[\t \x21\x23-\x5b\x5d-\x7e\x80-\xff]|\\[\t \x21-\x7e\x80-\xff])*\"))*"
)
# https://fetch.spec.whatwg.org/#forbidden-request-header
_FORBIDDEN = frozenset(
    {
        "accept-charset",
        "accept-encoding",
        "access-control-request-headers",
        "access-control-request-method",
        "authorization",
        "connection",
        "content-length",
        "cookie",
        "cookie2",
        "date",
        "dnt",
        "expect",
        "host",
        "keep-alive",
        "origin",
        "referer",
        "set-cookie",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "via",
    }
)
_METHOD_OVERRIDES = frozenset(
    {"x-http-method", "x-http-method-override", "x-method-override"}
)
_ROUTING = (
    frozenset({"forwarded", "x-original-url", "x-rewrite-url"}) | _METHOD_OVERRIDES
)
_FORM_HEADERS = frozenset(
    {
        "content-type",
        "content-encoding",
        "content-range",
        "content-digest",
        "repr-digest",
    }
)
_COMMON = frozenset({"url", "path", "headers", "credentials", "expiresAt", "_meta"})
_UPLOAD = _COMMON | {"fileId", "method", "form"}
_DOWNLOAD = _COMMON | {"name", "mimeType", "size", "digest"}


def valid_media_type(value: str) -> bool:
    return _MEDIA_TYPE.fullmatch(value) is not None


def check_target(raw: object, *, upload: bool) -> dict[str, Any]:
    """Return the target without null members, or raise Rejected (Targets 1-5)."""
    target = _members(raw, _UPLOAD if upload else _DOWNLOAD, "target")
    if ("url" in target) == ("path" in target):
        raise Rejected("it needs exactly one of url and path")
    if target.get("credentials") not in ("omit", "include"):
        raise Rejected("its credentials are not 'omit' or 'include'")
    if "expiresAt" in target:
        expires_at(target)
    if "url" in target:
        _check_url(target["url"])
    elif not (isinstance(target["path"], str) and _PATH.fullmatch(target["path"])):
        raise Rejected("its path is not a plain path")
    if upload:
        if not isinstance(target.get("fileId"), str):
            raise Rejected("it has no fileId")
        if target.get("method", "PUT") not in ("PUT", "POST"):
            raise Rejected("its method is not PUT or POST")
    else:
        if "name" in target and not isinstance(target["name"], str):
            raise Rejected("its name is not a string")
        if not isinstance(target.get("mimeType"), str):
            raise Rejected("it has no mimeType")
        size = target.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise Rejected("its size is not a non-negative integer")
        if "digest" in target:
            digest = _members(target["digest"], {"algorithm", "value"}, "digest")
            if digest.get("algorithm") != "sha-256" or not (
                isinstance(digest.get("value"), str)
                and _DIGEST.fullmatch(digest["value"])
            ):
                raise Rejected("its digest is not a SHA-256 digest")
            target["digest"] = digest
    headers = target.get("headers", {})
    if not isinstance(headers, dict):
        raise Rejected("its headers are not an object")
    names = [_check_header(name, value) for name, value in headers.items()]
    if len(set(names)) != len(names):
        raise Rejected("it repeats a header")
    if ("path" in target or target["credentials"] == "include") and any(
        name in _ROUTING or name.startswith("x-forwarded-") for name in names
    ):
        raise Rejected("it sets a routing header")
    if "form" in target:
        if target.get("method") != "POST":
            raise Rejected("its form needs method POST")
        if _FORM_HEADERS.intersection(names):
            raise Rejected("its form sets a content header")
        target["form"] = _check_form(target["form"])
    return target


def expires_at(target: Mapping[str, Any]) -> datetime | None:
    value = target.get("expiresAt")
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.tzinfo is None:
        raise Rejected("its expiresAt is not an RFC 3339 time")
    return parsed


def _members(
    raw: object, allowed: frozenset[str] | set[str], what: str
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise Rejected(f"its {what} is not an object")
    members = {key: value for key, value in raw.items() if value is not None}
    unknown = sorted(set(members) - allowed - {"_meta"})
    if unknown:
        raise Rejected(f"it has an unknown member {unknown[0]!r}")
    return members


def _check_url(value: object) -> None:
    try:
        url = httpx2.URL(value) if isinstance(value, str) else None
    except httpx2.InvalidURL:
        url = None
    # Plain http is only ever allowed by an operator allowlist, checked later.
    if url is None or url.scheme not in ("https", "http") or not url.host:
        raise Rejected("its url is not an absolute https URL")
    if url.userinfo:
        raise Rejected("its url carries userinfo")


def _check_header(name: object, value: object) -> str:
    if not (isinstance(name, str) and _TOKEN.fullmatch(name)):
        raise Rejected("it has an invalid header name")
    if not (isinstance(value, str) and _FIELD_VALUE.fullmatch(value)):
        raise Rejected("it has an invalid header value")
    lower = name.lower()
    if lower in _FORBIDDEN or lower.startswith(("proxy-", "sec-")):
        raise Rejected(f"it sets the forbidden header {name}")
    if lower in _METHOD_OVERRIDES and {
        method.strip().upper() for method in value.split(",")
    } & {"CONNECT", "TRACE", "TRACK"}:
        raise Rejected(f"it sets the forbidden header {name}")
    return lower


def _check_form(raw: object) -> dict[str, Any]:
    form = _members(raw, {"fields", "fileField"}, "form")
    names = [form.get("fileField")]
    fields = form.get("fields")
    if not isinstance(fields, list):
        raise Rejected("its form has no fields list")
    checked = []
    for field in fields:
        field = _members(field, {"name", "value"}, "form field")
        value = field.get("value")
        if not isinstance(value, str) or any(c in value for c in "\r\n\0"):
            raise Rejected("a form field value is invalid")
        names.append(field.get("name"))
        checked.append(field)
    for name in names:
        if (
            not isinstance(name, str)
            or not name
            or re.search(r'[\x00-\x1f\x7f"\\]', name)
        ):
            raise Rejected("a form field name is invalid")
    if form["fileField"] in names[1:]:
        raise Rejected("a form field reuses the file field's name")
    return {"fields": checked, "fileField": form["fileField"]}
