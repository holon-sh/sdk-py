# Holon Python SDK

[![CI](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/holon-sh)](https://pypi.org/project/holon-sh/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

This repository contains the Python runtime for [`holon`](https://github.com/holon-sh/cli).

## Install

```sh
uv add --prerelease allow holon-sh
uv add --dev holon-types
```

`holon-types` is the `holon` CLI; it brings mypy, which `holon types check` runs from the project's environment.

## Invoking MCP Servers

The Holon Python SDK allows generation of type-safe MCP clients. See [Example](https://github.com/holon-sh/cli#example) for an explanation, or [Dependencies](https://github.com/holon-sh/cli#dependencies) to understand dependency resolution.

### Type-checking MCP invocations

```sh
uv run holon types check
```

<details open>
<summary><h3>Call a tool</h3></summary>

```python
from holon.apps import apps

async def greet(name: str) -> str:
    greeting = await apps.bar.tools.greet({"timeout": 5.0}, name=name)
    return greeting["result"]
```

Arguments are keywords, validated before sending; the result is validated against the output schema and returned.

</details>

<details open>
<summary><h3>Handle errors</h3></summary>

```python
from holon.client import ToolCallError

try:
    await apps.bar.tools.greet(name="world")
except ToolCallError as error:
    error.code     # "tool_error" or "input_required"
    error.result   # the original MCP result
```

</details>

<details open>
<summary><h3>Read the whole result</h3></summary>

`call_tool` returns the full MCP result instead of raising:

```python
result = await apps.bar.call_tool("greet", name="world")
if result.result_type == "input_required":
    ...                          # call again with input_responses and request_state
elif result.is_error:
    ...                          # inspect result.content
else:
    result.structured_content    # typed as Outputs.bar.greet
```

The options dict also accepts `input_responses`, `request_state` and `meta`.

</details>

## Serve an [MCP App](https://modelcontextprotocol.io/extensions/apps/overview) view

`holon.view.app_view` registers a built HTML file as an MCP App resource:

```python
from holon.view import app_view

server.add_resource(app_view(
    "ui://foo/index.html", "Foo", "dist/index.html",
    connect_domains=["https://bar.example.com"],
))
```

The page itself uses [`holon-ui`](https://github.com/holon-sh/sdk-ui).

## Function apps

`holon.app.App` extends the native `MCPServer`; its decorators keep the SDK's
signature inference, validation, contexts, and middleware.

```python
from holon.app import App
from holon.view import app_view
from mcp.types import ToolAnnotations

server = App("example.com/counter", version="1.0.0", description="A shared counter")
count = 0

@server.resource("counter://state", mime_type="application/json")
def state() -> dict[str, int]:
    return {"count": count}

@server.tool(annotations=ToolAnnotations(read_only_hint=True),
             meta={"ui": {"resourceUri": "ui://counter/index.html"}})
def show() -> dict[str, int]:
    return state()

@server.tool(changes=["counter://state"])
def add(amount: int) -> dict[str, int]:
    global count
    count += amount
    return state()

server.add_resource(app_view("ui://counter/index.html", "Counter", "dist/index.html"))
```

`changes` sets `sh.holon/changes`: tool metadata listing the resource URIs a successful
call invalidates. It is a refresh declaration, not authorization or a transaction.
After a successful call `App` publishes `ResourceUpdated` for each URI; a notification
failure is logged without changing the committed result.

Run `holon types generate` to generate browser references from Python annotations.
No application modules run during extraction.
