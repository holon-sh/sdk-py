# Holon Python SDK

[![CI](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/holon-sh)](https://pypi.org/project/holon-sh/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

This repository contains the Python runtime for [`holon`](https://github.com/holon-sh/cli).

## Install

```sh
uv add --prerelease allow holon-sh
uv add --dev mypy==2.4.0
```

Then install the CLI, which generates the `holon.apps` module the examples import:

```sh
curl -fsSL https://holon-labs.dev/install.sh | sh
```

## Type Safety

The Holon Python SDK allows generation of type-safe MCP clients. See [Example](https://github.com/holon-sh/cli#example) for an explanation, or [Dependencies](https://github.com/holon-sh/cli#dependencies) to understand dependency resolution.

## The generated module

```python
from holon.apps import Inputs, Outputs, apps, create_apps
```

| Name | What it is |
|---|---|
| `apps` | One client per dependency (`apps.bar`), bound through `<ALIAS>_MCP_URL`. |
| `create_apps(bindings)` | Clients bound explicitly, for only the dependencies you pass. |
| `Inputs.<dep>.<tool>` | The tool's argument type, a `TypedDict`. |
| `Outputs.<dep>.<tool>` | The tool's structured result type, a `TypedDict`. |

<details open>
<summary><h3>Call a tool</h3></summary>

```python
from holon.apps import apps

async def greet(name: str) -> str:
    greeting = await apps.bar.tools.greet({"timeout": 5.0}, name=name)
    return greeting["result"]
```

Arguments are keywords, validated before sending; the result is validated against the output schema and returned. Request options are a positional dict; `timeout` is in seconds.

</details>

<details open>
<summary><h3>Handle errors</h3></summary>

`tools.<tool>` raises `ToolCallError` when the tool reports an error or asks for more input:

```python
from holon.client import ToolCallError

try:
    await apps.bar.tools.greet(name="world")
except ToolCallError as error:
    error.code     # "tool_error" or "input_required"
    error.result   # the original MCP result
```

Invalid arguments or a result that does not match the schema raise `ValueError`; transport failures propagate from the MCP client.

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

The options dict also accepts `input_responses`, `request_state` and `meta`. A dependency the CLI could not obtain a contract for offers only `call_tool`, untyped.

</details>

## Bind a dependency

By default a client reads its endpoint from an environment variable named after the dependency, upper-cased, with `-` as `_`:

```sh
BAR_MCP_URL=http://127.0.0.1:3001/mcp uv run python server.py
```

For authentication or a custom HTTP client, bind explicitly. `http_client` is a factory called once per call; the client it returns is closed afterwards:

```python
import os
import httpx2
from holon.apps import create_apps

apps = create_apps({
    "bar": {
        "endpoint": "https://bar.example.com/mcp",
        "http_client": lambda: httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {os.environ['BAR_TOKEN']}"},
        ),
    },
})
```

Every call opens and closes its own session; there is nothing to close. `holon.binding.set_default_binding` exists for hosts that bind every app at startup; apps do not need it.

## Serve an MCP App view

`holon.view.app_view` registers a built HTML file as an MCP App resource:

```python
from holon.view import app_view

server.add_resource(app_view(
    "ui://foo/index.html", "Foo", "dist/index.html",
    connect_domains=["https://bar.example.com"],
))
```

The file is read on first request and cached. `connect_domains` and `resource_domains` become the view's content security policy. The page itself uses [`holon-ui`](https://github.com/holon-sh/sdk-ui).
