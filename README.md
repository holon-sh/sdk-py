# holon-sh

The Python runtime for typed MCP clients. The `holon` CLI (PyPI `holon-types`) reads the MCP servers your server calls and generates a typed client module, `holon.apps`, into your project. This package is what that module imports at runtime: it connects over Streamable HTTP, validates arguments and results against each tool's JSON Schema, and raises on tool errors.

For the end-to-end walkthrough (declaring dependencies in `server.json`, generating, checking), see the [`holon-types` README](https://pypi.org/project/holon-types/). This page describes the Python API.

## Install the runtime

Only dev releases exist so far, so allow prereleases:

```sh
uv add --prerelease allow holon-sh
uv add --dev mypy==2.4.0        # holon types check runs mypy from the app's .venv
```

```sh
pip install --pre holon-sh
```

The distribution is `holon-sh`; the import package is `holon`. Python 3.11 or newer.

## Use the generated module

`holon types generate` writes `holon/apps/__init__.py` into your app. Import from it:

```python
from holon.apps import Inputs, Outputs, apps, create_apps
```

| Name | What it is |
|---|---|
| `apps` | One client per dependency declared in `server.json` (`apps.orders`, or `apps["orders"]`). Bound through `<NAME>_MCP_URL`. |
| `create_apps(bindings)` | A client set bound explicitly, containing only the dependencies you pass. |
| `Inputs.<dep>.<tool>` | The tool's argument type, a `TypedDict`. |
| `Outputs.<dep>.<tool>` | The tool's structured result type, a `TypedDict`. |

`holon` is a namespace package shared by this distribution and your generated module. Do not create `holon/__init__.py` in your app: it hides `holon.client` and `holon.view`, and the CLI refuses to generate while it exists. Do not edit `holon/apps/__init__.py` either; it starts with a marker line, and the CLI overwrites it on every run.

### Call a tool

Pass the arguments as one mapping. The result is the tool's validated structured output:

```python
from holon.apps import apps


async def total(id: str) -> float:
    order = await apps.orders.tools.lookup({"id": id}, timeout=5.0)
    return order["total"]
```

`tools.<tool>` validates the arguments against the tool's input schema before sending, validates the structured result against its output schema, and returns it. `timeout` is in seconds.

### Handle tool errors

`tools.<tool>` raises `ToolCallError` when the tool reports an error or asks for more input:

```python
from holon.client import ToolCallError

try:
    order = await apps.orders.tools.lookup({"id": id})
except ToolCallError as error:
    error.app      # "orders"
    error.tool     # "lookup"
    error.code     # "tool_error" or "input_required"
    error.result   # the original MCP result
```

Invalid arguments or an output that does not match the schema raise `ValueError`; transport and protocol failures propagate from the MCP client.

### Read the whole result

`call_tool` returns the full MCP result instead of raising on tool errors:

```python
from mcp.types import InputRequiredResult

result = await apps.orders.call_tool("lookup", {"id": id}, timeout=5.0)
if isinstance(result, InputRequiredResult):
    ...  # call again with input_responses=... and request_state=...
elif result.is_error:
    ...  # application error: inspect result.content
else:
    result.structured_content  # typed as Outputs.orders.lookup
```

`call_tool` also accepts `input_responses`, `request_state` and `meta` keyword arguments.

### Call an untyped dependency

When the CLI cannot obtain a contract for a dependency, its client offers only `call_tool`, which accepts any tool name and validates nothing.

## Bind a dependency

Without an explicit binding, a client reads its endpoint from an environment variable named after the dependency: upper-cased, `-` replaced by `_`, plus `_MCP_URL`.

```sh
ORDERS_MCP_URL=http://127.0.0.1:3001/mcp uv run python server.py
```

To add authentication or a custom HTTP client, bind explicitly with `create_apps`. `http_client` is a factory; it is called once per tool call and the client it returns is closed afterwards:

```python
import os

import httpx2

from holon.apps import create_apps

apps = create_apps(
    {
        "orders": {
            "endpoint": "https://orders.example.com/mcp",
            "http_client": lambda: httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {os.environ['ORDERS_TOKEN']}"},
            ),
        },
    }
)
```

Every call opens and closes its own MCP session; there is nothing to close.

`holon.binding.set_default_binding` exists for hosts that bind every app at startup; standalone apps do not need it.

## Serve an MCP App view

`holon.view.app_view` registers a built HTML file as an MCP App resource:

```python
from mcp.server import MCPServer

from holon.view import app_view

server = MCPServer("checkout")
server.add_resource(
    app_view(
        "ui://checkout/index.html",
        "Checkout",
        "dist/index.html",
        connect_domains=["https://orders.example.com"],
    )
)
```

The file is read on the first request and cached for the life of the process. `connect_domains` and `resource_domains` become the view's content security policy.

## Type-check against contracts

`holon types check` (or its shortcut `holon check`) regenerates the client in a temporary copy of your app and runs mypy on it with the interpreter of the nearest `.venv` or `venv` in the app directory or a parent, or else the active virtual environment. Install `mypy==2.4.0` there as a dev dependency.

## License

MIT
