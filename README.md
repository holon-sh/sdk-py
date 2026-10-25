# holon-sh

The Python runtime for typed MCP clients. The [`holon` CLI](https://github.com/holon-sh/cli) reads the MCP servers your server calls and generates `holon.apps`, a typed client module, into your project. This package is what that module runs on: it connects over Streamable HTTP, validates arguments and results against each tool's JSON Schema, and raises on tool errors.

Start with the [CLI README](https://github.com/holon-sh/cli) for the walkthrough. This page is the Python API.

## Install

```sh
uv add --prerelease allow holon-sh
uv add --dev mypy==2.4.0     # holon types check runs mypy from the app's .venv
```

The `holon` CLI itself is a Go binary; if you would rather not install Go, it is also on PyPI as `holon-types`:

```sh
uv tool install --prerelease allow holon-types
```

The distribution is `holon-sh`; the import package is `holon`, a namespace package shared with your generated module. Do not create `holon/__init__.py` in your app, and do not edit `holon/apps/__init__.py`: the CLI rewrites it on every run.

## The generated module

```python
from holon.apps import Inputs, Outputs, apps, create_apps
```

| Name | What it is |
|---|---|
| `apps` | One client per dependency in `server.json` (`apps.orders`), bound through `<NAME>_MCP_URL`. |
| `create_apps(bindings)` | Clients bound explicitly, for only the dependencies you pass. |
| `Inputs.<dep>.<tool>` | The tool's argument type, a `TypedDict`. |
| `Outputs.<dep>.<tool>` | The tool's structured result type, a `TypedDict`. |

### Call a tool

```python
from holon.apps import apps

async def total(id: str) -> float:
    order = await apps.orders.tools.lookup({"id": id}, timeout=5.0)
    return order["total"]
```

Arguments are one mapping, validated before sending; the result is validated against the output schema and returned. `timeout` is in seconds.

### Handle errors

`tools.<tool>` raises `ToolCallError` when the tool reports an error or asks for more input:

```python
from holon.client import ToolCallError

try:
    order = await apps.orders.tools.lookup({"id": id})
except ToolCallError as error:
    error.code     # "tool_error" or "input_required"
    error.result   # the original MCP result
```

Invalid arguments or a result that does not match the schema raise `ValueError`; transport failures propagate from the MCP client.

### Read the whole result

`call_tool` returns the full MCP result instead of raising:

```python
result = await apps.orders.call_tool("lookup", {"id": id})
if result.is_error:
    ...                          # inspect result.content
else:
    result.structured_content    # typed as Outputs.orders.lookup
```

It also accepts `input_responses`, `request_state` and `meta`. A dependency the CLI could not obtain a contract for offers only `call_tool`, untyped.

## Bind a dependency

By default a client reads its endpoint from an environment variable named after the dependency, upper-cased, with `-` as `_`:

```sh
ORDERS_MCP_URL=http://127.0.0.1:3001/mcp uv run python server.py
```

For authentication or a custom HTTP client, bind explicitly. `http_client` is a factory called once per call; the client it returns is closed afterwards:

```python
import os
import httpx2
from holon.apps import create_apps

apps = create_apps({
    "orders": {
        "endpoint": "https://orders.example.com/mcp",
        "http_client": lambda: httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {os.environ['ORDERS_TOKEN']}"},
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
    "ui://checkout/index.html", "Checkout", "dist/index.html",
    connect_domains=["https://orders.example.com"],
))
```

The file is read on first request and cached. `connect_domains` and `resource_domains` become the view's content security policy. The page itself uses [`holon-ui`](https://github.com/holon-sh/sdk-ui).

## License

MIT
