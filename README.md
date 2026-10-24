# holon-sh

Public Python runtime for typed MCP clients.
This repository prepares release artifacts; it does not publish them.

Install the `holon-sh` distribution and import its modules from the `holon`
namespace:

```python
from holon.binding import Binding, set_default_binding
from holon.client import Apps, ToolCallError
from holon.view import app_view
```

`holon` is a PEP 420 namespace package. It intentionally has no `__init__.py`.
`holon types generate` writes your app's contracts to
`holon/apps/__init__.py` beside `server.py`. Application code then imports:

```python
from holon.apps import apps, Inputs, Outputs

async def read_order(order_id: str) -> Outputs.orders.read:
    arguments: Inputs.orders.read = {"id": order_id}
    return await apps.orders.tools.read(arguments)
```

The generated namespace portion and installed runtime are resolved together.
Do not add `holon/__init__.py` to your application or copy runtime modules into
the application. The distribution does not include generated app contracts.

Clients use an explicit binding, the resolver registered with
`set_default_binding`, or the `<NAME>_MCP_URL` environment variable (uppercase,
with hyphens replaced by underscores). A binding can supply an `http_client`
factory for authentication and transport customization. No Holon host is
required. Runtime input/output validation is performed against the extracted
JSON schemas. `ToolCallError` exposes the original MCP result when a typed
tool call fails or requires additional input.

`app_view` serves a built HTML file as an MCP app resource, with optional
`connect_domains` and `resource_domains` CSP settings. It reads and caches the
file on the first resource request.

## Development

```sh
uv sync
uv run pytest
uv run mypy --strict -p holon.client -p holon.binding -p holon.view
uv build
```

The `dev` dependency group pins `mcptype==0.0.6` and `mypy==2.4.0`, which the
package tests use to generate and type-check a sample `holon/apps` namespace.
Before mcptype 0.0.6 is published, supply its locally built wheel with
`uv sync --find-links /path/to/mcptype/wheels`. Do not commit platform bindings
here.

Contract extraction and type checking belong to the `holon` CLI, which carries
its own tooling; this distribution ships only the runtime.
