# holon-sh

Public Python runtime for typed MCP clients.
This repository prepares release artifacts. Its CI does not publish them; dev
builds are published only by a manually dispatched workflow (see
[Dev releases](#dev-releases)).

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

## Dev releases

`.github/workflows/publish-dev.yml` publishes `holon-sh==0.0.1.dev<run number>`
to PyPI. It runs only when dispatched by hand, rewrites the version in
`pyproject.toml` inside the job without committing it, runs the test suite and
build, and prints the exact install line in the job summary. `main` stays at
`0.0.1`.

One-time setup on pypi.org: add a pending (trusted) publisher for the project
`holon-sh` with owner `holon-sh`, repository `sdk-py`, workflow
`publish-dev.yml`, and no environment. Alternatively store an API token as the
`PYPI_TOKEN` repository secret; the workflow uses it when present and OIDC
trusted publishing otherwise.

```sh
gh workflow run publish-dev.yml
```

Consumers install a dev build with `pip install --pre holon-sh` or the exact
version printed in the job summary, e.g. `uv add 'holon-sh==0.0.1.dev12'`.
