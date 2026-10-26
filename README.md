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

## Example

Declare dependencies under `sh.holon.requires` in your project's [`server.json`](https://modelcontextprotocol.io/registry/quickstart#step-4-create-server-json) ([publisher-provided metadata](https://modelcontextprotocol.io/registry/faq#can-i-add-custom-metadata-when-publishing)). This example uses the public [Context7](https://registry.modelcontextprotocol.io/v0.1/servers/io.github.upstash%2Fcontext7/versions/4.4.4) server.

```json
{
  "name": "com.example/foo",
  "version": "0.1.0",
  "description": "Example Foo",
  "_meta": {
    "io.modelcontextprotocol.registry/publisher-provided": {
      "sh.holon": {
        "requires": {
          "context7": { "name": "io.github.upstash/context7", "version": "4.4.4" }
        }
      }
    }
  }
}
```

Generate `holon/apps/__init__.py` and type-check the calls to it.

```sh
holon types check
```

Or name the servers yourself. Any `-s` replaces the whole `requires` list, so `server.json` is not needed.

```sh
holon types check -s https://mcp.context7.com/mcp
```

Call the server. A wrong tool name or argument is a type error.

```python
from holon.apps import apps

await apps.context7.call_tool("resolve-library-id", libraryName="Next.js", query="app router")

await apps.context7.call_tool("resolve-library-id", libraryName=1, query="app router")
# error: No overload variant of "call_tool" of "Context7App" matches argument types "str", "int", "str"  [call-overload]

await apps.context7.call_tool("resolve-library", libraryName="Next.js", query="app router")
# error: No overload variant of "call_tool" of "Context7App" matches argument types "str", "str", "str"  [call-overload]
```

Context7's tool names contain hyphens, so they are called through `call_tool`; `tools.<name>` holds the tools whose names are Python identifiers. Context7 publishes no output schema, so its results are `object`.

### Development Servers

Let's say you have a server that you're still developing, and it's not in any [MCP Registry](https://modelcontextprotocol.io/registry/about) yet.

```python
# bar/server.py
from mcp.server import MCPServer

server = MCPServer("bar")


@server.tool()
def greet(name: str) -> str:
    return f"hello {name}"
```

Give it a `server.json` beside the entry file.

```json
{
  "name": "com.example/bar",
  "version": "0.1.0",
  "description": "Example Bar"
}
```

Pass the entry file with `-s`. A path inside the project directory is the project's own server, so run from the parent directory and name the project.

```sh
holon types check foo -s bar/server.py
```

```python
from holon.apps import apps

greeting = await apps.bar.tools.greet(name="world")
greeting["result"]

await apps.bar.tools.greet(name=1)
# error: Argument "name" to "greet" of "BarTools" has incompatible type "int"; expected "str"  [arg-type]

await apps.bar.tools.hello(name="world")
# error: "BarTools" has no attribute "hello"  [attr-defined]

greeting["text"]
# error: TypedDict "BarGreetOutput" has no key "text"  [typeddict-item]
```

## Dependencies

To understand how dependencies work, see [Dependencies](https://github.com/holon-sh/cli#dependencies) in the CLI README.

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

## Development

The package test uses committed clients in `tests/fixtures/caller/holon/apps`. To refresh them, install the [holon CLI](https://github.com/holon-sh/cli) and run:

```sh
./scripts/fixtures.sh
```

This runs `holon types generate -s ../orders` from the caller fixture. The CLI reads `tests/fixtures/orders/server.py` without starting the server.

## License

MIT
