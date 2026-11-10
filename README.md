# Holon Python SDK

[![CI](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-holon--labs.dev-blue)](https://holon-labs.dev/docs/)
[![PyPI](https://img.shields.io/pypi/v/holon-sh)](https://pypi.org/project/holon-sh/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white)](https://discord.gg/jXExyBdy)

This repository contains the Python runtime for [`holon`](https://github.com/holon-sh/cli).

## Install

```sh
uv add --prerelease allow holon-sh
uv add --dev holon-types
```

## Create an app

```sh
uvx create-holon-app foo
```

## Invoking MCP Servers

Declare dependencies under `sh.holon.requires` in a [`server.json`](https://modelcontextprotocol.io/registry/quickstart#step-4-create-server-json) beside the server, here the public [Hugging Face](https://huggingface.co/docs/hub/en/hf-mcp-server) MCP server:

```json
{
  "name": "com.example/foo",
  "version": "0.1.0",
  "description": "Example Foo",
  "_meta": {
    "io.modelcontextprotocol.registry/publisher-provided": {
      "sh.holon": {
        "requires": {
          "huggingface": { "url": "https://huggingface.co/mcp" }
        }
      }
    }
  }
}
```

### Type-checking MCP invocations

```sh
uv run holon types check
```

A wrong tool name or argument is a type error:

```python
from holon.apps import apps

await apps.huggingface.tools.hub_repo_search(query="llama")

await apps.huggingface.tools.hub_repo_search(limit="3")
# error: Argument "limit" to "hub_repo_search" of "HuggingfaceTools" has incompatible type "str"; expected "float"  [arg-type]

await apps.huggingface.tools.hub_repo_details()
# error: Missing named argument "repo_ids" for "hub_repo_details" of "HuggingfaceTools"  [call-arg]

await apps.huggingface.tools.hub_search(query="llama")
# error: "HuggingfaceTools" has no attribute "hub_search"; maybe "hub_repo_search"?  [attr-defined]
```

### Files

Where a tool takes a file (the draft [files extension](https://github.com/modelcontextprotocol/experimental-ext-file-uploads)), pass a `File`. Small files travel inline; larger ones are uploaded where the server asks before the tool runs. Read the files a tool returns with `downloads`:

```python
from holon.files import File, downloads

await apps.storage.tools.save(folder="reports", file=File("q3.pdf"))

result = await apps.storage.call_tool("export", folder="reports")
for download in downloads(result):
    await download.save("exports")
```

A binding's `file_origins` limits where uploads and downloads may go. Servers accept file arguments with `holon.files.FileServer`.

## Events

`holon.events` serves [MCP Events](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md), the Triggers & Events Working Group's draft, by poll and push. Declare an event type once and emit:

```python
from pydantic import BaseModel
from mcp.server import MCPServer
from starlette.applications import Starlette
from starlette.routing import Mount
from holon.events import Events


class Board(BaseModel):
    board: str


class Change(BaseModel):
    board: str
    text: str


events = Events()
changed = events.event(
    "board.changed",
    input_schema=Board,
    payload_schema=Change,
    match=lambda context, event, arguments: event.data.board == arguments.board,
)
server = MCPServer("boards", extensions=[events])
server.streamable_http_app(stateless_http=True)
app = Starlette(
    routes=[Mount("/mcp", app=events.asgi(server.session_manager))],
    lifespan=lambda app: server.session_manager.run(),
)

changed.emit(Change(board="b1", text="hello"))
```

Push is offered over HTTP only where `events.asgi(manager)` is mounted in place of `manager.handle_request`, whether the manager answers by stream or in JSON; elsewhere clients poll. Emitted events are kept in memory for polls and streams that are behind. A type whose source keeps its own history takes a `check(context, arguments, cursor)` function instead. `authorize`, `on_subscribe`, `key`, `end`, `remove` and limits on what is kept and held are there when you need them.

For a snapshot and then its changes, a tool returns the state with `changed.cursor()`, read in the same synchronous run, and the client subscribes from that cursor. Several kinds of change in one ordered stream are one event type whose payload is a union, and events emitted one after another reach each subscriber in that order.

Every app client lists and subscribes, by push when the app offers it and by poll otherwise, reconnecting from its cursor. Leaving the `async with` block is what ends a subscription; breaking out of `async for` alone does not:

```python
async with apps.boards.events.subscribe("board.changed", {"board": "b1"}) as changes:
    async for event in changes:
        print(event.data, changes.cursor)
```

## Community

Ask questions and share what you're building on the [Holon Discord](https://discord.gg/jXExyBdy).
