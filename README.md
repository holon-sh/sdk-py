# Holon Python SDK

[![CI](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/sdk-py/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-holonlabs.ai-blue)](https://holonlabs.ai/docs/)
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

## Serve an app

```sh
uv run python -m holon.serve server.py
```

The entry, here `server.py` (or, say, `src/server.py`), defines `server`, an `MCPServer`, and may define `frontend`, the directory of its page. The app listens on `HOST` and `PORT`, or else on 127.0.0.1:3000. Its MCP endpoint is at `/mcp` and `/mcp/<app>`, and every other GET serves a file from `frontend`.

## Reading the caller

```python
from holon.context import get_context


@server.tool()
def whoami() -> str:
    context = get_context()
    return context.principal.subject if context.principal else "anonymous"
```

`get_context()` returns who the current MCP request comes from: `principal`, the person signed in, and `caller`, the app of the same session that called. Inside Holon these come from the headers Holon verified. Run anywhere else, every request is anonymous. A dependency the app calls during a request is called for the same person.

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

## Community

Ask questions and share what you're building on the [Holon Discord](https://discord.gg/jXExyBdy).
