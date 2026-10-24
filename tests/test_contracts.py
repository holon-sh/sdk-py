import asyncio
import importlib.util
import json
import sys

import pytest

from holon.extract import extract


def normalized(schema):
    return json.loads(
        json.dumps(schema),
        object_hook=lambda item: {
            key: value
            for key, value in item.items()
            if key not in {"title", "description"}
        },
    )


@pytest.mark.parametrize(
    "definition",
    [
        "if True:\n    @server.tool()\n    def read(id: str) -> str: return id",
        '@server.tool(name=os.getenv("NAME"))\ndef read(id: str) -> str: return id',
        "@server.tool()\ndef read(id: str = factory()) -> str: return id",
        'NAME = "before"\n@server.tool(name=NAME)\ndef read(id: str) -> str: return id\nNAME = "after"',
        'NAME = "before"\nNAME += "after"\n@server.tool(name=NAME)\ndef read(id: str) -> str: return id',
        'other = MCPServer("other")\n@other.tool()\ndef read(id: str) -> str: return id',
        'getattr(server, "add_tool")(unknown)',
        "def register():\n    @server.tool()\n    def read(id: str) -> str: return id\nregister()",
        'class Value(BaseModel):\n    total: int\nif True:\n    Value.model_fields["total"] = unknown\n@server.tool()\ndef read(id: str) -> Value: raise RuntimeError()',
        'class Value(BaseModel):\n    total: int\n    @field_validator("total")\n    def convert(cls, value): return value\n@server.tool()\ndef read(id: str) -> Value: raise RuntimeError()',
    ],
)
def test_unprovable_contracts_fail_visibly(tmp_path, definition):
    (tmp_path / "server.py").write_text(
        'from mcp.server import MCPServer\nfrom pydantic import BaseModel, field_validator\nserver=MCPServer("test")\n'
        + definition
    )
    result = extract(tmp_path, "server.py")
    assert result["tools"] == []
    assert "server.py:" in result["diagnostic"]


@pytest.mark.parametrize(
    "construction",
    [
        'MCPServer("test", tools=TOOLS)',
        'Server("test", on_list_tools=list_tools)',
        "MCPServer(**OPTIONS)",
    ],
)
def test_constructor_catalogs_fail_visibly(tmp_path, construction):
    (tmp_path / "server.py").write_text(
        f"from mcp.server import MCPServer, Server\nserver = {construction}\n"
    )
    result = extract(tmp_path, "server.py")
    assert result["tools"] == []
    assert result["diagnostic"].startswith("server.py:2:10: Register tools only")


def test_fixed_catalog_uses_json_schema(tmp_path):
    (tmp_path / "server.py").write_text("""from mcp.server import Server
from mcp.types import Tool
INPUT = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
server = Server("test")
@server.list_tools()
async def tools():
    return [Tool(name="read", input_schema=INPUT, output_schema={"type": "object"})]
""")
    result = extract(tmp_path, "server.py")
    assert "diagnostic" not in result, result
    assert result["tools"][0]["inputSchema"]["required"] == ["id"]


@pytest.mark.parametrize(
    "imports, typed",
    [
        ("from holon.binding import set_default_binding", False),
        ("from holon.apps import apps as clients", True),
        ("from holon.apps import Inputs, Outputs", True),
        ("from holon.apps import InputTypes, OutputTypes", True),
        ("import holon.apps", True),
        ("import holon.apps as h\nclients = h.apps", True),
        ("import holon.client as h\nclient = h.Apps", False),
    ],
)
def test_sdk_imports_only_record_typed_dependency_usage_when_needed(
    tmp_path, imports, typed
):
    (tmp_path / "server.py").write_text(
        imports + '\nfrom mcp.server import MCPServer\nserver = MCPServer("app")\n'
    )
    result = extract(tmp_path, "server.py")
    assert "diagnostic" not in result, result
    assert result["usesApps"] is typed


@pytest.mark.parametrize(
    "annotation",
    [
        "Order",
        "list[str]",
        "dict[str, float]",
        "Dict[str, float]",
        "Annotated[CallToolResult, Order]",
        "Order | InputRequiredResult",
    ],
)
def test_static_contract_matches_official_catalog_without_running_sources(
    tmp_path, monkeypatch, annotation
):
    (tmp_path / "models.py").write_text(
        "from pydantic import BaseModel\nclass Line(BaseModel):\n    name: str\nclass Order(Line):\n    total: float\n    lines: list[Line]\n"
    )
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "__init__.py").write_text("")
    (tmp_path / "tools" / "lines.py").write_text(
        "from models import Line\nasync def add_line(name: str) -> Line:\n    raise RuntimeError('handler not called')\n"
    )
    (tmp_path / "server.py").write_text(f"""from typing import Annotated, Dict
from pathlib import Path
from mcp.server import MCPServer
from mcp.types import CallToolResult, InputRequiredResult
from pydantic import Field
from models import Order
from tools import lines
Path(__file__).with_name("executed").write_text("")
server = MCPServer("orders")
server.add_tool(lines.add_line)
@server.tool()
async def read(id: Annotated[str, Field(pattern="^ord_")], limit: int = 5) -> {annotation}:
    raise RuntimeError("handler not called")
@server.tool()
async def unknown(id: str):
    return {{"guessed": "never"}}
""")
    extracted = extract(tmp_path, "server.py")
    assert "diagnostic" not in extracted, extracted
    assert set(extracted["sources"]) == {
        "server.py",
        "models.py",
        "tools/__init__.py",
        "tools/lines.py",
    }
    assert not (tmp_path / "executed").exists()
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "models", raising=False)
    spec = importlib.util.spec_from_file_location(
        "official_fixture", tmp_path / "server.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    catalog = asyncio.run(module.server.list_tools())
    for tool, official in zip(extracted["tools"], catalog, strict=True):
        assert normalized(tool["inputSchema"]) == normalized(official.input_schema)
        assert normalized(tool.get("outputSchema")) == normalized(
            official.output_schema
        )


def test_json_resources_derive_contents_schemas_like_tool_outputs(tmp_path):
    (tmp_path / "server.py").write_text("""from typing import TypedDict
from mcp.server import MCPServer
from mcp.server.apps import APP_MIME_TYPE
server = MCPServer("chat")
class Message(TypedDict):
    id: str
class Snapshot(TypedDict):
    messages: list[Message]
messages_uri = "chat://messages"
@server.tool()
def snapshot() -> Snapshot:
    return {"messages": []}
@server.resource(messages_uri, name="Messages", mime_type="application/json")
def read_messages() -> Snapshot:
    return {"messages": []}
@server.resource("chat://readme", mime_type="text/markdown")
def readme() -> str:
    return "# Chat"
@server.resource("chat://untyped", mime_type="application/json")
def untyped():
    return {}
@server.resource("ui://chat/app.html", mime_type=APP_MIME_TYPE, meta={"ui": {}})
def app() -> str:
    return "<html></html>"
""")
    extracted = extract(tmp_path, "server.py")
    assert "diagnostic" not in extracted, extracted
    assert extracted["resources"] == [
        {
            "uri": "chat://messages",
            "outputSchema": extracted["tools"][0]["outputSchema"],
        }
    ]
