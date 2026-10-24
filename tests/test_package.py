"""Consumer contracts at the generated namespace and standalone tooling boundaries."""

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

from mcptype.compiler import generate

ROOT = Path(__file__).resolve().parents[1]


def test_generated_namespace_combines_app_types_with_installed_runtime(tmp_path):
    identity = {"name": "caller", "version": "1"}
    contract = {
        "tools": [
            {
                "name": "read",
                "inputSchema": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                    "required": ["id"],
                    "additionalProperties": False,
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {"total": {"type": "number"}},
                    "required": ["total"],
                },
            }
        ]
    }
    result = generate(
        {
            "target": "python",
            "identity": identity,
            "requires": {"orders": {"name": "orders", "version": "1"}},
            "contracts": {"orders": contract},
            "python_module": "holon/apps/__init__.py",
            "python_runtime_module": "holon.client",
            "banner": "holon-generated-apps-v1 app=caller",
        }
    )
    for name, content in result["files"].items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    assert not (tmp_path / "holon/__init__.py").exists()
    (tmp_path / "check.py").write_text("""from pathlib import Path
import holon
import holon.client
from holon.apps import apps, Inputs, Outputs
from holon.binding import Binding, set_default_binding

binding: Binding = {"endpoint": "https://example.com/mcp"}
set_default_binding(lambda name: binding)

async def run() -> float:
    arguments: Inputs.orders.read = {"id": "order-1"}
    output: Outputs.orders.read = await apps.orders.tools.read(arguments)
    return output["total"]

async def invalid() -> None:
    await apps.orders.tools.read({"id": 1})  # type: ignore
    await apps.orders.tools.read({})  # type: ignore
    apps.missing  # type: ignore

assert holon.__file__ is None
assert not (Path(__file__).parent / "holon/client.py").exists()
assert Path(holon.client.__file__).parent != Path(__file__).parent / "holon"
""")
    # Runtime and typing must agree across the two namespace portions. No
    # MYPYPATH/PYTHONPATH override is allowed to hide packaging problems.
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"MYPYPATH", "PYTHONPATH"}
    }
    for command in [
        [sys.executable, "-m", "mypy", "--strict", "--no-incremental", "check.py"],
        [sys.executable, "check.py"],
    ]:
        checked = subprocess.run(
            command, cwd=tmp_path, env=env, text=True, capture_output=True, check=False
        )
        assert checked.returncode == 0, checked.stdout + checked.stderr


def test_cli_bundle_extracts_without_runtime_and_detects_changed_sources(tmp_path):
    subprocess.run([sys.executable, "scripts/build_tooling.py"], cwd=ROOT, check=True)
    archive = ROOT / "dist/cli-tooling-py-0.1.0.zip"
    with zipfile.ZipFile(archive) as bundle:
        assert set(bundle.namelist()) == {"runner.py", "extract.py"}
        bundle.extractall(tmp_path / "tooling")
    app = tmp_path / "app"
    app.mkdir()
    source = app / "server.py"
    source.write_text("""from holon.apps import apps
from mcp.server import MCPServer
server = MCPServer("orders")
@server.tool()
def read(id: str) -> str:
    raise RuntimeError("must not execute")
""")
    generated = app / "holon/apps/__init__.py"
    generated.parent.mkdir(parents=True)
    generated.write_text("not even valid Python")
    config = tmp_path / "config.json"
    output = tmp_path / "output.json"
    config.write_text(
        json.dumps(
            {"apps": [{"name": "orders", "path": str(app), "entry": "server.py"}]}
        )
    )
    runner = [sys.executable, "-S", str(tmp_path / "tooling/runner.py")]
    subprocess.run([*runner, "extract", str(config), str(output)], check=True)
    result = json.loads(output.read_text())["orders"]
    assert result["usesApps"] is True
    assert [tool["name"] for tool in result["tools"]] == ["read"]
    assert result["sources"] == {
        "server.py": hashlib.sha256(source.read_bytes()).hexdigest()
    }
    config.write_text(
        json.dumps(
            {
                "sources": {str(source): result["sources"]["server.py"]},
                "check": True,
                "entry": str(source),
            }
        )
    )
    subprocess.run([*runner, "verify", str(config)], check=True)
    source.write_text(source.read_text() + "# changed\n")
    checked = subprocess.run(
        [*runner, "verify", str(config)], text=True, capture_output=True, check=False
    )
    assert checked.returncode != 0
    assert "sources changed after extraction" in checked.stderr
