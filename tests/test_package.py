"""Consumer contracts at the generated namespace boundary."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

# tests/fixtures/caller is produced by scripts/fixtures.sh with `holon types compile`
# from caller.request.json (the same compiler the CLI uses to fill holon/apps/).
FIXTURES = Path(__file__).parent / "fixtures"


def test_generated_namespace_combines_app_types_with_installed_runtime(tmp_path):
    shutil.copytree(FIXTURES / "caller", tmp_path, dirs_exist_ok=True)
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
