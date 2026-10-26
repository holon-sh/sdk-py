"""Consumer contracts at the generated namespace boundary."""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def test_generated_namespace_combines_app_types_with_installed_runtime(tmp_path):
    # Refresh with scripts/fixtures.sh; generation is tested in holon-sh/cli.
    shutil.copytree(
        Path(__file__).parent / "fixtures/caller", tmp_path, dirs_exist_ok=True
    )
    (tmp_path / "check.py").write_text("""from pathlib import Path
import holon
import holon.client
from holon.apps import apps, Inputs, Outputs

async def run() -> float:
    arguments: Inputs.orders.read = {
        "id": "order-1", "limit": 2, "name": "n", "timeout": "t", "options": "o",
        "self": "s", "arguments": "a", "class": "c", "order-id": "id",
    }
    output: Outputs.orders.read = await apps.orders.tools.read(**arguments)
    await apps.orders.call_tool("read", {"timeout": 5}, **arguments)
    await apps.orders.call_tool("get-order", id="order-1")
    await apps.orders.tools.ping()
    await apps.orders.tools.counts(apples=2, oranges=3)
    await apps.orders.tools.find(id="order-1")
    await apps.orders.call_tool("find", email="user@example.com")
    return output["total"]

async def invalid() -> None:
    await apps.orders.tools.read(id=1)  # type: ignore
    await apps.orders.tools.read()  # type: ignore
    await apps.orders.call_tool("read", id=1)  # type: ignore
    await apps.orders.call_tool("read")  # type: ignore
    await apps.orders.tools.read({"id": "old form"})  # type: ignore
    await apps.orders.tools.ping(unexpected=True)  # type: ignore
    await apps.orders.tools.counts(apples="two")  # type: ignore
    await apps.orders.tools.find()  # type: ignore
    await apps.orders.call_tool("find", id=1)  # type: ignore
    await apps.orders.tools.find(id="x", email="user@example.com")  # type: ignore
    apps.missing  # type: ignore
    apps.orders.missing

assert holon.__file__ is None
assert Path(holon.client.__file__).parent != Path(__file__).parent / "holon"
""")
    # Runtime and typing must agree across the two namespace portions. No
    # MYPYPATH/PYTHONPATH override is allowed to hide packaging problems.
    env = os.environ.copy()
    env.pop("MYPYPATH", None)
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "--no-error-summary", "check.py"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert len(result.stdout.splitlines()) == 1, result.stdout
    assert '"OrdersApp" has no attribute "missing"' in result.stdout
    subprocess.run([sys.executable, "check.py"], cwd=tmp_path, env=env, check=True)
