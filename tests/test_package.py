"""Consumer contracts at the generated namespace boundary."""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def test_generated_namespace_combines_app_types_with_installed_runtime(tmp_path):
    # Refresh with scripts/fixtures.sh; generation is tested in holon-sh/cli.
    shutil.copytree(Path(__file__).parent / "fixtures/caller", tmp_path, dirs_exist_ok=True)
    (tmp_path / "check.py").write_text("""from pathlib import Path
import holon
import holon.client
from holon.apps import apps, Inputs, Outputs

async def run() -> float:
    arguments: Inputs.orders.read = {"id": "order-1"}
    output: Outputs.orders.read = await apps.orders.tools.read(arguments)
    return output["total"]

async def invalid() -> None:
    await apps.orders.tools.read({"id": 1})  # type: ignore
    await apps.orders.tools.read({})  # type: ignore
    apps.missing  # type: ignore

assert holon.__file__ is None
assert Path(holon.client.__file__).parent != Path(__file__).parent / "holon"
""")
    # Runtime and typing must agree across the two namespace portions. No
    # MYPYPATH/PYTHONPATH override is allowed to hide packaging problems.
    env = os.environ.copy()
    env.pop("MYPYPATH", None)
    env.pop("PYTHONPATH", None)
    for command in [
        [sys.executable, "-m", "mypy", "--strict", "--no-incremental", "check.py"],
        [sys.executable, "check.py"],
    ]:
        subprocess.run(command, cwd=tmp_path, env=env, check=True)
