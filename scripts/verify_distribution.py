"""Test each built distribution in an isolated uv environment using cached packages."""

import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

root = Path(__file__).resolve().parents[1]
artifacts = sorted(root.glob("dist/holon_sh-*.whl")) + sorted(
    root.glob("dist/holon_sh-*.tar.gz")
)
if len(artifacts) != 2:
    raise RuntimeError("Build exactly one wheel and one sdist before verification.")
for artifact in artifacts:
    subprocess.run(
        [
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--no-config",
            "--offline",
            "--python",
            sys.executable,
            "--with",
            str(artifact),
            "--with",
            f"pytest=={version('pytest')}",
            "--with",
            f"mypy=={version('mypy')}",
            "python",
            "-m",
            "pytest",
            str(root / "tests/test_package.py"),
        ],
        cwd=root / "dist",
        check=True,
    )
