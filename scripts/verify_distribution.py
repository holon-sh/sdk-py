"""Install each built distribution in an isolated environment and test it.

Dependency packages are copied from the invoking development environment to
avoid network access. The holon namespace is excluded so only the installed
artifact can satisfy the runtime imports.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import sysconfig
import tempfile
import venv
from pathlib import Path


def verify(root: Path) -> None:
    artifacts = sorted(root.glob("dist/holon_sh-*.whl")) + sorted(
        root.glob("dist/holon_sh-*.tar.gz")
    )
    if len(artifacts) != 2:
        raise RuntimeError("Build exactly one wheel and one sdist before verification.")
    for artifact in artifacts:
        with tempfile.TemporaryDirectory(prefix="holon-sh-consumer-") as temporary:
            directory = Path(temporary)
            environment = directory / "venv"
            venv.create(environment, with_pip=False)
            python = environment / (
                "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
            )
            installed = Path(
                subprocess.check_output(
                    [
                        str(python),
                        "-c",
                        "import sysconfig; print(sysconfig.get_path('purelib'))",
                    ],
                    text=True,
                ).strip()
            )
            for source in Path(sysconfig.get_path("purelib")).iterdir():
                if (
                    source.name.startswith(("holon", "_holon"))
                    or source.suffix == ".pth"
                ):
                    continue
                target = installed / source.name
                if source.is_dir():
                    shutil.copytree(source, target, dirs_exist_ok=True)
                else:
                    shutil.copy2(source, target)
            subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    "--no-deps",
                    "--no-build-isolation",
                    str(artifact),
                ],
                check=True,
            )
            subprocess.run(
                [
                    str(python),
                    "-m",
                    "pytest",
                    str(root / "tests/test_package.py"),
                    "-k",
                    "generated_namespace",
                ],
                cwd=directory,
                check=True,
            )


if __name__ == "__main__":
    verify(Path(__file__).resolve().parents[1])
