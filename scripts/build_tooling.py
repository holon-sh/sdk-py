"""Build the deterministic Python tooling archive consumed by the Holon CLI."""

from __future__ import annotations

import hashlib
import tomllib
import zipfile
from pathlib import Path


def build(root: Path) -> Path:
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    output = root / "dist"
    output.mkdir(exist_ok=True)
    archive = output / f"cli-tooling-py-{version}.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, source in [
            ("extract.py", "holon/extract.py"),
            ("runner.py", "runner.py"),
        ]:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            bundle.writestr(info, (root / source).read_bytes())
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (output / "SHA256SUMS").write_text(f"{digest}  {archive.name}\n")
    return archive


if __name__ == "__main__":
    print(build(Path(__file__).resolve().parents[1]))
