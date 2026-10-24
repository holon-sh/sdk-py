from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

# In the tooling bundle, extract.py (holon/extract.py here) sits beside this file.
from extract import extract  # type: ignore[import-not-found,unused-ignore]


def check(config: dict[str, Any], verify_only: bool) -> None:
    for path, expected in config["sources"].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
            raise ValueError(
                f"{path}: application sources changed after extraction; regenerate types before checking."
            )
    if verify_only or not config["check"]:
        return
    if importlib.util.find_spec("mypy") is not None:
        command = [sys.executable, "-m", "mypy"]
    else:
        raise SystemExit(
            f"mypy is not installed for {sys.executable}; install mypy==2.4.0 in the app's environment."
        )
    result = subprocess.run(
        [*command, "--no-incremental", "--check-untyped-defs", config["entry"]],
        check=False,
    )
    if result.returncode:
        raise SystemExit(result.returncode)


MODES: dict[str, Callable[[dict[str, Any]], object]] = {
    "extract": lambda config: {
        app["name"]: extract(app["path"], app["entry"]) for app in config["apps"]
    },
    "check": lambda config: check(config, False),
    "verify": lambda config: check(config, True),
}


def main() -> None:
    mode, path, *output = sys.argv[1:]
    result = MODES[mode](json.loads(Path(path).read_text()))
    if output:
        Path(output[0]).write_text(json.dumps(result))


if __name__ == "__main__":
    main()
