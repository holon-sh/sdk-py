"""Generate a portable reference bundle from Sphinx's Markdown output."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "docs/_build/markdown"
OUT = ROOT / "docs/_build/bundle"


def main() -> None:
    os.chdir(ROOT)
    inventory = json.loads((ROOT / "docs/public-api.json").read_text())
    for module_name, expected in inventory.items():
        module = importlib.import_module(module_name)
        actual = getattr(
            module,
            "__all__",
            [
                name
                for name, value in vars(module).items()
                if not name.startswith("_")
                and getattr(value, "__module__", None) == module_name
            ],
        )
        if set(actual) != set(expected):
            raise ValueError(f"Public API inventory changed: {module_name}: {actual}")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-E",
            "-b",
            "markdown",
            "-W",
            "--keep-going",
            "docs",
            str(RAW),
        ],
        check=True,
    )
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    pages, symbols, targets = [], {}, set()
    for name in ["index", "client", "binding", "view"]:
        markdown = (RAW / f"{name}.md").read_text()
        headings, counts, pending = [], {}, None
        lines = []
        fence = None
        for line in markdown.splitlines():
            marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
            if marker:
                if fence is None:
                    fence = marker[1]
                elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                    fence = None
            if marker or fence:
                lines.append(line)
                continue
            anchor = re.fullmatch(r'<a id="([^"]+)"></a>', line)
            if anchor:
                pending = anchor[1]
                continue
            heading = re.match(r"^#{1,6} (.+)", line)
            if heading:
                slug = re.sub(r"[^a-z0-9]+", "-", heading[1].lower()).strip("-")
                count = counts.get(slug, 0)
                counts[slug] = count + 1
                key = pending or slug + (f"-{count}" if count else "")
                route = f"sdk/python/{name}/{key}"
                headings.append({"id": route, "title": heading[1]})
                targets.add(route)
                if pending and pending.startswith("holon."):
                    symbols[pending] = route
                pending = None
            lines.append(line)
        pages.append(
            {
                "id": f"sdk/python/{name}",
                "title": "Python SDK" if name == "index" else f"holon.{name}",
                "headings": headings,
                "markdown": "\n".join(lines) + "\n",
            }
        )
        targets.add(f"sdk/python/{name}")
    for page in pages:
        name = page["id"].split("/")[-1]

        def link(match, current=name):
            path, _, fragment = match[1].partition("#")
            if path and not path.endswith(".md"):
                return match[0]
            if ":" in path or path.startswith("/"):
                return match[0]
            destination = Path(path).stem if path else current
            route = f"sdk/python/{destination}" + (f"/{fragment}" if fragment else "")
            if route not in targets:
                raise ValueError(f"Unresolved reference: {match[1]}")
            return f"](#{route})"

        page["markdown"] = re.sub(r"\]\(([^)]+)\)", link, page["markdown"])
        source = f"holon/{name}.py" if name != "index" else "pyproject.toml"
        page["markdown"] += (
            f"\n[Source](https://github.com/holon-sh/sdk-py/blob/{sha}/{source})\n"
        )
    symbols["holon.client.Binding"] = symbols["holon.binding.Binding"]
    for module, names in inventory.items():
        for name in names:
            if f"{module}.{name}" not in symbols:
                raise ValueError(f"Missing reference for {module}.{name}")
    OUT.mkdir(parents=True, exist_ok=True)
    data = (
        json.dumps({"pages": pages, "symbols": symbols}, indent=2, ensure_ascii=False)
        + "\n"
    ).encode()
    (OUT / "reference.json").write_bytes(data)
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    source = {
        "repository": "holon-sh/sdk-py",
        "sha": sha,
        "ref": os.getenv("GITHUB_REF", "local"),
        "runId": os.getenv("GITHUB_RUN_ID", ""),
        "runNumber": int(os.getenv("GITHUB_RUN_NUMBER", "0")),
        "dirty": bool(subprocess.check_output(["git", "diff", "--name-only"])),
    }
    manifest = {
        "schemaVersion": 1,
        "language": "python",
        "package": "holon-sh",
        "version": version,
        "source": source,
        "files": {"reference.json": hashlib.sha256(data).hexdigest()},
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Built {len(pages)} pages, {len(symbols)} symbols: {OUT}")


if __name__ == "__main__":
    main()
