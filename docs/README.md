# Building the Python reference

Install the SDK and its runtime dependencies, then the pinned documentation tools:

```sh
uv pip install -r docs/requirements.txt
uv run --no-sync python scripts/build_reference.py
```

The build uses Sphinx autodoc, autosummary, napoleon, and sphinx-markdown-builder.
Write Google-style docstrings on public objects. `public-api.json` records the
supported module surface; adding or removing an export requires updating it and
its Sphinx page. The build fails on missing public symbols and unresolved links.

`docs/_build/bundle/` contains Markdown page content and a symbol index in
`reference.json`, plus checksums and source provenance in `manifest.json`.
CI uploads `sdk-reference-python` for the website to import and render.
