"""Build SDK reference pages; the website owns the documentation presentation."""

project = "Holon Python SDK"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_markdown_builder",
]
master_doc = "index"
exclude_patterns = ["_build"]
autodoc_typehints = "signature"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False
markdown_anchor_sections = True
markdown_anchor_signatures = True
markdown_http_base = ""
markdown_uri_doc_suffix = ".md"
nitpicky = True
# Third-party protocol types remain in signatures; this reference owns only holon.
nitpick_ignore_regex = [("py:.*", r"(?!holon\.).*")]
nitpick_ignore = [("py:obj", "holon.client.Output"), ("py:class", "holon.client.Output")]


def setup(app):
    # The Markdown builder does not yet handle Sphinx's keyword-only '*' node.
    from docutils import nodes

    app.add_node(
        nodes.abbreviation,
        override=True,
        markdown=(lambda self, node: None, lambda self, node: None),
    )
