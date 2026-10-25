Python SDK
==========

The ``holon-sh`` package provides typed MCP clients, connection bindings, and
server-side HTML views. Requires Python 3.11 or later.

These pages describe the standalone SDK. The platform tutorials currently use
their separately pinned platform runtime.

.. toctree::
   :maxdepth: 2

   client
   binding
   view

Generated clients
-----------------

``holon.apps`` is generated for the consuming project; it is not a module shipped
in the SDK wheel. Generation supplies ``apps``, ``create_apps``, and the project's
input/output types. This reference describes those conventions without publishing
application-specific tool catalogs.

The generator is a separate executable: installing ``holon-sh`` does not install
it. Install the standalone ``holon-cli`` wheel as a development dependency, then
run these commands from the directory containing your project manifest:

.. code-block:: sh

   uv run holon types generate --no-install
   uv run holon check --no-published

``holon types`` alone shows help. The CLI packages are currently a development
preview requiring a local wheel or binary; the guide below explains how to obtain
one, install it with uv, declare dependencies, and find the generated files.

See the `client generation guide <https://holon-labs.dev/docs/#generate-clients>`_
and `bindings guide <https://holon-labs.dev/docs/#bind-sdk-clients>`_.
