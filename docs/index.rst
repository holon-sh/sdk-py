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

Client generation writes ``holon.apps`` for your project, supplying ``apps``,
``create_apps``, and its input/output types.

Install the ``holon-cli`` wheel as a development dependency, then run from the
directory containing your project manifest:

.. code-block:: sh

   uv run holon types generate --no-install
   uv run holon check --no-published

The CLI is a development preview distributed as a local wheel or binary.
The generation guide covers setup, dependencies, and generated files.

See the `client generation guide <https://holon-labs.dev/docs/#generate-clients>`_
and `bindings guide <https://holon-labs.dev/docs/#bind-sdk-clients>`_.
