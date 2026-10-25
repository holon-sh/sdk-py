holon.client
============

.. autoclass:: holon.client.Apps
   :members:

.. autoclass:: holon.client.ToolCallError
   :members:

.. autodata:: holon.client.ToolResult

.. autoclass:: holon.client.CompleteResult
   :members:
   :undoc-members:

.. autoclass:: holon.client.ErrorResult
   :members:
   :undoc-members:

Returned app clients
--------------------

An ``Apps`` lookup returns a client with ``tools`` and ``call_tool``. The runtime
class name is internal; the following method is part of the usable client surface.

.. automethod:: holon.client._App.call_tool

``client.tools.<tool_name>(arguments, timeout=None)`` returns structured output.
Only contracted tools have convenience methods. For continuation handling, use
``call_tool`` and inspect ``result_type``, ``request_state``, and ``input_requests``.

Python currently exposes tool calls, not resource read/watch methods.

See `error handling <https://holon-labs.dev/docs/#handle-tool-results>`_.
