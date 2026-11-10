"""Files in MCP tool calls: the draft ``io.modelcontextprotocol/files`` extension.

Clients pass a :class:`File` where a tool takes a file and read what a tool
returns with :func:`downloads`. Servers accept file arguments through
:class:`FileServer`, staging uploads in a :class:`Storage` such as
:class:`LocalStorage`.
"""

from holon.ext.files._client import (
    Download,
    File,
    FileTransferError,
    OutcomeUnknownError,
    downloads,
)
from holon.ext.files._server import FileServer, LocalStorage, ReceivedFile, Storage
from holon.ext.files._wire import (
    EXTENSION,
    DownloadTarget,
    UploadTarget,
    file_input_schema,
)

__all__ = [
    "EXTENSION",
    "Download",
    "DownloadTarget",
    "File",
    "FileServer",
    "FileTransferError",
    "LocalStorage",
    "OutcomeUnknownError",
    "ReceivedFile",
    "Storage",
    "UploadTarget",
    "downloads",
    "file_input_schema",
]
