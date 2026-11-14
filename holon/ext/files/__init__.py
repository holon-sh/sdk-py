"""Files in MCP tool calls.

Clients pass a :class:`File` and read returned files with :func:`downloads`. Servers accept files with
:class:`FileServer` and a :class:`Storage` such as :class:`LocalStorage`; a tool declares each file argument a
:class:`ReceivedFile`.

https://github.com/modelcontextprotocol/experimental-ext-file-uploads/blob/main/specification/draft/file-uploads.md
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
