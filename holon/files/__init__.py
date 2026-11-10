"""Files in MCP tool calls: the draft ``io.modelcontextprotocol/files`` extension.

Clients pass a :class:`File` where a tool takes a file and read what a tool
returns with :func:`downloads`. Servers accept file arguments through
:class:`FileServer`, staging uploads in a :class:`Storage` such as
:class:`LocalStorage`.
"""

from holon.files._client import (
    Download,
    File,
    FileTransferError,
    OutcomeUnknownError,
    downloads,
)
from holon.files._server import FileServer, LocalStorage, ReceivedFile, Storage
from holon.files._wire import (
    EXTENSION,
    DownloadTarget,
    FileInput,
    FileInputDescriptor,
    ResultFilesMeta,
    UploadForm,
    UploadRequiredResult,
    UploadTarget,
    file_input_schema,
)

__all__ = [
    "EXTENSION",
    "Download",
    "DownloadTarget",
    "File",
    "FileInput",
    "FileInputDescriptor",
    "FileServer",
    "FileTransferError",
    "LocalStorage",
    "OutcomeUnknownError",
    "ReceivedFile",
    "ResultFilesMeta",
    "Storage",
    "UploadForm",
    "UploadRequiredResult",
    "UploadTarget",
    "downloads",
    "file_input_schema",
]
