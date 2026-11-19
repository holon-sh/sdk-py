import asyncio
import socket
from contextlib import asynccontextmanager

import uvicorn


@asynccontextmanager
async def listening(app):
    """Serves ``app`` on a loopback port, for url targets; yields its origin."""
    sock = socket.create_server(("127.0.0.1", 0))
    http = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning"))
    serving = asyncio.create_task(http.serve(sockets=[sock]))
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        http.should_exit = True
        await serving
