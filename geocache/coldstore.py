"""Local cold-archive source for the demo stack.

Serves the immutable Parquet files over HTTP with optional fault
injection so the API's slow-transfer and dropped-connection handling
can be demonstrated:

* CHUNK_BYTES      bytes per streamed chunk            (default 65536)
* CHUNK_DELAY_MS   delay after each chunk              (default 0)
* CUT_AFTER_BYTES  cut the connection after N bytes    (default 0 = off)
* CUT_TIMES        0 = cut every request; N>0 = cut only the first N
                   requests per file, then serve normally (default 0)

A cut declares the full Content-Length but closes the stream early,
which is exactly how a flaky cold archive fails mid-transfer.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
CHUNK_BYTES = int(os.environ.get("CHUNK_BYTES", "65536"))
CHUNK_DELAY_MS = float(os.environ.get("CHUNK_DELAY_MS", "0"))
CUT_AFTER_BYTES = int(os.environ.get("CUT_AFTER_BYTES", "0"))
CUT_TIMES = int(os.environ.get("CUT_TIMES", "0"))

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

app = FastAPI(title="coldstore", version="0.1.0")
_request_counts: dict[str, int] = defaultdict(int)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> dict:
    """Request counts per file (observability for single-flight demos)."""
    return {"requests": dict(_request_counts)}


@app.get("/files/{name}")
async def get_file(name: str) -> StreamingResponse:
    if not _SAFE_NAME.match(name) or ".." in name:
        raise HTTPException(404, "not found")
    path = DATA_DIR / name
    if not path.is_file():
        raise HTTPException(404, "not found")
    size = path.stat().st_size

    idx = _request_counts[name]
    _request_counts[name] += 1
    cut_at: int | None = None
    if CUT_AFTER_BYTES > 0 and (CUT_TIMES == 0 or idx < CUT_TIMES):
        cut_at = CUT_AFTER_BYTES

    async def stream():
        sent = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK_BYTES)
                if not chunk:
                    break
                if cut_at is not None and sent + len(chunk) > cut_at:
                    partial = chunk[: max(0, cut_at - sent)]
                    if partial:
                        yield partial
                    return  # drop the connection mid-transfer
                sent += len(chunk)
                yield chunk
                if CHUNK_DELAY_MS:
                    await asyncio.sleep(CHUNK_DELAY_MS / 1000.0)

    return StreamingResponse(
        stream(),
        media_type="application/octet-stream",
        headers={"Content-Length": str(size)},
    )
