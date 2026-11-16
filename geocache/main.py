"""FastAPI single-instance API: query arsenic by borehole + depth interval.

GET /query?borehole=BH-001&depth_min=20&depth_max=120 streams NDJSON:
one JSON object per row, then exactly one terminal line
{"status": "complete", ...} or {"status": "failed", ...}. A client that
has not seen a "complete" terminal line must treat the result as
incomplete.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .cache import CacheError, CacheManager, FetchFn
from .fetcher import HttpFetcher
from .manifest import Manifest, load_manifest
from .parquet_ops import filter_lines

log = logging.getLogger("geocache.api")


@dataclass
class Settings:
    manifest_path: str = "/data/manifest.json"
    cache_dir: str = "/cache"
    cache_capacity_bytes: int = 0  # 0 -> auto: 2x largest file
    fetch_attempts: int = 2
    http_timeout: float = 30.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            manifest_path=os.environ.get("MANIFEST_PATH", "/data/manifest.json"),
            cache_dir=os.environ.get("CACHE_DIR", "/cache"),
            cache_capacity_bytes=int(os.environ.get("CACHE_CAPACITY_BYTES", "0")),
            fetch_attempts=int(os.environ.get("FETCH_ATTEMPTS", "2")),
            http_timeout=float(os.environ.get("HTTP_TIMEOUT_SECONDS", "30")),
        )


def _terminal(status: str, **kw) -> str:
    return json.dumps({"status": status, **kw}, separators=(",", ":")) + "\n"


def create_app(
    settings: Settings | None = None,
    manifest: Manifest | None = None,
    fetcher_factory: Callable[[httpx.AsyncClient], FetchFn] | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.manifest = manifest or load_manifest(settings.manifest_path)
        capacity = settings.cache_capacity_bytes
        if capacity <= 0:
            capacity = 2 * app.state.manifest.max_file_bytes
        client = httpx.AsyncClient(timeout=settings.http_timeout)
        fetch = fetcher_factory(client) if fetcher_factory else HttpFetcher(client, settings.fetch_attempts)
        app.state.cache = CacheManager(settings.cache_dir, capacity, app.state.manifest, fetch)
        await app.state.cache.startup_scan()
        log.info("cache capacity %d bytes at %s", capacity, settings.cache_dir)
        try:
            yield
        finally:
            await app.state.cache.close()
            await client.aclose()

    app = FastAPI(
        title="geocache",
        version="0.1.0",
        description=(
            "Query arsenic measurements from immutable cold-archive Parquet files "
            "by borehole and half-open depth interval [depth_min, depth_max). "
            "Responses are NDJSON; the last line is a terminal marker "
            "('complete' or 'failed')."
        ),
        lifespan=lifespan,
    )

    @app.exception_handler(CacheError)
    async def _cache_error_handler(_req: Request, exc: CacheError) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": str(exc)})

    @app.get("/")
    async def root() -> dict:
        return {"service": "geocache", "docs": "/docs", "manifest": "/manifest", "cache": "/cache/stats"}

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/manifest")
    async def get_manifest(request: Request) -> dict:
        m: Manifest = request.app.state.manifest
        return {"files": [f.model_dump() for f in m.files]}

    @app.get("/cache/stats")
    async def cache_stats(request: Request) -> dict:
        return await request.app.state.cache.stats()

    @app.get("/query")
    async def query(request: Request, borehole: str, depth_min: float, depth_max: float) -> StreamingResponse:
        m: Manifest = request.app.state.manifest
        if not borehole:
            raise HTTPException(400, "borehole must not be empty")
        if not depth_max > depth_min:
            raise HTTPException(400, "require depth_min < depth_max (half-open interval)")
        if not m.known_borehole(borehole):
            raise HTTPException(404, f"unknown borehole: {borehole}")
        selected = m.select(borehole, depth_min, depth_max)
        cache: CacheManager = request.app.state.cache

        async def gen() -> AsyncIterator[str]:
            total = 0
            done_files: list[str] = []
            try:
                for meta in selected:
                    try:
                        entry = await cache.acquire(meta.id)
                    except CacheError as exc:
                        yield _terminal("failed", error=str(exc), file=meta.id, rows=total)
                        return
                    try:
                        lines = await asyncio.to_thread(
                            filter_lines, entry.path, borehole, depth_min, depth_max
                        )
                    except Exception as exc:  # noqa: BLE001 - reported as terminal line
                        yield _terminal("failed", error=f"read failed on {meta.id}: {exc}", file=meta.id, rows=total)
                        return
                    finally:
                        await cache.release(meta.id)
                    for line in lines:
                        total += 1
                        yield line + "\n"
                    done_files.append(meta.id)
                yield _terminal("complete", rows=total, files=done_files)
            except asyncio.CancelledError:
                # Client disconnected. In-flight shared downloads keep
                # running in their own tasks; nothing to clean up here
                # beyond what the finally blocks already did.
                raise
            except Exception as exc:  # noqa: BLE001 - last-resort terminal line
                yield _terminal("failed", error=f"internal: {type(exc).__name__}: {exc}", rows=total)

        return StreamingResponse(gen(), media_type="application/x-ndjson")

    return app


app = create_app()
