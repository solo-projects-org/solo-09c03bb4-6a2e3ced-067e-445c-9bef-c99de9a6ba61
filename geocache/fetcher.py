"""Download + verification of immutable cold-archive files.

Every download is written to a temp file, then checked against the
manifest's exact byte length and SHA-256 digest. Only after both pass
may the caller publish the file into the cache. Any failure removes the
partial temp file; a partial file is never a cache hit.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

import httpx

from .manifest import ManifestFile

log = logging.getLogger("geocache.fetch")


class FetchError(Exception):
    pass


class HttpFetcher:
    """Fetches manifest files over HTTP with retries."""

    def __init__(self, client: httpx.AsyncClient, attempts: int = 2) -> None:
        self.client = client
        self.attempts = max(1, attempts)

    async def __call__(self, meta: ManifestFile, tmp_path: Path) -> None:
        last: Exception | None = None
        for attempt in range(1, self.attempts + 1):
            try:
                await self._once(meta, tmp_path)
                return
            except Exception as exc:  # noqa: BLE001 - retried, then reported
                last = exc
                tmp_path.unlink(missing_ok=True)
                log.warning(
                    "fetch %s attempt %d/%d failed: %s",
                    meta.id, attempt, self.attempts, exc,
                )
                if attempt < self.attempts:
                    await asyncio.sleep(0.2 * attempt)
        raise FetchError(f"download of {meta.id} failed after {self.attempts} attempt(s): {last}")

    async def _once(self, meta: ManifestFile, tmp_path: Path) -> None:
        received = 0
        digest = hashlib.sha256()
        async with self.client.stream("GET", meta.url) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as out:
                async for chunk in resp.aiter_bytes(1 << 16):
                    out.write(chunk)
                    digest.update(chunk)
                    received += len(chunk)
        if received != meta.bytes:
            raise FetchError(
                f"length mismatch for {meta.id}: got {received} bytes, manifest says {meta.bytes}"
            )
        actual = digest.hexdigest()
        if actual != meta.sha256:
            raise FetchError(f"sha256 mismatch for {meta.id}: got {actual}")
