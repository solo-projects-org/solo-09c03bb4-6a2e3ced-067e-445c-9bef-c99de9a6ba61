"""Byte-budgeted hot cache in front of the cold archive.

Design invariants:

* ``used_bytes`` counts every byte the cache directory may occupy,
  including in-progress download temp files (reserved up-front from the
  manifest's exact byte length). It never exceeds ``capacity_bytes``.
* Capacity may be smaller than the total a query needs, but it must hold
  the largest single manifest file (validated at startup).
* Concurrent queries share one retrieval per file (single-flight). The
  download runs in its own task, decoupled from any request: a client
  disconnect never cancels a download others still need.
* A file being read is pinned and is never evicted.
* Queries never reserve space for files they are not actively reading;
  only the single-flight downloader reserves, exactly one file at a
  time. Readers therefore never wait on space, so queries cannot hold
  partial space and deadlock against each other; when space is freed a
  waiting downloader is notified and proceeds.
* Waiting for space never blocks cache hits: hits only take the lock
  briefly to bump a pin count.
* Failed downloads and half-written temp files are removed and never
  count as hits; the same cleanup runs at startup for crash recovery.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from .manifest import Manifest, ManifestFile

log = logging.getLogger("geocache.cache")

TMP_SUFFIX = ".tmp-"
FINAL_SUFFIX = ".parquet"


class CacheError(Exception):
    """Raised to waiters when a file could not be made available."""


# Fetch signature: download the manifest file to tmp_path, verifying
# length and digest; raise on any failure. Must not rename.
FetchFn = Callable[[ManifestFile, Path], Awaitable[None]]


@dataclass
class Entry:
    file_id: str
    size: int
    path: Path
    state: str  # "downloading" | "ready"
    pins: int = 0
    last_used: float = 0.0
    done: asyncio.Event = field(default_factory=asyncio.Event)
    error: str | None = None
    task: asyncio.Task | None = None


class CacheManager:
    def __init__(
        self,
        cache_dir: str | Path,
        capacity_bytes: int,
        manifest: Manifest,
        fetch: FetchFn,
    ) -> None:
        self.dir = Path(cache_dir)
        self.capacity = int(capacity_bytes)
        self.manifest = manifest
        self.fetch = fetch
        self.entries: dict[str, Entry] = {}
        self.used_bytes = 0
        self.cond = asyncio.Condition()
        if self.manifest.files and self.capacity < self.manifest.max_file_bytes:
            raise ValueError(
                f"cache capacity {self.capacity} cannot hold the largest "
                f"manifest file ({self.manifest.max_file_bytes} bytes)"
            )

    # ------------------------------------------------------------------
    # startup / shutdown
    # ------------------------------------------------------------------

    async def startup_scan(self) -> None:
        """Recover from a restart: drop half downloads, revalidate the rest."""
        self.dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._scan_sync)
        log.info(
            "startup scan: %d ready file(s), %d/%d bytes used",
            len(self.entries),
            self.used_bytes,
            self.capacity,
        )

    def _scan_sync(self) -> None:
        by_id = self.manifest.by_id
        for child in sorted(self.dir.iterdir()):
            if child.name.startswith(TMP_SUFFIX) or TMP_SUFFIX in child.name:
                log.warning("removing half-downloaded temp file %s", child.name)
                child.unlink(missing_ok=True)
                continue
            if not child.name.endswith(FINAL_SUFFIX):
                continue
            file_id = child.name[: -len(FINAL_SUFFIX)]
            meta = by_id.get(file_id)
            if meta is None:
                log.warning("removing cache file %s: not in manifest", child.name)
                child.unlink(missing_ok=True)
                continue
            if not self._verify_file(child, meta):
                log.warning("removing cache file %s: failed verification", child.name)
                child.unlink(missing_ok=True)
                continue
            self.entries[file_id] = Entry(
                file_id=file_id,
                size=meta.bytes,
                path=child,
                state="ready",
                last_used=time.monotonic(),
            )
            self.used_bytes += meta.bytes

    @staticmethod
    def _verify_file(path: Path, meta: ManifestFile) -> bool:
        try:
            if path.stat().st_size != meta.bytes:
                return False
            h = hashlib.sha256()
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
            return h.hexdigest() == meta.sha256
        except OSError:
            return False

    async def close(self) -> None:
        """Cancel in-flight downloads (their temp files are cleaned up)."""
        tasks = [e.task for e in self.entries.values() if e.task]
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    # ------------------------------------------------------------------
    # acquire / release
    # ------------------------------------------------------------------

    async def acquire(self, file_id: str) -> Entry:
        """Pin a ready file and return its entry. Raises CacheError on failure."""
        meta = self.manifest.by_id.get(file_id)
        if meta is None:
            raise CacheError(f"unknown file id: {file_id}")
        async with self.cond:
            entry = self.entries.get(file_id)
            if entry is None:
                entry = Entry(
                    file_id=file_id,
                    size=meta.bytes,
                    path=self.dir / f"{file_id}{FINAL_SUFFIX}",
                    state="downloading",
                )
                self.entries[file_id] = entry
                # The download runs in its own task, created atomically with
                # the entry, so no request cancellation can ever orphan or
                # interrupt it. Others simply join on entry.done.
                entry.task = asyncio.create_task(self._run_download(entry, meta))
            entry.pins += 1
            if entry.state == "ready":
                entry.last_used = time.monotonic()
                return entry
        try:
            await entry.done.wait()
        except BaseException:
            # Our waiter went away (e.g. client disconnect); the download
            # itself is unaffected and keeps running for everyone else.
            async with self.cond:
                entry.pins -= 1
                self.cond.notify_all()
            raise
        if entry.state == "ready":
            async with self.cond:
                entry.last_used = time.monotonic()
            return entry
        async with self.cond:
            entry.pins -= 1
            self.cond.notify_all()
        raise CacheError(entry.error or f"fetch failed for {file_id}")

    async def release(self, file_id: str) -> None:
        async with self.cond:
            entry = self.entries.get(file_id)
            if entry is not None:
                entry.pins = max(0, entry.pins - 1)
                entry.last_used = time.monotonic()
            self.cond.notify_all()

    # ------------------------------------------------------------------
    # download (single-flight, space-reserving)
    # ------------------------------------------------------------------

    async def _run_download(self, entry: Entry, meta: ManifestFile) -> None:
        tmp = self.dir / f"{TMP_SUFFIX}{uuid.uuid4().hex}"
        reserved = False
        try:
            await self._reserve(meta.bytes)
            reserved = True
            log.info("download start %s (%d bytes)", meta.id, meta.bytes)
            await self.fetch(meta, tmp)
            os.replace(tmp, entry.path)  # atomic publish within same dir
            async with self.cond:
                entry.state = "ready"
                entry.last_used = time.monotonic()
                self.cond.notify_all()
            log.info("download ok %s; cache %d/%d bytes", meta.id, self.used_bytes, self.capacity)
        except asyncio.CancelledError:
            # Shutdown path. Remove the half file; reservation is released.
            tmp.unlink(missing_ok=True)
            async with self.cond:
                if reserved:
                    self.used_bytes -= meta.bytes
                self.entries.pop(entry.file_id, None)
                entry.error = "download cancelled (shutdown)"
                self.cond.notify_all()
            raise
        except Exception as exc:
            # Failure: half file removed, reservation freed, never a hit.
            tmp.unlink(missing_ok=True)
            async with self.cond:
                if reserved:
                    self.used_bytes -= meta.bytes
                self.entries.pop(entry.file_id, None)
                entry.error = f"{type(exc).__name__}: {exc}"
                self.cond.notify_all()
            log.warning("download failed %s: %s", meta.id, entry.error)
        finally:
            entry.done.set()

    async def _reserve(self, size: int) -> None:
        """Reserve ``size`` bytes, evicting unpinned files, waiting if needed.

        Only ever called from download tasks (one file at a time), never
        from readers, so readers can always finish and free space.
        """
        async with self.cond:
            while True:
                self._evict_until_fits_locked(size)
                if self.used_bytes + size <= self.capacity:
                    self.used_bytes += size
                    return
                # Everything evictable is gone; the rest is pinned by active
                # readers who will release. Wait for them; cache hits are
                # unaffected because cond.wait() releases the lock.
                await self.cond.wait()

    def _evict_until_fits_locked(self, incoming: int) -> None:
        while self.used_bytes + incoming > self.capacity:
            victims = [
                e
                for e in self.entries.values()
                if e.state == "ready" and e.pins == 0
            ]
            if not victims:
                return
            victim = min(victims, key=lambda e: e.last_used)
            log.info("evict %s (%d bytes)", victim.file_id, victim.size)
            victim.path.unlink(missing_ok=True)
            self.used_bytes -= victim.size
            del self.entries[victim.file_id]

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    async def stats(self) -> dict:
        async with self.cond:
            return {
                "capacity_bytes": self.capacity,
                "used_bytes": self.used_bytes,
                "entries": [
                    {
                        "id": e.file_id,
                        "state": e.state,
                        "size": e.size,
                        "pins": e.pins,
                        "error": e.error,
                    }
                    for e in sorted(self.entries.values(), key=lambda e: e.file_id)
                ],
            }
