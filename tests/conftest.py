"""Shared helpers: in-memory manifests and fake fetchers."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

from geocache.manifest import Manifest, ManifestFile


def make_manifest(specs: dict[str, bytes]) -> Manifest:
    """Manifest from {file_id: content}; sizes/digests match the content."""
    files = [
        ManifestFile(
            id=fid,
            boreholes=["BH-001"],
            depth_min=0.0,
            depth_max=100.0,
            url=f"http://coldstore:8001/files/{fid}.parquet",
            bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        for fid, content in specs.items()
    ]
    return Manifest(files=files)


def make_fetch(
    contents: dict[str, bytes],
    *,
    fail: set[str] | None = None,
    blocks: dict[str, asyncio.Event] | None = None,
    calls: list[str] | None = None,
):
    """Fake fetch: writes content to tmp_path; can block on events / fail."""

    async def fetch(meta: ManifestFile, tmp_path: Path) -> None:
        if calls is not None:
            calls.append(meta.id)
        if blocks and meta.id in blocks:
            await blocks[meta.id].wait()
        if fail and meta.id in fail:
            tmp_path.write_bytes(b"half-written junk")
            raise RuntimeError(f"boom fetching {meta.id}")
        tmp_path.write_bytes(contents[meta.id])

    return fetch
