"""CacheManager behaviour: single-flight, pins, eviction, space waits,
failure cleanup, restart recovery."""

from __future__ import annotations

import asyncio

import pytest

from geocache.cache import CacheError, CacheManager
from tests.conftest import make_fetch, make_manifest

S = 1000  # every fake file is 1000 bytes


def contents(*ids: str) -> dict[str, bytes]:
    return {fid: (fid.encode() * S)[:S] for fid in ids}


async def make_cache(tmp_path, manifest, fetch, capacity: int) -> CacheManager:
    cm = CacheManager(tmp_path / "cache", capacity, manifest, fetch)
    await cm.startup_scan()
    return cm


async def test_download_then_hit(tmp_path):
    files = contents("f1")
    calls: list[str] = []
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files, calls=calls), S)
    e1 = await cm.acquire("f1")
    assert e1.path.read_bytes() == files["f1"]
    assert cm.used_bytes == S
    await cm.release("f1")
    e2 = await cm.acquire("f1")  # cache hit: no second fetch
    assert calls == ["f1"]
    await cm.release("f1")
    await cm.close()


async def test_capacity_must_hold_largest_file(tmp_path):
    files = contents("big")
    with pytest.raises(ValueError, match="largest"):
        CacheManager(tmp_path / "c", S - 1, make_manifest(files), make_fetch(files))


async def test_single_flight_and_disconnect_keeps_download(tmp_path):
    files = contents("f1")
    gate = asyncio.Event()
    calls: list[str] = []
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files, blocks={"f1": gate}, calls=calls), S)

    t1 = asyncio.create_task(cm.acquire("f1"))
    await asyncio.sleep(0.05)  # let the download start and block
    assert calls == ["f1"]

    # t1's "client" disconnects: the shared download must not be cancelled.
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1

    t2 = asyncio.create_task(cm.acquire("f1"))
    await asyncio.sleep(0.05)
    assert calls == ["f1"]  # still one fetch, t2 joined the same download
    gate.set()
    entry = await asyncio.wait_for(t2, 1)
    assert entry.state == "ready"
    await cm.release("f1")
    await cm.close()


async def test_eviction_skips_pinned(tmp_path):
    files = contents("f1", "f2", "f3")
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files), 2 * S)
    pinned = await cm.acquire("f1")          # stays pinned -> not evictable
    e2 = await cm.acquire("f2")
    await cm.release("f2")                   # f2 unpinned, LRU victim
    e3 = await cm.acquire("f3")              # needs room: must evict f2, not f1
    assert set(cm.entries) == {"f1", "f3"}
    assert cm.used_bytes == 2 * S
    assert not (cm.dir / "f2.parquet").exists()
    await cm.release("f1")
    await cm.release("f3")
    await cm.close()


async def test_wait_for_space_resumes_and_hits_not_blocked(tmp_path):
    files = contents("f1", "f2")
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files), S)
    await cm.acquire("f1")  # pinned by "reader" Q1, cache now full

    # Q2 wants f2: its download must wait for space (f1 pinned).
    q2 = asyncio.create_task(cm.acquire("f2"))
    await asyncio.sleep(0.05)
    assert not q2.done()

    # A cache hit (Q3 re-reading f1) must not be blocked by Q2's wait.
    hit = await asyncio.wait_for(cm.acquire("f1"), timeout=0.5)
    assert hit.file_id == "f1"

    # Freeing space lets the waiting download proceed.
    await cm.release("f1")
    await cm.release("f1")
    e2 = await asyncio.wait_for(q2, timeout=1)
    assert e2.state == "ready"
    assert set(cm.entries) == {"f2"}
    await cm.release("f2")
    await cm.close()


async def test_multi_file_queries_no_deadlock(tmp_path):
    # Capacity holds exactly one file; two queries each need both files.
    # Sequential per-file acquire/release must let both finish.
    files = contents("f1", "f2")
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files), S)

    async def query(first: str, second: str) -> list[str]:
        seen = []
        for fid in (first, second):
            e = await cm.acquire(fid)
            await asyncio.sleep(0.01)  # pretend to read
            seen.append(fid)
            await cm.release(fid)
        return seen

    r1, r2 = await asyncio.wait_for(
        asyncio.gather(query("f1", "f2"), query("f2", "f1")), timeout=5
    )
    assert r1 == ["f1", "f2"] and r2 == ["f2", "f1"]
    assert cm.used_bytes <= S
    await cm.close()


async def test_failed_download_cleans_up_and_never_hits(tmp_path):
    files = contents("f1")
    calls: list[str] = []
    fetch = make_fetch(files, fail={"f1"}, calls=calls)
    cm = await make_cache(tmp_path, make_manifest(files), fetch, S)

    with pytest.raises(CacheError, match="boom"):
        await cm.acquire("f1")
    assert cm.used_bytes == 0            # reservation released
    assert cm.entries == {}              # not cached
    assert list(cm.dir.iterdir()) == []  # half file removed

    # A later query retries from scratch and can succeed.
    fetch2 = make_fetch(files, calls=calls)
    cm.fetch = fetch2
    e = await cm.acquire("f1")
    assert e.state == "ready"
    assert cm.used_bytes == S
    await cm.release("f1")
    await cm.close()


async def test_temp_bytes_count_against_budget(tmp_path):
    # While f2 is downloading, its temp file already occupies budget:
    # nothing else may use that space.
    files = contents("f1", "f2")
    gate = asyncio.Event()
    cm = await make_cache(tmp_path, make_manifest(files), make_fetch(files, blocks={"f2": gate}), 2 * S)
    e1 = await cm.acquire("f1")
    await cm.release("f1")
    q = asyncio.create_task(cm.acquire("f2"))
    await asyncio.sleep(0.05)
    stats = await cm.stats()
    assert stats["used_bytes"] == 2 * S  # f1 + reserved temp for f2
    assert {e["id"] for e in stats["entries"]} == {"f1", "f2"}
    gate.set()
    e2 = await asyncio.wait_for(q, 1)
    await cm.release("f2")
    await cm.close()


async def test_startup_scan_recovers_and_cleans(tmp_path):
    files = contents("f1", "f2")
    manifest = make_manifest(files)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    # A fully downloaded, verifiable file survives a restart.
    (cache_dir / "f1.parquet").write_bytes(files["f1"])
    # A half-written temp file from a crash is removed.
    (cache_dir / ".tmp-deadbeef").write_bytes(b"partial")
    # A corrupt / truncated file fails revalidation and is removed.
    (cache_dir / "f2.parquet").write_bytes(b"corrupt")

    cm = CacheManager(cache_dir, 2 * S, manifest, make_fetch(files))
    await cm.startup_scan()

    assert set(cm.entries) == {"f1"}
    assert cm.used_bytes == S
    remaining = {p.name for p in cache_dir.iterdir()}
    assert remaining == {"f1.parquet"}
    await cm.close()
