"""End-to-end API behaviour: NDJSON rows, terminal lines, error paths."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.testclient import TestClient

from geocache.main import Settings, create_app
from geocache.manifest import Manifest, ManifestFile


def write_parquet(path: Path, borehole: str, depths: list[float]) -> bytes:
    table = pa.table(
        {
            "borehole": [borehole] * len(depths),
            "depth": depths,
            "arsenic": [float(i) for i in range(len(depths))],
        }
    )
    pq.write_table(table, path)
    return path.read_bytes()


def build_manifest(files: dict[str, tuple[bytes, list[str], float, float]]) -> Manifest:
    return Manifest(
        files=[
            ManifestFile(
                id=fid,
                boreholes=boreholes,
                depth_min=dmin,
                depth_max=dmax,
                url=f"http://coldstore:8001/files/{fid}.parquet",
                bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )
            for fid, (content, boreholes, dmin, dmax) in files.items()
        ]
    )


@pytest.fixture()
def stack(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    bx = write_parquet(src / "x.parquet", "BH-1", [0.0, 10.0, 20.0, 30.0, 40.0])
    by = write_parquet(src / "y.parquet", "BH-1", [50.0, 60.0, 70.0, 80.0, 90.0])
    manifest = build_manifest(
        {
            "core_X": (bx, ["BH-1"], 0.0, 50.0),
            "core_Y": (by, ["BH-1"], 50.0, 100.0),
        }
    )
    contents = {"core_X": bx, "core_Y": by}

    def make_client(*, fail: set[str] | None = None, capacity: int | None = None) -> TestClient:
        async def fetch(meta, tmp_path):
            if fail and meta.id in fail:
                tmp_path.write_bytes(b"junk")
                raise RuntimeError("cold archive unreachable")
            tmp_path.write_bytes(contents[meta.id])

        settings = Settings(
            manifest_path="unused",
            cache_dir=str(tmp_path / f"cache-{id(fail)}-{capacity}"),
            cache_capacity_bytes=capacity or (len(bx) + len(by)),
        )
        app = create_app(settings, manifest=manifest, fetcher_factory=lambda _client: fetch)
        return TestClient(app)

    return make_client, max(len(bx), len(by))


def parse_ndjson(body: str):
    lines = [json.loads(l) for l in body.strip().splitlines()]
    return lines[:-1], lines[-1]


def test_query_streams_rows_then_complete(stack):
    make_client, _ = stack
    with make_client() as client:
        resp = client.get("/query", params={"borehole": "BH-1", "depth_min": 0, "depth_max": 100})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/x-ndjson")
        rows, terminal = parse_ndjson(resp.text)
        assert terminal["status"] == "complete"
        assert terminal["rows"] == 10
        assert terminal["files"] == ["core_X", "core_Y"]
        depths = [r["depth"] for r in rows]
        assert depths == [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]
        assert all(set(r) == {"borehole", "depth", "arsenic"} for r in rows)


def test_query_with_capacity_for_one_file_only(stack):
    make_client, one_file = stack
    with make_client(capacity=one_file) as client:
        resp = client.get("/query", params={"borehole": "BH-1", "depth_min": 0, "depth_max": 100})
        rows, terminal = parse_ndjson(resp.text)
        assert terminal["status"] == "complete"
        assert terminal["rows"] == 10
        stats = client.get("/cache/stats").json()
        assert stats["used_bytes"] <= stats["capacity_bytes"] == one_file


def test_half_open_boundary(stack):
    make_client, _ = stack
    with make_client() as client:
        resp = client.get("/query", params={"borehole": "BH-1", "depth_min": 10, "depth_max": 50})
        rows, terminal = parse_ndjson(resp.text)
        assert terminal["status"] == "complete"
        assert [r["depth"] for r in rows] == [10.0, 20.0, 30.0, 40.0]  # 50 excluded


def test_invalid_interval_and_unknown_borehole(stack):
    make_client, _ = stack
    with make_client() as client:
        assert client.get("/query", params={"borehole": "BH-1", "depth_min": 50, "depth_max": 50}).status_code == 400
        assert client.get("/query", params={"borehole": "BH-1", "depth_min": 60, "depth_max": 50}).status_code == 400
        assert client.get("/query", params={"borehole": "BH-9", "depth_min": 0, "depth_max": 10}).status_code == 404


def test_fetch_failure_yields_failed_terminal(stack):
    make_client, _ = stack
    with make_client(fail={"core_Y"}) as client:
        resp = client.get("/query", params={"borehole": "BH-1", "depth_min": 0, "depth_max": 100})
        rows, terminal = parse_ndjson(resp.text)
        assert terminal["status"] == "failed"
        assert "core_Y" in terminal["error"] or terminal.get("file") == "core_Y"
        assert terminal["rows"] == 5  # core_X rows were delivered before the failure
        # Failed file must not be cached; only core_X may occupy the cache.
        stats = client.get("/cache/stats").json()
        assert {e["id"] for e in stats["entries"]} == {"core_X"}


def test_empty_result_still_completes(stack):
    make_client, _ = stack
    with make_client() as client:
        resp = client.get("/query", params={"borehole": "BH-1", "depth_min": 200, "depth_max": 300})
        rows, terminal = parse_ndjson(resp.text)
        assert rows == []
        assert terminal["status"] == "complete" and terminal["rows"] == 0
