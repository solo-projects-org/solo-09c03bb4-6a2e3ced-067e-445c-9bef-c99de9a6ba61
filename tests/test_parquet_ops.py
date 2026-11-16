"""Batch filtering: half-open interval boundaries, borehole selection."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from geocache.parquet_ops import filter_lines


def make_parquet(tmp_path) -> str:
    table = pa.table(
        {
            "borehole": ["BH-1"] * 4 + ["BH-2"] * 4,
            "depth": [0.0, 10.0, 20.0, 30.0, 0.0, 10.0, 20.0, 30.0],
            "arsenic": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        }
    )
    path = tmp_path / "core.parquet"
    pq.write_table(table, path)
    return str(path)


def test_half_open_interval_and_borehole(tmp_path):
    path = make_parquet(tmp_path)
    lines = filter_lines(path, "BH-1", 10.0, 30.0)
    rows = [json.loads(l) for l in lines]
    # [10, 30): includes 10.0 and 20.0, excludes 30.0; only BH-1.
    assert [(r["borehole"], r["depth"]) for r in rows] == [("BH-1", 10.0), ("BH-1", 20.0)]
    assert rows[0]["arsenic"] == 2.0


def test_sorted_and_empty(tmp_path):
    path = make_parquet(tmp_path)
    assert filter_lines(path, "BH-2", 25.0, 30.0) == []  # 30 excluded
    rows = [json.loads(l) for l in filter_lines(path, "BH-2", 0.0, 40.0)]
    assert [r["depth"] for r in rows] == [0.0, 10.0, 20.0, 30.0]
