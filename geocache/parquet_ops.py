"""Batch filtering of verified Parquet files into NDJSON lines.

Runs in a worker thread (called via ``asyncio.to_thread``). The file is
read through an open file handle, so a concurrent cache eviction
(unlink) can never corrupt an in-progress read on POSIX systems.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

COLUMNS = ["borehole", "depth", "arsenic"]


def filter_lines(
    path: str | Path,
    borehole: str,
    depth_min: float,
    depth_max: float,
    batch_size: int = 8192,
) -> list[str]:
    """Return NDJSON lines for one borehole over the half-open interval
    [depth_min, depth_max), filtered batch by batch, sorted by depth."""
    rows: list[tuple[str, float, float]] = []
    with open(path, "rb") as fh:
        parquet = pq.ParquetFile(fh)
        for batch in parquet.iter_batches(batch_size=batch_size, columns=COLUMNS):
            mask = pc.and_(
                pc.and_(
                    pc.equal(batch.column("borehole"), borehole),
                    pc.greater_equal(batch.column("depth"), depth_min),
                ),
                pc.less(batch.column("depth"), depth_max),
            )
            cols = batch.filter(mask).to_pydict()
            rows.extend(zip(cols["borehole"], cols["depth"], cols["arsenic"]))
    rows.sort(key=lambda r: (r[1], r[0]))
    return [
        json.dumps(
            {"borehole": b, "depth": d, "arsenic": a},
            separators=(",", ":"),
        )
        for b, d, a in rows
    ]
