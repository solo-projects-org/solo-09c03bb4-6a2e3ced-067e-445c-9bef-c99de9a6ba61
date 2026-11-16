"""Generate the demo cold archive: immutable Parquet files + manifest.json.

Deterministic: the same spec always produces the same bytes, so a
regenerated archive still matches a cache that survived a restart.

Each file holds columns (borehole, depth, arsenic) for a fixed set of
boreholes over a half-open depth range.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# (file_id, boreholes, depth_min, depth_max)
SPEC: list[tuple[str, list[str], float, float]] = [
    ("core_A", ["BH-001", "BH-002"], 0.0, 50.0),
    ("core_B", ["BH-001", "BH-002"], 50.0, 100.0),
    ("core_C", ["BH-001", "BH-002"], 100.0, 150.0),
    ("core_D", ["BH-003"], 0.0, 75.0),
    ("core_E", ["BH-003"], 75.0, 150.0),
    ("core_F", ["BH-004"], 0.0, 60.0),
]

DEPTH_STEP_M = 0.05  # one sample every 5 cm


def _rows_for(file_id: str, borehole: str, depth_min: float, depth_max: float):
    seed = int(hashlib.sha256(f"{file_id}:{borehole}".encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    n = round((depth_max - depth_min) / DEPTH_STEP_M)
    depths, arsenic = [], []
    for i in range(n):
        depths.append(round(depth_min + i * DEPTH_STEP_M, 4))
        # log-normal-ish arsenic concentration in ppm
        arsenic.append(round(rng.lognormvariate(1.5, 0.9), 3))
    return depths, arsenic


def main() -> None:
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    base_url = os.environ.get("COLDSTORE_BASE_URL", "http://coldstore:8001").rstrip("/")
    data_dir.mkdir(parents=True, exist_ok=True)

    entries = []
    for file_id, boreholes, depth_min, depth_max in SPEC:
        bh_col, depth_col, as_col = [], [], []
        for bh in boreholes:
            depths, arsenic = _rows_for(file_id, bh, depth_min, depth_max)
            bh_col.extend([bh] * len(depths))
            depth_col.extend(depths)
            as_col.extend(arsenic)
        table = pa.table(
            {
                "borehole": pa.array(bh_col, type=pa.string()),
                "depth": pa.array(depth_col, type=pa.float64()),
                "arsenic": pa.array(as_col, type=pa.float64()),
            }
        )
        path = data_dir / f"{file_id}.parquet"
        pq.write_table(table, path, compression="zstd")
        blob = path.read_bytes()
        entries.append(
            {
                "id": file_id,
                "boreholes": boreholes,
                "depth_min": depth_min,
                "depth_max": depth_max,
                "url": f"{base_url}/files/{file_id}.parquet",
                "bytes": len(blob),
                "sha256": hashlib.sha256(blob).hexdigest(),
            }
        )
        print(f"{file_id}: {len(blob)} bytes, {table.num_rows} rows")

    manifest_path = data_dir / "manifest.json"
    manifest_path.write_text(json.dumps({"files": entries}, indent=2) + "\n", encoding="utf-8")
    sizes = sorted(e["bytes"] for e in entries)
    print(f"manifest written to {manifest_path}")
    print(f"file sizes: {sizes}; largest={sizes[-1]}")
    print(f"suggested CACHE_CAPACITY_BYTES (fits 2 largest, not 3 needed for BH-001): "
          f"{entries[0]['bytes'] + entries[1]['bytes'] + 1024}")


if __name__ == "__main__":
    main()
