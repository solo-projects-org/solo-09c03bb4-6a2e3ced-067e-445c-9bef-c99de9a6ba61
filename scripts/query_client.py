#!/usr/bin/env python3
"""Demo query client: streams NDJSON and enforces the terminal-line rule.

A result is only complete when the last line is {"status": "complete"}.
Anything else (failed terminal, truncated stream, connection drop)
means the result is incomplete and must not be trusted.

Usage:
    python scripts/query_client.py BH-001 20 120 [--base http://localhost:8000]
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("borehole")
    parser.add_argument("depth_min", type=float)
    parser.add_argument("depth_max", type=float)
    parser.add_argument("--base", default="http://localhost:8000")
    args = parser.parse_args()

    rows = 0
    terminal: dict | None = None
    try:
        with httpx.stream(
            "GET",
            f"{args.base}/query",
            params={
                "borehole": args.borehole,
                "depth_min": args.depth_min,
                "depth_max": args.depth_max,
            },
            timeout=None,
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if not line:
                    continue
                obj = json.loads(line)
                if "status" in obj:
                    terminal = obj
                else:
                    rows += 1
                    if rows <= 3:
                        print(obj)
    except (httpx.HTTPError, json.JSONDecodeError) as exc:
        print(f"INCOMPLETE: stream error after {rows} rows: {exc}", file=sys.stderr)
        return 2

    if terminal is None:
        print(f"INCOMPLETE: no terminal line after {rows} rows", file=sys.stderr)
        return 2
    if terminal.get("status") != "complete":
        print(f"FAILED after {rows} rows: {terminal}", file=sys.stderr)
        return 1
    print(f"COMPLETE: {terminal['rows']} rows from {terminal.get('files')} "
          f"(client counted {rows})")
    return 0 if terminal["rows"] == rows else 2


if __name__ == "__main__":
    sys.exit(main())
