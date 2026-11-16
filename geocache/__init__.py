"""Geological cold-archive cache service.

Single-instance API that serves arsenic measurements from immutable
Parquet files listed in a fixed manifest, with a byte-budgeted hot cache
in front of a slow cold HTTP archive.
"""

__version__ = "0.1.0"
