"""Fixed manifest: the immutable list of cold-archive Parquet files.

The manifest is loaded once at startup and never mutated. Each entry
describes one immutable Parquet file: which boreholes it covers, the
depth range (half-open [depth_min, depth_max)), the cold-storage URL,
the exact byte length and the SHA-256 digest. Length and digest are
verified after every download before the file may be used.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class ManifestFile(BaseModel):
    """One immutable cold-archive file."""

    id: str
    boreholes: list[str]
    depth_min: float
    depth_max: float
    url: str
    bytes: int = Field(gt=0)
    sha256: str

    @field_validator("id")
    @classmethod
    def _id_is_safe_filename(cls, v: str) -> str:
        # The id is used as a cache filename; it must not escape the dir.
        if not _SAFE_ID.match(v) or ".." in v:
            raise ValueError(f"unsafe file id: {v!r}")
        return v

    @field_validator("sha256")
    @classmethod
    def _sha256_is_hex(cls, v: str) -> str:
        v = v.lower()
        if len(v) != 64 or any(c not in "0123456789abcdef" for c in v):
            raise ValueError("sha256 must be 64 lowercase hex chars")
        return v

    @model_validator(mode="after")
    def _depth_range_sane(self) -> "ManifestFile":
        if not self.depth_max > self.depth_min:
            raise ValueError("depth_max must be > depth_min")
        if not self.boreholes:
            raise ValueError("boreholes must not be empty")
        return self

    def covers(self, borehole: str, depth_min: float, depth_max: float) -> bool:
        """True if this file may hold rows for the half-open query interval."""
        return (
            borehole in self.boreholes
            and self.depth_min < depth_max
            and self.depth_max > depth_min
        )


class Manifest(BaseModel):
    files: list[ManifestFile]

    @model_validator(mode="after")
    def _unique_ids(self) -> "Manifest":
        ids = [f.id for f in self.files]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate file ids in manifest")
        return self

    @property
    def by_id(self) -> dict[str, ManifestFile]:
        return {f.id: f for f in self.files}

    @property
    def max_file_bytes(self) -> int:
        return max((f.bytes for f in self.files), default=0)

    def select(self, borehole: str, depth_min: float, depth_max: float) -> list[ManifestFile]:
        """Files needed for a query, ordered by depth range."""
        chosen = [f for f in self.files if f.covers(borehole, depth_min, depth_max)]
        chosen.sort(key=lambda f: (f.depth_min, f.id))
        return chosen

    def known_borehole(self, borehole: str) -> bool:
        return any(borehole in f.boreholes for f in self.files)


def load_manifest(path: str | Path) -> Manifest:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return Manifest.model_validate(raw)
