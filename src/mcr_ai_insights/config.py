from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    processed: Path


def get_paths(root: str | Path = ".") -> Paths:
    r = Path(root).resolve()
    raw = r / "data" / "raw"
    processed = r / "data" / "processed"
    raw.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    return Paths(root=r, raw=raw, processed=processed)


def fred_key() -> str:
    key = os.getenv("FRED_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Missing FRED_API_KEY. Put it in .env or your environment.")
    return key
