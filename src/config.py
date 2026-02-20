from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    processed: Path

def get_paths() -> Paths:
    root = Path(__file__).resolve().parents[1]
    raw = root / "data_raw"
    processed = root / "data_processed"
    raw.mkdir(exist_ok=True, parents=True)
    processed.mkdir(exist_ok=True, parents=True)
    return Paths(root=root, raw=raw, processed=processed)

def fred_key() -> str:
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("Missing FRED_API_KEY. Put it in .env or export it.")
    return key