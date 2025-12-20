from __future__ import annotations

from pathlib import Path

import polars as pl


def _load_dir(dirpath: Path) -> dict[str, pl.DataFrame]:
    out: dict[str, pl.DataFrame] = {}
    if not dirpath.exists():
        return out
    for p in sorted(dirpath.glob("*.parquet")):
        out[p.stem] = pl.read_parquet(p)
    return out


def load_canonical_tables(tables_dir: Path) -> dict[str, pl.DataFrame]:
    return _load_dir(tables_dir)


def load_mappings(mappings_dir: Path) -> dict[str, pl.DataFrame]:
    return _load_dir(mappings_dir)


def load_unresolved(unresolved_dir: Path) -> dict[str, pl.DataFrame]:
    return _load_dir(unresolved_dir)
