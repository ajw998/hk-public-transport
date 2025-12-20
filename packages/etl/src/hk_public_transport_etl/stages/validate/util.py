from __future__ import annotations

from typing import Any

import polars as pl


def df_rows(df: pl.DataFrame) -> int:
    return int(df.height)


def take_samples(
    df: pl.DataFrame,
    *,
    cols: list[str],
    sort_cols: list[str],
    n: int,
) -> list[dict[str, Any]]:
    if df.height == 0:
        return []
    keep = [c for c in cols if c in df.columns]
    out = df.select(keep)
    if sort_cols:
        sc = [c for c in sort_cols if c in out.columns]
        if sc:
            out = out.sort(sc)
    return out.head(n).to_dicts()
