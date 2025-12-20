from __future__ import annotations

import numpy as np
import polars as pl
from hk_public_transport_etl.core.errors import NormalizeError

_TRANSFORMER = None


def _get_hk80_to_wgs84_transformer():
    global _TRANSFORMER
    if _TRANSFORMER is not None:
        return _TRANSFORMER
    try:
        from pyproj import Transformer  # type: ignore
    except Exception as e:  # pragma: no cover
        raise NormalizeError(
            "pyproj is required for HK80(EPSG:2326) to WGS84(EPSG:4326). Install `pyproj`."
        ) from e

    _TRANSFORMER = Transformer.from_crs("EPSG:2326", "EPSG:4326", always_xy=True)
    return _TRANSFORMER


def add_lat_lon_from_hk80(df: pl.DataFrame, *, x_col: str, y_col: str) -> pl.DataFrame:
    if df.height == 0:
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("lat"),
            pl.lit(None, dtype=pl.Float64).alias("lon"),
        )

    t = _get_hk80_to_wgs84_transformer()

    xs = df.get_column(x_col).cast(pl.Float64).to_numpy()
    ys = df.get_column(y_col).cast(pl.Float64).to_numpy()

    valid = np.isfinite(xs) & np.isfinite(ys)

    lon = np.full(df.height, np.nan, dtype="float64")
    lat = np.full(df.height, np.nan, dtype="float64")

    if valid.any():
        lon_v, lat_v = t.transform(xs[valid], ys[valid])
        lon[valid] = lon_v
        lat[valid] = lat_v

    out = df.with_columns(
        pl.Series("lat", lat).cast(pl.Float64),
        pl.Series("lon", lon).cast(pl.Float64),
    ).with_columns(
        pl.when(pl.col("lat").is_nan())
        .then(None)
        .otherwise(pl.col("lat"))
        .alias("lat"),
        pl.when(pl.col("lon").is_nan())
        .then(None)
        .otherwise(pl.col("lon"))
        .alias("lon"),
    )
    return out
