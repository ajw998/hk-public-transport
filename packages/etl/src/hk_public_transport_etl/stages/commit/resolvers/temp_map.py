from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from hk_public_transport_etl.core import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class TempMapLoadStats:
    table: str
    rows: int
    path: Path


def load_temp_map_route_source(
    conn: sqlite3.Connection,
    *,
    parquet_path: Path,
    table_name: str = "map_route_source",
) -> TempMapLoadStats:
    """
    Load normalized mappings/map_route_source.parquet into a TEMP SQLite table.

    This table is used by headway resolution and should not be persisted in the final DB.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing mapping parquet: {parquet_path}")

    df = pl.read_parquet(parquet_path)

    required = [
        "source",
        "mode",
        "source_route_id",
        "source_file",
        "source_row",
        "route_id",
        "route_key",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{parquet_path} missing columns: {missing}; got={df.columns}")

    df = df.select(required).with_columns(
        pl.col("source").cast(pl.Utf8),
        pl.col("mode").cast(pl.Utf8),
        pl.col("source_route_id").cast(pl.Utf8),
        pl.col("source_file").cast(pl.Utf8),
        pl.col("source_row").cast(pl.Int64),
        pl.col("route_id").cast(pl.Int64),
        pl.col("route_key").cast(pl.Utf8),
    )

    conn.execute(f"DROP TABLE IF EXISTS temp.{table_name};")
    conn.execute(
        f"""
        CREATE TEMP TABLE {table_name} (
          source          TEXT NOT NULL,
          mode            TEXT NOT NULL,
          source_route_id TEXT NOT NULL,
          source_file     TEXT,
          source_row      INTEGER,
          route_id        INTEGER,
          route_key       TEXT
        );
        """
    )

    rows = df.rows()
    conn.executemany(
        f"""
        INSERT INTO {table_name} (
          source, mode, source_route_id, source_file, source_row, route_id, route_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )

    # Helpful indexes for resolver joins
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS temp.idx_{table_name}_source_route "
        f"ON {table_name}(source, source_route_id);"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS temp.idx_{table_name}_route_id "
        f"ON {table_name}(route_id);"
    )

    stats = TempMapLoadStats(table=table_name, rows=len(rows), path=parquet_path)
    log.info(
        "Loaded TEMP mapping table",
        extra={"table": table_name, "rows": stats.rows, "path": str(parquet_path)},
    )
    return stats
