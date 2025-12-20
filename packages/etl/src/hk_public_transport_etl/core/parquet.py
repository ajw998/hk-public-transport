from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any, Union

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .fs import fsync_dir, fsync_file, safe_unlink
from .hashing import sha256_file
from .json import stable_json_dumps


def schema_fingerprint(schema: pa.Schema) -> str:
    rep: list[dict[str, Any]] = []
    for f in schema:
        rep.append({"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)})
    b = stable_json_dumps(rep, indent=None).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def read_parquet_df(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


ParquetWritable = Union[
    "pa.Table", "pa.RecordBatch", "pa.RecordBatchReader", "pl.DataFrame"
]


def _to_arrow_table(obj: ParquetWritable) -> pa.Table:
    """
    Normalize inputs to a pyarrow.Table.

    Important: passing a polars.DataFrame directly into pq.write_table triggers:
      TypeError: expected pyarrow.lib.Schema, got polars.Schema
    because pq.write_table reads obj.schema.
    """
    if isinstance(obj, pa.Table):
        return obj

    if isinstance(obj, pa.RecordBatch):
        return pa.Table.from_batches([obj])

    if pl is not None and isinstance(obj, pl.DataFrame):
        return obj.to_arrow()

    raise TypeError(f"Unsupported parquet input type: {type(obj).__name__}")


def write_parquet_atomic(
    table: ParquetWritable,
    out_path: Path,
    compression: str = "zstd",
) -> None:
    """
    Atomic Parquet write:

    Flow:
        1. Temporary file in same directory
        2. Perform `pq.write_table`
        3. fsync
        4. `os.replace`
        5. fsync(dir)
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fd: int | None = None
    tmp_path: Path | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{out_path.name}.",
            suffix=".tmp",
            dir=str(out_path.parent),
        )
        os.close(fd)
        fd = None
        tmp_path = Path(tmp_name)

        table = _to_arrow_table(table)
        pq.write_table(table, tmp_path, compression=compression)

        fsync_file(tmp_path)
        os.replace(tmp_path, out_path)
        fsync_dir(out_path.parent)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if tmp_path is not None and tmp_path.exists():
            safe_unlink(tmp_path)


def table_meta_from_df(path: Path, df: pl.DataFrame) -> dict[str, Any]:
    digest = sha256_file(path)
    schema = df.to_arrow().schema
    return {
        "relpath": path.as_posix(),
        "row_count": int(df.height),
        "sha256": digest.sha256,
        "bytes": int(digest.bytes),
        "schema_hash": schema_fingerprint(schema),
        "columns": list(df.columns),
    }
