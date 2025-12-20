from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, cast

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from hk_public_transport_etl.core.errors import ParseError


@dataclass(frozen=True, slots=True)
class Artifact:
    kind: str
    path: Path
    name: str | None = None
    prefix: str | None = None
    mode: str | None = None


@dataclass(frozen=True, slots=True)
class ParseJob:
    table_name: str
    parse: Callable[[], pa.Table]


def discover_files(artifacts_dir: Path) -> list[Path]:
    if not artifacts_dir.exists():
        raise ParseError(f"Artifacts dir does not exist: {artifacts_dir}")
    return sorted(
        (p for p in artifacts_dir.iterdir() if p.is_file()), key=lambda p: p.name
    )


def run_jobs(*, jobs: Iterable[ParseJob]) -> dict[str, pa.Table]:
    out: dict[str, pa.Table] = {}
    for job in sorted(jobs, key=lambda j: j.table_name):
        out[job.table_name] = job.parse()
    return out


def parse_data_last_updated_csv(csv_path: Path, *, table_name: str) -> pa.Table:
    try:
        df = pl.read_csv(csv_path, infer_schema_length=500)
        df = df.with_columns(
            pl.lit(csv_path.name).alias("source_file"),
            (pl.arange(0, df.height) + 1).cast(pl.Int32).alias("source_row"),
        )
        return sort_table(df.to_arrow(), sort_keys=[("source_row", "ascending")])
    except Exception:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        cols = sorted(rows[0].keys()) if rows else []
        data: dict[str, list[object]] = {c: [] for c in cols}
        for r in rows:
            for c in cols:
                data[c].append(r.get(c))

        data["source_file"] = [csv_path.name] * len(rows)
        data["source_row"] = list(range(1, len(rows) + 1))

        schema = pa.schema(
            [pa.field(c, pa.string()) for c in cols]
            + [pa.field("source_file", pa.string()), pa.field("source_row", pa.int32())]
        )
        arrays = [pa.array(data[c], type=schema.field(c).type) for c in schema.names]
        t = pa.Table.from_arrays(arrays, schema=schema)
        return sort_table(t, sort_keys=[("source_row", "ascending")])


def ordered_columns(
    *,
    present: Iterable[str],
    known_first: list[str],
    extra_constant_cols: Iterable[str] = (),
    include_trace_cols: bool = True,
) -> list[str]:
    s = set(present)
    out: list[str] = []
    for c in known_first:
        if c in s and c not in out:
            out.append(c)
    for c in sorted(s):
        if c not in out:
            out.append(c)
    for c in sorted(set(extra_constant_cols)):
        if c not in out:
            out.append(c)
    if include_trace_cols:
        for t in ("source_file", "source_row"):
            if t not in out:
                out.append(t)
    return out


def sort_table(table: pa.Table, *, sort_keys: list[tuple[str, str]]) -> pa.Table:
    if not sort_keys:
        return table

    if hasattr(table, "sort_by"):
        try:
            return table.sort_by(sort_keys)  # type: ignore[call-arg]
        except Exception as e:
            raise ParseError(
                f"Failed to sort table via Table.sort_by keys={sort_keys}: {e}"
            ) from e

    fn = getattr(pc, "sort_indices", None)
    if fn is None:
        raise ParseError(
            "pyarrow missing both Table.sort_by and compute.sort_indices; upgrade pyarrow."
        )

    sort_indices = cast(Callable[..., object], fn)
    try:
        idx = sort_indices(table, sort_keys=sort_keys, null_placement="at_end")
        return table.take(idx)
    except Exception as e:
        raise ParseError(
            f"Failed to sort table via compute.sort_indices keys={sort_keys}: {e}"
        ) from e
