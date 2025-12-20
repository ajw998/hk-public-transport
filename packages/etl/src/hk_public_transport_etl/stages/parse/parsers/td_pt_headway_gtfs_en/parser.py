from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
from hk_public_transport_etl.core.errors import ParseError
from hk_public_transport_etl.stages.parse.common import (
    Artifact,
    ParseJob,
    discover_files,
    ordered_columns,
    parse_data_last_updated_csv,
    run_jobs,
    sort_table,
)
from hk_public_transport_etl.stages.parse.parsers.td_pt_headway_gtfs_en.specs import (
    GTFS_FILES,
    GTFS_PLANS,
    TxtTablePlan,
)


def parse(artifacts_dir: Path) -> dict[str, pa.Table]:
    files = discover_files(artifacts_dir)
    artifacts: list[Artifact] = []
    for p in files:
        a = _classify(p)
        if a is not None:
            artifacts.append(a)

    jobs = (exec_job(a) for a in artifacts)
    return run_jobs(jobs=jobs)


def _classify(path: Path) -> Artifact | None:
    name_u = path.name.upper()
    if name_u == "DATA_LAST_UPDATED_DATE.CSV":
        return Artifact(kind="data_last_updated_csv", path=path)
    if name_u in GTFS_FILES:
        return Artifact(kind="gtfs_txt", path=path, name=GTFS_FILES[name_u])
    return None


def exec_job(a: Artifact) -> ParseJob:
    if a.kind == "data_last_updated_csv":
        return ParseJob(
            table_name="td_headway_data_last_updated",
            parse=lambda p=a.path: parse_data_last_updated_csv(
                p, table_name="td_headway_data_last_updated"
            ),
        )
    if a.kind == "gtfs_txt":
        assert a.name is not None
        plan = GTFS_PLANS[a.name]
        return ParseJob(
            table_name=plan.table_name,
            parse=lambda plan=plan, p=a.path: _parse_gtfs_txt(p, plan),
        )
    raise ParseError(f"Unhandled artifact kind: {a.kind!r} path={a.path}")


def _parse_gtfs_txt(txt_path: Path, plan: TxtTablePlan) -> pa.Table:
    if not txt_path.exists():
        raise ParseError(f"Missing GTFS text file: {txt_path}")

    try:
        df = pl.read_csv(
            txt_path,
            infer_schema_length=5000,
            ignore_errors=False,
            try_parse_dates=False,
        )
    except Exception as e:
        raise ParseError(f"Failed to read GTFS txt {txt_path.name}: {e}") from e

    if df.height == 0:
        return _empty_txt_table(plan)

    for f in plan.required_fields:
        if f not in df.columns:
            raise ParseError(
                f"[{plan.table_name}] missing required column: {f!r} in {txt_path.name}"
            )

    df = df.with_columns(
        pl.lit(txt_path.name).alias("source_file"),
        (pl.arange(0, df.height) + 1).cast(pl.Int32).alias("source_row"),
    )

    for col, dtype in plan.type_hints.items():
        if col in df.columns:
            df = df.with_columns(_polars_cast_expr(col, dtype).alias(col))

    ordered = ordered_columns(
        present=df.columns,
        known_first=plan.known_first,
        extra_constant_cols=(),
        include_trace_cols=True,
    )
    df = df.select(ordered)
    table = df.to_arrow()

    sort_cols = [k for k in plan.stable_sort_keys if k in table.schema.names]
    for t in ("source_file", "source_row"):
        if t in table.schema.names and t not in sort_cols:
            sort_cols.append(t)

    return sort_table(table, sort_keys=[(k, "ascending") for k in sort_cols])


def _empty_txt_table(plan: TxtTablePlan) -> pa.Table:
    cols = ordered_columns(
        present=plan.known_first,
        known_first=plan.known_first,
        extra_constant_cols=(),
        include_trace_cols=True,
    )

    fields: list[pa.Field] = []
    for c in cols:
        if c == "source_row":
            fields.append(pa.field("source_row", pa.int32()))
        elif c == "source_file":
            fields.append(pa.field("source_file", pa.string()))
        else:
            fields.append(pa.field(c, plan.type_hints.get(c, pa.string())))

    schema = pa.schema(fields)
    arrays = [pa.array([], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _polars_cast_expr(col: str, dtype: pa.DataType) -> pl.Expr:
    if pa.types.is_int8(dtype):
        return pl.col(col).cast(pl.Int8, strict=False)
    if pa.types.is_int32(dtype):
        return pl.col(col).cast(pl.Int32, strict=False)
    if pa.types.is_int64(dtype):
        return pl.col(col).cast(pl.Int64, strict=False)
    return pl.col(col).cast(pl.Utf8, strict=False)
