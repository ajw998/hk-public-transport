from __future__ import annotations

import json
from pathlib import Path

import polars as pl
from hk_public_transport_etl.core import JsonObject, NormalizeError

from ...common import (
    NormalizeWriter,
    list_tables,
    read_parquet_df,
    require_columns,
    stable_sort,
)
from ...types import NormalizeContext, NormalizeOutput

RULES_VERSION = "td_pt_headway_gtfs_en.normalize.v1"


def _must_df(table_paths: dict[str, Path], name: str) -> pl.DataFrame:
    p = table_paths.get(name)
    if not p:
        raise NormalizeError(f"missing required parsed table: {name}.parquet")
    return read_parquet_df(p)


def normalize_td_pt_headway_gtfs_en(ctx: NormalizeContext) -> NormalizeOutput:
    source_id = ctx.source_id
    version = ctx.version
    data_root = Path(ctx.data_root)

    parsed_root = data_root / "staged" / source_id / version
    table_paths = list_tables(parsed_root / "tables")

    calendar = _must_df(table_paths, "td_headway_calendar")
    trips = _must_df(table_paths, "td_headway_trips")
    freqs = _must_df(table_paths, "td_headway_frequencies")

    out_dir = data_root / "normalized" / source_id / version
    out = NormalizeWriter(out_dir=out_dir)

    # service_calendars
    require_columns(
        calendar,
        table="td_headway_calendar",
        cols=[
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
    )

    service_calendars = calendar.select(
        pl.col("service_id").cast(pl.Int64),
        pl.col("monday").cast(pl.Int8),
        pl.col("tuesday").cast(pl.Int8),
        pl.col("wednesday").cast(pl.Int8),
        pl.col("thursday").cast(pl.Int8),
        pl.col("friday").cast(pl.Int8),
        pl.col("saturday").cast(pl.Int8),
        pl.col("sunday").cast(pl.Int8),
        pl.col("start_date").cast(pl.Int32),
        pl.col("end_date").cast(pl.Int32),
    ).unique(subset=["service_id"], keep="first")
    service_calendars = stable_sort(service_calendars, ["service_id"])
    out.write_parquet(kind="canonical", name="service_calendars", df=service_calendars)

    # headway_trips
    require_columns(
        trips, table="td_headway_trips", cols=["route_id", "service_id", "trip_id"]
    )

    tid = pl.col("trip_id").cast(pl.Utf8).str.strip_chars()

    parts_us = tid.str.split("_")
    parts_dash = tid.str.split("-")
    len_us = parts_us.list.len()
    len_dash = parts_dash.list.len()

    bound_us = parts_us.list.get(1, null_on_oob=True)
    dep_us = parts_us.list.get(3, null_on_oob=True)
    bound_dash = parts_dash.list.get(1, null_on_oob=True)
    dep_dash = parts_dash.list.get(3, null_on_oob=True)

    bound_rx = tid.str.extract(r"^\s*\d+[_-]([^_-]+)[_-]\d+[_-]", 1)
    dep_rx = tid.str.extract(r"^\s*\d+[_-][^_-]+[_-]\d+[_-]([^_-]+)", 1)

    bound_raw = (
        pl.when(len_us >= 2)
        .then(bound_us)
        .when(len_dash >= 2)
        .then(bound_dash)
        .otherwise(bound_rx)
        .alias("_route_bound_raw")
    )

    dep_raw = (
        pl.when(len_us >= 4)
        .then(dep_us)
        .when(len_dash >= 4)
        .then(dep_dash)
        .otherwise(dep_rx)
        .alias("_dep_raw")
    )

    bound_u = (
        pl.col("_route_bound_raw")
        .cast(pl.Utf8)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
    )
    is_digits = bound_u.str.contains(r"^[0-9]+$")

    route_seq = (
        pl.when(is_digits)
        .then(bound_u.cast(pl.Int64, strict=False))
        .when(bound_u.is_in(["O", "OUT", "OUTBOUND", "OB"]))
        .then(pl.lit(1, dtype=pl.Int64))
        .when(bound_u.is_in(["I", "IN", "INBOUND", "IB"]))
        .then(pl.lit(2, dtype=pl.Int64))
        .otherwise(pl.lit(None, dtype=pl.Int64))
        .alias("route_seq")
    )

    dep = pl.col("_dep_raw").cast(pl.Utf8)
    dep_norm = (
        pl.when(dep.is_null() | (dep == ""))
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(dep.str.contains(":"))
        .then(dep)
        .when(dep.str.contains(r"^[0-9]{6}$"))
        .then(
            dep.str.slice(0, 2) + ":" + dep.str.slice(2, 2) + ":" + dep.str.slice(4, 2)
        )
        .when(dep.str.contains(r"^[0-9]{4}$"))
        .then(dep.str.slice(0, 2) + ":" + dep.str.slice(2, 2) + ":00")
        .otherwise(dep)
        .alias("departure_time")
    )

    headway_trips = (
        trips.select(
            pl.col("trip_id").cast(pl.Utf8),
            pl.col("route_id").cast(pl.Int64).alias("upstream_route_id"),
            pl.col("service_id").cast(pl.Int64),
        )
        .with_columns(bound_raw, dep_raw)
        .with_columns(route_seq, dep_norm)
        .drop(["_route_bound_raw", "_dep_raw"])
        .unique(subset=["trip_id"], keep="first")
    )

    headway_trips = stable_sort(
        headway_trips, ["upstream_route_id", "service_id", "trip_id"]
    )
    out.write_parquet(kind="canonical", name="headway_trips", df=headway_trips)

    # headway_frequencies
    require_columns(
        freqs,
        table="td_headway_frequencies",
        cols=["trip_id", "start_time", "end_time", "headway_secs"],
    )

    freq_join = freqs.select(
        pl.col("trip_id").cast(pl.Utf8),
        pl.col("start_time").cast(pl.Utf8),
        pl.col("end_time").cast(pl.Utf8),
        pl.col("headway_secs").cast(pl.Int64),
    ).join(
        headway_trips.select(
            ["trip_id", "upstream_route_id", "route_seq", "service_id"]
        ),
        on="trip_id",
        how="left",
    )

    freq_unresolved = freq_join.filter(pl.col("upstream_route_id").is_null()).select(
        [
            pl.lit(source_id).alias("source"),
            pl.col("trip_id"),
            pl.col("start_time"),
            pl.col("end_time"),
            pl.col("headway_secs"),
            pl.lit("missing_trip", dtype=pl.Utf8).alias("reason"),
        ]
    )

    freq_resolved = freq_join.filter(pl.col("upstream_route_id").is_not_null())

    headway_frequencies = (
        freq_resolved.group_by(
            ["upstream_route_id", "route_seq", "service_id", "start_time", "end_time"]
        )
        .agg(
            pl.col("headway_secs").min().alias("headway_secs"),
            pl.col("trip_id").first().alias("sample_trip_id"),
        )
        .select(
            [
                "upstream_route_id",
                "route_seq",
                "service_id",
                "start_time",
                "end_time",
                "headway_secs",
                "sample_trip_id",
            ]
        )
    )

    headway_frequencies = stable_sort(
        headway_frequencies,
        ["upstream_route_id", "route_seq", "service_id", "start_time", "end_time"],
    )
    out.write_parquet(
        kind="canonical", name="headway_frequencies", df=headway_frequencies
    )
    out.write_parquet(
        kind="unresolved",
        name="frequencies_unresolved_trip",
        df=stable_sort(freq_unresolved, ["trip_id"]),
    )

    # metadata
    warnings: list[JsonObject] = []
    if freq_unresolved.height > 0:
        warnings.append(
            {
                "type": "frequencies_unresolved_trip",
                "count": int(freq_unresolved.height),
            }
        )

    inputs: JsonObject = {}
    pm = parsed_root / "parsed_metadata.json"
    if pm.exists():
        inputs = json.loads(pm.read_text("utf-8"))

    meta_path = out.write_metadata(
        source_id=source_id,
        version=version,
        rules_version=RULES_VERSION,
        config={},
        inputs=inputs,
        warnings=warnings,
    )

    return NormalizeOutput(
        source_id=source_id, version=version, out_dir=out_dir, metadata_path=meta_path
    )
