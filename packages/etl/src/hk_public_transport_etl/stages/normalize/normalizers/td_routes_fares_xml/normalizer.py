from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import polars as pl
from hk_public_transport_etl.core import JsonObject, NormalizeError, stable_json_dumps

from ...common import (
    NormalizeWriter,
    drop_if_present,
    list_tables,
    read_parquet_df,
    require_columns,
    stable_sort,
)
from ...types import NormalizeContext, NormalizeOutput
from .coordinates import add_lat_lon_from_hk80
from .keys import (
    direction_id_from_route_seq,
    operator_id,
    pattern_key,
    route_key,
    sequence_fingerprint,
    stop_key,
)

FINGERPRINT_LEN = 16
NORMALIZE_RULES_VERSION = "td_routes_fares_xml.normalize.v1"
MODES = ("bus", "gmb", "ferry", "tram", "peak_tram")


@dataclass(frozen=True, slots=True)
class NormalizeConfig:
    route_seq_outbound_is_1: bool = True
    fail_on_missing_route_ref: bool = False
    fail_on_missing_stop_ref: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def clean_name(s: str | None) -> str | None:
    if s is None:
        return None
    out = s.strip()
    return out if out else None


def _pick_canonical_name(values: Iterable[str | None]) -> str | None:
    cleaned = sorted({v for v in (clean_name(x) for x in values) if v})
    return cleaned[0] if cleaned else None


# Core normalizations
def _mode_place_type(mode: str) -> str:
    if mode == "ferry":
        return "pier"
    if mode == "peak_tram":
        return "station"
    return "stop"


def _mode_primary_mode(mode: str) -> str:
    return "peak_tram" if mode == "peak_tram" else mode


def _names_by_stop_from_rstop(rstop: pl.DataFrame) -> pl.DataFrame:
    require_columns(
        rstop,
        table="RSTOP",
        cols=["STOP_ID", "STOP_NAMEC", "STOP_NAMES", "STOP_NAMEE"],
    )

    return (
        rstop.select(
            pl.col("STOP_ID").cast(pl.Utf8).alias("STOP_ID"),
            pl.col("STOP_NAMEC")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("STOP_NAMEC"),
            pl.col("STOP_NAMES")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("STOP_NAMES"),
            pl.col("STOP_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("STOP_NAMEE"),
        )
        .group_by("STOP_ID")
        .agg(
            pl.col("STOP_NAMEE").unique().alias("_en_vals"),
            pl.col("STOP_NAMEC").unique().alias("_tc_vals"),
            pl.col("STOP_NAMES").unique().alias("_sc_vals"),
        )
        .with_columns(
            pl.col("_en_vals")
            .map_elements(lambda xs: _pick_canonical_name(xs), return_dtype=pl.Utf8)
            .alias("name_en"),
            pl.col("_tc_vals")
            .map_elements(lambda xs: _pick_canonical_name(xs), return_dtype=pl.Utf8)
            .alias("name_tc"),
            pl.col("_sc_vals")
            .map_elements(lambda xs: _pick_canonical_name(xs), return_dtype=pl.Utf8)
            .alias("name_sc"),
        )
        .select(["STOP_ID", "name_en", "name_tc", "name_sc"])
    )


def _load_mode_tables(
    table_paths: dict[str, Path], mode: str
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame | None]:
    def must(name: str) -> pl.DataFrame:
        p = table_paths.get(name)
        if not p:
            raise NormalizeError(f"missing required parsed table: {name}.parquet")
        return read_parquet_df(p)

    route = must(f"td_route_{mode}")
    rstop = must(f"td_rstop_{mode}")
    stop = must(f"td_stop_{mode}")
    fare = (
        read_parquet_df(table_paths[f"td_fare_{mode}"])
        if f"td_fare_{mode}" in table_paths
        else None
    )
    return route, rstop, stop, fare


def _normalize_operators(
    _: NormalizeConfig, company_code: pl.DataFrame
) -> pl.DataFrame:
    require_columns(
        company_code,
        table="COMPANY_CODE",
        cols=["COMPANY_CODE", "COMPANY_NAMEE", "COMPANY_NAMEC", "COMPANY_NAMES"],
    )

    ops = (
        company_code.select(
            pl.col("COMPANY_CODE").cast(pl.Utf8),
            pl.col("COMPANY_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("operator_name_en"),
            pl.col("COMPANY_NAMEC")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("operator_name_tc"),
            pl.col("COMPANY_NAMES")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("operator_name_sc"),
            pl.lit(1, dtype=pl.Int8).alias("is_active"),
        )
        .with_columns(
            pl.col("COMPANY_CODE")
            .map_elements(
                lambda c: operator_id(company_code=c),
                return_dtype=pl.Utf8,
            )
            .alias("operator_id")
        )
        .select(
            [
                "operator_id",
                "operator_name_en",
                "operator_name_tc",
                "operator_name_sc",
                "is_active",
            ]
        )
    )

    return stable_sort(ops.unique(subset=["operator_id"]), ["operator_id"])


def _normalize_places_for_mode(
    _: NormalizeConfig,
    *,
    source_id: str,
    mode: str,
    stop: pl.DataFrame,
    rstop: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    require_columns(
        stop,
        table=f"STOP[{mode}]",
        cols=["STOP_ID", "X", "Y", "source_file", "source_row"],
    )
    names = _names_by_stop_from_rstop(rstop)

    places = (
        stop.select(
            pl.col("STOP_ID").cast(pl.Utf8).alias("source_stop_id"),
            pl.col("X").cast(pl.Int64).alias("hk80_x"),
            pl.col("Y").cast(pl.Int64).alias("hk80_y"),
            pl.col("source_file").cast(pl.Utf8).alias("source_file"),
            pl.col("source_row").cast(pl.Int64).alias("source_row"),
        )
        .join(names, left_on="source_stop_id", right_on="STOP_ID", how="left")
        .with_columns(
            pl.lit(_mode_place_type(mode), dtype=pl.Utf8).alias("place_type"),
            pl.lit(_mode_primary_mode(mode), dtype=pl.Utf8).alias("primary_mode"),
            pl.col("source_stop_id")
            .map_elements(
                lambda sid: stop_key(mode=mode, upstream_stop_id=sid),
                return_dtype=pl.Utf8,
            )
            .alias("place_key"),
            pl.col("name_en").alias("display_name_en"),
            pl.col("name_tc").alias("display_name_tc"),
            pl.col("name_sc").alias("display_name_sc"),
            pl.lit(None, dtype=pl.Int64).alias("parent_place_id"),
            pl.lit(1, dtype=pl.Int8).alias("is_active"),
        )
    )
    places = add_lat_lon_from_hk80(places, x_col="hk80_x", y_col="hk80_y")

    places = places.select(
        [
            "place_key",
            "place_type",
            "primary_mode",
            "name_en",
            "name_tc",
            "name_sc",
            "display_name_en",
            "display_name_tc",
            "display_name_sc",
            "lat",
            "lon",
            "hk80_x",
            "hk80_y",
            "parent_place_id",
            "is_active",
            "source_stop_id",
            "source_file",
            "source_row",
        ]
    )

    places_keyed = stable_sort(
        places.drop(["source_stop_id", "source_file", "source_row"]), ["place_key"]
    )

    map_place_source = stable_sort(
        places.select(
            pl.lit(source_id, dtype=pl.Utf8).alias("source"),
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
            pl.col("source_stop_id"),
            pl.col("source_file"),
            pl.col("source_row"),
            pl.col("place_key"),
        ),
        ["source", "mode", "source_stop_id"],
    )
    return places_keyed, map_place_source


def _normalize_routes_for_mode(
    _: NormalizeConfig, *, source_id: str, mode: str, route: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    require_columns(
        route,
        table=f"ROUTE[{mode}]",
        cols=[
            "ROUTE_ID",
            "COMPANY_CODE",
            "ROUTE_NAMEE",
            "ROUTE_NAMEC",
            "ROUTE_NAMES",
            "SERVICE_MODE",
            "SPECIAL_TYPE",
            "JOURNEY_TIME",
            "LOC_START_NAMEE",
            "LOC_END_NAMEE",
            "LOC_START_NAMEC",
            "LOC_END_NAMEC",
            "LOC_START_NAMES",
            "LOC_END_NAMES",
            "HYPERLINK_E",
            "HYPERLINK_C",
            "HYPERLINK_S",
            "FULL_FARE",
            "source_file",
            "source_row",
        ],
    )

    # Optional per-mode field(s)
    service_area_expr: pl.Expr
    if "DISTRICT" in route.columns:
        service_area_expr = pl.col("DISTRICT").cast(pl.Utf8)
    else:
        service_area_expr = pl.lit(None, dtype=pl.Utf8)

    r = (
        route.select(
            pl.col("ROUTE_ID").cast(pl.Utf8).str.strip_chars().alias("source_route_id"),
            pl.col("COMPANY_CODE").cast(pl.Utf8).alias("COMPANY_CODE"),
            pl.col("ROUTE_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("route_short_name"),
            pl.col("ROUTE_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("route_long_name_en"),
            pl.col("ROUTE_NAMEC")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("route_long_name_tc"),
            pl.col("ROUTE_NAMES")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("route_long_name_sc"),
            pl.col("LOC_START_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("origin_text_en"),
            pl.col("LOC_END_NAMEE")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("destination_text_en"),
            pl.col("LOC_START_NAMEC")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("origin_text_tc"),
            pl.col("LOC_END_NAMEC")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("destination_text_tc"),
            pl.col("LOC_START_NAMES")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("origin_text_sc"),
            pl.col("LOC_END_NAMES")
            .map_elements(clean_name, return_dtype=pl.Utf8)
            .alias("destination_text_sc"),
            service_area_expr.alias("service_area_code"),
            pl.col("JOURNEY_TIME").cast(pl.Int64).alias("journey_time_minutes"),
            pl.col("SERVICE_MODE").cast(pl.Utf8).alias("SERVICE_MODE"),
            pl.col("SPECIAL_TYPE").cast(pl.Int64).alias("SPECIAL_TYPE"),
            pl.col("HYPERLINK_E").cast(pl.Utf8).alias("HYPERLINK_E"),
            pl.col("HYPERLINK_C").cast(pl.Utf8).alias("HYPERLINK_C"),
            pl.col("HYPERLINK_S").cast(pl.Utf8).alias("HYPERLINK_S"),
            pl.col("FULL_FARE").cast(pl.Float64).alias("FULL_FARE"),
            pl.col("source_file").cast(pl.Utf8).alias("source_file"),
            pl.col("source_row").cast(pl.Int64).alias("source_row"),
        )
        .with_columns(
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
            pl.col("COMPANY_CODE")
            .map_elements(
                lambda c: operator_id(company_code=c),
                return_dtype=pl.Utf8,
            )
            .alias("operator_id"),
            pl.col("source_route_id")
            .map_elements(
                lambda rid: route_key(mode=mode, upstream_route_id=rid),
                return_dtype=pl.Utf8,
            )
            .alias("route_key"),
            pl.col("source_route_id").alias("upstream_route_id"),
            pl.lit(1, dtype=pl.Int8).alias("is_active"),
        )
        .with_columns(
            pl.col("SPECIAL_TYPE").alias("_td_special_type"),
        )
        .select(
            [
                "route_key",
                "upstream_route_id",
                "mode",
                "operator_id",
                "route_short_name",
                "route_long_name_en",
                "route_long_name_tc",
                "route_long_name_sc",
                "origin_text_en",
                "origin_text_tc",
                "origin_text_sc",
                "destination_text_en",
                "destination_text_tc",
                "destination_text_sc",
                "service_area_code",
                "journey_time_minutes",
                "is_active",
                "_td_special_type",
                "source_route_id",
                "source_file",
                "source_row",
            ]
        )
    )

    routes_keyed = stable_sort(
        r.drop(["source_route_id", "source_file", "source_row"]), ["route_key"]
    )

    map_route_source = stable_sort(
        r.select(
            pl.lit(source_id, dtype=pl.Utf8).alias("source"),
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
            pl.col("source_route_id"),
            pl.col("source_file"),
            pl.col("source_row"),
            pl.col("route_key"),
        ),
        ["source", "mode", "source_route_id"],
    )

    return routes_keyed, map_route_source


def _derive_patterns_for_mode(
    cfg: NormalizeConfig,
    *,
    source_id: str,
    mode: str,
    routes_keyed: pl.DataFrame,
    rstop: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    require_columns(
        rstop,
        table=f"RSTOP[{mode}]",
        cols=[
            "ROUTE_ID",
            "ROUTE_SEQ",
            "STOP_SEQ",
            "STOP_ID",
            "source_file",
            "source_row",
        ],
    )

    route_keys = routes_keyed.select(pl.col("route_key"))

    r = rstop.select(
        pl.col("ROUTE_ID").cast(pl.Utf8).str.strip_chars().alias("source_route_id"),
        pl.col("ROUTE_SEQ").cast(pl.Int64).alias("route_seq"),
        pl.col("STOP_SEQ").cast(pl.Int64).alias("stop_seq"),
        pl.col("STOP_ID").cast(pl.Utf8).alias("source_stop_id"),
        pl.col("source_file").cast(pl.Utf8).alias("source_file"),
        pl.col("source_row").cast(pl.Int64).alias("source_row"),
    ).with_columns(
        pl.col("source_route_id")
        .map_elements(
            lambda rid: route_key(mode=mode, upstream_route_id=rid),
            return_dtype=pl.Utf8,
        )
        .alias("route_key"),
        pl.col("source_stop_id")
        .map_elements(
            lambda sid: stop_key(mode=mode, upstream_stop_id=sid),
            return_dtype=pl.Utf8,
        )
        .alias("place_key"),
    )

    if cfg.fail_on_missing_route_ref:
        missing_routes = (
            r.select("route_key").unique().join(route_keys, on="route_key", how="anti")
        )
        if missing_routes.height > 0:
            raise NormalizeError(
                f"[{mode}] RSTOP references unknown routes: {missing_routes.head(10).to_dicts()}"
            )

    r_sorted = stable_sort(
        r, ["route_key", "route_seq", "stop_seq", "place_key", "source_row"]
    )

    grouped = (
        r_sorted.group_by(["route_key", "route_seq"])
        .agg(
            pl.col("place_key").alias("stop_keys"),
            pl.len().alias("stop_count"),
            pl.col("source_file").sort().first().alias("source_file"),
            pl.col("source_row").min().alias("source_row_min"),
        )
        .with_columns(
            pl.col("stop_keys")
            .map_elements(
                lambda xs: sequence_fingerprint(list(xs), n=16),
                return_dtype=pl.Utf8,
            )
            .alias("sequence_fingerprint")
        )
        .with_columns(
            pl.struct(["route_key", "route_seq", "sequence_fingerprint"])
            .map_elements(
                lambda s: pattern_key(
                    route_key=s["route_key"],
                    route_seq=int(s["route_seq"]),
                    fingerprint=s["sequence_fingerprint"],
                ),
                return_dtype=pl.Utf8,
            )
            .alias("pattern_key"),
            pl.col("route_seq")
            .map_elements(
                lambda rs: direction_id_from_route_seq(
                    int(rs), outbound_is_1=cfg.route_seq_outbound_is_1
                ),
                return_dtype=pl.Int64,
            )
            .alias("direction_id"),
        )
    )

    routes_for_join = routes_keyed.select(
        [
            "route_key",
            "route_short_name",
            "route_long_name_en",
            "origin_text_en",
            "origin_text_tc",
            "origin_text_sc",
            "destination_text_en",
            "destination_text_tc",
            "destination_text_sc",
            "_td_special_type",
        ]
    )

    rsu = pl.col("route_short_name").cast(pl.Utf8).fill_null("").str.to_uppercase()
    special_type = pl.col("_td_special_type").cast(pl.Int64)

    service_type_expr = (
        pl.when(pl.lit(mode).is_in(["bus", "gmb"]) & rsu.str.starts_with("N"))
        .then(pl.lit("night", dtype=pl.Utf8))
        .when(
            pl.lit(mode).is_in(["bus", "gmb"])
            & (rsu.str.ends_with("X") | rsu.str.starts_with("X"))
        )
        .then(pl.lit("express", dtype=pl.Utf8))
        .when(special_type.is_in([1, 3]))
        .then(pl.lit("special", dtype=pl.Utf8))
        .otherwise(pl.lit("regular", dtype=pl.Utf8))
    )

    patterns = (
        grouped.join(routes_for_join, on="route_key", how="left")
        .with_columns(
            pl.when(pl.col("direction_id") == 1)
            .then(pl.col("destination_text_en"))
            .when(pl.col("direction_id") == 2)
            .then(pl.col("origin_text_en"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("headsign_en"),
            pl.when(pl.col("direction_id") == 1)
            .then(pl.col("destination_text_tc"))
            .when(pl.col("direction_id") == 2)
            .then(pl.col("origin_text_tc"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("headsign_tc"),
            pl.when(pl.col("direction_id") == 1)
            .then(pl.col("destination_text_sc"))
            .when(pl.col("direction_id") == 2)
            .then(pl.col("origin_text_sc"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("headsign_sc"),
            service_type_expr.alias("service_type"),
            pl.when((pl.lit(mode) == "gmb") & (pl.col("stop_count") <= 2))
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("sequence_incomplete"),
            pl.when(pl.col("stop_count") >= 2)
            .then(
                pl.col("stop_keys").map_elements(
                    lambda xs: 1 if len(xs) != len(set(xs)) else 0, return_dtype=pl.Int8
                )
            )
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("is_circular"),
            pl.lit(1, dtype=pl.Int8).alias("is_active"),
        )
        .select(
            [
                "pattern_key",
                "route_key",
                "route_seq",
                "direction_id",
                "headsign_en",
                "headsign_tc",
                "headsign_sc",
                "service_type",
                "sequence_incomplete",
                "is_circular",
                "is_active",
                "source_file",
                "source_row_min",
            ]
        )
    )

    patterns_keyed = stable_sort(
        patterns.drop(["source_file", "source_row_min"]), ["pattern_key"]
    )

    stop_rows = r_sorted.join(
        patterns.select(["route_key", "route_seq", "pattern_key", "is_circular"]),
        on=["route_key", "route_seq"],
        how="inner",
    )
    stop_rows = stable_sort(
        stop_rows, ["pattern_key", "stop_seq", "place_key", "source_row"]
    )

    stop_rows = stop_rows.with_columns(
        pl.col("place_key").cum_count().over("pattern_key").cast(pl.Int64).alias("seq"),
        pl.col("is_circular").cast(pl.Int8).alias("allow_repeat"),
    ).select(["pattern_key", "seq", "place_key", "allow_repeat"])

    pattern_stops_keyed = stable_sort(stop_rows, ["pattern_key", "seq"])

    map_pattern_source = stable_sort(
        patterns.select(
            pl.lit(source_id, dtype=pl.Utf8).alias("source"),
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
            pl.col("route_key"),
            pl.col("pattern_key"),
            pl.col("route_seq").cast(pl.Int64).alias("route_seq"),
            pl.col("source_file"),
            pl.col("source_row_min").alias("source_row"),
        ),
        ["source", "mode", "route_key", "pattern_key"],
    )

    return patterns_keyed, pattern_stops_keyed, map_pattern_source


# Fares
def _normalize_fares_for_mode(
    cfg: NormalizeConfig,
    *,
    source_id: str,
    mode: str,
    fare: pl.DataFrame | None,
    routes_keyed: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    if fare is None or fare.height == 0:
        empty = pl.DataFrame(schema={"_": pl.Utf8}).head(0).drop("_")
        return empty, empty, empty, empty

    require_columns(
        fare,
        table=f"FARE[{mode}]",
        cols=[
            "ROUTE_ID",
            "ROUTE_SEQ",
            "ON_SEQ",
            "OFF_SEQ",
            "PRICE",
            "source_file",
            "source_row",
        ],
    )
    require_columns(
        routes_keyed, table=f"ROUTES_KEYED[{mode}]", cols=["route_key", "operator_id"]
    )

    def norm_route_id(expr: pl.Expr) -> pl.Expr:
        raw = expr.cast(pl.Utf8).str.strip_chars()
        is_digits = raw.str.contains(r"^[0-9]+$")
        stripped = raw.str.replace(r"^0+", "")
        return (
            pl.when(is_digits)
            .then(pl.when(stripped == "").then(pl.lit("0")).otherwise(stripped))
            .otherwise(raw)
        )

    product_key = f"hk:fare_product:{mode}:default"
    fare_products_keyed = pl.DataFrame(
        {
            "product_key": [product_key],
            "mode": [mode],
            "is_active": [1],
        }
    )

    routes_lookup = (
        routes_keyed.select(["route_key", "operator_id"])
        .with_columns(
            pl.col("route_key")
            .cast(pl.Utf8)
            .str.extract(r"^td:[^:]+:(.+)$", 1)
            .alias("_route_id_from_key")
        )
        .with_columns(
            norm_route_id(pl.col("_route_id_from_key")).alias("route_id_norm"),
            pl.lit(1, dtype=pl.Int8).alias("_has_route"),
        )
        .select(["route_id_norm", "route_key", "operator_id", "_has_route"])
        .unique(subset=["route_id_norm"], keep="first")
    )

    f = (
        fare.select(
            pl.col("ROUTE_ID").cast(pl.Utf8).alias("source_route_id"),
            pl.col("ROUTE_SEQ").cast(pl.Int64).alias("route_seq"),
            pl.col("ON_SEQ").cast(pl.Int64).alias("origin_seq"),
            pl.col("OFF_SEQ").cast(pl.Int64).alias("destination_seq"),
            pl.col("PRICE").cast(pl.Float64).alias("price"),
            pl.col("source_file").cast(pl.Utf8).alias("source_file"),
            pl.col("source_row").cast(pl.Int64).alias("source_row"),
        )
        .with_columns(
            norm_route_id(pl.col("source_route_id")).alias("route_id_norm"),
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
        )
        .join(routes_lookup, on="route_id_norm", how="left")
    )

    fare_orphans = f.filter(pl.col("_has_route").is_null()).select(
        [
            pl.lit(source_id, dtype=pl.Utf8).alias("source"),
            pl.lit(mode, dtype=pl.Utf8).alias("mode"),
            pl.col("source_route_id"),
            pl.col("route_id_norm"),
            pl.col("route_seq"),
            pl.col("origin_seq"),
            pl.col("destination_seq"),
            pl.col("price"),
            pl.col("source_file"),
            pl.col("source_row"),
            pl.lit("missing_route", dtype=pl.Utf8).alias("reason"),
        ]
    )

    if fare_orphans.height > 0 and cfg.fail_on_missing_route_ref:
        missing_ids = (
            fare_orphans.select("route_id_norm")
            .unique()
            .sort("route_id_norm")
            .to_series()
            .to_list()
        )
        sample_rows = fare_orphans.head(50).to_dicts()
        raise NormalizeError(
            f"[{mode}] FARE references ROUTE_IDs not present in ROUTE_{mode}. "
            f"missing_ids={missing_ids} sample_rows={sample_rows}"
        )

    if fare_orphans.height > 0 and not cfg.fail_on_missing_route_ref:
        f = f.filter(pl.col("_has_route").is_not_null())

    def price_to_cents(v: float) -> int:
        return int(round(float(v) * 100.0))

    fare_rules_keyed = f.with_columns(
        pl.struct(
            ["operator_id", "route_key", "route_seq", "origin_seq", "destination_seq"]
        )
        .map_elements(
            lambda s: (
                f"td:fare_rule:{mode}:{s['operator_id']}:{s['route_key']}:"
                f"{int(s['route_seq'])}:{int(s['origin_seq'])}:{int(s['destination_seq'])}:"
            ),
            return_dtype=pl.Utf8,
        )
        .alias("rule_key"),
        pl.lit(None, dtype=pl.Int64).alias("pattern_id"),
        pl.lit("section", dtype=pl.Utf8).alias("fare_type"),
        pl.lit("HKD", dtype=pl.Utf8).alias("currency"),
        pl.lit(1, dtype=pl.Int8).alias("is_active"),
    ).select(
        [
            "rule_key",
            "operator_id",
            "mode",
            "route_key",
            "pattern_id",
            "origin_seq",
            "destination_seq",
            "fare_type",
            "currency",
            "is_active",
            "price",
        ]
    )

    fare_amounts_keyed = (
        fare_rules_keyed.select(["rule_key", "price"])
        .with_columns(
            pl.lit(product_key, dtype=pl.Utf8).alias("product_key"),
            pl.col("price")
            .map_elements(price_to_cents, return_dtype=pl.Int64)
            .alias("amount_cents"),
            pl.lit(1, dtype=pl.Int8).alias("is_default"),
        )
        .select(["rule_key", "product_key", "amount_cents", "is_default"])
    )

    return (
        stable_sort(fare_products_keyed, ["product_key"]),
        stable_sort(fare_rules_keyed.drop("price"), ["rule_key"]),
        stable_sort(fare_amounts_keyed, ["rule_key", "product_key"]),
        stable_sort(
            fare_orphans,
            ["source", "mode", "route_id_norm", "source_file", "source_row"],
        ),
    )


# Entry point
def normalize_td_routes_fares_xml(ctx: NormalizeContext) -> NormalizeOutput:
    cfg = NormalizeConfig()

    source_id = ctx.source_id
    version = ctx.version
    data_root = Path(ctx.data_root)

    parsed_root = data_root / "staged" / source_id / version
    parsed_tables_dir = parsed_root / "tables"
    table_paths = list_tables(parsed_tables_dir)

    if "td_company_code" not in table_paths:
        raise NormalizeError("missing required parsed table: td_company_code.parquet")
    company_code = read_parquet_df(table_paths["td_company_code"])
    operators = _normalize_operators(cfg, company_code)

    fare_orphans_all: list[pl.DataFrame] = []

    places_all: list[pl.DataFrame] = []
    map_place_all: list[pl.DataFrame] = []
    routes_all: list[pl.DataFrame] = []
    map_route_all: list[pl.DataFrame] = []
    patterns_all: list[pl.DataFrame] = []
    pattern_stops_all: list[pl.DataFrame] = []
    map_pattern_all: list[pl.DataFrame] = []
    fare_products_all: list[pl.DataFrame] = []
    fare_rules_all: list[pl.DataFrame] = []
    fare_amounts_all: list[pl.DataFrame] = []

    for mode in MODES:
        if f"td_route_{mode}" not in table_paths:
            continue

        route, rstop, stop, fare = _load_mode_tables(table_paths, mode)

        places_keyed, map_place = _normalize_places_for_mode(
            cfg, source_id=source_id, mode=mode, stop=stop, rstop=rstop
        )
        routes_keyed, map_route = _normalize_routes_for_mode(
            cfg, source_id=source_id, mode=mode, route=route
        )
        patterns_keyed, pattern_stops_keyed, map_pattern = _derive_patterns_for_mode(
            cfg, source_id=source_id, mode=mode, routes_keyed=routes_keyed, rstop=rstop
        )

        fp, fr, fa, fo = _normalize_fares_for_mode(
            cfg, source_id=source_id, mode=mode, fare=fare, routes_keyed=routes_keyed
        )
        if fo.height > 0:
            fare_orphans_all.append(fo)

        places_all.append(places_keyed)
        map_place_all.append(map_place)
        routes_all.append(routes_keyed)
        map_route_all.append(map_route)
        patterns_all.append(patterns_keyed)
        pattern_stops_all.append(pattern_stops_keyed)
        map_pattern_all.append(map_pattern)

        if fp.height > 0:
            fare_products_all.append(fp)
            fare_rules_all.append(fr)
            fare_amounts_all.append(fa)

    if not places_all or not routes_all:
        raise NormalizeError(
            "no mode tables found to normalize (expected td_route_{mode}.parquet etc.)"
        )

    places_keyed = stable_sort(pl.concat(places_all, how="vertical"), ["place_key"])
    routes_keyed = stable_sort(pl.concat(routes_all, how="vertical"), ["route_key"])
    patterns_keyed = stable_sort(
        pl.concat(patterns_all, how="vertical"), ["pattern_key"]
    )
    pattern_stops_keyed = stable_sort(
        pl.concat(pattern_stops_all, how="vertical"), ["pattern_key", "seq"]
    )

    map_place = stable_sort(
        pl.concat(map_place_all, how="vertical"), ["source", "mode", "source_stop_id"]
    )
    map_route = stable_sort(
        pl.concat(map_route_all, how="vertical"), ["source", "mode", "source_route_id"]
    )
    map_pattern = stable_sort(
        pl.concat(map_pattern_all, how="vertical"),
        ["source", "mode", "route_key", "pattern_key"],
    )

    # Deterministic integer IDs
    places = places_keyed.with_row_index(name="place_id", offset=1)
    routes = routes_keyed.with_row_index(name="route_id", offset=1)

    patterns = patterns_keyed.join(
        routes.select(["route_key", "route_id"]), on="route_key", how="left"
    )
    if patterns.filter(pl.col("route_id").is_null()).height > 0:
        raise NormalizeError("pattern->route join failed (missing route_id)")
    patterns = patterns.with_row_index(name="pattern_id", offset=1)

    # Resolve pattern_stops ids
    pattern_ids = patterns.select(["pattern_key", "pattern_id"])
    place_ids = places.select(["place_key", "place_id"])

    pattern_stops = (
        pattern_stops_keyed.join(pattern_ids, on="pattern_key", how="left")
        .join(place_ids, on="place_key", how="left")
        .select(["pattern_id", "seq", "place_id", "allow_repeat"])
    )

    if pattern_stops.filter(pl.col("pattern_id").is_null()).height > 0:
        raise NormalizeError("pattern_stops contains unresolved pattern_id")
    if (
        cfg.fail_on_missing_stop_ref
        and pattern_stops.filter(pl.col("place_id").is_null()).height > 0
    ):
        raise NormalizeError(
            "pattern_stops contains unresolved place_id (missing stop reference)"
        )
    pattern_stops = stable_sort(pattern_stops, ["pattern_id", "seq"])

    # Mapping tables with numeric IDs
    map_place2 = map_place.join(
        places.select(["place_key", "place_id"]), on="place_key", how="left"
    ).select(
        [
            "source",
            "mode",
            "source_stop_id",
            "source_file",
            "source_row",
            "place_id",
            "place_key",
        ]
    )
    map_route2 = map_route.join(
        routes.select(["route_key", "route_id"]), on="route_key", how="left"
    ).select(
        [
            "source",
            "mode",
            "source_route_id",
            "source_file",
            "source_row",
            "route_id",
            "route_key",
        ]
    )
    map_pattern2 = map_pattern.join(pattern_ids, on="pattern_key", how="left").select(
        [
            "source",
            "mode",
            "route_key",
            "pattern_id",
            "pattern_key",
            "route_seq",
            "source_file",
            "source_row",
        ]
    )

    # Fares: resolve deterministic IDs + unresolved outputs
    fare_rules_unresolved_route: pl.DataFrame | None = None
    fare_amounts_unresolved_ids: pl.DataFrame | None = None

    if fare_products_all:
        fare_products = stable_sort(
            pl.concat(fare_products_all, how="vertical").unique(subset=["product_key"]),
            ["product_key"],
        ).with_row_index(name="fare_product_id", offset=1)

        fare_rules_keyed = stable_sort(
            pl.concat(fare_rules_all, how="vertical"), ["rule_key"]
        )
        if "route_id" in fare_rules_keyed.columns:
            fare_rules_keyed = fare_rules_keyed.drop("route_id")

        fare_rules_joined = fare_rules_keyed.join(
            routes.select(["route_key", "route_id"]),
            on="route_key",
            how="left",
            suffix="_right",
        )

        if (
            "route_id_right" in fare_rules_joined.columns
            and "route_id" in fare_rules_joined.columns
        ):
            fare_rules_joined = fare_rules_joined.with_columns(
                pl.coalesce([pl.col("route_id"), pl.col("route_id_right")]).alias(
                    "route_id"
                )
            ).drop("route_id_right")
        elif (
            "route_id_right" in fare_rules_joined.columns
            and "route_id" not in fare_rules_joined.columns
        ):
            fare_rules_joined = fare_rules_joined.rename({"route_id_right": "route_id"})

        fare_rules_unresolved_route = fare_rules_joined.filter(
            pl.col("route_id").is_null()
        )
        if fare_rules_unresolved_route.height > 0 and cfg.fail_on_missing_route_ref:
            sample = (
                fare_rules_unresolved_route.select(
                    [
                        "rule_key",
                        "mode",
                        "operator_id",
                        "route_key",
                        "origin_seq",
                        "destination_seq",
                    ]
                )
                .head(50)
                .to_dicts()
            )
            raise NormalizeError(
                f"fare_rules contains unresolved route_id (sample={sample})"
            )

        fare_rules_resolved = fare_rules_joined.filter(pl.col("route_id").is_not_null())
        fare_rules = fare_rules_resolved.drop("route_key").with_row_index(
            name="fare_rule_id", offset=1
        )

        fare_amounts_keyed = stable_sort(
            pl.concat(fare_amounts_all, how="vertical"), ["rule_key", "product_key"]
        )
        fare_amounts_joined = (
            fare_amounts_keyed.join(
                fare_rules.select(["rule_key", "fare_rule_id"]),
                on="rule_key",
                how="left",
            )
            .join(
                fare_products.select(["product_key", "fare_product_id"]),
                on="product_key",
                how="left",
            )
            .with_columns(
                (
                    pl.col("fare_rule_id").is_null()
                    | pl.col("fare_product_id").is_null()
                ).alias("_unresolved")
            )
        )

        fare_amounts_unresolved_ids = fare_amounts_joined.filter(
            pl.col("_unresolved")
        ).drop("_unresolved")
        if fare_amounts_unresolved_ids.height > 0 and cfg.fail_on_missing_route_ref:
            sample = fare_amounts_unresolved_ids.head(50).to_dicts()
            raise NormalizeError(
                f"fare_amounts contains unresolved IDs (sample={sample})"
            )

        fare_amounts = fare_amounts_joined.filter(~pl.col("_unresolved")).select(
            ["fare_rule_id", "fare_product_id", "amount_cents", "is_default"]
        )
    else:
        fare_products = pl.DataFrame(schema={"fare_product_id": pl.Int64}).head(0)
        fare_rules = pl.DataFrame(schema={"fare_rule_id": pl.Int64}).head(0)
        fare_amounts = pl.DataFrame(schema={"fare_rule_id": pl.Int64}).head(0)

    # Write outputs
    out_dir = data_root / "normalized" / source_id / version
    w = NormalizeWriter(out_dir=out_dir)

    # canonical
    w.write_parquet(kind="canonical", name="operators", df=operators)
    w.write_parquet(
        kind="canonical", name="places", df=stable_sort(places, ["place_id"])
    )

    routes_out = drop_if_present(routes, ["_td_service_mode", "_td_special_type"])
    w.write_parquet(
        kind="canonical", name="routes", df=stable_sort(routes_out, ["route_id"])
    )

    w.write_parquet(
        kind="canonical",
        name="route_patterns",
        df=stable_sort(
            patterns.select(
                [
                    "pattern_id",
                    "pattern_key",
                    "route_id",
                    "route_seq",
                    "direction_id",
                    "headsign_en",
                    "headsign_tc",
                    "headsign_sc",
                    "service_type",
                    "sequence_incomplete",
                    "is_circular",
                    "is_active",
                ]
            ),
            ["pattern_id"],
        ),
    )
    w.write_parquet(kind="canonical", name="pattern_stops", df=pattern_stops)

    if fare_products.height > 0:
        w.write_parquet(
            kind="canonical",
            name="fare_products",
            df=stable_sort(fare_products, ["fare_product_id"]),
        )
        w.write_parquet(
            kind="canonical",
            name="fare_rules",
            df=stable_sort(fare_rules, ["fare_rule_id"]),
        )
        w.write_parquet(
            kind="canonical",
            name="fare_amounts",
            df=stable_sort(fare_amounts, ["fare_rule_id", "fare_product_id"]),
        )

    # mappings
    w.write_parquet(kind="mapping", name="map_place_source", df=map_place2)
    w.write_parquet(kind="mapping", name="map_route_source", df=map_route2)
    w.write_parquet(kind="mapping", name="map_pattern_source", df=map_pattern2)

    # unresolved
    if fare_orphans_all:
        fare_orphans = stable_sort(
            pl.concat(fare_orphans_all, how="vertical"),
            ["source", "mode", "route_id_norm", "source_file", "source_row"],
        )
        w.write_parquet(kind="unresolved", name="fare_orphans", df=fare_orphans)

    if (
        fare_rules_unresolved_route is not None
        and fare_rules_unresolved_route.height > 0
    ):
        w.write_parquet(
            kind="unresolved",
            name="fare_rules_unresolved_route",
            df=stable_sort(
                fare_rules_unresolved_route.select(
                    [
                        "rule_key",
                        "mode",
                        "operator_id",
                        "route_key",
                        "origin_seq",
                        "destination_seq",
                        "fare_type",
                        "currency",
                        "is_active",
                    ]
                ),
                ["mode", "route_key", "rule_key"],
            ),
        )

    if (
        fare_amounts_unresolved_ids is not None
        and fare_amounts_unresolved_ids.height > 0
    ):
        w.write_parquet(
            kind="unresolved",
            name="fare_amounts_unresolved_ids",
            df=stable_sort(fare_amounts_unresolved_ids, ["rule_key", "product_key"]),
        )

    parsed_meta_path = parsed_root / "parsed_metadata.json"
    inputs: JsonObject = {}
    if parsed_meta_path.exists():
        inputs = json.loads(parsed_meta_path.read_text("utf-8"))

    warnings: list[JsonObject] = []
    if fare_orphans_all:
        warnings.append(
            {
                "type": "fare_orphans",
                "count": int(pl.concat(fare_orphans_all, how="vertical").height),
                "note": "FARE references ROUTE_IDs missing from ROUTE_*; emitted to unresolved/fare_orphans.parquet",
            }
        )
    if (
        fare_rules_unresolved_route is not None
        and fare_rules_unresolved_route.height > 0
    ):
        warnings.append(
            {
                "type": "fare_rules_unresolved_route",
                "count": int(fare_rules_unresolved_route.height),
                "note": "Dropped fare_rules that could not resolve route_id; emitted to unresolved/fare_rules_unresolved_route.parquet",
            }
        )
    if (
        fare_amounts_unresolved_ids is not None
        and fare_amounts_unresolved_ids.height > 0
    ):
        warnings.append(
            {
                "type": "fare_amounts_unresolved_ids",
                "count": int(fare_amounts_unresolved_ids.height),
                "note": "Dropped fare_amounts that could not resolve fare_rule_id/fare_product_id; emitted to unresolved/fare_amounts_unresolved_ids.parquet",
            }
        )

    meta_path = w.write_metadata(
        source_id=source_id,
        version=version,
        rules_version=NORMALIZE_RULES_VERSION,
        config=json.loads(stable_json_dumps(cfg.to_dict())),
        inputs=inputs,
        warnings=warnings,
    )

    return NormalizeOutput(
        source_id=source_id, version=version, out_dir=out_dir, metadata_path=meta_path
    )
