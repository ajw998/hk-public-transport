from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .types import Severity

ColumnRef = str | tuple[str, ...]


@dataclass(frozen=True)
class UniquenessSpec:
    columns: tuple[ColumnRef, ...]
    code: str
    severity: Severity = Severity.ERROR
    message: str | None = None


@dataclass(frozen=True)
class ForeignKeySpec:
    child_table: str
    child_col: ColumnRef
    parent_table: str
    parent_col: ColumnRef
    code: str
    severity: Severity = Severity.ERROR
    message: str | None = None
    only_check_non_null: bool = True
    hint_join_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class TableSpec:
    required_columns: tuple[ColumnRef, ...]
    not_null_columns: tuple[ColumnRef, ...]
    sample_sort: tuple[ColumnRef, ...]
    uniqueness: tuple[UniquenessSpec, ...] = ()


@dataclass(frozen=True)
class UnresolvedSpec:
    fail_if_nonempty: bool = True
    sample_sort: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationSpec:
    spec_id: str
    spec_version: str
    core_tables: frozenset[str]
    required_tables: frozenset[str]
    tables: Mapping[str, TableSpec]
    foreign_keys: Sequence[ForeignKeySpec]
    unresolved: Mapping[str, UnresolvedSpec]


# Canonical base
def _canonical_base_spec() -> ValidationSpec:
    tables: dict[str, TableSpec] = {
        "operators": TableSpec(
            required_columns=("operator_id",),
            not_null_columns=("operator_id",),
            sample_sort=("operator_id",),
            uniqueness=(
                UniquenessSpec(columns=("operator_id",), code="OPERATOR_ID_NOT_UNIQUE"),
            ),
        ),
        "places": TableSpec(
            required_columns=("place_id", ("place_key", "stop_key")),
            not_null_columns=("place_id", ("place_key", "stop_key")),
            sample_sort=("place_id",),
            uniqueness=(
                UniquenessSpec(columns=("place_id",), code="PLACE_ID_NOT_UNIQUE"),
                UniquenessSpec(
                    columns=(("place_key", "stop_key"),), code="PLACE_KEY_NOT_UNIQUE"
                ),
            ),
        ),
        "routes": TableSpec(
            required_columns=("route_id", "route_key", "operator_id"),
            not_null_columns=("route_id", "route_key", "operator_id"),
            sample_sort=("route_id",),
            uniqueness=(
                UniquenessSpec(columns=("route_id",), code="ROUTE_ID_NOT_UNIQUE"),
                UniquenessSpec(columns=("route_key",), code="ROUTE_KEY_NOT_UNIQUE"),
            ),
        ),
        "route_patterns": TableSpec(
            required_columns=("pattern_id", ("pattern_key",), "route_id"),
            not_null_columns=("pattern_id", "route_id"),
            sample_sort=("pattern_id",),
            uniqueness=(
                UniquenessSpec(columns=("pattern_id",), code="PATTERN_ID_NOT_UNIQUE"),
                UniquenessSpec(
                    columns=(("pattern_key",),), code="PATTERN_KEY_NOT_UNIQUE"
                ),
            ),
        ),
        "pattern_stops": TableSpec(
            required_columns=("pattern_id", "seq", "place_id"),
            not_null_columns=("pattern_id", "seq", "place_id"),
            sample_sort=("pattern_id", "seq"),
            uniqueness=(
                UniquenessSpec(
                    columns=("pattern_id", "seq"), code="PATTERN_STOP_SEQ_NOT_UNIQUE"
                ),
            ),
        ),
        # Fare tables are defined here so their column/uniqueness checks exist,
        # but they are only REQUIRED by the td_routes_fares_xml spec.
        "fare_products": TableSpec(
            required_columns=("fare_product_id",),
            not_null_columns=("fare_product_id",),
            sample_sort=("fare_product_id",),
            uniqueness=(
                UniquenessSpec(
                    columns=("fare_product_id",), code="FARE_PRODUCT_ID_NOT_UNIQUE"
                ),
            ),
        ),
        "fare_rules": TableSpec(
            required_columns=("fare_rule_id",),
            not_null_columns=("fare_rule_id",),
            sample_sort=("fare_rule_id",),
            uniqueness=(
                UniquenessSpec(
                    columns=("fare_rule_id",), code="FARE_RULE_ID_NOT_UNIQUE"
                ),
                UniquenessSpec(
                    columns=(("rule_key",),), code="FARE_RULE_KEY_NOT_UNIQUE"
                ),
            ),
        ),
        "fare_amounts": TableSpec(
            required_columns=("fare_rule_id",),
            not_null_columns=("fare_rule_id",),
            sample_sort=("fare_rule_id",),
            uniqueness=(
                UniquenessSpec(
                    columns=("fare_rule_id", ("fare_product_id",)),
                    code="FARE_AMOUNT_PK_NOT_UNIQUE",
                ),
            ),
        ),
    }

    fks: list[ForeignKeySpec] = [
        ForeignKeySpec(
            child_table="routes",
            child_col="operator_id",
            parent_table="operators",
            parent_col="operator_id",
            code="ROUTE_MISSING_OPERATOR",
            hint_join_keys=("route_id",),
        ),
        ForeignKeySpec(
            child_table="route_patterns",
            child_col="route_id",
            parent_table="routes",
            parent_col="route_id",
            code="PATTERN_MISSING_ROUTE",
            hint_join_keys=("pattern_id", "route_id"),
        ),
        ForeignKeySpec(
            child_table="pattern_stops",
            child_col="pattern_id",
            parent_table="route_patterns",
            parent_col="pattern_id",
            code="PATTERN_STOP_MISSING_PATTERN",
            hint_join_keys=("pattern_id",),
        ),
        ForeignKeySpec(
            child_table="pattern_stops",
            child_col="place_id",
            parent_table="places",
            parent_col="place_id",
            code="PATTERN_STOP_MISSING_PLACE",
            hint_join_keys=("pattern_id", "place_id"),
        ),
        ForeignKeySpec(
            child_table="fare_amounts",
            child_col="fare_rule_id",
            parent_table="fare_rules",
            parent_col="fare_rule_id",
            code="FARE_AMOUNT_MISSING_RULE",
            hint_join_keys=("fare_rule_id",),
        ),
        ForeignKeySpec(
            child_table="fare_amounts",
            child_col=("fare_product_id",),
            parent_table="fare_products",
            parent_col="fare_product_id",
            code="FARE_AMOUNT_MISSING_PRODUCT",
            hint_join_keys=("fare_rule_id", "fare_product_id"),
        ),
        ForeignKeySpec(
            child_table="fare_rules",
            child_col=("route_id",),
            parent_table="routes",
            parent_col="route_id",
            code="FARE_RULE_MISSING_ROUTE",
            only_check_non_null=True,
            hint_join_keys=("fare_rule_id", "route_id"),
        ),
        ForeignKeySpec(
            child_table="fare_rules",
            child_col=("pattern_id",),
            parent_table="route_patterns",
            parent_col="pattern_id",
            code="FARE_RULE_MISSING_PATTERN",
            only_check_non_null=True,
            hint_join_keys=("fare_rule_id", "pattern_id"),
        ),
    ]

    core = frozenset(
        {"operators", "places", "routes", "route_patterns", "pattern_stops"}
    )
    return ValidationSpec(
        spec_id="canonical_base",
        spec_version="1.0",
        core_tables=core,
        required_tables=core,
        tables=tables,
        foreign_keys=fks,
        unresolved={},
    )


def td_routes_fares_xml_spec() -> ValidationSpec:
    base = _canonical_base_spec()

    required = set(base.required_tables) | {
        "fare_products",
        "fare_rules",
        "fare_amounts",
    }
    unresolved = dict(base.unresolved)
    unresolved["fare_orphans"] = UnresolvedSpec(
        fail_if_nonempty=True,
        sample_sort=("mode", "route_id_norm", "source_file", "source_row"),
    )

    return ValidationSpec(
        spec_id="td_routes_fares_xml",
        spec_version="1.0",
        core_tables=base.core_tables,
        required_tables=frozenset(required),
        tables=base.tables,
        foreign_keys=base.foreign_keys,
        unresolved=unresolved,
    )


def td_pt_headway_en_spec() -> ValidationSpec:
    tables: dict[str, TableSpec] = {
        "service_calendars": TableSpec(
            required_columns=("service_id",),
            not_null_columns=("service_id",),
            sample_sort=("service_id",),
            uniqueness=(
                UniquenessSpec(columns=("service_id",), code="SERVICE_ID_NOT_UNIQUE"),
            ),
        ),
        "headway_trips": TableSpec(
            required_columns=("trip_id", "upstream_route_id", "service_id"),
            not_null_columns=("trip_id", "upstream_route_id", "service_id"),
            sample_sort=("upstream_route_id", "service_id", "trip_id"),
            uniqueness=(
                UniquenessSpec(columns=("trip_id",), code="TRIP_ID_NOT_UNIQUE"),
            ),
        ),
        "headway_frequencies": TableSpec(
            required_columns=(
                "upstream_route_id",
                "service_id",
                "start_time",
                "end_time",
                "headway_secs",
            ),
            not_null_columns=(
                "upstream_route_id",
                "service_id",
                "start_time",
                "end_time",
                "headway_secs",
            ),
            sample_sort=("upstream_route_id", "service_id", "start_time", "end_time"),
            uniqueness=(
                UniquenessSpec(
                    columns=(
                        "upstream_route_id",
                        ("route_seq",),
                        "service_id",
                        "start_time",
                        "end_time",
                    ),
                    code="HEADWAY_FREQ_KEY_NOT_UNIQUE",
                ),
            ),
        ),
    }

    fks: list[ForeignKeySpec] = [
        ForeignKeySpec(
            child_table="headway_trips",
            child_col="service_id",
            parent_table="service_calendars",
            parent_col="service_id",
            code="TRIP_MISSING_SERVICE",
            hint_join_keys=("trip_id", "service_id"),
        ),
        ForeignKeySpec(
            child_table="headway_frequencies",
            child_col="service_id",
            parent_table="service_calendars",
            parent_col="service_id",
            code="FREQ_MISSING_SERVICE",
            hint_join_keys=("upstream_route_id", "service_id"),
        ),
        ForeignKeySpec(
            child_table="headway_frequencies",
            child_col=("sample_trip_id",),
            parent_table="headway_trips",
            parent_col="trip_id",
            code="FREQ_SAMPLE_TRIP_MISSING",
            severity=Severity.WARN,
            only_check_non_null=True,
            hint_join_keys=("upstream_route_id", "service_id"),
        ),
    ]

    unresolved: dict[str, UnresolvedSpec] = {
        # "trip_id_parse_failures": UnresolvedSpec(fail_if_nonempty=True, sample_sort=("source_file","source_row")),
        # "frequency_orphans": UnresolvedSpec(fail_if_nonempty=True, sample_sort=("upstream_route_id","service_id")),
    }

    core = frozenset({"service_calendars", "headway_trips", "headway_frequencies"})
    return ValidationSpec(
        spec_id="td_pt_headway_en",
        spec_version="1.0",
        core_tables=core,
        required_tables=core,
        tables=tables,
        foreign_keys=fks,
        unresolved=unresolved,
    )
