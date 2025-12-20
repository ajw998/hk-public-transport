from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class XmlTablePlan:
    table_name: str
    record_tag_hint: str
    required_fields: set[str]
    type_hints: dict[str, pa.DataType]
    stable_sort_keys: list[str]
    known_first: list[str]
    extra_constant_cols: dict[str, str]


@dataclass(frozen=True, slots=True)
class ModePlanSpec:
    table_prefix: str
    record_tag_hint: str
    required_fields: set[str]
    type_hints: dict[str, pa.DataType]
    stable_sort_keys: list[str]
    known_first: list[str]


COMMON_TYPE_HINTS: dict[str, pa.DataType] = {
    "LAST_UPDATE_DATE": pa.timestamp("ms"),
    "LAST_UPDATED_DATE": pa.timestamp("ms"),
}

STOP_TYPE_HINTS: dict[str, pa.DataType] = {
    "STOP_TYPE": pa.int32(),
    "X": pa.int32(),
    "Y": pa.int32(),
}

ROUTE_TYPE_HINTS: dict[str, pa.DataType] = {
    "ROUTE_TYPE": pa.int32(),
    "SPECIAL_TYPE": pa.int32(),
    "JOURNEY_TIME": pa.int32(),
    "FULL_FARE": pa.float64(),
}

RSTOP_TYPE_HINTS: dict[str, pa.DataType] = {
    "ROUTE_SEQ": pa.int32(),
    "STOP_SEQ": pa.int32(),
    "STOP_PICK_DROP": pa.int32(),
}

FARE_TYPE_HINTS: dict[str, pa.DataType] = {
    "ROUTE_SEQ": pa.int32(),
    "DAY_CODE": pa.int32(),
    "ON_SEQ": pa.int32(),
    "OFF_SEQ": pa.int32(),
    "PRICE": pa.float64(),
}

MODE_PLAN_SPECS: dict[str, ModePlanSpec] = {
    "STOP": ModePlanSpec(
        table_prefix="td_stop",
        record_tag_hint="STOP",
        required_fields={"STOP_ID"},
        type_hints={**COMMON_TYPE_HINTS, **STOP_TYPE_HINTS},
        stable_sort_keys=["STOP_ID"],
        known_first=[
            "STOP_ID",
            "STOP_TYPE",
            "X",
            "Y",
            "LAST_UPDATE_DATE",
            "STOP_NAMEC",
            "STOP_NAMES",
            "STOP_NAMEE",
        ],
    ),
    "ROUTE": ModePlanSpec(
        table_prefix="td_route",
        record_tag_hint="ROUTE",
        required_fields={"ROUTE_ID", "COMPANY_CODE"},
        type_hints={**COMMON_TYPE_HINTS, **ROUTE_TYPE_HINTS},
        stable_sort_keys=["ROUTE_ID"],
        known_first=[
            "ROUTE_ID",
            "COMPANY_CODE",
            "DISTRICT",
            "ROUTE_NAMEC",
            "ROUTE_NAMES",
            "ROUTE_NAMEE",
            "ROUTE_TYPE",
            "SERVICE_MODE",
            "SPECIAL_TYPE",
            "JOURNEY_TIME",
            "LOC_START_NAMEC",
            "LOC_START_NAMES",
            "LOC_START_NAMEE",
            "LOC_END_NAMEC",
            "LOC_END_NAMES",
            "LOC_END_NAMEE",
            "HYPERLINK_C",
            "HYPERLINK_S",
            "HYPERLINK_E",
            "FULL_FARE",
            "LAST_UPDATE_DATE",
        ],
    ),
    "RSTOP": ModePlanSpec(
        table_prefix="td_rstop",
        record_tag_hint="RSTOP",
        required_fields={"ROUTE_ID", "ROUTE_SEQ", "STOP_SEQ", "STOP_ID"},
        type_hints={**COMMON_TYPE_HINTS, **RSTOP_TYPE_HINTS},
        stable_sort_keys=["ROUTE_ID", "ROUTE_SEQ", "STOP_SEQ", "STOP_ID"],
        known_first=[
            "ROUTE_ID",
            "ROUTE_SEQ",
            "STOP_SEQ",
            "STOP_ID",
            "STOP_NAMEC",
            "STOP_NAMES",
            "STOP_NAMEE",
            "STOP_PICK_DROP",
            "LAST_UPDATE_DATE",
        ],
    ),
    "FARE": ModePlanSpec(
        table_prefix="td_fare",
        record_tag_hint="FARE",
        required_fields={"ROUTE_ID", "ROUTE_SEQ", "ON_SEQ", "OFF_SEQ", "PRICE"},
        type_hints={**COMMON_TYPE_HINTS, **FARE_TYPE_HINTS},
        stable_sort_keys=["ROUTE_ID", "ROUTE_SEQ", "DAY_CODE", "ON_SEQ", "OFF_SEQ"],
        known_first=[
            "ROUTE_ID",
            "ROUTE_SEQ",
            "ON_SEQ",
            "OFF_SEQ",
            "PRICE",
            "LAST_UPDATE_DATE",
        ],
    ),
}


def company_code_plan() -> XmlTablePlan:
    return XmlTablePlan(
        table_name="td_company_code",
        record_tag_hint="COMPANY_CODE",
        required_fields={"COMPANY_CODE"},
        type_hints=COMMON_TYPE_HINTS,
        stable_sort_keys=["COMPANY_CODE"],
        known_first=[
            "COMPANY_CODE",
            "COMPANY_NAMEC",
            "COMPANY_NAMES",
            "COMPANY_NAMEE",
            "DESCRIPTION",
            "LAST_UPDATE_DATE",
        ],
        extra_constant_cols={},
    )
