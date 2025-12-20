from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class TxtTablePlan:
    table_name: str
    required_fields: set[str]
    type_hints: dict[str, pa.DataType]
    stable_sort_keys: list[str]
    known_first: list[str]


_CALENDAR_HINTS: dict[str, pa.DataType] = {
    "service_id": pa.int64(),
    "monday": pa.int8(),
    "tuesday": pa.int8(),
    "wednesday": pa.int8(),
    "thursday": pa.int8(),
    "friday": pa.int8(),
    "saturday": pa.int8(),
    "sunday": pa.int8(),
    "start_date": pa.int32(),
    "end_date": pa.int32(),
}

_TRIPS_HINTS: dict[str, pa.DataType] = {
    "route_id": pa.int64(),
    "service_id": pa.int64(),
    "trip_id": pa.string(),
}

_FREQUENCIES_HINTS: dict[str, pa.DataType] = {
    "trip_id": pa.string(),
    "start_time": pa.string(),
    "end_time": pa.string(),
    "headway_secs": pa.int32(),
}

_STOP_TIMES_HINTS: dict[str, pa.DataType] = {
    "trip_id": pa.string(),
    "arrival_time": pa.string(),
    "departure_time": pa.string(),
    "stop_id": pa.int64(),
    "stop_sequence": pa.int32(),
    "pickup_type": pa.int32(),
    "drop_off_type": pa.int32(),
    "timepoint": pa.int32(),
}

GTFS_PLANS: dict[str, TxtTablePlan] = {
    "calendar": TxtTablePlan(
        table_name="td_headway_calendar",
        required_fields={"service_id", "start_date", "end_date"},
        type_hints=_CALENDAR_HINTS,
        stable_sort_keys=["service_id"],
        known_first=[
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
    ),
    "trips": TxtTablePlan(
        table_name="td_headway_trips",
        required_fields={"route_id", "service_id", "trip_id"},
        type_hints=_TRIPS_HINTS,
        stable_sort_keys=["route_id", "service_id", "trip_id"],
        known_first=["route_id", "service_id", "trip_id"],
    ),
    "frequencies": TxtTablePlan(
        table_name="td_headway_frequencies",
        required_fields={"trip_id", "start_time", "end_time", "headway_secs"},
        type_hints=_FREQUENCIES_HINTS,
        stable_sort_keys=["trip_id", "start_time", "end_time"],
        known_first=["trip_id", "start_time", "end_time", "headway_secs"],
    ),
    "stop_times": TxtTablePlan(
        table_name="td_headway_stop_times",
        required_fields={"trip_id", "stop_id", "stop_sequence"},
        type_hints=_STOP_TIMES_HINTS,
        stable_sort_keys=["trip_id", "stop_sequence", "stop_id"],
        known_first=[
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "pickup_type",
            "drop_off_type",
            "timepoint",
        ],
    ),
}

GTFS_FILES: dict[str, str] = {
    "CALENDAR.TXT": "calendar",
    "TRIPS.TXT": "trips",
    "FREQUENCIES.TXT": "frequencies",
    "STOP_TIMES.TXT": "stop_times",
}
