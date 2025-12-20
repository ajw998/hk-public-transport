from __future__ import annotations

from typing import Final, Protocol

from .normalizers.td_pt_headway_gtfs_en.normalizer import (
    normalize_td_pt_headway_gtfs_en,
)
from .normalizers.td_routes_fares_xml.normalizer import normalize_td_routes_fares_xml
from .types import NormalizeFn

SKIP: Final[dict[str, str]] = {
    "td_routes_fares_delta": "delta feed not supported yet (skipped)",
}

NORMALIZERS: Final[dict[str, NormalizeFn]] = {
    "td_routes_fares_xml": normalize_td_routes_fares_xml,
    "td_pt_headway_gtfs_en": normalize_td_pt_headway_gtfs_en,
}


def get_normalizer(source_id: str) -> NormalizeFn | None:
    return NORMALIZERS.get(source_id)


def should_skip(source_id: str) -> str | None:
    return SKIP.get(source_id)
