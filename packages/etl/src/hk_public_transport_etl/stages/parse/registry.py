from __future__ import annotations

from hk_public_transport_etl.core.errors import ParseError

from .parsers.td_pt_headway_gtfs_en.parser import parse as parse_td_pt_headway_gtfs_en
from .parsers.td_routes_fares_xml.parser import parse as parse_td_routes_fares_xml
from .types import ParserFn, SkipPolicy

_SKIP: dict[str, SkipPolicy] = {
    "td_routes_fares_delta": SkipPolicy(
        reason="delta parser not implemented yet (skipping)"
    ),
}


def skip_reason(parser_id: str) -> str | None:
    p = _SKIP.get(parser_id)
    return p.reason if p else None


PARSERS: dict[str, ParserFn] = {
    "td_routes_fares_xml": parse_td_routes_fares_xml,
    "td_pt_headway_gtfs_en": parse_td_pt_headway_gtfs_en,
    "td_routes_fares_delta": SkipPolicy(
        reason="delta parser not implemented yet (skipping)"
    ),
}


def get_parser(parser_id: str) -> ParserFn:
    fn = PARSERS.get(parser_id)
    if fn is None:
        raise ParseError(f"No parser registered for parser_id={parser_id!r}")
    return fn
