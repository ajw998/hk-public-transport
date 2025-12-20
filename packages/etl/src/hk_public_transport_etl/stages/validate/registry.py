from typing import Callable, Final

from .specs import ValidationSpec, td_pt_headway_en_spec, td_routes_fares_xml_spec

SKIP: Final[dict[str, str]] = {
    "td_routes_fares_delta": "delta feed not supported yet (skipped)",
}

# Registry
SPEC_REGISTRY: Final[dict[str, Callable[[], ValidationSpec]]] = {
    "td_routes_fares_xml": td_routes_fares_xml_spec,
    "td_pt_headway_gtfs_en": td_pt_headway_en_spec,
}


def spec_for_source(source_id: str) -> ValidationSpec | None:
    if source_id in SKIP:
        return None

    fn = SPEC_REGISTRY.get(source_id)
    if fn is None:
        raise KeyError(f"No ValidationSpec registered for source_id={source_id!r}")
    return fn()
