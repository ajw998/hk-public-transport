from __future__ import annotations

from pathlib import Path

from hk_public_transport_etl.core import NormalizeError
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry.models import SourceSpec

from .registry import get_normalizer, should_skip
from .types import NormalizeContext, NormalizeOutput


def run_normalize_source(
    *, spec: SourceSpec, version: str, data_root: Path
) -> NormalizeOutput | None:
    source_id = spec.id
    reason = should_skip(source_id)
    if reason:
        return None

    fn = get_normalizer(source_id)
    if fn is None:
        return None

    layout = DataLayout(root=Path(data_root))
    parsed_dir = layout.staged(source_id, version)
    parsed_meta = layout.parsed_metadata_json(source_id, version)

    if not parsed_meta.exists():
        raise NormalizeError(
            f"Missing parsed metadata for {source_id}/{version}: {parsed_meta}"
        )

    ctx = NormalizeContext(source_id=source_id, version=version, data_root=layout.root)
    return fn(ctx)
