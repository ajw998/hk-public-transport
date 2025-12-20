from __future__ import annotations

from pathlib import Path
from typing import Any

from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.pipeline import RunContext
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)
from hk_public_transport_etl.stages.parse.runner import run_parse_source

from .registry import skip_reason


def stage_parse(ctx: RunContext) -> dict[str, Any]:
    if "version" not in ctx.meta:
        raise ValueError("stage_parse requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    config_dir = ctx.meta.get("config_dir")
    cfg_dir = resolve_config_dir(Path(str(config_dir)) if config_dir else None)

    reg = get_source_registry(cfg_dir)
    layout = DataLayout(root=Path(ctx.data_root))

    only_ids = ctx.meta.get("source_ids")
    if only_ids is not None:
        wanted = {str(x) for x in only_ids}
        reg = {k: v for k, v in reg.items() if k in wanted}

    source_ids = sorted(reg.keys())
    if not source_ids:
        raise ValueError("No sources selected (registry empty or filtered to nothing).")

    ctx.emit(
        "parse.plan",
        stage="parse",
        config_dir=str(cfg_dir),
        version=version,
        sources=source_ids,
    )

    results: list[dict[str, Any]] = []
    for sid in source_ids:
        reason = skip_reason(sid)
        if reason is not None:
            ctx.emit(
                "parse.source.skip",
                stage="parse",
                source_id=sid,
                version=version,
                reason=reason,
            )
            results.append(
                {
                    "source_id": sid,
                    "version": version,
                    "status": "skipped",
                    "reason": reason,
                }
            )
            continue

        spec = reg[sid]
        ctx.emit("parse.source.start", stage="parse", source_id=sid, version=version)

        ds = run_parse_source(spec=spec, version=version, data_root=Path(ctx.data_root))

        parsed_meta_path = layout.parsed_metadata_json(sid, version)
        results.append(
            {
                "source_id": sid,
                "version": version,
                "parsed_metadata_path": str(parsed_meta_path),
                "tables": len(ds.output_tables),
            }
        )

        ctx.emit(
            "parse.source.finish",
            stage="parse",
            source_id=sid,
            version=version,
            tables=len(ds.output_tables),
        )

    return {
        "version": version,
        "sources": results,
        "_metrics": {"sources": len(results)},
    }
