from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from hk_public_transport_etl.pipeline.context import RunContext
from hk_public_transport_etl.pipeline.events import EventType
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)

from .registry import should_skip
from .runner import run_normalize_source


class _NormalizeSourceRow(TypedDict):
    source_id: str
    version: str
    normalized_metadata_path: str
    status: str
    note: str | None


class NormalizeStageOutput(TypedDict):
    version: str
    sources: list[_NormalizeSourceRow]
    _metrics: dict[str, int]


def stage_normalize(ctx: RunContext) -> NormalizeStageOutput:
    if "version" not in ctx.meta:
        raise ValueError("stage_normalize requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    config_dir = ctx.meta.get("config_dir")
    cfg_dir = resolve_config_dir(Path(str(config_dir)) if config_dir else None)

    reg = get_source_registry(cfg_dir)

    only_ids = ctx.meta.get("source_ids")
    if only_ids is not None:
        wanted = {str(x) for x in only_ids}  # JsonObject -> object
        reg = {k: v for k, v in reg.items() if k in wanted}

    source_ids = sorted(reg.keys())
    if not source_ids:
        raise ValueError("No sources selected (registry empty or filtered to nothing).")

    ctx.emit(
        EventType.NORMALIZE_PLAN,
        stage="normalize",
        config_dir=str(cfg_dir),
        version=version,
        sources=source_ids,
    )

    rows: list[_NormalizeSourceRow] = []
    ok = 0
    skipped = 0

    for sid in source_ids:
        note = should_skip(sid)
        if note:
            rows.append(
                {
                    "source_id": sid,
                    "version": version,
                    "normalized_metadata_path": "",
                    "status": "skipped",
                    "note": note,
                }
            )
            skipped += 1
            continue

        spec = reg[sid]
        ctx.emit(
            EventType.NORMALIZE_SOURCE_START,
            stage="normalize",
            source_id=sid,
            version=version,
        )

        out = run_normalize_source(
            spec=spec, version=version, data_root=Path(ctx.data_root)
        )
        if out is None:
            rows.append(
                {
                    "source_id": sid,
                    "version": version,
                    "normalized_metadata_path": "",
                    "status": "skipped",
                    "note": "no normalizer registered (skipped)",
                }
            )
            skipped += 1
            continue

        rows.append(
            {
                "source_id": sid,
                "version": version,
                "normalized_metadata_path": str(out.metadata_path),
                "status": "ok",
                "note": None,
            }
        )
        ok += 1
        ctx.emit(
            EventType.NORMALIZE_SOURCE_FINISH,
            stage="normalize",
            source_id=sid,
            version=version,
            out_dir=str(out.out_dir),
        )

    return {
        "version": version,
        "sources": rows,
        "_metrics": {"sources": len(rows), "ok": ok, "skipped": skipped},
    }
