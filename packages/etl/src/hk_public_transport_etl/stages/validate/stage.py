from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from hk_public_transport_etl.pipeline import RunContext
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)
from hk_public_transport_etl.registry.models import SourceSpec

from .runner import run_validate_source


class ValidateStageOutput(TypedDict):
    version: str
    reports: dict[str, str]
    _metrics: dict[str, int]


def stage_validate(ctx: RunContext) -> ValidateStageOutput:
    if "version" not in ctx.meta:
        raise ValueError("stage_validate requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    config_dir = ctx.meta.get("config_dir")
    cfg_dir = resolve_config_dir(Path(config_dir) if config_dir else None)

    reg: dict[str, SourceSpec] = get_source_registry(cfg_dir)

    only_ids = ctx.meta.get("source_ids")
    if only_ids is not None:
        wanted = set(str(x) for x in only_ids)
        reg = {k: v for k, v in reg.items() if k in wanted}

    source_ids = sorted(reg.keys())
    if not source_ids:
        raise ValueError("No sources selected (registry empty or filtered to nothing).")

    ctx.emit(
        "validate.plan",
        stage="validate",
        config_dir=str(cfg_dir),
        version=version,
        sources=source_ids,
    )

    reports: dict[str, str] = {}
    ok = 0
    failed = 0

    for sid in source_ids:
        spec = reg[sid]
        ctx.emit(
            "validate.source.start", stage="validate", source_id=sid, version=version
        )

        exit_code, report_path = run_validate_source(
            spec=spec,
            version=version,
            data_root=Path(ctx.data_root),
        )

        reports[sid] = str(report_path)
        if exit_code == 0:
            ok += 1
        else:
            failed += 1

        ctx.emit(
            "validate.source.finish",
            stage="validate",
            source_id=sid,
            version=version,
            exit_code=int(exit_code),
            report_path=str(report_path),
        )

    return {
        "version": version,
        "reports": reports,
        "_metrics": {"sources": len(reports), "ok": ok, "failed": failed},
    }
