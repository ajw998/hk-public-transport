from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.pipeline.context import RunContext
from hk_public_transport_etl.pipeline.events import EventType
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)
from hk_public_transport_etl.stages.fetch.http import make_http_client
from hk_public_transport_etl.stages.fetch.runner import fetch_source


def stage_fetch(ctx: RunContext) -> dict[str, Any]:
    if "version" not in ctx.meta:
        raise ValueError("stage_fetch requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    force = bool(ctx.meta.get("force", False))
    max_attempts = int(ctx.meta.get("max_attempts", 3))

    config_dir = ctx.meta.get("config_dir")
    cfg_dir = resolve_config_dir(Path(config_dir) if config_dir else None)

    # Load all SourceSpec objects
    reg = get_source_registry(cfg_dir)

    # Optional filtering: Fetch subset of source_ids
    only_ids = ctx.meta.get("source_ids")
    if only_ids is not None:
        wanted = set(str(x) for x in only_ids)
        reg = {k: v for k, v in reg.items() if k in wanted}

    source_ids = sorted(reg.keys())
    if not source_ids:
        raise ValueError("No sources selected (registry empty or filtered to nothing).")

    layout = DataLayout(root=Path(ctx.data_root))

    ctx.emit(
        EventType.FETCH_PLAN,
        stage="fetch",
        config_dir=str(cfg_dir),
        version=version,
        force=force,
        sources=source_ids,
        max_attempts=max_attempts,
    )

    # One shared client for the whole stage
    client: httpx.Client | None = None
    results: list[dict[str, Any]] = []
    total_artifacts = 0

    try:
        client = make_http_client()

        for sid in source_ids:
            spec = reg[sid]

            ctx.emit(
                EventType.FETCH_SOURCE_START,
                stage="fetch",
                source_id=sid,
                version=version,
            )

            res = fetch_source(
                spec=spec,
                version=version,
                layout=layout,
                force=force,
                client=client,
                max_attempts=max_attempts,
            )

            artifacts = [a.to_dict() for a in res.artifacts]
            total_artifacts += len(artifacts)

            results.append(
                {
                    "source_id": res.source_id,
                    "version": res.version,
                    "raw_metadata_path": res.raw_metadata_path,
                    "artifacts": artifacts,
                }
            )

            ctx.emit(
                EventType.FETCH_SOURCE_FINISH,
                stage="fetch",
                source_id=sid,
                artifacts=len(artifacts),
                raw_metadata_path=res.raw_metadata_path,
            )

        return {
            "config_dir": str(cfg_dir),
            "version": version,
            "sources": results,
            "_metrics": {"sources": len(results), "artifacts": total_artifacts},
        }

    finally:
        if client is not None:
            client.close()
