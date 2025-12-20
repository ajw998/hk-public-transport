from __future__ import annotations

from pathlib import Path
from typing import Any

from hk_public_transport_etl.pipeline import RunContext

from .config import PublishConfig
from .runner import run_publish_bundle


def stage_publish(ctx: RunContext) -> dict[str, Any]:
    if "version" not in ctx.meta:
        raise ValueError("stage_publish requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    config_dir = ctx.meta.get("config_dir")

    cfg = PublishConfig(
        bundle_id=str(ctx.meta.get("bundle_id") or "hk_public_transport"),
        overwrite=bool(ctx.meta.get("overwrite", False)),
    )

    ctx.emit("publish.start", stage="publish", version=version, bundle_id=cfg.bundle_id)

    out = run_publish_bundle(
        version=version,
        data_root=Path(ctx.data_root),
        cfg=cfg,
        config_dir=(Path(str(config_dir)) if config_dir else None),
    )

    ctx.emit(
        "publish.finish", stage="publish", version=version, out_dir=str(out.out_dir)
    )

    # TODO: Type this
    return {
        "version": version,
        "bundle_id": cfg.bundle_id,
        "published_dir": str(out.out_dir),
        "manifest_path": str(out.manifest_path),
        "sha256sums_path": str(out.sha256sums_path),
        "_metrics": {"files": 1},
    }
