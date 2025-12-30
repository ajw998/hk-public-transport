from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from hk_public_transport_etl.pipeline import RunContext
from hk_public_transport_etl.pipeline.events import EventType

from .config import PublishConfig
from .runner import run_publish_bundle


class StagePublishResult(TypedDict):
    version: str
    bundle_id: str
    published_dir: str
    manifest_path: str
    sha256sums_path: str
    _metrics: dict[str, int]


def stage_publish(ctx: RunContext) -> StagePublishResult:
    if "version" not in ctx.meta:
        raise ValueError("stage_publish requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    config_dir = ctx.meta.get("config_dir")

    cfg = PublishConfig(
        bundle_id=str(ctx.meta.get("bundle_id") or "hk_public_transport"),
        overwrite=bool(ctx.meta.get("overwrite", True)),
    )

    ctx.emit(
        EventType.PUBLISH_START,
        stage="publish",
        version=version,
        bundle_id=cfg.bundle_id,
    )

    out = run_publish_bundle(
        version=version,
        data_root=Path(ctx.data_root),
        cfg=cfg,
        config_dir=(Path(str(config_dir)) if config_dir else None),
    )

    ctx.emit(
        EventType.PUBLISH_FINISH,
        stage="publish",
        version=version,
        out_dir=str(out.out_dir),
    )

    return {
        "version": version,
        "bundle_id": cfg.bundle_id,
        "published_dir": str(out.out_dir),
        "manifest_path": str(out.manifest_path),
        "sha256sums_path": str(out.sha256sums_path),
        "_metrics": {"files": 1},
    }
