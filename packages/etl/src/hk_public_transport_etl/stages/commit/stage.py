from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.pipeline import RunContext
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)
from hk_public_transport_etl.registry.models import SourceSpec

from .config import CommitConfig
from .runner import run_commit_bundle


class CommitSourceSummary(TypedDict):
    source_id: str
    version: str
    normalized_tables_dir: str
    validation_report_path: str | None
    included_in_bundle: bool
    reason: str | None


class CommitStageOutput(TypedDict):
    bundle_id: str
    version: str
    sqlite_path: str
    build_metadata_path: str
    sources: list[CommitSourceSummary]
    _metrics: dict[str, int]


def _select_specs(ctx: RunContext, *, version: str) -> list[SourceSpec]:
    config_dir = ctx.meta.get("config_dir")
    cfg_dir = resolve_config_dir(Path(str(config_dir)) if config_dir else None)

    reg = get_source_registry(cfg_dir)

    only_ids = ctx.meta.get("source_ids")
    if only_ids is not None:
        wanted = {str(x) for x in only_ids}
        reg = {k: v for k, v in reg.items() if k in wanted}

    specs = [reg[k] for k in sorted(reg.keys())]
    if not specs:
        raise ValueError("No sources selected (registry empty or filtered to nothing).")

    ctx.emit(
        "commit.plan",
        stage="commit",
        config_dir=str(cfg_dir),
        version=version,
        sources=[s.id for s in specs],
    )
    return specs


def stage_commit(ctx: RunContext) -> CommitStageOutput:
    """
    Build a bundled transport.sqlite from normalized parquet tables
    """
    if "version" not in ctx.meta:
        raise ValueError("stage_commit requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    bundle_id = str(ctx.meta.get("bundle_id") or "hk_public_transport")
    routes_fares_source_id = str(
        ctx.meta.get("routes_fares_source_id") or "td_routes_fares_xml"
    )

    specs = _select_specs(ctx, version=version)

    layout = DataLayout(root=Path(ctx.data_root))

    # Pre-scan: track which selected sources actually have normalized tables,
    # so commit can be strict-but-informative.
    sources_summary: list[CommitSourceSummary] = []
    included_specs: list[SourceSpec] = []

    for spec in specs:
        sid = spec.id
        tables_dir = layout.normalized_tables(sid, version)
        report_path = layout.validation_report_json(sid, version)

        parquet_files = (
            sorted(tables_dir.glob("*.parquet")) if tables_dir.exists() else []
        )
        included = len(parquet_files) > 0

        summary: CommitSourceSummary = {
            "source_id": sid,
            "version": version,
            "normalized_tables_dir": str(tables_dir),
            "validation_report_path": (
                str(report_path) if report_path.exists() else None
            ),
            "included_in_bundle": included,
            "reason": None if included else "no_normalized_tables",
        }
        sources_summary.append(summary)

        if included:
            included_specs.append(spec)

    if not included_specs:
        raise FileNotFoundError(
            f"No normalized parquet tables found for any selected source at version={version!r} "
            f"under {layout.normalized_root()}"
        )

    ctx.emit(
        "commit.start",
        stage="commit",
        version=version,
        bundle_id=bundle_id,
        sources_selected=[s.id for s in specs],
        sources_included=[s.id for s in included_specs],
    )

    cfg = CommitConfig()

    res = run_commit_bundle(
        specs=included_specs,
        version=version,
        data_root=Path(ctx.data_root),
        cfg=cfg,
        bundle_id=bundle_id,
        routes_fares_source_id=routes_fares_source_id,
    )

    ctx.emit(
        "commit.finish",
        stage="commit",
        version=version,
        bundle_id=bundle_id,
        sqlite_path=str(res.sqlite_path),
        build_metadata_path=str(res.build_metadata_path),
    )

    out: CommitStageOutput = {
        "bundle_id": bundle_id,
        "version": version,
        "sqlite_path": str(res.sqlite_path),
        "build_metadata_path": str(res.build_metadata_path),
        "sources": sources_summary,
        "_metrics": {
            "sources_selected": len(specs),
            "sources_included": len(included_specs),
        },
    }
    return out
