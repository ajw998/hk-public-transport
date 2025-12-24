from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TypedDict

from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.pipeline import RunContext

from .runner import run_build_app_sqlite


class ServeStageOutput(TypedDict):
    bundle_id: str
    version: str
    canonical_sqlite_path: str
    app_sqlite_path: str
    _metrics: dict[str, int]


def stage_serve(ctx: RunContext) -> ServeStageOutput:
    if "version" not in ctx.meta:
        raise ValueError("stage_serve requires ctx.meta['version']")

    version = str(ctx.meta["version"])
    bundle_id = str(ctx.meta.get("bundle_id") or "hk_public_transport")

    layout = DataLayout(root=Path(ctx.data_root))
    canonical_path = layout.transport_sqlite(bundle_id, version)
    app_path = layout.app_sqlite(bundle_id, version)

    if not canonical_path.exists():
        raise FileNotFoundError(f"Missing canonical sqlite: {canonical_path}")

    conn = sqlite3.connect(f"file:{canonical_path.as_posix()}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT schema_version, bundle_version FROM meta WHERE meta_id = 1;"
        ).fetchone()
        schema_version = int(row[0]) if row else 1
        bundle_version = str(row[1]) if row else version
    finally:
        conn.close()

    res = run_build_app_sqlite(
        canonical_sqlite_path=canonical_path,
        app_sqlite_path=app_path,
        schema_version=schema_version,
        bundle_version=bundle_version,
    )

    ctx.emit(
        "serve.finish",
        stage="serve",
        version=version,
        bundle_id=bundle_id,
        canonical_size_bytes=int(res.get("canonical_size_bytes", 0)),
        app_size_bytes=int(res.get("app_size_bytes", 0)),
        metrics=res,
    )

    return {
        "bundle_id": bundle_id,
        "version": version,
        "canonical_sqlite_path": str(canonical_path),
        "app_sqlite_path": str(app_path),
        "_metrics": res,
    }
