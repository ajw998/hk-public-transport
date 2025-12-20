from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hk_public_transport_etl.core.json import stable_json_dumps
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry import SourceSpec

from .config import CommitConfig
from .ddl import load_canonical_ddl, load_schema_version
from .sql_writer import build_sqlite_bundle


@dataclass(frozen=True, slots=True)
class CommitResult:
    exit_code: int
    sqlite_path: Path
    build_metadata_path: Path


def run_commit_bundle(
    *,
    specs: list[SourceSpec],
    version: str,
    data_root: Path,
    cfg: CommitConfig | None = None,
    bundle_id: str = "hk_public_transport",
    routes_fares_source_id: str = "td_routes_fares_xml",
) -> CommitResult:
    cfg = cfg or CommitConfig()

    layout = DataLayout(root=Path(data_root))
    normalized_root = layout.normalized_root()
    out_dir = layout.out(bundle_id, version)
    out_dir.mkdir(parents=True, exist_ok=True)

    validation_reports: dict[str, Path] = {}
    table_inputs: dict[str, Path] = {}
    table_sources: dict[str, str] = {}

    for spec in specs:
        sid = spec.id

        vr = layout.validation_report_json(sid, version)
        if vr.exists():
            validation_reports[sid] = vr

        tables_dir = layout.normalized_tables(sid, version)
        if not tables_dir.exists():
            continue

        for p in sorted(tables_dir.glob("*.parquet")):
            table = p.stem
            if cfg.enforce_single_source_per_table and table in table_inputs:
                raise RuntimeError(
                    "Same table emitted by multiple sources (merge policy not implemented yet).\n"
                    + stable_json_dumps(
                        {
                            "table": table,
                            "existing_source": table_sources[table],
                            "new_source": sid,
                            "existing_path": str(table_inputs[table]),
                            "new_path": str(p),
                        }
                    )
                )
            table_inputs[table] = p
            table_sources[table] = sid

    if not table_inputs:
        raise FileNotFoundError(
            f"No parquet tables found under {normalized_root}/<source>/{version}/tables"
        )

    ddl_sql = load_canonical_ddl()
    schema_version = load_schema_version()

    sqlite_path = layout.transport_sqlite(bundle_id, version)
    build_metadata_path = layout.out(bundle_id, version) / "build_metadata.json"

    map_route_pq = (
        layout.normalized_mappings(routes_fares_source_id, version)
        / "map_route_source.parquet"
    )

    build_sqlite_bundle(
        table_inputs=table_inputs,
        validation_reports=validation_reports,
        ddl_sql=ddl_sql,
        schema_version=schema_version,
        out_path=sqlite_path,
        bundle_id=bundle_id,
        bundle_version=version,
        cfg=cfg,
        routes_fares_source_id=routes_fares_source_id,
        map_route_source=map_route_pq,
    )

    return CommitResult(
        exit_code=0, sqlite_path=sqlite_path, build_metadata_path=build_metadata_path
    )
