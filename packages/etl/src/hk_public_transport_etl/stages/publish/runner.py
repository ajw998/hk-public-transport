from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hk_public_transport_etl.core import (
    PublishError,
    atomic_dir_commit,
    atomic_write_bytes,
    atomic_write_json,
    copy_or_hardlink,
    sha256_file,
    write_sha256_sum_txt,
)
from hk_public_transport_etl.core.fs import make_tmp_dir_for
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry.loader import (
    get_source_registry,
    resolve_config_dir,
)

from .config import PublishConfig
from .manifest import (
    build_manifest,
    default_contracts_schema_path,
    file_entries_from_dir,
    serialize_manifest_bytes,
    sources_from_inputs,
    validate_manifest_against_jsonschema,
    validation_from_reports,
)
from .signing import public_key_b64_from_private_key, sign_bytes_ed25519


def _query_stats(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM places;")
        stops = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM routes;")
        routes = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM route_patterns;")
        patterns = int(cur.fetchone()[0])
        return {"stops": stops, "routes": routes, "patterns": patterns}
    finally:
        conn.close()


@dataclass(frozen=True, slots=True)
class PublishOutput:
    out_dir: Path
    manifest_path: Path
    sha256sums_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "out_dir": str(self.out_dir),
            "manifest_path": str(self.manifest_path),
            "sha256sums_path": str(self.sha256sums_path),
        }


def _require_file(path: Path, *, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise PublishError(f"Missing required {label}: {path}")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _finalize_sqlite_in_place(
    db_path: Path, _: PublishConfig, *, optimize: bool
) -> None:
    """
    Ensure the DB is single-file (no WAL/shm) and consistent.
    Runs on the TEMP copy inside the publish temp directory.
    """
    conn = sqlite3.connect(db_path.as_posix())
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        # If WAL exists (because commit used WAL), checkpoint + switch to DELETE journal.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.execute("PRAGMA journal_mode = DELETE;")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        if optimize:
            conn.execute("ANALYZE;")
            conn.execute("VACUUM;")
            conn.execute("PRAGMA optimize;")
        conn.commit()
    finally:
        conn.close()

    # Remove sidecars if present (after checkpoint)
    for suffix in ("-wal", "-shm"):
        side = db_path.with_name(db_path.name + suffix)
        if side.exists():
            side.unlink()


def run_publish_bundle(
    *,
    version: str,
    data_root: Path,
    cfg: PublishConfig | None = None,
    config_dir: Path | None = None,
) -> PublishOutput:
    cfg = cfg or PublishConfig()
    layout = DataLayout(root=Path(data_root))

    bundle_id = cfg.bundle_id

    # Inputs
    app_db_path = layout.app_sqlite(bundle_id, version)
    transport_db_path = layout.transport_sqlite(bundle_id, version)
    build_meta_path = layout.out(bundle_id, version) / "build_metadata.json"

    _require_file(app_db_path, label="app.sqlite")
    _require_file(transport_db_path, label="transport.sqlite")

    # Discover sources
    source_ids: list[str] = []
    if build_meta_path.exists():
        bm = _load_json(build_meta_path)
        for key in ("source_ids", "sources", "inputs"):
            v = bm.get(key)
            if isinstance(v, list) and all(isinstance(x, str) for x in v):
                source_ids = list(v)
                break

    if not source_ids:
        cfg_dir = resolve_config_dir(config_dir)
        reg = get_source_registry(cfg_dir)
        source_ids = sorted(
            [sid for sid in reg.keys() if sid != "td_routes_fares_delta"]
        )

    # Load registry specs to get authority
    cfg_dir = resolve_config_dir(config_dir)
    reg = get_source_registry(cfg_dir)
    source_specs: dict[str, Any] = {}
    for sid in source_ids:
        if sid in reg:
            source_specs[sid] = {"authority": reg[sid].authority}

    # Input metadata paths
    raw_meta_paths: dict[str, Path] = {
        sid: layout.raw_metadata_json(sid, version) for sid in source_ids
    }
    val_paths: dict[str, Path] = {
        sid: layout.validation_report_json(sid, version) for sid in source_ids
    }

    # Require validation reports for included sources
    reports: dict[str, dict[str, Any]] = {}
    for sid, p in val_paths.items():
        _require_file(p, label=f"validation_report.json for {sid}")
        reports[sid] = _load_json(p)

    validation_obj, all_passed = validation_from_reports(
        reports=reports, fail_on_warn=cfg.fail_on_warn
    )
    if cfg.refuse_on_failed_validation and not all_passed:
        raise PublishError(
            "Refusing to publish: validation failed (see per-source validation_report.json)."
        )

    # Publish dir
    final_dir = layout.published(bundle_id, version)
    atomic = make_tmp_dir_for(final_dir)
    tmp_dir = atomic

    # Materialize files
    copy_or_hardlink(app_db_path, tmp_dir / "app.sqlite")
    copy_or_hardlink(transport_db_path, tmp_dir / "transport.sqlite")

    if build_meta_path.exists():
        copy_or_hardlink(build_meta_path, tmp_dir / "build_metadata.json")

    summary = {
        "bundle_id": bundle_id,
        "version": version,
        "passed": bool(all_passed),
        "checks": validation_obj["checks"],
    }
    atomic_write_json(tmp_dir / "validation_summary.json", summary)

    # Finalize DB in-place in tmp dir (WAL -> single file)
    _finalize_sqlite_in_place(tmp_dir / "app.sqlite", cfg, optimize=True)
    _finalize_sqlite_in_place(tmp_dir / "transport.sqlite", cfg, optimize=False)

    # Sources section
    for sid, p in raw_meta_paths.items():
        _require_file(p, label=f"raw_metadata.json for {sid}")
    sources = sources_from_inputs(
        source_specs=source_specs, raw_metadata_paths=raw_meta_paths
    )

    # Build section
    build: dict[str, Any] = {
        "etl_version": cfg.etl_version,
        "git_commit": cfg.git_commit,
        "deterministic": bool(cfg.deterministic),
    }
    # If build_metadata exists, allow it to override basic fields
    if build_meta_path.exists():
        bm = _load_json(build_meta_path)
        if isinstance(bm.get("etl_version"), str):
            build["etl_version"] = bm["etl_version"]
        if isinstance(bm.get("git_commit"), str):
            build["git_commit"] = bm["git_commit"]
        if isinstance(bm.get("deterministic"), bool):
            build["deterministic"] = bm["deterministic"]

    # Stats from SQLite
    stats = _query_stats(tmp_dir / "app.sqlite")

    # Create manifest *after* all files exist
    # First write a placeholder; then compute file list and write final manifest.
    manifest_path = tmp_dir / "manifest.json"

    # Signing prep (optional)
    signing_pub_b64: str | None = None
    do_sign = cfg.signing_private_key_path is not None
    priv_path = (
        Path(cfg.signing_private_key_path) if cfg.signing_private_key_path else None
    )
    if do_sign and priv_path is not None:
        _require_file(priv_path, label="signing_private_key_path")
        signing_pub_b64 = public_key_b64_from_private_key(private_key_path=priv_path)

    # Build manifest now with current bundle files
    manifest = build_manifest(
        bundle_id=bundle_id,
        bundle_version=version,
        schema_version=cfg.schema_version,
        min_app_version=cfg.min_app_version,
        files=[],  # filled after write
        sources=sources,
        build=build,
        validation=validation_obj,
        stats=stats,
    )

    # Write manifest (without files list yet), then compute file entries and rewrite with files.
    atomic_write_json(manifest_path, manifest)
    files = file_entries_from_dir(tmp_dir)

    # Rewrite manifest with full file list
    manifest = build_manifest(
        bundle_id=bundle_id,
        bundle_version=version,
        schema_version=cfg.schema_version,
        min_app_version=cfg.min_app_version,
        files=files,
        sources=sources,
        build=build,
        validation=validation_obj,
        stats=stats,
    )
    atomic_write_json(manifest_path, manifest)

    # Validate against contracts schema (if available)
    schema_path = default_contracts_schema_path()
    if schema_path is not None and schema_path.exists():
        validate_manifest_against_jsonschema(manifest=manifest, schema_path=schema_path)

    sha_entries: dict[str, str] = {}
    sha_entries["manifest.json"] = sha256_file(manifest_path).sha256
    sha_entries["app.sqlite"] = sha256_file(tmp_dir / "app.sqlite").sha256
    sha_entries["transport.sqlite"] = sha256_file(tmp_dir / "transport.sqlite").sha256

    if (tmp_dir / "build_metadata.json").exists():
        sha_entries["build_metadata.json"] = sha256_file(
            tmp_dir / "build_metadata.json"
        ).sha256
    if (tmp_dir / "validation_summary.json").exists():
        sha_entries["validation_summary.json"] = sha256_file(
            tmp_dir / "validation_summary.json"
        ).sha256

    if do_sign and priv_path is not None and signing_pub_b64 is not None:
        # Sign manifest bytes exactly (single default behavior)
        mbytes = serialize_manifest_bytes(manifest)
        sig = sign_bytes_ed25519(payload=mbytes, private_key_path=priv_path)
        atomic_write_bytes(tmp_dir / "manifest.sig", sig)
        sha_entries["manifest.sig"] = sha256_file(tmp_dir / "manifest.sig").sha256

        atomic_write_bytes(
            tmp_dir / "public_key.b64.txt", (signing_pub_b64 + "\n").encode("utf-8")
        )
        sha_entries["public_key.b64.txt"] = sha256_file(
            tmp_dir / "public_key.b64.txt"
        ).sha256

    # sha256sums.txt
    sha_path = tmp_dir / "sha256sums.txt"
    write_sha256_sum_txt(sha_path, sha_entries)
    sha_entries["sha256sums.txt"] = sha256_file(sha_path).sha256

    # Commit atomic dir
    atomic_dir_commit(tmp_dir=tmp_dir, final_dir=final_dir, overwrite=cfg.overwrite)

    return PublishOutput(
        out_dir=final_dir,
        manifest_path=final_dir / "manifest.json",
        sha256sums_path=final_dir / "sha256sums.txt",
    )
