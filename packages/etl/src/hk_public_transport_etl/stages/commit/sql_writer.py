from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import polars as pl
from hk_public_transport_etl.core import (
    CommitError,
    atomic_replace,
    sha256_bytes,
    sha256_file,
    stable_json_dumps,
)
from hk_public_transport_etl.stages.commit.resolvers.headway_resolver import (
    HeadwayResolveStats,
)

from .checks import sql_integrity_checks
from .config import CommitConfig
from .resolvers import load_temp_map_route_source, resolve_pattern_headways


def build_sqlite_bundle(
    *,
    table_inputs: Mapping[str, Path],
    validation_reports: Mapping[str, Path],
    ddl_sql: str,
    schema_version: int,
    out_path: Path,
    bundle_id: str,
    bundle_version: str,
    cfg: CommitConfig | None = None,
    routes_fares_source_id: str = "td_routes_fares_xml",
    map_route_source: Path,
) -> dict[str, Any]:
    """
    Builds a single SQLite bundle from normalized parquet tables.
    Returns build_metadata
    """
    cfg = cfg or CommitConfig()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = _make_temp_db_path(out_path)

    timings: dict[str, float] = {}
    row_counts: dict[str, int] = {}

    t0 = time.perf_counter()
    conn = sqlite3.connect(tmp_path, isolation_level=None)
    try:
        _apply_import_pragmas(conn, cfg)

        # Load DDL
        t1 = time.perf_counter()
        conn.executescript(ddl_sql)
        _set_schema_user_version(conn, schema_version)
        timings["ddl_pre_seconds"] = time.perf_counter() - t1

        # Bulk Insert
        t2 = time.perf_counter()
        conn.execute("BEGIN;")
        try:
            for table_name in sorted(table_inputs.keys()):
                if table_name == "pattern_headways":
                    # always derived; ignore any parquet version
                    continue
                df = pl.read_parquet(table_inputs[table_name])
                row_counts[table_name] = _insert_table(
                    conn, table_name, df, batch_rows=cfg.batch_rows
                )

            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise CommitError(Exception)

        timings["load_seconds"] = time.perf_counter() - t2

        # Derived rows
        t2b = time.perf_counter()

        load_temp_map_route_source(conn, parquet_path=map_route_source)

        headway_stats = resolve_pattern_headways(
            conn,
            routes_fares_source_id=routes_fares_source_id,
            create_debug_tables=cfg.create_headway_debug_tables,
        )
        timings["headway_resolve_seconds"] = time.perf_counter() - t2b
        row_counts["pattern_headways"] = int(headway_stats.inserted_rows)

        # TODO: Build FTS

        # Meta rows
        t3 = time.perf_counter()
        _populate_meta_row(
            conn,
            schema_version=schema_version,
            bundle_id=bundle_id,
            bundle_version=bundle_version,
            validation_reports=validation_reports,
        )
        timings["meta_seconds"] = time.perf_counter() - t3

        # Maintenance
        t4 = time.perf_counter()
        _post_load_maintenance(conn, cfg)
        timings["post_seconds"] = time.perf_counter() - t4

        # Final pragmas and check
        t5 = time.perf_counter()
        _apply_final_pragmas(conn, cfg)
        sql_integrity_checks(conn)
        timings["checks_seconds"] = time.perf_counter() - t5

        timings["total_seconds"] = time.perf_counter() - t0

        metadata = _build_metadata(
            cfg=cfg,
            ddl_sql=ddl_sql,
            conn=conn,
            row_counts=row_counts,
            timings=timings,
            bundle_id=bundle_id,
            bundle_version=bundle_version,
            validation_reports=validation_reports,
            headway_stats=headway_stats,
        )
    finally:
        conn.close()

    atomic_replace(tmp_path, out_path)
    _write_json(out_path.with_name("build_metadata.json"), metadata)
    return metadata


def _apply_import_pragmas(conn: sqlite3.Connection, cfg: CommitConfig) -> None:
    conn.execute(f"PRAGMA journal_mode = {cfg.import_journal_mode};")
    conn.execute(f"PRAGMA synchronous = {cfg.import_synchronous};")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute(f"PRAGMA cache_size = {-int(cfg.cache_size_kb)};")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = OFF;")  # import first, validate at end


def _apply_final_pragmas(conn: sqlite3.Connection, cfg: CommitConfig) -> None:
    conn.execute(f"PRAGMA journal_mode = {cfg.final_journal_mode};")
    conn.execute(f"PRAGMA synchronous = {cfg.final_synchronous};")
    conn.execute("PRAGMA foreign_keys = ON;")


def _post_load_maintenance(conn: sqlite3.Connection, cfg: CommitConfig) -> None:
    if cfg.run_analyze:
        conn.execute("ANALYZE;")
    if cfg.run_optimize:
        conn.execute("PRAGMA optimize;")
    if cfg.run_vacuum:
        conn.execute("VACUUM;")


def _set_schema_user_version(conn: sqlite3.Connection, schema_version: int) -> None:
    if schema_version < 0:
        raise ValueError("schema_version must be >= 0")
    conn.execute(f"PRAGMA user_version = {int(schema_version)};")


# Insertions


def _insert_table(
    conn: sqlite3.Connection, table_name: str, df: pl.DataFrame, batch_rows: int
) -> int:
    expected_cols, pk_cols = _expected_cols_and_pk(conn, table_name)
    _validate_columns(table_name, expected_cols, df.columns)

    df2 = df.select(expected_cols)
    if pk_cols:
        # determinism: stable content order in sqlite pages (good enough w/ same sqlite version)
        df2 = df2.sort(pk_cols)

    sql = _make_insert_sql(table_name, expected_cols)
    total = int(df2.height)

    row_iter = df2.iter_rows(named=False)
    for batch in _chunked(row_iter, batch_rows):
        conn.executemany(sql, [tuple(_coerce_cell(v) for v in row) for row in batch])

    return total


def _expected_cols_and_pk(
    conn: sqlite3.Connection, table_name: str
) -> tuple[list[str], list[str]]:
    rows = conn.execute(f"PRAGMA table_info({_quote_ident(table_name)});").fetchall()
    if not rows:
        raise RuntimeError(
            f"DDL mismatch: table '{table_name}' does not exist (did pre-DDL run?)."
        )
    cols = [r[1] for r in rows]
    rows_sorted = sorted(rows, key=lambda r: int(r[5] or 0))
    pk_cols = [r[1] for r in rows_sorted if int(r[5] or 0) > 0]
    return cols, pk_cols


def _validate_columns(
    table: str, expected: Iterable[str], actual: Iterable[str]
) -> None:
    exp = list(expected)
    act = list(actual)
    missing = [c for c in exp if c not in act]
    extra = [c for c in act if c not in exp]
    if missing or extra:
        raise RuntimeError(
            f"Column mismatch for table '{table}'.\n"
            f"Missing columns: {missing}\n"
            f"Extra columns: {extra}\n"
            f"Expected: {exp}\n"
            f"Actual:   {act}"
        )


def _make_insert_sql(table_name: str, columns: list[str]) -> str:
    cols = ", ".join(_quote_ident(c) for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {_quote_ident(table_name)} ({cols}) VALUES ({placeholders});"


def _chunked(it: Iterator[tuple[Any, ...]], n: int) -> Iterator[list[tuple[Any, ...]]]:
    if n <= 0:
        raise ValueError("chunk size must be > 0")
    batch: list[tuple[Any, ...]] = []
    for row in it:
        batch.append(row)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _coerce_cell(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    return v


def _populate_meta_row(
    conn: sqlite3.Connection,
    *,
    schema_version: int,
    bundle_id: str,
    bundle_version: str,
    validation_reports: Mapping[str, Path],
) -> None:
    meta_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta' LIMIT 1;"
    ).fetchone()
    if not meta_exists:
        return

    now_utc = dt.datetime.now(tz=dt.timezone.utc).isoformat()

    src_versions = {
        sid: {"version": bundle_version} for sid in sorted(validation_reports.keys())
    }
    src_versions_json = json.dumps(
        src_versions, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )

    digests = {
        sid: sha256_file(p).sha256 for sid, p in sorted(validation_reports.items())
    }
    notes = stable_json_dumps(
        {"bundle_id": bundle_id, "validation_reports_sha256": digests}
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO meta(
          meta_id, schema_version, bundle_version, generated_at, source_versions_json, notes
        ) VALUES (1, ?, ?, ?, ?, ?);
        """,
        (int(schema_version), str(bundle_version), now_utc, src_versions_json, notes),
    )


def _build_metadata(
    *,
    cfg: CommitConfig,
    ddl_sql: str,
    conn: sqlite3.Connection,
    row_counts: Mapping[str, int],
    timings: Mapping[str, float],
    bundle_id: str,
    bundle_version: str,
    validation_reports: Mapping[str, Path],
    headway_stats: HeadwayResolveStats,
) -> dict[str, Any]:
    sqlite_version = conn.execute("select sqlite_version();").fetchone()[0]
    pragmas = {
        "journal_mode": conn.execute("PRAGMA journal_mode;").fetchone()[0],
        "synchronous": conn.execute("PRAGMA synchronous;").fetchone()[0],
        "foreign_keys": conn.execute("PRAGMA foreign_keys;").fetchone()[0],
        "cache_size": conn.execute("PRAGMA cache_size;").fetchone()[0],
        "user_version": conn.execute("PRAGMA user_version;").fetchone()[0],
    }

    vr = {sid: sha256_file(p).sha256 for sid, p in sorted(validation_reports.items())}

    return {
        "schema_version": int(pragmas["user_version"]),
        "sqlite_version": sqlite_version,
        "pragmas": pragmas,
        "commit_config": asdict(cfg),
        "bundle": {"bundle_id": bundle_id, "bundle_version": bundle_version},
        "inputs": {
            "canonical_ddl_sha256": sha256_bytes(ddl_sql.encode("utf-8")),
            "validation_reports_sha256": vr,
        },
        "row_counts": dict(row_counts),
        "timings_seconds": dict(timings),
        "headway_resolution": headway_stats.to_dict(),
        "build_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    }


def _write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(".tmp.json")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _make_temp_db_path(out_path: Path) -> Path:
    fd, tmp = tempfile.mkstemp(
        prefix=out_path.stem + ".", suffix=".tmp.sqlite", dir=str(out_path.parent)
    )
    os.close(fd)
    return Path(tmp)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
