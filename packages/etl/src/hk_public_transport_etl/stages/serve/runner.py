from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from hk_public_transport_etl.core import (
    MODE_ID,
    PLACE_TYPE_ID,
    SERVICE_TYPE_ID,
    atomic_replace,
)

from .ddl import load_app_ddl
from .text import normalize_en, segment_cjk


def _case(expr: str, mapping: dict[str, int], *, default: int = 0) -> str:
    parts = [f"WHEN {expr} = '{k}' THEN {int(v)}" for k, v in mapping.items()]
    return "CASE " + " ".join(parts) + f" ELSE {int(default)} END"


def _table_exists(conn: sqlite3.Connection, name: str, *, schema: str = "main") -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?;",
        (name,),
    ).fetchone()
    return row is not None


def _query_operator_rows(
    conn: sqlite3.Connection, *, canon_schema: str = "canon"
) -> list[tuple[str, str, str | None, str | None, str | None]]:
    rows = conn.execute(
        f"SELECT operator_id, operator_name_en, operator_name_tc, operator_name_sc "
        f"FROM {canon_schema}.operators ORDER BY operator_id;"
    ).fetchall()
    out: list[tuple[str, str, str | None, str | None, str | None]] = []
    for oid, en, tc, sc in rows:
        operator_id = str(oid)
        operator_code = operator_id.split(":")[-1]
        out.append((operator_id, operator_code, en, tc, sc))
    return out


def _insert_many(
    conn: sqlite3.Connection,
    sql: str,
    rows: Iterable[tuple[Any, ...]],
    *,
    batch: int = 50_000,
) -> int:
    total = 0
    buf: list[tuple[Any, ...]] = []
    for r in rows:
        buf.append(r)
        if len(buf) >= batch:
            conn.executemany(sql, buf)
            total += len(buf)
            buf = []
    if buf:
        conn.executemany(sql, buf)
        total += len(buf)
    return total


def _build_fare_segments(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "route_id": pl.Int64,
                "fare_product_id": pl.Int64,
                "origin_seq": pl.Int64,
                "dest_from_seq": pl.Int64,
                "dest_to_seq": pl.Int64,
                "amount_cents": pl.Int64,
                "is_default": pl.Int8,
            }
        )

    keys = ["route_id", "fare_product_id", "origin_seq"]
    df = df.sort(keys + ["destination_seq"])
    prev_dest = pl.col("destination_seq").shift(1).over(keys)
    prev_amt = pl.col("amount_cents").shift(1).over(keys)

    seg_start = (
        prev_dest.is_null()
        | (pl.col("destination_seq") != prev_dest + 1)
        | (pl.col("amount_cents") != prev_amt)
    ).alias("_seg_start")

    df = df.with_columns(seg_start).with_columns(
        pl.col("_seg_start").cast(pl.Int64).cum_sum().over(keys).alias("_seg_id")
    )

    segments = (
        df.group_by(keys + ["_seg_id"])
        .agg(
            pl.col("destination_seq").min().alias("dest_from_seq"),
            pl.col("destination_seq").max().alias("dest_to_seq"),
            pl.col("amount_cents").first().alias("amount_cents"),
        )
        .drop("_seg_id")
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("is_default"))
        .sort(keys + ["dest_from_seq"])
    )
    return segments


def _populate_operators(app: sqlite3.Connection) -> None:
    operator_rows = _query_operator_rows(app, canon_schema="canon")
    app.execute("BEGIN;")
    try:
        app.execute(
            "CREATE TEMP TABLE operator_map(operator_id TEXT PRIMARY KEY, operator_pk INTEGER NOT NULL);"
        )
        op_map_rows: list[tuple[str, int]] = []
        ops_insert = []
        for i, (operator_id, operator_code, en, tc, sc) in enumerate(
            operator_rows, start=1
        ):
            ops_insert.append((i, operator_code, en, tc, sc))
            op_map_rows.append((operator_id, i))

        _insert_many(
            app,
            "INSERT INTO operators(operator_id, operator_code, operator_name_en, operator_name_tc, operator_name_sc) "
            "VALUES (?,?,?,?,?);",
            ops_insert,
        )
        _insert_many(
            app,
            "INSERT INTO operator_map(operator_id, operator_pk) VALUES (?,?);",
            op_map_rows,
        )
        app.execute("COMMIT;")
    except Exception:
        app.execute("ROLLBACK;")
        raise


def _copy_places(app: sqlite3.Connection) -> None:
    place_type_case = _case("c.place_type", PLACE_TYPE_ID, default=0)
    primary_mode_case = _case("c.primary_mode", MODE_ID, default=0)
    app.execute(
        f"""
        INSERT INTO places(
          place_id, place_type_id, primary_mode_id,
          name_en, name_tc, name_sc,
          lat_e7, lon_e7,
          parent_place_id
        )
        SELECT
          c.place_id,
          {place_type_case} AS place_type_id,
          {primary_mode_case} AS primary_mode_id,
          c.name_en, c.name_tc, c.name_sc,
          CASE WHEN c.lat IS NULL THEN NULL ELSE CAST(ROUND(c.lat * 10000000.0) AS INTEGER) END AS lat_e7,
          CASE WHEN c.lon IS NULL THEN NULL ELSE CAST(ROUND(c.lon * 10000000.0) AS INTEGER) END AS lon_e7,
          c.parent_place_id
        FROM canon.places c
        ORDER BY c.place_id;
        """
    )


def _copy_routes(app: sqlite3.Connection) -> None:
    mode_case_routes = _case("r.mode", MODE_ID, default=0)
    app.execute(
        f"""
        INSERT INTO routes(
          route_id, operator_id, mode_id,
          route_short_name,
          origin_text_en, origin_text_tc, origin_text_sc,
          destination_text_en, destination_text_tc, destination_text_sc,
          journey_time_minutes,
          upstream_route_id
        )
        SELECT
          r.route_id,
          om.operator_pk,
          {mode_case_routes} AS mode_id,
          r.route_short_name,
          r.origin_text_en, r.origin_text_tc, r.origin_text_sc,
          r.destination_text_en, r.destination_text_tc, r.destination_text_sc,
          r.journey_time_minutes,
          r.upstream_route_id
        FROM canon.routes r
        JOIN operator_map om ON om.operator_id = r.operator_id
        ORDER BY r.route_id;
        """
    )


def _copy_route_patterns(app: sqlite3.Connection) -> None:
    service_type_case = _case("p.service_type", SERVICE_TYPE_ID, default=0)
    app.execute(
        f"""
        INSERT INTO route_patterns(
          pattern_id, route_id, route_seq, direction_id, service_type_id,
          sequence_incomplete, is_circular
        )
        SELECT
          p.pattern_id,
          p.route_id,
          p.route_seq,
          p.direction_id,
          {service_type_case} AS service_type_id,
          p.sequence_incomplete,
          p.is_circular
        FROM canon.route_patterns p
        ORDER BY p.pattern_id;
        """
    )


def _copy_pattern_stops(app: sqlite3.Connection) -> None:
    app.execute(
        """
        INSERT INTO pattern_stops(pattern_id, seq, place_id, allow_repeat)
        SELECT pattern_id, seq, place_id, allow_repeat
        FROM canon.pattern_stops
        ORDER BY pattern_id, seq;
        """
    )


def _copy_fare_products(app: sqlite3.Connection) -> None:
    if not _table_exists(app, "fare_products", schema="canon"):
        return
    mode_case_fare_products = _case("fp.mode", MODE_ID, default=0)
    app.execute(
        f"""
        INSERT INTO fare_products(fare_product_id, mode_id)
        SELECT fp.fare_product_id, {mode_case_fare_products} AS mode_id
        FROM canon.fare_products fp
        ORDER BY fp.fare_product_id;
        """
    )


def _copy_fare_segments(app: sqlite3.Connection) -> None:
    if not (
        _table_exists(app, "fare_rules", schema="canon")
        and _table_exists(app, "fare_amounts", schema="canon")
    ):
        return

    q = """
    WITH choice AS (
      SELECT
        fare_rule_id,
        COALESCE(
          MIN(CASE WHEN is_default = 1 THEN fare_product_id END),
          MIN(fare_product_id)
        ) AS fare_product_id
      FROM canon.fare_amounts
      GROUP BY fare_rule_id
    ),
    chosen AS (
      SELECT fa.fare_rule_id, c.fare_product_id, fa.amount_cents
      FROM choice c
      JOIN canon.fare_amounts fa
        ON fa.fare_rule_id = c.fare_rule_id
       AND fa.fare_product_id = c.fare_product_id
    )
    SELECT
      fr.route_id,
      chosen.fare_product_id,
      fr.origin_seq,
      fr.destination_seq,
      chosen.amount_cents
    FROM canon.fare_rules fr
    JOIN chosen ON chosen.fare_rule_id = fr.fare_rule_id
    WHERE fr.origin_seq IS NOT NULL AND fr.destination_seq IS NOT NULL
    ORDER BY fr.route_id, chosen.fare_product_id, fr.origin_seq, fr.destination_seq;
    """
    cur = app.execute(q)
    chunks: list[pl.DataFrame] = []
    schema = [
        ("route_id", pl.Int64),
        ("fare_product_id", pl.Int64),
        ("origin_seq", pl.Int64),
        ("destination_seq", pl.Int64),
        ("amount_cents", pl.Int64),
    ]
    while True:
        rows = cur.fetchmany(200_000)
        if not rows:
            break
        chunks.append(pl.DataFrame(rows, schema=schema, orient="row"))
    df = pl.concat(chunks, how="vertical") if chunks else pl.DataFrame(schema=schema)
    df = (
        df.group_by(["route_id", "fare_product_id", "origin_seq", "destination_seq"])
        .agg(pl.col("amount_cents").min().alias("amount_cents"))
        .sort(["route_id", "fare_product_id", "origin_seq", "destination_seq"])
    )
    segments = _build_fare_segments(df)
    seg_rows = segments.select(
        [
            "route_id",
            "fare_product_id",
            "origin_seq",
            "dest_from_seq",
            "dest_to_seq",
            "amount_cents",
            "is_default",
        ]
    ).iter_rows()

    app.execute("BEGIN;")
    try:
        _insert_many(
            app,
            "INSERT INTO fare_segments(route_id, fare_product_id, origin_seq, dest_from_seq, dest_to_seq, amount_cents, is_default) "
            "VALUES (?,?,?,?,?,?,?);",
            seg_rows,
        )
        app.execute("COMMIT;")
    except Exception:
        app.execute("ROLLBACK;")
        raise


def _populate_search(app: sqlite3.Connection) -> None:
    if not (_table_exists(app, "search_fts") and _table_exists(app, "search_docs")):
        return

    app.execute("BEGIN;")
    try:
        doc_buf: list[tuple[Any, ...]] = []
        fts_buf: list[tuple[Any, ...]] = []
        doc_id = 0

        def flush() -> None:
            nonlocal doc_buf, fts_buf
            if not doc_buf:
                return
            app.executemany(
                "INSERT INTO search_docs(doc_id, kind, ref_id, mode_id, operator_id, code) "
                "VALUES (?,?,?,?,?,?);",
                doc_buf,
            )
            app.executemany(
                "INSERT INTO search_fts(rowid, kind, ref_id, mode_id, operator_id, code, en, tc, sc) "
                "VALUES (?,?,?,?,?,?,?,?,?);",
                fts_buf,
            )
            doc_buf = []
            fts_buf = []

        cur = app.execute(
            "SELECT place_id, primary_mode_id, name_en, name_tc, name_sc "
            "FROM places ORDER BY place_id;"
        )
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            for place_id, mode_id, name_en, name_tc, name_sc in rows:
                doc_id += 1
                doc_buf.append(
                    (int(doc_id), "p", int(place_id), int(mode_id), None, "")
                )
                fts_buf.append(
                    (
                        int(doc_id),
                        "p",
                        int(place_id),
                        int(mode_id),
                        None,
                        "",
                        normalize_en(name_en),
                        segment_cjk(name_tc),
                        segment_cjk(name_sc),
                    )
                )
            if len(doc_buf) >= 50_000:
                flush()

        cur = app.execute(
            "SELECT route_id, mode_id, operator_id, route_short_name, "
            "origin_text_en, destination_text_en, "
            "origin_text_tc, destination_text_tc, "
            "origin_text_sc, destination_text_sc "
            "FROM routes ORDER BY route_id;"
        )
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            for (
                route_id,
                mode_id,
                operator_id,
                route_short_name,
                origin_en,
                dest_en,
                origin_tc,
                dest_tc,
                origin_sc,
                dest_sc,
            ) in rows:
                doc_id += 1
                code = route_short_name or ""
                doc_buf.append(
                    (
                        int(doc_id),
                        "r",
                        int(route_id),
                        int(mode_id),
                        int(operator_id),
                        code,
                    )
                )
                fts_buf.append(
                    (
                        int(doc_id),
                        "r",
                        int(route_id),
                        int(mode_id),
                        int(operator_id),
                        code,
                        normalize_en(
                            " ".join(
                                [str(code), str(origin_en or ""), str(dest_en or "")]
                            )
                        ),
                        segment_cjk(
                            " ".join([str(origin_tc or ""), str(dest_tc or "")])
                        ),
                        segment_cjk(
                            " ".join([str(origin_sc or ""), str(dest_sc or "")])
                        ),
                    )
                )
            if len(doc_buf) >= 50_000:
                flush()

        flush()
        app.execute("COMMIT;")
    except Exception:
        app.execute("ROLLBACK;")
        raise


def run_build_app_sqlite(
    *,
    canonical_sqlite_path: Path,
    app_sqlite_path: Path,
    schema_version: int,
    bundle_version: str,
) -> dict[str, int]:
    canonical_sqlite_path = Path(canonical_sqlite_path)
    app_sqlite_path = Path(app_sqlite_path)

    if not canonical_sqlite_path.exists():
        raise FileNotFoundError(f"Missing canonical sqlite: {canonical_sqlite_path}")

    app_sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = app_sqlite_path.with_suffix(".tmp.sqlite")
    if tmp_path.exists():
        tmp_path.unlink()

    t0 = time.perf_counter()
    canonical_size = int(canonical_sqlite_path.stat().st_size)
    app = sqlite3.connect(tmp_path.as_posix(), isolation_level=None)
    try:
        app.execute("PRAGMA journal_mode = WAL;")
        app.execute("PRAGMA synchronous = NORMAL;")
        app.execute("PRAGMA temp_store = MEMORY;")
        app.execute("PRAGMA foreign_keys = ON;")

        ddl = load_app_ddl()
        app.executescript(ddl)

        # Attach canonical db for set-based copies.
        app.execute("ATTACH DATABASE ? AS canon;", (canonical_sqlite_path.as_posix(),))

        try:
            _populate_operators(app)
            _copy_places(app)
            _copy_routes(app)
            _copy_route_patterns(app)
            _copy_pattern_stops(app)
            _copy_fare_products(app)
            _copy_fare_segments(app)
        finally:
            try:
                app.execute("DETACH DATABASE canon;")
            except Exception:
                pass

        _populate_search(app)

        # meta row
        now = app.execute("SELECT STRFTIME('%Y-%m-%dT%H:%M:%fZ','now');").fetchone()[0]
        app.execute(
            "INSERT OR REPLACE INTO meta(meta_id, schema_version, bundle_version, generated_at, notes) "
            "VALUES (1, ?, ?, ?, ?);",
            (int(schema_version), str(bundle_version), str(now), "app.sqlite build"),
        )

        app.execute("ANALYZE;")
        app.execute("VACUUM;")
        app.execute("PRAGMA optimize;")
        # Ensure single-file output (no WAL sidecars) before atomic replace.
        app.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        app.execute("PRAGMA journal_mode = DELETE;")
        app.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    finally:
        app.close()

    for suffix in ("-wal", "-shm"):
        side = tmp_path.with_name(tmp_path.name + suffix)
        if side.exists():
            try:
                side.unlink()
            except Exception:
                pass

    atomic_replace(tmp_path, app_sqlite_path)

    app_size = int(app_sqlite_path.stat().st_size)

    conn2 = sqlite3.connect(f"file:{app_sqlite_path.as_posix()}?mode=ro", uri=True)
    try:
        tables = {
            r[0]
            for r in conn2.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        if "fare_rules" in tables or "fare_amounts" in tables:
            raise RuntimeError(
                "app.sqlite must not contain canonical fare_rules/fare_amounts"
            )

        def count(name: str) -> int:
            return int(conn2.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0])

        metrics: dict[str, int] = {
            "canonical_size_bytes": canonical_size,
            "app_size_bytes": app_size,
            "build_ms": int((time.perf_counter() - t0) * 1000),
            "operators_rows": count("operators"),
            "places_rows": count("places"),
            "routes_rows": count("routes"),
            "route_patterns_rows": count("route_patterns"),
            "pattern_stops_rows": count("pattern_stops"),
            "fare_segments_rows": count("fare_segments"),
        }
        if "pattern_headways" in tables:
            metrics["pattern_headways_rows"] = count("pattern_headways")
        return metrics
    finally:
        conn2.close()
