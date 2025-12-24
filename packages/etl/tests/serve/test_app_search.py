from __future__ import annotations

import sqlite3
from pathlib import Path

from hk_public_transport_etl.stages.commit.ddl import load_canonical_ddl
from hk_public_transport_etl.stages.serve.runner import run_build_app_sqlite
from hk_public_transport_etl.stages.serve.text import segment_cjk


def _mk_canonical_db(path: Path) -> None:
    ddl = load_canonical_ddl()
    conn = sqlite3.connect(path.as_posix())
    try:
        conn.executescript(ddl)

        conn.execute(
            "INSERT INTO operators(operator_id, operator_name_en) VALUES (?,?);",
            ("operator:CTB", "CTB"),
        )

        conn.execute(
            "INSERT INTO places(place_id, place_key, place_type, primary_mode, name_tc) "
            "VALUES (?,?,?,?,?);",
            (1, "bus:1", "stop", "bus", "測試站"),
        )

        conn.execute(
            "INSERT INTO routes(route_id, route_key, upstream_route_id, mode, operator_id, route_short_name, "
            "origin_text_tc, destination_text_tc) "
            "VALUES (?,?,?,?,?,?,?,?);",
            (
                1,
                "bus:100",
                "100",
                "bus",
                "operator:CTB",
                "X1",
                "高鐵西九龍站",
                "啟德郵輪碼頭(經沐安街)",
            ),
        )
        conn.execute(
            "INSERT INTO routes(route_id, route_key, upstream_route_id, mode, operator_id, route_short_name, "
            "origin_text_en, destination_text_en) "
            "VALUES (?,?,?,?,?,?,?,?);",
            (2, "gmb:690", "690", "gmb", "operator:CTB", "69", "Central", "Kowloon"),
        )

        conn.execute(
            "INSERT INTO meta(meta_id, schema_version, bundle_version, generated_at, source_versions_json) "
            "VALUES (1, 1, 'v', 'now', '{}');"
        )
        conn.commit()
    finally:
        conn.close()


def test_app_search_fts_and_route_prefix(tmp_path: Path) -> None:
    canon = tmp_path / "canonical.sqlite"
    app = tmp_path / "app.sqlite"
    _mk_canonical_db(canon)

    run_build_app_sqlite(
        canonical_sqlite_path=canon,
        app_sqlite_path=app,
        schema_version=1,
        bundle_version="v",
    )

    conn = sqlite3.connect(app.as_posix())
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        assert "search_fts" in tables

        route_hits = conn.execute(
            """
            SELECT route_id
            FROM routes
            WHERE route_short_name LIKE ? || '%'
              AND mode_id = ?
            ORDER BY route_short_name ASC
            LIMIT 10;
            """,
            ("69", 2),
        ).fetchall()
        assert route_hits

        place_name_tc = conn.execute(
            "SELECT name_tc FROM places WHERE place_id = 1;"
        ).fetchone()[0]
        fts_q = segment_cjk(place_name_tc)
        found = conn.execute(
            """
            SELECT d.ref_id
            FROM search_fts
            JOIN search_docs d ON d.doc_id = search_fts.rowid
            WHERE search_fts MATCH ?
              AND d.kind = 'p'
            LIMIT 1;
            """,
            (fts_q,),
        ).fetchone()
        assert found is not None
        assert int(found[0]) == 1
    finally:
        conn.close()
