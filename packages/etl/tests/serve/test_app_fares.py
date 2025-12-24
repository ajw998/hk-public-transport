from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from hk_public_transport_etl.stages.commit.ddl import load_canonical_ddl
from hk_public_transport_etl.stages.serve.runner import run_build_app_sqlite


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
            "INSERT INTO places(place_id, place_key, place_type, primary_mode, name_tc, display_name_tc) "
            "VALUES (?,?,?,?,?,?);",
            (1, "bus:1", "stop", "bus", "測試站", "測試站"),
        )

        conn.execute(
            "INSERT INTO routes(route_id, route_key, upstream_route_id, mode, operator_id, route_short_name) "
            "VALUES (?,?,?,?,?,?);",
            (1, "bus:100", "100", "bus", "operator:CTB", "9"),
        )

        conn.execute(
            "INSERT INTO route_patterns(pattern_id, pattern_key, route_id, route_seq, direction_id, service_type, sequence_incomplete, is_circular) "
            "VALUES (?,?,?,?,?,?,?,?);",
            (1, "bus:100:1:abcd", 1, 1, 1, "regular", 0, 0),
        )
        conn.execute(
            "INSERT INTO pattern_stops(pattern_id, seq, place_id, allow_repeat) VALUES (?,?,?,?);",
            (1, 1, 1, 0),
        )

        conn.execute(
            "INSERT INTO fare_products(fare_product_id, product_key, mode) VALUES (?,?,?);",
            (1, "hk:fare_product:bus:default", "bus"),
        )

        # Generate >= 2000 OD pairs for determinism tests.
        # origin_seq=1, destination_seq=1..2000, with stepwise fares to force segmentation.
        fare_rules = []
        fare_amounts = []
        for i, dest_seq in enumerate(range(2, 2002), start=1):
            rule_key = f"bus:100:1:1:{dest_seq}"
            fare_rules.append((i, rule_key, "operator:CTB", "bus", 1, 1, dest_seq))
            amount = 500 if dest_seq <= 1000 else 700
            fare_amounts.append((i, 1, amount, 1))

        conn.executemany(
            "INSERT INTO fare_rules(fare_rule_id, rule_key, operator_id, mode, route_id, origin_seq, destination_seq) "
            "VALUES (?,?,?,?,?,?,?);",
            fare_rules,
        )
        conn.executemany(
            "INSERT INTO fare_amounts(fare_rule_id, fare_product_id, amount_cents, is_default) VALUES (?,?,?,?);",
            fare_amounts,
        )

        conn.execute(
            "INSERT INTO meta(meta_id, schema_version, bundle_version, generated_at, source_versions_json) "
            "VALUES (1, 1, 'v', 'now', '{}');"
        )
        conn.commit()
    finally:
        conn.close()


def test_app_fares_match_canonical(tmp_path: Path) -> None:
    canon = tmp_path / "canonical.sqlite"
    app = tmp_path / "app.sqlite"
    _mk_canonical_db(canon)

    run_build_app_sqlite(
        canonical_sqlite_path=canon,
        app_sqlite_path=app,
        schema_version=1,
        bundle_version="v",
    )

    c = sqlite3.connect(canon.as_posix())
    a = sqlite3.connect(app.as_posix())
    try:
        od = c.execute(
            """
            WITH choice AS (
              SELECT
                fare_rule_id,
                COALESCE(
                  MIN(CASE WHEN is_default = 1 THEN fare_product_id END),
                  MIN(fare_product_id)
                ) AS fare_product_id
              FROM fare_amounts
              GROUP BY fare_rule_id
            ),
            chosen AS (
              SELECT fa.fare_rule_id, c.fare_product_id, fa.amount_cents
              FROM choice c
              JOIN fare_amounts fa
                ON fa.fare_rule_id = c.fare_rule_id
               AND fa.fare_product_id = c.fare_product_id
            )
            SELECT fr.route_id, chosen.fare_product_id, fr.origin_seq, fr.destination_seq, chosen.amount_cents
            FROM fare_rules fr
            JOIN chosen ON chosen.fare_rule_id = fr.fare_rule_id
            ORDER BY fr.route_id, chosen.fare_product_id, fr.origin_seq, fr.destination_seq;
            """
        ).fetchall()

        for route_id, product_id, origin_seq, dest_seq, amt in od:
            got = a.execute(
                """
                SELECT amount_cents
                FROM fare_segments
                WHERE route_id = ?
                  AND fare_product_id = ?
                  AND origin_seq = ?
                  AND ? BETWEEN dest_from_seq AND dest_to_seq
                ORDER BY dest_from_seq DESC
                LIMIT 1;
                """,
                (route_id, product_id, origin_seq, dest_seq),
            ).fetchone()
            assert got is not None
            assert int(got[0]) == int(amt)

        od_count = len(od)
        seg_count = a.execute("SELECT COUNT(*) FROM fare_segments;").fetchone()[0]
        assert seg_count < od_count

        tables = {
            r[0]
            for r in a.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        assert "fare_rules" not in tables
        assert "fare_amounts" not in tables
    finally:
        c.close()
        a.close()
