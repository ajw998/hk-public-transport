from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class HeadwayResolveStats:
    inserted_rows: int
    unresolved_missing_route: int
    unresolved_ambiguous_route: int
    unresolved_missing_route_seq: int
    unresolved_missing_pattern: int

    def to_dict(self) -> dict[str, object]:
        return {
            "inserted_rows": int(self.inserted_rows),
            "unresolved_missing_route": int(self.unresolved_missing_route),
            "unresolved_ambiguous_route": int(self.unresolved_ambiguous_route),
            "unresolved_missing_route_seq": int(self.unresolved_missing_route_seq),
            "unresolved_missing_pattern": int(self.unresolved_missing_pattern),
        }


def resolve_pattern_headways(
    conn: sqlite3.Connection,
    *,
    routes_fares_source_id: str = "td_routes_fares_xml",
    create_debug_tables: bool = False,
) -> HeadwayResolveStats:
    """
    Resolve GTFS headway frequencies onto canonical route_patterns.pattern_id.

    Assumptions / invariants:
      - routes.route_id is INTERNAL
      - upstream identity lives in map_route_source.source_route_id (string)
      - headway_frequencies.upstream_route_id is INTEGER (GTFS route_id)
      - route_patterns.route_seq corresponds to "route bound" in headway trip_id parsing

    Determinism:
      - For each (route_id, route_seq), choose pattern with max stop_count (ties -> min pattern_id)
      - For each resolved key, aggregate duplicates by MIN(headway_secs), sample_trip_id = MIN(sample_trip_id)
    """
    conn.execute("PRAGMA foreign_keys = ON;")

    # Temporary tables used for resolution
    # We avoid window functions to reduce SQLite version assumptions.

    # Normalization for numeric strings in SQLite:
    # If purely digits: ltrim leading zeros; if result empty to "0"; cast to INTEGER
    norm_route_id_expr = """
    CASE
      WHEN source_route_id GLOB '[0-9]*' AND source_route_id <> ''
      THEN CAST(
        CASE
          WHEN ltrim(source_route_id, '0') = '' THEN '0'
          ELSE ltrim(source_route_id, '0')
        END AS INTEGER
      )
      ELSE NULL
    END
    """

    conn.executescript(
        f"""
        DROP TABLE IF EXISTS temp._ru_raw;
        CREATE TEMP TABLE _ru_raw AS
        SELECT
          route_id,
          {norm_route_id_expr} AS upstream_route_id
        FROM map_route_source
        WHERE source = {sql_quote(routes_fares_source_id)};

        DROP TABLE IF EXISTS temp._ru_ambiguous;
        CREATE TEMP TABLE _ru_ambiguous AS
        SELECT upstream_route_id
        FROM _ru_raw
        WHERE upstream_route_id IS NOT NULL
        GROUP BY upstream_route_id
        HAVING COUNT(DISTINCT route_id) > 1;

        DROP TABLE IF EXISTS temp._ru_unique;
        CREATE TEMP TABLE _ru_unique AS
        SELECT
          upstream_route_id,
          MIN(route_id) AS route_id
        FROM _ru_raw
        WHERE upstream_route_id IS NOT NULL
          AND upstream_route_id NOT IN (SELECT upstream_route_id FROM _ru_ambiguous)
        GROUP BY upstream_route_id;

        -- Pattern stop counts (active patterns only)
        DROP TABLE IF EXISTS temp._pat_counts;
        CREATE TEMP TABLE _pat_counts AS
        SELECT
          rp.route_id,
          rp.route_seq,
          rp.pattern_id,
          COUNT(ps.seq) AS stop_count
        FROM route_patterns rp
        JOIN pattern_stops ps ON ps.pattern_id = rp.pattern_id
        GROUP BY rp.route_id, rp.route_seq, rp.pattern_id;

        -- Max stop_count per (route_id, route_seq)
        DROP TABLE IF EXISTS temp._pat_max;
        CREATE TEMP TABLE _pat_max AS
        SELECT route_id, route_seq, MAX(stop_count) AS max_stop_count
        FROM _pat_counts
        GROUP BY route_id, route_seq;

        -- Deterministic chosen pattern per (route_id, route_seq): max stop_count, tie -> min(pattern_id)
        DROP TABLE IF EXISTS temp._pat_best;
        CREATE TEMP TABLE _pat_best AS
        SELECT
          c.route_id,
          c.route_seq,
          MIN(c.pattern_id) AS pattern_id
        FROM _pat_counts c
        JOIN _pat_max m
          ON m.route_id = c.route_id
         AND m.route_seq = c.route_seq
         AND m.max_stop_count = c.stop_count
        GROUP BY c.route_id, c.route_seq;

        -- Map upstream (route_id, route_seq) -> chosen pattern_id
        DROP TABLE IF EXISTS temp._pat_upstream;
        CREATE TEMP TABLE _pat_upstream AS
        SELECT
          ru.upstream_route_id,
          pb.route_seq,
          pb.pattern_id
        FROM _ru_unique ru
        JOIN _pat_best pb
          ON pb.route_id = ru.route_id;

        -- Clear existing resolved rows (idempotent rebuild)
        DELETE FROM pattern_headways;

        -- Insert resolved headways (aggregate duplicates deterministically)
        INSERT INTO pattern_headways (
          pattern_id, service_id, start_time, end_time, headway_secs, sample_trip_id
        )
        SELECT
          pu.pattern_id,
          hf.service_id,
          hf.start_time,
          hf.end_time,
          MIN(hf.headway_secs) AS headway_secs,
          MIN(hf.sample_trip_id) AS sample_trip_id
        FROM headway_frequencies hf
        JOIN _pat_upstream pu
          ON pu.upstream_route_id = hf.upstream_route_id
         AND pu.route_seq = hf.route_seq
        WHERE hf.route_seq IS NOT NULL
        GROUP BY pu.pattern_id, hf.service_id, hf.start_time, hf.end_time;

        """
    )

    inserted = conn.execute("SELECT COUNT(*) FROM pattern_headways;").fetchone()[0]

    # ---- unresolved counts
    missing_route = conn.execute(
        """
        SELECT COUNT(*)
        FROM headway_frequencies hf
        LEFT JOIN _ru_unique ru ON ru.upstream_route_id = hf.upstream_route_id
        WHERE ru.route_id IS NULL;
        """
    ).fetchone()[0]

    ambiguous_route = conn.execute(
        """
        SELECT COUNT(*)
        FROM headway_frequencies hf
        WHERE hf.upstream_route_id IN (SELECT upstream_route_id FROM _ru_ambiguous);
        """
    ).fetchone()[0]

    missing_route_seq = conn.execute(
        """
        SELECT COUNT(*)
        FROM headway_frequencies hf
        WHERE hf.route_seq IS NULL;
        """
    ).fetchone()[0]

    missing_pattern = conn.execute(
        """
        SELECT COUNT(*)
        FROM headway_frequencies hf
        JOIN _ru_unique ru ON ru.upstream_route_id = hf.upstream_route_id
        LEFT JOIN _pat_upstream pu
          ON pu.upstream_route_id = hf.upstream_route_id
         AND pu.route_seq = hf.route_seq
        WHERE hf.route_seq IS NOT NULL
          AND pu.pattern_id IS NULL;
        """
    ).fetchone()[0]

    if create_debug_tables:
        conn.executescript(
            """
            DROP TABLE IF EXISTS unresolved_headway_frequencies;
            CREATE TABLE unresolved_headway_frequencies AS
            SELECT
              hf.*,
              CASE
                WHEN hf.route_seq IS NULL THEN 'missing_route_seq'
                WHEN hf.upstream_route_id IN (SELECT upstream_route_id FROM _ru_ambiguous) THEN 'ambiguous_upstream_route_id'
                WHEN hf.upstream_route_id NOT IN (SELECT upstream_route_id FROM _ru_unique) THEN 'missing_upstream_route_id'
                WHEN NOT EXISTS (
                  SELECT 1 FROM _pat_upstream pu
                  WHERE pu.upstream_route_id = hf.upstream_route_id AND pu.route_seq = hf.route_seq
                ) THEN 'missing_pattern'
                ELSE 'unknown'
              END AS reason
            FROM headway_frequencies hf
            WHERE hf.upstream_route_id NOT IN (SELECT upstream_route_id FROM _ru_unique)
               OR hf.upstream_route_id IN (SELECT upstream_route_id FROM _ru_ambiguous)
               OR hf.route_seq IS NULL
               OR NOT EXISTS (
                  SELECT 1 FROM _pat_upstream pu
                  WHERE pu.upstream_route_id = hf.upstream_route_id AND pu.route_seq = hf.route_seq
               );
            """
        )

    # Clean up temp tables (optional; TEMP tables die with connection anyway)
    conn.executescript(
        """
        DROP TABLE IF EXISTS temp._ru_raw;
        DROP TABLE IF EXISTS temp._ru_ambiguous;
        DROP TABLE IF EXISTS temp._ru_unique;
        DROP TABLE IF EXISTS temp._pat_counts;
        DROP TABLE IF EXISTS temp._pat_max;
        DROP TABLE IF EXISTS temp._pat_best;
        DROP TABLE IF EXISTS temp._pat_upstream;
        """
    )

    return HeadwayResolveStats(
        inserted_rows=int(inserted),
        unresolved_missing_route=int(missing_route),
        unresolved_ambiguous_route=int(ambiguous_route),
        unresolved_missing_route_seq=int(missing_route_seq),
        unresolved_missing_pattern=int(missing_pattern),
    )


def sql_quote(s: str) -> str:
    # Minimal, safe SQL string quoting for executescript usage
    return "'" + s.replace("'", "''") + "'"
