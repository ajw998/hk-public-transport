from __future__ import annotations

import sqlite3


def sql_integrity_checks(conn: sqlite3.Connection) -> None:
    integrity_check(conn)
    foreign_key_check(conn)
    pattern_stop_contiguity_check(conn)


def integrity_check(conn: sqlite3.Connection) -> None:
    row = conn.execute("PRAGMA integrity_check;").fetchone()
    if not row or row[0] != "ok":
        raise RuntimeError(f"SQLite integrity_check failed: {row[0] if row else row!r}")


def foreign_key_check(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA foreign_key_check;").fetchall()
    if rows:
        preview = "\n".join(repr(r) for r in rows[:25])
        raise RuntimeError(
            f"SQLite foreign_key_check failed ({len(rows)} rows). Preview:\n{preview}"
        )


def pattern_stop_contiguity_check(conn: sqlite3.Connection) -> None:
    # Strong invariant: seq should be contiguous 1..N for each pattern.
    # This is cheap and catches “holes” that hurt UI rendering.
    rows = conn.execute(
        """
        SELECT pattern_id, min_seq, max_seq, cnt
        FROM (
          SELECT pattern_id,
                 MIN(seq) AS min_seq,
                 MAX(seq) AS max_seq,
                 COUNT(*)  AS cnt
          FROM pattern_stops
          GROUP BY pattern_id
        )
        WHERE min_seq != 1 OR max_seq != cnt
        LIMIT 50;
        """
    ).fetchall()
    if rows:
        preview = "\n".join(repr(r) for r in rows[:25])
        raise RuntimeError(
            "pattern_stops seq contiguity failed for some patterns "
            "(expected min(seq)=1 and max(seq)=count(*)):\n" + preview
        )
