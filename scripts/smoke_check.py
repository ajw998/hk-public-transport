from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import polars as pl


def run_query(
    con: duckdb.DuckDBPyConnection, sql_path: Path, route_short_name: str
) -> None:
    sql = sql_path.read_text("utf-8").replace("{route_short_name}", route_short_name)
    print(f"\n-- {sql_path.name} --")
    df = con.execute(sql).pl()
    with pl.Config(
        tbl_rows=500, tbl_cols=100, tbl_width_chars=2000, fmt_str_lengths=200
    ):
        print(df)


def _discover_db(published_root: Path) -> Path:
    """
    Find the newest transport.sqlite under published/{bundle_id}/{version}/.
    """
    if not published_root.exists():
        raise FileNotFoundError(f"Published root not found: {published_root}")

    candidates = sorted(
        published_root.glob("**/transport.sqlite"), key=lambda p: p.stat().st_mtime
    )
    if not candidates:
        raise FileNotFoundError(f"No transport.sqlite found under {published_root}")
    return candidates[-1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run golden tests against the published transport.sqlite."
    )
    parser.add_argument(
        "--route",
        default="9",
        help="Bus route short_name to check",
    )

    args = parser.parse_args()

    scripts_dir = Path(__file__).parent

    db_path = _discover_db(Path("data/published"))
    print(f"Using DB: {db_path}")
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    con = duckdb.connect()
    con.execute(f"ATTACH '{db_path.as_posix()}' AS db (TYPE SQLITE);")
    con.execute("SET search_path = db.main;")

    for sql_path in sorted(scripts_dir.glob("*.sql")):
        if sql_path.is_file():
            run_query(con, sql_path, args.route)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
