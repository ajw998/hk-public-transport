from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import polars as pl
from hk_public_transport_etl.stages.serve.text import segment_cjk


def _discover_db(published_root: Path) -> Path:
    if not published_root.exists():
        raise FileNotFoundError(f"Published root not found: {published_root}")

    candidates = sorted(
        published_root.glob("**/app.sqlite"), key=lambda p: p.stat().st_mtime
    )
    if not candidates:
        raise FileNotFoundError(f"No app.sqlite found under {published_root}")
    return candidates[-1]


def _df(conn: sqlite3.Connection, sql: str, params: dict[str, object]) -> pl.DataFrame:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    return pl.DataFrame(rows, schema=cols, orient="row") if cols else pl.DataFrame()


def _load_sql(name: str) -> str:
    return (Path(__file__).parent / name).read_text("utf-8")


def _auto_segment_if_cjk(q: str) -> str:
    if any(0x4E00 <= ord(ch) <= 0x9FFF for ch in q):
        return segment_cjk(q)
    return q


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run interactive search queries against the published app.sqlite."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def _opt_int(s: str) -> int | None:
        s = s.strip()
        if not s:
            return None
        return int(s)

    p_prefix = sub.add_parser("prefix", help="Route-number prefix search")
    p_prefix.add_argument("--q", default="9", help="Query prefix (default: 9)")
    p_prefix.add_argument("--mode-id", type=_opt_int, default=None)
    p_prefix.add_argument("--operator-id", type=_opt_int, default=None)

    p_fts = sub.add_parser("fts", help="FTS5 search (names across EN/TC/SC)")
    p_fts.add_argument(
        "--q",
        default="9",
        help="FTS query string (default: 9). For CJK, auto-segmented.",
    )

    args = parser.parse_args()

    db_path = _discover_db(Path("data/published"))
    print(f"Using DB: {db_path}")

    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        if args.cmd == "prefix":
            sql = _load_sql("search_route_prefix.sql")
            df = _df(
                conn,
                sql,
                {
                    "q": args.q,
                    "mode_id": args.mode_id,
                    "operator_id": args.operator_id,
                },
            )
        elif args.cmd == "fts":
            sql = _load_sql("search_fts.sql")
            df = _df(conn, sql, {"fts_q": _auto_segment_if_cjk(args.q)})
        else:
            raise AssertionError("unreachable")

        with pl.Config(
            tbl_rows=500, tbl_cols=100, tbl_width_chars=2000, fmt_str_lengths=200
        ):
            print(df)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
