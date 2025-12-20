from pathlib import Path

import polars as pl


def main():
    p = Path("data/staged/td_routes_fares_xml/2025-12-18/tables")
    for f in sorted(p.glob("td_route_*.parquet")):
        df = pl.read_parquet(f)
        print(f.name, "DISTRICT" in df.columns, "cols=", len(df.columns))


if __name__ == "__main__":
    main()
