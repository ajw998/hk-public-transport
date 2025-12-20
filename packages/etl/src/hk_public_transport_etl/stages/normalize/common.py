from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import polars as pl
from hk_public_transport_etl.core import (
    JsonObject,
    NormalizeError,
    atomic_write_json,
    read_parquet_df,
    table_meta_from_df,
    write_parquet_atomic,
)

from .types import NormalizedMetadata, OutputTableMeta


def drop_if_present(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    present = [c for c in cols if c in df.columns]
    return df.drop(present) if present else df


def list_tables(tables_dir: Path) -> dict[str, Path]:
    if not tables_dir.exists():
        return {}
    out: dict[str, Path] = {}
    for p in sorted(tables_dir.glob("*.parquet"), key=lambda x: x.name):
        out[p.stem] = p
    return out


def require_columns(df: pl.DataFrame, *, table: str, cols: Iterable[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise NormalizeError(f"[{table}] missing required columns: {missing}")


def stable_sort(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    keys2 = [k for k in keys if k in df.columns]
    if not keys2 or df.height <= 1:
        return df
    return df.sort(keys2, nulls_last=True)


@dataclass(frozen=True)
class ParsedTableStore:
    """
    Convenience wrapper around data/staged/{source}/{version}/tables/*.parquet
    """

    tables_dir: Path
    _paths: dict[str, Path]

    @classmethod
    def open(cls, parsed_dir: Path) -> "ParsedTableStore":
        return cls(
            tables_dir=parsed_dir / "tables", _paths=list_tables(parsed_dir / "tables")
        )

    def has(self, name: str) -> bool:
        return name in self._paths

    def must_df(self, name: str) -> pl.DataFrame:
        p = self._paths.get(name)
        if not p:
            raise NormalizeError(f"missing required parsed table: {name}.parquet")
        return read_parquet_df(p)

    def maybe_df(self, name: str) -> pl.DataFrame | None:
        p = self._paths.get(name)
        return read_parquet_df(p) if p else None


@dataclass(slots=True)
class NormalizeWriter:
    out_dir: Path
    canonical_paths: dict[str, Path] = field(default_factory=dict)
    mapping_paths: dict[str, Path] = field(default_factory=dict)
    unresolved_paths: dict[str, Path] = field(default_factory=dict)

    def write_parquet(self, *, kind: str, name: str, df: pl.DataFrame) -> Path:
        if kind == "canonical":
            base = self.out_dir / "tables"
        elif kind == "mapping":
            base = self.out_dir / "mappings"
        elif kind == "unresolved":
            base = self.out_dir / "unresolved"
        else:
            raise ValueError(f"Unknown kind: {kind!r}")

        base.mkdir(parents=True, exist_ok=True)
        path = base / f"{name}.parquet"
        write_parquet_atomic(df, path)

        if kind == "canonical":
            self.canonical_paths[name] = path
        elif kind == "mapping":
            self.mapping_paths[name] = path
        else:
            self.unresolved_paths[name] = path
        return path

    def write_metadata(
        self,
        *,
        source_id: str,
        version: str,
        rules_version: str,
        config: JsonObject,
        inputs: JsonObject,
        warnings: list[JsonObject],
    ) -> Path:
        outputs: dict[str, OutputTableMeta] = {}

        def add_meta(kind: str, name: str, path: Path) -> None:
            df = read_parquet_df(path)
            m = table_meta_from_df(path, df)
            m["kind"] = kind  # TypedDict field overwrite is OK
            outputs[name] = m

        for n, p in self.canonical_paths.items():
            add_meta("canonical", n, p)
        for n, p in self.mapping_paths.items():
            add_meta("mapping", n, p)
        for n, p in self.unresolved_paths.items():
            add_meta("unresolved", n, p)

        meta: NormalizedMetadata = {
            "source_id": source_id,
            "version": version,
            "rules_version": rules_version,
            "config": config,
            "inputs": inputs,
            "outputs": outputs,
            "warnings": warnings,
        }

        meta_path = self.out_dir / "normalized_metadata.json"
        atomic_write_json(meta_path, meta)
        return meta_path
