from __future__ import annotations

import importlib.metadata
import shutil
import tempfile
from pathlib import Path
from typing import cast

import pyarrow as pa
from hk_public_transport_etl.core import (
    ParseError,
    atomic_dir_swap,
    atomic_write_json,
    schema_fingerprint,
    sha256_file,
    utc_now_iso,
    write_parquet_atomic,
)
from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry.models import SourceSpec
from hk_public_transport_etl.stages.fetch.models import RawMetadata

from .models import InputArtifact, ParsedDataset, ParsedTable
from .registry import get_parser
from .types import ParserFn, ParserResult, ParserTables


def run_parse_source(
    *, spec: SourceSpec, version: str, data_root: Path
) -> ParsedDataset:
    source_id = spec.id
    layout = DataLayout(root=Path(data_root))
    raw_dir = layout.raw(source_id, version)
    artifacts_dir = layout.raw_artifacts(source_id, version)
    raw_meta_path = layout.raw_metadata_json(source_id, version)

    _require_raw_cache(
        source_id=source_id,
        version=version,
        artifacts_dir=artifacts_dir,
        raw_meta_path=raw_meta_path,
    )
    meta = _load_raw_metadata(
        raw_meta_path, expected_source_id=source_id, expected_version=version
    )

    warnings: list[str] = []
    warnings.extend(_warn_extra_files_on_disk(artifacts_dir=artifacts_dir, meta=meta))

    # Required-vs-optional is a config contract, not a filename contract.
    required_ids = {e.id for e in spec.endpoints if getattr(e, "required", True)}
    bad_required = sorted([eid for eid in meta.errors.keys() if eid in required_ids])
    if bad_required:
        raise ParseError(
            f"Raw cache contains errors for required endpoints: {bad_required}"
        )

    # Strong provenance list (and corruption detection)
    input_artifacts = _materialize_input_artifacts(raw_dir=raw_dir, meta=meta)

    parser_id = source_id  # MVP convention
    parser = get_parser(parser_id)

    tables, parse_warnings = _run_parser(parser=parser, artifacts_dir=artifacts_dir)
    warnings.extend(parse_warnings)

    staged_parent = layout.staged(source_id, version).parent
    staged_parent.mkdir(parents=True, exist_ok=True)
    final_dir = layout.staged(source_id, version)
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"{version}.tmp.", dir=str(staged_parent)))

    try:
        parsed_tables = _write_tables(staged_dir=tmp_dir, tables=tables)

        dataset = ParsedDataset(
            source_id=source_id,
            version=version,
            input_artifacts=sorted(input_artifacts, key=lambda a: a.name),
            output_tables=sorted(parsed_tables, key=lambda t: t.table_name),
            parser_id=parser_id,
            parser_version=_parser_version(),
            generated_at_utc=utc_now_iso(),
            warnings=sorted(set(warnings)),
        )

        atomic_write_json(
            tmp_dir / "parsed_metadata.json", dataset.model_dump(mode="json")
        )
        atomic_dir_swap(final_dir=final_dir, tmp_dir=tmp_dir)
        return dataset
    except Exception:
        _safe_rmtree(tmp_dir)
        raise


def _materialize_input_artifacts(
    *, raw_dir: Path, meta: RawMetadata
) -> list[InputArtifact]:
    """
    Robustness:
      - reject unsafe relpaths
      - require artifacts/ prefix
      - require file exists
      - verify sha256/bytes match metadata (corruption detection)
    """
    out: list[InputArtifact] = []

    for a in sorted(meta.artifacts, key=lambda x: x.endpoint_id):
        _require_safe_relpath(a.path, context=f"endpoint_id={a.endpoint_id}")
        _require_under_artifacts_dir(a.path, context=f"endpoint_id={a.endpoint_id}")

        p = raw_dir / Path(a.path)
        if not p.exists():
            raise ParseError(
                f"Missing cached artifact on disk: endpoint_id={a.endpoint_id} path={a.path}"
            )

        d = sha256_file(p)
        if d.bytes != a.bytes or d.sha256.lower() != a.sha256.lower():
            raise ParseError(
                f"Cached artifact hash/size mismatch: endpoint_id={a.endpoint_id} "
                f"expected={a.sha256}/{a.bytes} got={d.sha256}/{d.bytes} path={p}"
            )

        out.append(
            InputArtifact(
                name=a.filename,
                relpath=a.path,
                sha256=a.sha256,
                bytes=a.bytes,
            )
        )

    return out


def _warn_extra_files_on_disk(*, artifacts_dir: Path, meta: RawMetadata) -> list[str]:
    """
    Warning-only: files in artifacts_dir that aren't referenced by raw_metadata.json.
    """
    disk = {p.name for p in artifacts_dir.iterdir() if p.is_file()}
    referenced = {Path(a.path).name for a in meta.artifacts}  # safer than a.filename
    extras = sorted(disk - referenced)
    if not extras:
        return []
    sample = extras[:20]
    tail = " ..." if len(extras) > 20 else ""
    return [
        f"Extra files present in artifacts_dir but not referenced by raw_metadata.json: {sample}{tail}"
    ]


def _require_safe_relpath(relpath: str, *, context: str) -> None:
    p = Path(relpath)
    if p.is_absolute():
        raise ParseError(
            f"Unsafe absolute artifact path in metadata ({context}): {relpath!r}"
        )
    if any(part == ".." for part in p.parts):
        raise ParseError(f"Unsafe path traversal in metadata ({context}): {relpath!r}")


def _require_under_artifacts_dir(relpath: str, *, context: str) -> None:
    if not Path(relpath).as_posix().startswith("artifacts/"):
        raise ParseError(f"Artifact path not under artifacts/ ({context}): {relpath!r}")


def _require_raw_cache(
    *, source_id: str, version: str, artifacts_dir: Path, raw_meta_path: Path
) -> None:
    if not artifacts_dir.exists():
        raise ParseError(
            f"Raw cache missing for {source_id}/{version}: {artifacts_dir} does not exist"
        )
    if not raw_meta_path.exists():
        raise ParseError(
            f"Raw cache missing for {source_id}/{version}: {raw_meta_path} does not exist"
        )


def _load_raw_metadata(
    raw_meta_path: Path, *, expected_source_id: str, expected_version: str
) -> RawMetadata:
    try:
        meta = RawMetadata.model_validate_json(
            raw_meta_path.read_text(encoding="utf-8")
        )
    except Exception as e:  # noqa: BLE001
        raise ParseError(
            f"raw_metadata.json missing/unreadable: {raw_meta_path}"
        ) from e

    if meta.source_id != expected_source_id:
        raise ParseError(
            f"raw_metadata.json source_id mismatch: expected={expected_source_id!r} got={meta.source_id!r}"
        )
    if meta.version != expected_version:
        raise ParseError(
            f"raw_metadata.json version mismatch: expected={expected_version!r} got={meta.version!r}"
        )

    return meta


def _run_parser(
    *, parser: ParserFn, artifacts_dir: Path
) -> tuple[ParserTables, list[str]]:
    try:
        result: ParserResult = parser(artifacts_dir)
    except Exception as e:  # noqa: BLE001
        raise ParseError(f"Parse failed for artifacts_dir={artifacts_dir}: {e}") from e

    tables: ParserTables
    warnings: list[str]

    if isinstance(result, tuple):
        t0, w0 = result
        if not isinstance(t0, dict) or not isinstance(w0, list):
            raise ParseError("Parser returned malformed (tables, warnings) tuple")
        tables = cast(ParserTables, t0)
        warnings = [str(x) for x in w0]
    else:
        tables = cast(ParserTables, result)
        warnings = []

    for name, t in tables.items():
        _require_safe_table_name(name)
        if not isinstance(t, pa.Table):
            raise ParseError(
                f"Parser produced non-Arrow table for {name}: {type(t).__name__}"
            )

    return tables, sorted(set(warnings))


def _require_safe_table_name(name: str) -> None:
    if not name or "/" in name or "\\" in name or name.startswith("."):
        raise ParseError(f"Unsafe table name: {name!r}")


def _write_tables(
    *, staged_dir: Path, tables: dict[str, pa.Table]
) -> list[ParsedTable]:
    tables_dir = staged_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    out: list[ParsedTable] = []
    for name in sorted(tables.keys()):
        out.append(
            _write_one_table(staged_dir=staged_dir, name=name, table=tables[name])
        )
    return out


def _write_one_table(*, staged_dir: Path, name: str, table: pa.Table) -> ParsedTable:
    out_rel = Path("tables") / f"{name}.parquet"
    out_path = staged_dir / out_rel

    write_parquet_atomic(table=table, out_path=out_path)

    if not out_path.exists():
        raise ParseError(f"Parquet write succeeded but file missing: {out_path}")

    d = sha256_file(out_path)

    return ParsedTable(
        table_name=name,
        relpath=out_rel.as_posix(),
        schema_hash=schema_fingerprint(table.schema),
        row_count=table.num_rows,
        sha256=d.sha256,
        bytes=d.bytes,
    )


def _safe_rmtree(p: Path) -> None:
    try:
        if p.exists():
            shutil.rmtree(p)
    except OSError:
        pass


def _parser_version() -> str:
    for dist in ("hk-public_transport-etl", "hk_public_transport_etl"):
        try:
            return importlib.metadata.version(dist)
        except importlib.metadata.PackageNotFoundError:
            pass
    return "0.0.0+unknown"
