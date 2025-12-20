from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import jsonschema
from hk_public_transport_etl.core import (
    atomic_write_json,
    file_size,
    sha256_file,
    utc_now_iso,
)


@dataclass(frozen=True, slots=True)
class FileEntry:
    path: str
    bytes: int
    sha256: str
    content_type: str | None = None


def _read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _extract_validation_counts(report: dict[str, Any]) -> tuple[int, int]:
    # permissive across report variants
    errors = 0
    warns = 0

    if isinstance(report.get("issues"), list):
        for it in report["issues"]:
            if not isinstance(it, dict):
                continue
            sev = str(it.get("severity") or "").lower()
            if sev == "error":
                errors += 1
            elif sev in ("warn", "warning"):
                warns += 1

    # fallback keys
    if isinstance(report.get("error_count"), int):
        errors = int(report["error_count"])
    if isinstance(report.get("warn_count"), int):
        warns = int(report["warn_count"])

    return errors, warns


def serialize_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    s = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (s + "\n").encode("utf-8")


def validate_manifest_against_jsonschema(
    *, manifest: dict[str, Any], schema_path: Path
) -> None:
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    jsonschema.validate(instance=manifest, schema=schema)


def default_contracts_schema_path() -> Path | None:
    try:
        import importlib.resources as r

        import hk_public_transport_contracts  # type: ignore
    except Exception:
        return None

    candidates = [
        ("schemas", "manifest.schema.json"),
        ("manifest.schema.json",),
        ("contracts", "manifest.schema.json"),
    ]
    for parts in candidates:
        try:
            p = r.files(hk_public_transport_contracts).joinpath(*parts)
            fp = Path(str(p))
            if fp.exists():
                return fp
        except Exception:
            continue
    return None


def build_manifest(
    *,
    bundle_id: str,
    bundle_version: str,
    schema_version: int,
    min_app_version: str,
    files: Iterable[FileEntry],
    sources: list[dict[str, Any]],
    build: dict[str, Any],
    validation: dict[str, Any],
    stats: dict[str, int] | None,
) -> dict[str, Any]:
    m: dict[str, Any] = {
        "manifest_version": 1,
        "bundle_id": bundle_id,
        "bundle_version": bundle_version,
        "created_at_utc": utc_now_iso(),
        "compatibility": {
            "schema_version": int(schema_version),
            "min_app_version": str(min_app_version),
        },
        "files": [
            {
                "path": f.path,
                "content_type": f.content_type,
                "bytes": int(f.bytes),
                "sha256": f.sha256,
            }
            for f in files
        ],
        "sources": sources,
        "build": build,
        "validation": validation,
    }
    if stats is not None:
        m["stats"] = {k: int(v) for k, v in stats.items()}
    return m


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    atomic_write_json(Path(path), manifest)


def sources_from_inputs(
    *,
    source_specs: dict[str, Any],
    raw_metadata_paths: dict[str, Path],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sid in sorted(raw_metadata_paths.keys()):
        raw_path = raw_metadata_paths[sid]
        spec = source_specs.get(sid) or {}
        authority = str(spec.get("authority") or "unknown")

        raw = _read_json(raw_path) if raw_path.exists() else {}
        items = raw.get("artifacts") if isinstance(raw.get("artifacts"), list) else None
        if items is None and isinstance(raw.get("raw_artifacts"), list):
            items = raw["raw_artifacts"]
        if items is None:
            items = []

        retrieved_at = None
        upstream_ref = None
        for it in items:
            if not isinstance(it, dict):
                continue
            retrieved_at = (
                retrieved_at or it.get("retrieved_at_utc") or it.get("retrieved_at")
            )
            upstream_ref = upstream_ref or it.get("uri") or it.get("url")
        if not isinstance(retrieved_at, str) or not retrieved_at:
            retrieved_at = utc_now_iso()

        out.append(
            {
                "id": sid,
                "authority": authority,
                "retrieved_at_utc": str(retrieved_at),
                "upstream_ref": (
                    str(upstream_ref)
                    if isinstance(upstream_ref, str) and upstream_ref
                    else None
                ),
                "sha256": sha256_file(raw_path).sha256,
            }
        )

    # drop None keys (schema allows optional upstream_ref)
    cleaned: list[dict[str, Any]] = []
    for s in out:
        d = {k: v for k, v in s.items() if v is not None}
        cleaned.append(d)
    return cleaned


def validation_from_reports(
    *,
    reports: dict[str, dict[str, Any]],
    fail_on_warn: bool,
) -> tuple[dict[str, Any], bool]:
    """
    Contract wants:
      { passed: bool, checks: [{id, passed, details?}] }
    """
    checks: list[dict[str, Any]] = []
    all_passed = True

    for sid in sorted(reports.keys()):
        rep = reports[sid]
        err, warn = _extract_validation_counts(rep)
        passed = (err == 0) and ((warn == 0) or (not fail_on_warn))
        all_passed = all_passed and passed
        details = f"errors={err} warnings={warn}"
        checks.append({"id": sid, "passed": bool(passed), "details": details})

    return {"passed": bool(all_passed), "checks": checks}, bool(all_passed)


def file_entries_from_dir(bundle_dir: Path) -> list[FileEntry]:
    """
    Walk bundle_dir and create FileEntry list with relative paths.
    """
    out: list[FileEntry] = []
    for p in sorted(bundle_dir.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(bundle_dir).as_posix()
        ct = None
        if rel.endswith(".json"):
            ct = "application/json"
        elif rel.endswith(".txt"):
            ct = "text/plain"
        elif rel.endswith(".sqlite"):
            ct = "application/x-sqlite3"
        out.append(
            FileEntry(
                path=rel,
                bytes=file_size(p),
                sha256=sha256_file(p).sha256,
                content_type=ct,
            )
        )
    # Deterministic ordering
    out.sort(key=lambda e: e.path)
    return out
