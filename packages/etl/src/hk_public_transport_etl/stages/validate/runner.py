from __future__ import annotations

from pathlib import Path
from typing import Any

from hk_public_transport_etl.core.paths import DataLayout
from hk_public_transport_etl.registry.models import SourceSpec

from .checks import hard_validate, soft_validate
from .config import ValidateConfig
from .loader import load_canonical_tables, load_mappings, load_unresolved
from .registry import spec_for_source
from .report import write_validation_report
from .types import IssueCode, Severity, ValidationIssue


def run_validate_source(
    *,
    spec: SourceSpec,
    version: str,
    data_root: Path,
    cfg: ValidateConfig | None = None,
) -> tuple[int, Path]:
    cfg = cfg or ValidateConfig()
    v_spec = spec_for_source(spec.id)

    layout = DataLayout(root=Path(data_root))

    tables_dir = layout.normalized_tables(spec.id, version)
    mappings_dir = layout.normalized_mappings(spec.id, version)
    unresolved_dir = layout.normalized_unresolved(spec.id, version)

    report_path = layout.validation_report_json(spec.id, version)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(tables_dir.glob("*.parquet")) if tables_dir.exists() else []

    if v_spec is None:
        metrics: dict[str, Any] = {
            "skipped": True,
            "skip_reason": "No validation spec registered",
            "paths": {
                "tables_dir": str(tables_dir),
                "mappings_dir": str(mappings_dir),
                "unresolved_dir": str(unresolved_dir),
            },
        }
        write_validation_report(
            out_path=report_path,
            source_id=spec.id,
            version=version,
            rules_version="skipped",
            spec={},
            issues=[],
            cfg=cfg,
            metrics=metrics,
        )
        return 0, report_path

    if not parquet_files:
        issues = [
            ValidationIssue(
                severity=Severity.ERROR,
                code=IssueCode.NORMALIZED_TABLES_NOT_FOUND,
                table="(runner)",
                message=f"No parquet tables found under: {tables_dir}",
                count=1,
            )
        ]
        metrics: dict[str, Any] = {
            "spec": {"spec_id": v_spec.spec_id, "spec_version": v_spec.spec_version},
            "paths": {
                "tables_dir": str(tables_dir),
                "mappings_dir": str(mappings_dir),
                "unresolved_dir": str(unresolved_dir),
            },
            "counts": {"tables": 0, "unresolved": 0, "mappings": 0},
        }
        write_validation_report(
            out_path=report_path,
            source_id=spec.id,
            version=version,
            rules_version=f"{v_spec.spec_id}@{v_spec.spec_version}",
            spec={"spec_id": v_spec.spec_id, "spec_version": v_spec.spec_version},
            issues=issues,
            cfg=cfg,
            metrics=metrics,
        )
        return 1, report_path

    tables = load_canonical_tables(tables_dir)
    unresolved = load_unresolved(unresolved_dir)
    mappings = load_mappings(mappings_dir)

    issues: list[ValidationIssue] = []
    issues.extend(
        hard_validate(
            spec=v_spec,
            tables=tables,
            unresolved=unresolved,
            mappings=mappings,
            cfg=cfg,
        )
    )
    issues.extend(soft_validate(tables=tables, cfg=cfg))

    metrics = {
        "spec": {"spec_id": v_spec.spec_id, "spec_version": v_spec.spec_version},
        "paths": {
            "tables_dir": str(tables_dir),
            "mappings_dir": str(mappings_dir),
            "unresolved_dir": str(unresolved_dir),
        },
        "counts": {
            "tables": len(tables),
            "unresolved": len(unresolved),
            "mappings": len(mappings),
        },
    }

    write_validation_report(
        out_path=report_path,
        source_id=spec.id,
        version=version,
        rules_version=f"{v_spec.spec_id}@{v_spec.spec_version}",
        spec={"spec_id": v_spec.spec_id, "spec_version": v_spec.spec_version},
        issues=issues,
        cfg=cfg,
        metrics=metrics,
    )

    errors = sum(1 for i in issues if i.severity == Severity.ERROR)
    warns = sum(1 for i in issues if i.severity == Severity.WARN)
    exit_code = 1 if errors > 0 or (cfg.fail_on_warn and warns > 0) else 0
    return exit_code, report_path
