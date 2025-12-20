from __future__ import annotations

from pathlib import Path
from typing import Any

from hk_public_transport_etl.core import atomic_write_json, utc_now_iso

from .config import ValidateConfig
from .types import Severity, ValidationIssue, ValidationReport, ValidationSummary

REPORT_VERSION = "1.0"


def write_validation_report(
    *,
    out_path: Path,
    source_id: str,
    version: str,
    rules_version: str,
    spec: dict[str, Any],
    issues: list[ValidationIssue],
    cfg: ValidateConfig,
    metrics: dict[str, Any],
) -> None:
    errors = sum(1 for i in issues if i.severity is Severity.ERROR)
    warns = sum(1 for i in issues if i.severity is Severity.WARN)

    summary = ValidationSummary(
        errors=errors,
        warnings=warns,
        tables_checked=int(metrics.get("counts", {}).get("tables", 0)),
        unresolved_checked=int(metrics.get("counts", {}).get("unresolved", 0)),
        mappings_checked=int(metrics.get("counts", {}).get("mappings", 0)),
    )

    report = ValidationReport(
        report_version=REPORT_VERSION,
        source_id=source_id,
        version=version,
        rules_version=rules_version,
        generated_at_utc=utc_now_iso(),
        spec=spec,
        summary=summary,
        issues=tuple(issues),
        metrics=metrics,
        config=cfg.to_dict(),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    atomic_write_json(out_path, report.to_dict())
