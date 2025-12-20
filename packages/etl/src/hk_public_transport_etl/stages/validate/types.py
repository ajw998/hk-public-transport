from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Mapping


class Severity(StrEnum):
    ERROR = "ERROR"
    WARN = "WARN"


class IssueCode(StrEnum):
    FK_MISSING = "FK_MISSING"
    KEY_COLUMN_NULL = "KEY_COLUMN_NULL"
    NORMALIZED_TABLES_NOT_FOUND = "NORMALIZED_TABLES_NOT_FOUND"
    PATTERN_SEQ_BASE_MISMATCH = "PATTERN_SEQ_BASE_MISMATCH"
    PATTERN_SEQ_GAPS_OR_DUPES = "PATTERN_SEQ_GAPS_OR_DUPES"
    PATTERN_TOO_LONG = "PATTERN_TOO_LONG"
    PATTERN_TOO_SHORT = "PATTERN_TOO_SHORT"
    ROUTE_MISSING_FARES = "ROUTE_MISSING_FARES"
    SCHEMA_MISSING_COLUMNS = "SCHEMA_MISSING_COLUMNS"
    TABLE_MISSING = "TABLE_MISSING"
    UNIQUENESS_VIOLATION = "UNIQUENESS_VIOLATION"
    UNRESOLVED_NONEMPTY = "UNRESOLVED_NONEMPTY"


IssueCodeLike = IssueCode | str


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    severity: Severity
    code: IssueCodeLike
    table: str
    message: str
    count: int = 1
    columns: tuple[str, ...] = ()
    samples: tuple[Mapping[str, Any], ...] = ()
    source_hint: str | None = None
    details: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        code_str = (
            self.code.value if isinstance(self.code, IssueCode) else str(self.code)
        )
        d: dict[str, Any] = {
            "severity": self.severity.value,
            "code": code_str,
            "table": self.table,
            "message": self.message,
            "count": int(self.count),
            "columns": list(self.columns),
            "samples": [dict(s) for s in self.samples],
        }
        if self.source_hint:
            d["source_hint"] = self.source_hint
        if self.details:
            d["details"] = dict(self.details)
        return d


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    errors: int
    warnings: int
    tables_checked: int
    unresolved_checked: int
    mappings_checked: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ValidationReport:
    report_version: str
    source_id: str
    version: str
    rules_version: str
    generated_at_utc: str
    spec: dict[str, Any]
    summary: ValidationSummary
    issues: tuple[ValidationIssue, ...]
    metrics: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "source_id": self.source_id,
            "version": self.version,
            "rules_version": self.rules_version,
            "generated_at_utc": self.generated_at_utc,
            "spec": self.spec,
            "summary": self.summary.to_dict(),
            "issues": [i.to_dict() for i in self.issues],
            "metrics": self.metrics,
            "config": self.config,
        }
