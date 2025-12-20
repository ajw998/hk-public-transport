from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from .stage import StageResult


@dataclass(slots=True)
class RunReport:
    run_id: str
    started_at_utc: str
    finished_at_utc: str
    status: str  # "success" | "failed"
    duration_ms: int

    stages: list[StageResult] = field(default_factory=list)
    events_jsonl: Optional[str] = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    def write_json(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )


def build_run_report(
    *,
    run_id: str,
    started_at_utc: str,
    finished_at_utc: str,
    duration_ms: int,
    stage_results: list[StageResult],
    events_jsonl: str | None,
    meta: dict[str, Any] | None = None,
) -> RunReport:
    status = "success" if all(s.status != "failed" for s in stage_results) else "failed"
    return RunReport(
        run_id=run_id,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
        status=status,
        duration_ms=duration_ms,
        stages=stage_results,
        events_jsonl=events_jsonl,
        meta=meta or {},
    )
