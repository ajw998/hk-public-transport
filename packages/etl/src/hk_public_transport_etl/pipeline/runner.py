from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from hk_public_transport_etl.core import (
    ILogger,
    configure_logging,
    get_logger,
    monotonic_ms,
)

from .context import RunContext
from .events import EventSink, EventType, make_event, utc_now_iso
from .report import build_run_report
from .stage import (
    FunctionStage,
    Stage,
    StageResult,
    format_duration_ms,
    run_stage,
)


@dataclass(slots=True)
class RunnerConfig:
    stop_on_failure: bool = True


def default_logger() -> ILogger:
    """
    Provide a structlog BoundLogger that satisfies ILogger.

    This avoids mixing stdlib loggers (which lack .bind) with the ILogger
    protocol expected by the pipeline.
    """
    configure_logging()
    return get_logger("pipeline")


class PipelineRunner:
    def __init__(
        self,
        *,
        stages: Sequence[Stage],
        cfg: RunnerConfig | None = None,
        logger: ILogger | None = None,
    ) -> None:
        self.stages = list(stages)
        self.cfg = cfg or RunnerConfig()
        self.logger: ILogger = logger or default_logger()

        ids = [s.stage_id for s in self.stages]
        if len(ids) != len(set(ids)):
            dupes = sorted({x for x in ids if ids.count(x) > 1})
            raise ValueError(f"Duplicate stage_id(s): {dupes}")

    @staticmethod
    def fn(stage_id: str, fn) -> Stage:
        return FunctionStage(stage_id=stage_id, fn=fn)

    def run(
        self,
        *,
        data_root: Path,
        run_root: Path,
        run_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> tuple[int, Path]:
        """
        Execute the pipeline and write:
          - events.jsonl
          - run_report.json

        Returns: (exit_code, report_path)
        """
        meta = meta or {}
        rid = run_id or uuid.uuid4().hex
        run_root = Path(run_root) / rid
        run_root.mkdir(parents=True, exist_ok=True)

        events_path = run_root / "events.jsonl"
        sink = EventSink(events_path)

        ctx = RunContext(
            run_id=rid,
            run_root=run_root,
            data_root=Path(data_root),
            logger=self.logger,
            events=sink,
            meta=meta,
        )

        started_at = utc_now_iso()
        t0 = monotonic_ms()

        self.logger.info(
            "Pipeline starting",
            run_id=rid,
            stages=[s.stage_id for s in self.stages],
            data_root=str(ctx.data_root),
            run_root=str(run_root),
            meta_keys=sorted(meta.keys()),
        )
        ctx.events.emit(
            make_event(event_type=EventType.RUN_START, run_id=rid, stage=None, **meta)
        )

        results: list[StageResult] = []

        total = len(self.stages)
        for idx, st in enumerate(self.stages, start=1):
            res = run_stage(ctx=ctx, stage=st, index=idx, total=total)
            results.append(res)

            if res.status == "failed" and self.cfg.stop_on_failure:
                self.logger.error("Stopping on first failure", stage=st.stage_id)
                break

        finished_at = utc_now_iso()
        duration = monotonic_ms() - t0

        report = build_run_report(
            run_id=rid,
            started_at_utc=started_at,
            finished_at_utc=finished_at,
            duration_ms=duration,
            stage_results=results,
            events_jsonl=str(events_path),
            meta=meta,
        )

        report_json = run_root / "run_report.json"
        report.write_json(report_json)

        ctx.events.emit(
            make_event(
                event_type=EventType.RUN_FINISH,
                run_id=rid,
                stage=None,
                status=report.status,
                duration_ms=duration,
                report_json=str(report_json),
            )
        )
        sink.close()

        self.logger.info(
            "Run Complete",
            duration_ms=duration,
            duration=format_duration_ms(duration),
            report=str(report_json),
            events=str(events_path),
            status=report.status,
        )

        exit_code = 0 if report.status == "success" else 1
        return exit_code, report_json
