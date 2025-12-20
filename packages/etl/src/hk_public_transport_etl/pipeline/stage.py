from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from hk_public_transport_etl.core import StageError, monotonic_ms, utc_now_iso

from .context import RunContext
from .types import ArtifactRef


@dataclass(slots=True)
class FunctionStage:
    """
    Adapter that turns a plain function into a Stage.
    """

    stage_id: str
    fn: StageFn

    def run(self, ctx: RunContext) -> dict[str, Any] | None:
        return self.fn(ctx)


@dataclass(slots=True)
class StageResult:
    stage: str
    status: str  # "success" | "failed" | "skipped"
    started_at_utc: str
    finished_at_utc: str
    duration_ms: int

    outputs: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    artifacts: list[ArtifactRef] = field(default_factory=list)
    error: Optional[StageError] = None


class Stage(Protocol):
    stage_id: str

    def run(self, ctx: RunContext) -> dict[str, Any] | None: ...


StageFn = Callable[[RunContext], dict[str, Any] | None]


def run_stage(
    *,
    ctx: RunContext,
    stage: Stage,
) -> StageResult:
    stage_id = stage.stage_id
    log = ctx.stage_logger(stage_id)

    t0 = monotonic_ms()
    started_at = utc_now_iso()

    ctx.emit("stage.start", stage=stage_id)
    log.info("stage start")

    warnings: list[str] = []
    artifacts: list[ArtifactRef] = []
    metrics: dict[str, Any] = {}

    try:
        out = stage.run(ctx) or {}
        if not isinstance(out, dict):
            raise TypeError(
                f"Stage {stage_id} returned {type(out).__name__}, expected dict or None"
            )

        if "_warnings" in out:
            w = out.pop("_warnings")
            if isinstance(w, list):
                warnings.extend(str(x) for x in w)

        if "_metrics" in out:
            m = out.pop("_metrics")
            if isinstance(m, dict):
                metrics.update(m)

        if "_artifacts" in out:
            a = out.pop("_artifacts")
            if isinstance(a, list):
                artifacts.extend(a)

        status = "success"
        err = None

        for w in warnings:
            ctx.emit("stage.warn", stage=stage_id, message=w)
            log.warning(w)

        if metrics:
            ctx.emit("stage.metrics", stage=stage_id, metrics=metrics)

        finished_at = utc_now_iso()
        duration = monotonic_ms() - t0

        ctx.emit("stage.success", stage=stage_id, duration_ms=duration)
        log.info("stage success", extra={"duration_ms": duration})

        return StageResult(
            stage=stage_id,
            status=status,
            started_at_utc=started_at,
            finished_at_utc=finished_at,
            duration_ms=duration,
            outputs=out,
            metrics=metrics,
            warnings=warnings,
            artifacts=artifacts,
            error=err,
        )

    except Exception as e:
        tb = traceback.format_exc()
        finished_at = utc_now_iso()
        duration = monotonic_ms() - t0

        ctx.emit(
            "stage.failed",
            stage=stage_id,
            duration_ms=duration,
            exc_type=type(e).__name__,
            message=str(e),
        )
        log.error("stage failed", extra={"duration_ms": duration})
        log.error(tb)

        return StageResult(
            stage=stage_id,
            status="failed",
            started_at_utc=started_at,
            finished_at_utc=finished_at,
            duration_ms=duration,
            outputs={},
            metrics=metrics,
            warnings=warnings,
            artifacts=artifacts,
            error=StageError(exc_type=type(e).__name__, message=str(e), traceback=tb),
        )
