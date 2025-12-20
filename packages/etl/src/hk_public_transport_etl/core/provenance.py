from __future__ import annotations

import os
import platform
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .time import monotonic_ms


def new_run_id() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class RunProvenance:
    """
    Stable identity for a pipeline run.
    """

    run_id: str
    started_at_utc: str
    hostname: str = field(default_factory=platform.node)
    pid: int = field(default_factory=os.getpid)
    python: str = field(default_factory=lambda: platform.python_version())
    platform: str = field(default_factory=lambda: platform.platform())


@dataclass(frozen=True, slots=True)
class StageProvenance:
    stage_id: str


@dataclass(slots=True)
class Timer:
    """
    Minimal timing primitive. Use as context manager.

      with Timer() as t:
          ...
      duration = t.duration_ms
    """

    _t0_ms: int = field(default_factory=monotonic_ms, init=False)
    duration_ms: Optional[int] = field(default=None, init=False)

    def __enter__(self) -> "Timer":
        self._t0_ms = monotonic_ms()
        self.duration_ms = None
        return self

    def __exit__(self) -> None:
        self.duration_ms = monotonic_ms() - self._t0_ms
