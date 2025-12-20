from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """
    A reference to an artifact produced by a stage.
    """

    path: str
    bytes: int
    sha256: str
    content_type: Optional[str] = None


@dataclass(frozen=True, slots=True)
class Event:
    """
    Structured event emitted by the pipeline.
    """

    type: str
    ts_utc: str
    run_id: str
    stage: Optional[str] = None
    data: dict[str, Any] = field(default_factory=dict)
