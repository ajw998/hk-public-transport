from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hk_public_transport_etl.core import ILogger, sha256_file

from .events import EventSink, EventType
from .types import ArtifactRef


@dataclass(slots=True)
class RunContext:
    """
    Context shared across stages for a single pipeline run.
    """

    run_id: str
    run_root: Path
    data_root: Path
    logger: ILogger
    events: EventSink

    # optional free-form metadata
    meta: dict[str, Any] = field(default_factory=dict)

    def stage_logger(self, stage: str) -> ILogger:
        """
        Return a logger suitable for this stage.

        - If `self.logger` is a structlog BoundLogger, use `.bind(stage=...)`.
        - If it is stdlib logging.Logger (or anything else), fall back to structlog.get_logger().
        """
        return self.logger.bind(stage=stage)

    def emit(self, event: EventType | str, **kw: object) -> None:
        # Keep event chatter at debug level to leave console logs readable.
        event_value = event.value if isinstance(event, EventType) else str(event)
        self.logger.debug(event_value, event_type=event_value, **kw)

    # Convenience helpers that standardize artifact emission
    def record_artifact(
        self,
        *,
        stage: str,
        path: Path,
        content_type: str | None = None,
        rel_to: Path | None = None,
    ) -> ArtifactRef:
        p = Path(path)
        size = p.stat().st_size
        digest = sha256_file(p)
        rel = str(p if rel_to is None else p.relative_to(rel_to))
        art = ArtifactRef(
            path=rel, bytes=size, sha256=digest.sha256, content_type=content_type
        )
        self.emit(
            EventType.ARTIFACT_WRITTEN,
            stage=stage,
            path=art.path,
            bytes=art.bytes,
            sha256=art.sha256,
            content_type=art.content_type,
        )
        return art
