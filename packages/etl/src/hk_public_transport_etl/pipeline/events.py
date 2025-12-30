from __future__ import annotations

import json
import os
import socket
import threading
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from hk_public_transport_etl.core import utc_now_iso

from .types import Event


class EventType(str, Enum):
    RUN_ENV = "run.env"
    RUN_START = "run.start"
    RUN_FINISH = "run.finish"

    STAGE_START = "stage.start"
    STAGE_WARN = "stage.warn"
    STAGE_METRICS = "stage.metrics"
    STAGE_SUCCESS = "stage.success"
    STAGE_FAILED = "stage.failed"

    ARTIFACT_WRITTEN = "artifact.written"

    FETCH_PLAN = "fetch.plan"
    FETCH_SOURCE_START = "fetch.source.start"
    FETCH_SOURCE_FINISH = "fetch.source.finish"

    PARSE_PLAN = "parse.plan"
    PARSE_SOURCE_START = "parse.source.start"
    PARSE_SOURCE_FINISH = "parse.source.finish"
    PARSE_SOURCE_SKIP = "parse.source.skip"

    NORMALIZE_PLAN = "normalize.plan"
    NORMALIZE_SOURCE_START = "normalize.source.start"
    NORMALIZE_SOURCE_FINISH = "normalize.source.finish"

    VALIDATE_PLAN = "validate.plan"
    VALIDATE_SOURCE_START = "validate.source.start"
    VALIDATE_SOURCE_FINISH = "validate.source.finish"

    COMMIT_PLAN = "commit.plan"
    COMMIT_START = "commit.start"
    COMMIT_FINISH = "commit.finish"

    SERVE_FINISH = "serve.finish"

    PUBLISH_START = "publish.start"
    PUBLISH_FINISH = "publish.finish"


class EventSink:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        self.emit(
            Event(
                type=EventType.RUN_ENV.value,
                ts_utc=utc_now_iso(),
                run_id="__init__",
                data={
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "cwd": str(Path.cwd()),
                },
            )
        )

    def emit(self, event: Event) -> None:
        line = json.dumps(asdict(event), ensure_ascii=False)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.write("\n")

    def close(self) -> None:
        return


def make_event(
    *,
    event_type: EventType | str,
    run_id: str,
    stage: Optional[str] = None,
    **data: Any,
) -> Event:
    type_value = (
        event_type.value if isinstance(event_type, EventType) else str(event_type)
    )
    return Event(
        type=type_value,
        ts_utc=utc_now_iso(),
        run_id=run_id,
        stage=stage,
        data=dict(data),
    )
