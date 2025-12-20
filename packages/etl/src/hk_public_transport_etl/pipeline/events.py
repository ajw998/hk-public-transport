from __future__ import annotations

import json
import os
import socket
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from hk_public_transport_etl.core import utc_now_iso

from .types import Event


class EventSink:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        self.emit(
            Event(
                type="run.env",
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
    type: str,
    run_id: str,
    stage: Optional[str] = None,
    **data: Any,
) -> Event:
    return Event(
        type=type, ts_utc=utc_now_iso(), run_id=run_id, stage=stage, data=dict(data)
    )
