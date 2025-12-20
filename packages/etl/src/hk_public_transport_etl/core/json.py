import json
from pathlib import Path
from typing import Any

from .fs import atomic_write_text


def atomic_write_json(path: Path, obj: Any, *, indent: int = 2) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=indent) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def stable_json_dumps(obj: Any, *, indent: int | None = 2) -> str:
    """
    Deterministic JSON:
      - sort_keys=True
      - ensure_ascii=False (keep CJK readable)
      - stable separators when indent is None
    """
    if indent is None:
        return json.dumps(
            obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=indent)
