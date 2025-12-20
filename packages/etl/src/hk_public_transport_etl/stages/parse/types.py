from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeAlias

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class SkipPolicy:
    reason: str


ParserTables: TypeAlias = dict[str, pa.Table]
ParserResult: TypeAlias = ParserTables | tuple[ParserTables, list[str]]
ParserFn: TypeAlias = Callable[[Path], ParserResult] | SkipPolicy
