from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, TypedDict

JsonPrimitive = str | int | float | bool | None
JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject = dict[str, JsonValue]


@dataclass(frozen=True)
class NormalizeWarning:
    type: str
    count: int
    note: str | None = None

    def to_dict(self) -> JsonObject:
        out: JsonObject = {"type": self.type, "count": self.count}
        if self.note is not None:
            out["note"] = self.note
        return out


@dataclass(frozen=True)
class NormalizeInfo:
    rules_version: str
    config: JsonObject
    warnings: list[NormalizeWarning]

    def to_dict(self) -> JsonObject:
        return {
            "rules_version": self.rules_version,
            "config": self.config,
            "warnings": [w.to_dict() for w in self.warnings],
        }


@dataclass(frozen=True)
class NormalizeSourceResult:
    source_id: str
    version: str
    normalized_metadata_path: str
    tables: int
    mappings: int
    unresolved: int

    def to_dict(self) -> JsonObject:
        return {
            "source_id": self.source_id,
            "version": self.version,
            "metadata_path": self.normalized_metadata_path,
            "tables": self.tables,
            "mappings": self.mappings,
            "unresolved": self.unresolved,
        }


Kind = Literal["canonical", "mapping", "unresolved"]


class OutputTableMeta(TypedDict):
    kind: Kind
    relpath: str
    sha256: str
    bytes: int
    row_count: int
    schema_hash: str


class NormalizedMetadata(TypedDict):
    source_id: str
    version: str
    rules_version: str
    config: JsonObject
    inputs: JsonObject
    outputs: dict[str, OutputTableMeta]
    warnings: list[JsonObject]


@dataclass(frozen=True, slots=True)
class NormalizeContext:
    source_id: str
    version: str
    data_root: Path


@dataclass(frozen=True, slots=True)
class NormalizeOutput:
    source_id: str
    version: str
    out_dir: Path
    metadata_path: Path


NormalizeFn = Callable[[NormalizeContext], NormalizeOutput]
