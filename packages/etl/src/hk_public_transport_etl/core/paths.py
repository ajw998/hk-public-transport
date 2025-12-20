from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DataLayout:
    """
    Canonical path layout for pipeline artifacts:

      {root}/raw/{source_id}/{version}/
      {root}/staged/{source_id}/{version}/
      {root}/normalized/{source_id}/{version}/
      {root}/validated/{source_id}/{version}/
      {root}/out/{source_id}/{version}/
      {root}/published/{source_id}/{version}/
    """

    root: Path

    def raw_root(self) -> Path:
        return self.root / "raw"

    def staged_root(self) -> Path:
        return self.root / "staged"

    def normalized_root(self) -> Path:
        return self.root / "normalized"

    def validated_root(self) -> Path:
        return self.root / "validated"

    def out_root(self) -> Path:
        return self.root / "out"

    def published_root(self) -> Path:
        return self.root / "published"

    def raw(self, source_id: str, version: str) -> Path:
        return self.raw_root() / source_id / version

    def raw_artifacts(self, source_id: str, version: str) -> Path:
        return self.raw(source_id, version) / "artifacts"

    def raw_metadata_json(self, source_id: str, version: str) -> Path:
        return self.raw(source_id, version) / "raw_metadata.json"

    def staged(self, source_id: str, version: str) -> Path:
        return self.staged_root() / source_id / version

    def normalized(self, source_id: str, version: str) -> Path:
        return self.normalized_root() / source_id / version

    def validated(self, source_id: str, version: str) -> Path:
        return self.validated_root() / source_id / version

    def out(self, source_id: str, version: str) -> Path:
        return self.out_root() / source_id / version

    def published(self, source_id: str, version: str) -> Path:
        return self.published_root() / source_id / version

    def parsed_metadata_json(self, source_id: str, version: str) -> Path:
        return self.staged(source_id, version) / "parsed_metadata.json"

    def normalized_metadata_json(self, source_id: str, version: str) -> Path:
        return self.normalized(source_id, version) / "normalized_metadata.json"

    def validation_report_json(self, source_id: str, version: str) -> Path:
        return self.validated(source_id, version) / "validation_report.json"

    def transport_sqlite(self, source_id: str, version: str) -> Path:
        return self.out(source_id, version) / "transport.sqlite"

    def manifest_json(self, source_id: str, version: str) -> Path:
        return self.out(source_id, version) / "manifest.json"

    def normalized_tables(self, source_id: str, version: str) -> Path:
        return self.normalized(source_id, version) / "tables"

    def normalized_mappings(self, source_id: str, version: str) -> Path:
        return self.normalized(source_id, version) / "mappings"

    def normalized_unresolved(self, source_id: str, version: str) -> Path:
        return self.normalized(source_id, version) / "unresolved"

    def ensure_dirs(self, source_id: str, version: str) -> None:
        for p in (
            self.raw(source_id, version),
            self.staged(source_id, version),
            self.normalized(source_id, version),
            self.validated(source_id, version),
            self.out(source_id, version),
            self.published(source_id, version),
            self.normalized_tables(source_id, version),
            self.normalized_mappings(source_id, version),
            self.normalized_unresolved(source_id, version),
        ):
            p.mkdir(parents=True, exist_ok=True)
