from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


class RawMetadataArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    endpoint_id: str
    bytes: int = Field(ge=0)
    cache_control: Optional[str] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    filename: str
    uri: str
    final_url: str
    last_modified: Optional[str] = None
    path: str
    retrieved_at_utc: str
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    status_code: int


class RawMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    updated_at_utc: str
    artifacts: list[RawMetadataArtifact] = Field(default_factory=list)
    created_at_utc: str
    errors: dict[str, str] = Field(default_factory=dict)
    version: str
    _artifact_map: dict[str, RawMetadataArtifact] = PrivateAttr(default_factory=dict)

    def model_post_init(self, _: object) -> None:
        self._artifact_map = {a.endpoint_id: a for a in self.artifacts}

    def by_endpoint(self) -> dict[str, RawMetadataArtifact]:
        return dict(self._artifact_map)

    def get_artifact(self, endpoint_id: str) -> RawMetadataArtifact | None:
        return self._artifact_map.get(endpoint_id)

    def upsert_artifact(self, a: RawMetadataArtifact) -> None:
        self._artifact_map[a.endpoint_id] = a
        self.artifacts = list(self._artifact_map.values())

    def set_error(self, endpoint_id: str, msg: str) -> None:
        self.errors[endpoint_id] = msg

    def clear_error(self, endpoint_id: str) -> None:
        self.errors.pop(endpoint_id, None)


@dataclass(frozen=True, slots=True)
class RawArtifact:
    bytes: int
    endpoint_id: str
    etag: str | None
    last_modified: str | None
    path: str
    retrieved_at_utc: str
    sha256: str
    uri: str

    def to_dict(self) -> dict[str, object]:
        return {
            "endpoint_id": self.endpoint_id,
            "uri": self.uri,
            "path": self.path,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "retrieved_at_utc": self.retrieved_at_utc,
            "etag": self.etag,
            "last_modified": self.last_modified,
        }


@dataclass(frozen=True, slots=True)
class RawFetchResult:
    source_id: str
    version: str
    artifacts: tuple[RawArtifact, ...]
    raw_metadata_path: str
