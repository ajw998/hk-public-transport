from __future__ import annotations

from enum import StrEnum
from functools import cached_property
from typing import Annotated, Optional, Sequence
from urllib.parse import urljoin

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    model_validator,
)

IdPattern = r"^[a-z0-9][a-z0-9_\-\.]*[a-z0-9]$"

SourceId = Annotated[
    str,
    StringConstraints(min_length=3, max_length=120, pattern=IdPattern),
]
EndpointId = Annotated[
    str,
    StringConstraints(min_length=3, max_length=120, pattern=IdPattern),
]
Tag = Annotated[
    str,
    StringConstraints(
        min_length=2, max_length=40, pattern=r"^[a-z0-9][a-z0-9_\-]*[a-z0-9]$"
    ),
]


class DataFormat(StrEnum):
    xml = "xml"
    csv = "csv"
    json = "json"
    zip = "zip"
    other = "other"


class DatasetInfo(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str = Field(
        ..., min_length=3, examples=["hk-td-tis_14-routes-fares.xml"]
    )
    dataset_url: HttpUrl
    provider: str = Field(..., min_length=1, examples=["HK Transport Department"])
    update_frequency: Optional[str] = None
    notes: Optional[str] = None
    tags: list[Tag] = Field(default_factory=list)


class BaseURL(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(..., min_length=1, examples=["static.data.gov.hk"])
    url: HttpUrl
    priority: int = 100
    description: Optional[str] = None

    @model_validator(mode="after")
    def _normalize_trailing_slash(self) -> "BaseURL":
        s = str(self.url)
        if not s.endswith("/"):
            return self.model_copy(update={"url": HttpUrl(s + "/")})  # type: ignore[arg-type]
        return self


class EndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: EndpointId
    title: str = Field(..., min_length=1)
    description: Optional[str] = None

    path: Optional[str] = None
    url: Optional[HttpUrl] = None

    format: DataFormat
    content_type: Optional[str] = None
    tags: list[Tag] = Field(default_factory=list)

    required: bool = True
    filename: Optional[str] = None

    @model_validator(mode="after")
    def _validate_location(self) -> "EndpointSpec":
        # Exactly one of {path, url}
        if (self.path is None) == (self.url is None):
            raise ValueError("EndpointSpec must set exactly one of {path, url}")
        if self.path is not None:
            p = self.path.strip()
            if not p:
                raise ValueError("EndpointSpec.path must not be empty")
        return self

    def resolved_url_candidates(self, bases: Sequence[BaseURL]) -> list[str]:
        """
        If url is specified, returns [url].
        otherwise joins `path` against all base_urls ordered by (priority, name, url) to be deterministic.
        """
        if self.url is not None:
            return [str(self.url)]

        assert self.path is not None
        rel = self.path.lstrip("/")

        ordered = sorted(bases, key=lambda b: (b.priority, b.name, str(b.url)))
        return [urljoin(str(b.url), rel) for b in ordered]

    def effective_filename(self) -> str:
        if self.filename:
            return self.filename

        if self.url is not None:
            return str(self.url).rstrip("/").split("/")[-1]

        assert self.path is not None
        return self.path.rstrip("/").split("/")[-1]


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    spec_version: int = Field(..., ge=1)
    id: SourceId
    authority: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)

    dataset: DatasetInfo
    base_urls: list[BaseURL] = Field(default_factory=list)
    endpoints: list[EndpointSpec] = Field(..., min_length=1)
    tags: list[Tag] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self) -> "SourceSpec":
        ids = [e.id for e in self.endpoints]
        if len(ids) != len(set(ids)):
            raise ValueError(f"Duplicate endpoint ids in SourceSpec {self.id}")

        if any(e.path is not None for e in self.endpoints) and not self.base_urls:
            raise ValueError(
                f"SourceSpec {self.id} has path-based endpoints but no base_urls"
            )

        return self

    @cached_property
    def endpoint_map(self) -> dict[str, EndpointSpec]:
        return {e.id: e for e in self.endpoints}


class RegistryFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    spec_version: int = Field(..., ge=1)
    sources: list[SourceId] = Field(..., min_length=1)
