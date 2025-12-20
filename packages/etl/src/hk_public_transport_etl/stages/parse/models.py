from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class InputArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    relpath: str
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    bytes: int = Field(ge=0)


class ParsedTable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    table_name: str
    relpath: str
    schema_hash: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    row_count: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    bytes: int = Field(ge=0)


class ParsedDataset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    version: str
    input_artifacts: list[InputArtifact]
    output_tables: list[ParsedTable]
    parser_id: str
    parser_version: str
    generated_at_utc: str
    warnings: list[str] = []
