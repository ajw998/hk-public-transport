from __future__ import annotations

import json
from importlib.resources import files
from typing import Any, Final

from .errors import ContractsResourceError

PKG: Final[str] = "hk_public_transport_contracts"

MANIFEST_SCHEMA_REL: Final[str] = "schema/jsonschema/manifest.schema.json"
CANONICAL_DDL_REL: Final[str] = "schema/ddl/canonical.sql"
SCHEMA_VERSION_REL: Final[str] = "schema/VERSION"


def traversable(rel_path: str):
    return files(PKG).joinpath(rel_path)


def read_text(rel_path: str) -> str:
    try:
        return traversable(rel_path).read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise ContractsResourceError(f"Missing contracts resource: {rel_path}") from e
    except Exception as e:
        # pragma: no cover
        raise ContractsResourceError(
            f"Failed reading contracts resource: {rel_path}: {e}"
        ) from e


def read_json(rel_path: str) -> dict[str, Any]:
    raw = read_text(rel_path)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ContractsResourceError(
            f"Invalid JSON in contracts resource: {rel_path}: {e}"
        ) from e
    if not isinstance(obj, dict):
        raise ContractsResourceError(
            f"Expected JSON object in {rel_path}, got {type(obj).__name__}"
        )
    return obj


def canonical_ddl() -> str:
    """
    Canonical SQLite DDL
    """
    return read_text(CANONICAL_DDL_REL)


def manifest_schema() -> dict[str, Any]:
    """
    JSON Schema for manifest.json
    """
    return read_json(MANIFEST_SCHEMA_REL)


def schema_version_text() -> str:
    """
    Human-readable schema version for compatibility gate
    """
    return read_text(SCHEMA_VERSION_REL).strip()


def schema_version_int() -> int:
    """
    Parse schema/VERSION as an integer compatibility gate
    """
    s = schema_version_text()
    try:
        v = int(s)
    except ValueError as e:
        raise ContractsResourceError(
            f"schema/VERSION must be an integer, got: {s!r}"
        ) from e
    if v < 1:
        raise ContractsResourceError(f"schema/VERSION must be >= 1, got: {v}")
    return v
