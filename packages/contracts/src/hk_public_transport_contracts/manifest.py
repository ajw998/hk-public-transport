from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Iterable

from jsonschema import Draft202012Validator

from .errors import ManifestValidationError
from .resources import manifest_schema


@lru_cache(maxsize=1)
def validator() -> Draft202012Validator:
    schema = manifest_schema()
    return Draft202012Validator(schema)


def format_errors(errors: Iterable[Any]) -> str:
    lines: list[str] = []
    for e in errors:
        path = (
            ".".join(str(p) for p in e.path) if getattr(e, "path", None) else "<root>"
        )
        lines.append(f"- {path}: {e.message}")
    return "\n".join(lines)


def validate_manifest_dict(obj: dict[str, Any]) -> None:
    """
    Validate a manifest object against the shipped JSON schema.
    Raises ManifestValidationError with a readable message on failure.
    """
    v = validator()
    errs = sorted(v.iter_errors(obj), key=lambda e: list(getattr(e, "path", [])))
    if errs:
        raise ManifestValidationError(
            "Manifest validation failed:\n" + format_errors(errs)
        )


def validate_manifest_json(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ManifestValidationError(f"Manifest is not valid JSON: {e}") from e

    if not isinstance(obj, dict):
        raise ManifestValidationError(
            f"Manifest must be a JSON object, got {type(obj).__name__}"
        )

    validate_manifest_dict(obj)
    return obj
