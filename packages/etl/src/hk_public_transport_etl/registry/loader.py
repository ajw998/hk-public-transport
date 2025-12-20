from __future__ import annotations

import os
from pathlib import Path

from hk_public_transport_etl.core import read_json
from pydantic import TypeAdapter

from .models import RegistryFile, SourceSpec

try:
    import jsonschema
except Exception:
    jsonschema = None


def _validate_with_jsonschema(instance: dict, schema: dict) -> None:
    if jsonschema is None:
        raise RuntimeError(
            "jsonschema is required for external config validation; pip install jsonschema"
        )
    jsonschema.validate(instance=instance, schema=schema)


def resolve_config_dir(explicit: Path | None = None) -> Path:
    """
    Resolve the directory containing registry.json and sources/*.json.

    Priority:
      1) explicit argument
      2) env hk_public_transport_CONFIG_DIR
      3) ./config (when running from an etl/ working dir)
      4) discover ./config by walking upwards from this module (dev checkout)
    """

    def _is_config_dir(p: Path) -> bool:
        return (p / "registry.json").is_file()

    if explicit is not None:
        p = explicit.expanduser().resolve()
        if _is_config_dir(p):
            return p
        raise RuntimeError(f"--config-dir does not look like a config directory: {p}")

    env = os.environ.get("hk_public_transport_CONFIG_DIR")
    if env:
        p = Path(env).expanduser().resolve()
        if _is_config_dir(p):
            return p
        raise RuntimeError(
            f"hk_public_transport_CONFIG_DIR does not look like a config directory: {p}"
        )

    for cand in (Path.cwd() / "config", Path.cwd() / "packages" / "etl" / "config"):
        if _is_config_dir(cand):
            return cand.resolve()

    # Walk upwards from this file (works when running from repo root)
    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "config"
        if _is_config_dir(cand):
            return cand.resolve()

    raise RuntimeError(
        "Could not resolve ETL config directory. "
        "Pass --config-dir or set hk_public_transport_CONFIG_DIR."
    )


def load_registry(
    config_dir: Path,
    *,
    registry_schema: dict,
    spec_schema: dict,
) -> dict[str, SourceSpec]:
    """
    Loads config/sources/registry.json
    """
    reg_path = config_dir / "registry.json"
    reg_raw = read_json(reg_path)
    _validate_with_jsonschema(reg_raw, registry_schema)
    reg = RegistryFile.model_validate(reg_raw)

    out: dict[str, SourceSpec] = {}
    for rel in reg.sources:
        spec_path = (config_dir / "sources" / rel).resolve()
        raw = read_json(spec_path)
        _validate_with_jsonschema(raw, spec_schema)
        spec = SourceSpec.model_validate(raw)

        if spec.id in out:
            raise ValueError(f"Duplicate SourceSpec.id across files: {spec.id}")
        out[spec.id] = spec

    return out


def schema_for_source_spec() -> dict:
    return TypeAdapter(SourceSpec).json_schema()


def schema_for_registry_file() -> dict:
    return TypeAdapter(RegistryFile).json_schema()


def get_source_registry(config_dir: Path | None = None) -> dict[str, SourceSpec]:
    cfg = resolve_config_dir(config_dir)
    return load_registry(
        cfg,
        registry_schema=schema_for_registry_file(),
        spec_schema=schema_for_source_spec(),
    )
