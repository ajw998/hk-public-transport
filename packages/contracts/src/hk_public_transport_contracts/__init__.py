from __future__ import annotations

from .errors import ContractsError, ContractsResourceError, ManifestValidationError
from .hashes import (
    CONTRACT_RESOURCE_PATHS,
    compute_contract_fingerprint,
    compute_resource_hashes,
    read_bytes,
    sha256_hex,
)
from .manifest import validate_manifest_dict, validate_manifest_json
from .resources import (
    CANONICAL_DDL_REL,
    MANIFEST_SCHEMA_REL,
    SCHEMA_VERSION_REL,
    canonical_ddl,
    manifest_schema,
    read_json,
    read_text,
    schema_version_int,
    schema_version_text,
)
from .version import (
    CONTRACTS_DIST_VERSION,
    ContractVersionInfo,
    get_contract_version_info,
)

__all__ = [
    "ContractsError",
    "ContractsResourceError",
    "ManifestValidationError",
    "read_text",
    "read_bytes",
    "read_json",
    "canonical_ddl",
    "manifest_schema",
    "schema_version_text",
    "MANIFEST_SCHEMA_REL",
    "CANONICAL_DDL_REL",
    "SCHEMA_VERSION_REL",
    "validate_manifest_dict",
    "validate_manifest_json",
    "CONTRACTS_DIST_VERSION",
    "ContractVersionInfo",
    "get_contract_version_info",
    "sha256_hex",
    "schema_version_int",
    "CONTRACT_RESOURCE_PATHS",
    "compute_resource_hashes",
    "compute_contract_fingerprint",
]
