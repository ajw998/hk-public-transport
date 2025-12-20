from __future__ import annotations

import hashlib
from typing import Final

from .errors import ContractsResourceError
from .resources import (
    CANONICAL_DDL_REL,
    MANIFEST_SCHEMA_REL,
    SCHEMA_VERSION_REL,
    traversable,
)

CONTRACT_RESOURCE_PATHS: Final[tuple[str, ...]] = (
    SCHEMA_VERSION_REL,
    CANONICAL_DDL_REL,
    MANIFEST_SCHEMA_REL,
)


def read_bytes(rel_path: str) -> bytes:
    """
    Read a binary resource shipped inside the contracts wheel.
    """
    try:
        return traversable(rel_path).read_bytes()
    except FileNotFoundError as e:
        raise ContractsResourceError(f"Missing contracts resource: {rel_path}") from e
    except Exception as e:  # pragma: no cover
        raise ContractsResourceError(
            f"Failed reading contracts resource: {rel_path}: {e}"
        ) from e


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_resource_hashes(
    rel_paths: tuple[str, ...] = CONTRACT_RESOURCE_PATHS,
) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in rel_paths:
        out[p] = sha256_hex(read_bytes(p))
    return out


def compute_contract_fingerprint(
    rel_paths: tuple[str, ...] = CONTRACT_RESOURCE_PATHS,
) -> str:
    h = hashlib.sha256()
    for p in rel_paths:
        h.update(read_bytes(p))
        h.update(b"\n")
    return h.hexdigest()
