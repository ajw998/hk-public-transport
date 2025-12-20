from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as dist_version

from .hashes import compute_contract_fingerprint, compute_resource_hashes
from .resources import schema_version_text


def safe_dist_version(dist_name: str) -> str:
    try:
        return dist_version(dist_name)
    except PackageNotFoundError:
        return "0.0.0+unknown"


CONTRACTS_DIST_VERSION: str = safe_dist_version("hk_public_transport_contracts")


@dataclass(frozen=True, slots=True)
class ContractVersionInfo:
    schema_version: str
    python_version: str
    fingerprint: str
    sha256: dict[str, str]


def get_contract_version_info() -> ContractVersionInfo:
    return ContractVersionInfo(
        schema_version=schema_version_text(),
        python_version=CONTRACTS_DIST_VERSION,
        fingerprint=compute_contract_fingerprint(),
        sha256=compute_resource_hashes(),
    )
