from __future__ import annotations

import base64
from pathlib import Path

from hk_public_transport_etl.core import PublishError


def _load_ed25519_private_key(private_key_path: Path):
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except Exception as e:  # pragma: no cover
        raise PublishError(
            "cryptography is required for Ed25519 signing. Install with: pip install cryptography"
        ) from e

    data = Path(private_key_path).read_bytes()
    try:
        return load_pem_private_key(data, password=None)
    except Exception as e:
        raise PublishError(
            f"Failed to load Ed25519 private key: {private_key_path}"
        ) from e


def sign_bytes_ed25519(*, payload: bytes, private_key_path: Path) -> bytes:
    key = _load_ed25519_private_key(Path(private_key_path))
    try:
        return key.sign(payload)
    except Exception as e:
        raise PublishError("Ed25519 signing failed") from e


def public_key_b64_from_private_key(*, private_key_path: Path) -> str:
    key = _load_ed25519_private_key(Path(private_key_path))
    try:
        pub = key.public_key()
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

        raw = pub.public_bytes(encoding=Encoding.Raw, format=PublicFormat.Raw)
        return base64.b64encode(raw).decode("ascii")
    except Exception as e:
        raise PublishError("Failed to derive Ed25519 public key") from e
