from __future__ import annotations

import json
from pathlib import Path

import pytest
from hk_public_transport_contracts import (
    ManifestValidationError,
    get_contract_version_info,
    validate_manifest_json,
)

FIX = Path(__file__).parent / "fixtures" / "manifest.valid.json"


def test_valid_manifest_fixture_passes():
    raw = FIX.read_text(encoding="utf-8")
    obj = validate_manifest_json(raw)
    assert obj["manifest_version"] == 1


def test_invalid_manifest_rejected():
    bad = json.loads(FIX.read_text(encoding="utf-8"))
    bad.pop("files")
    with pytest.raises(ManifestValidationError):
        validate_manifest_json(json.dumps(bad))


def test_contract_version_info_present():
    info = get_contract_version_info()
    assert len(info.fingerprint) == 64
    assert "schema/VERSION" in info.sha256
