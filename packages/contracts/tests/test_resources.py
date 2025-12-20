from __future__ import annotations

from hk_public_transport_contracts import (
    canonical_ddl,
    manifest_schema,
    schema_version_int,
)


def test_schema_version_is_int_ge_1():
    assert schema_version_int() >= 1


def test_manifest_schema_loads():
    s = manifest_schema()
    assert s["type"] == "object"
    assert "properties" in s


def test_canonical_ddl_loads():
    ddl = canonical_ddl()
    assert "CREATE TABLE" in ddl.upper()
