from __future__ import annotations

from pathlib import Path

from hk_public_transport_etl.core import hashing, json


def test_sha256_helpers(tmp_path: Path) -> None:
    assert (
        hashing.sha256_bytes(b"abc")
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )

    f = tmp_path / "data.bin"
    f.write_bytes(b"abc")
    digest = hashing.sha256_file(f)
    assert digest.sha256 == hashing.sha256_bytes(b"abc")
    assert digest.bytes == 3

    out = tmp_path / "sha256sums.txt"
    hashing.write_sha256_sum_txt(out, {"data.bin": digest.sha256})
    assert out.read_text() == f"{digest.sha256}  data.bin\n"


def test_json_helpers(tmp_path: Path) -> None:
    obj = {"b": 1, "a": 2}
    out = tmp_path / "sample.json"
    json.atomic_write_json(out, obj)
    assert json.read_json(out) == {"a": 2, "b": 1}

    compact = json.stable_json_dumps(obj, indent=None)
    assert compact == '{"a":2,"b":1}'
