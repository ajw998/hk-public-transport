from __future__ import annotations

import os
from pathlib import Path

from hk_public_transport_etl.core import fs


def test_atomic_write_text_and_bytes_roundtrip(tmp_path: Path) -> None:
    text_path = tmp_path / "d1" / "sample.txt"
    fs.atomic_write_text(text_path, "hello\n")
    assert text_path.read_text() == "hello\n"

    fs.atomic_write_text(text_path, "updated")
    assert text_path.read_text() == "updated"

    bytes_path = tmp_path / "d2" / "sample.bin"
    fs.atomic_write_bytes(bytes_path, b"\x00\x01")
    assert bytes_path.read_bytes() == b"\x00\x01"


def test_relpath_size_and_copy_or_hardlink(tmp_path: Path) -> None:
    src = tmp_path / "a" / "file.txt"
    fs.ensure_parent(src)
    src.write_text("data")

    dst = tmp_path / "b" / "copied.txt"
    fs.copy_or_hardlink(src, dst)
    assert dst.read_text() == "data"
    assert fs.file_size(dst) == 4
    assert fs.relpath_posix(dst, tmp_path) == "b/copied.txt"

    # Prefer hardlink when available; fall back to copy otherwise.
    same_inode = os.stat(src).st_ino == os.stat(dst).st_ino
    assert same_inode or dst.read_text() == src.read_text()
