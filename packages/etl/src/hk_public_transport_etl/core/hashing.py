import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class FileDigest:
    sha256: str
    bytes: int


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_file(path: Path, *, chunk_bytes: int = 1024 * 1024) -> FileDigest:
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_bytes)
            if not b:
                break
            h.update(b)
            total += len(b)

    return FileDigest(sha256=h.hexdigest(), bytes=total)


def write_sha256_sum_txt(path: Path, entries: Mapping[str, str]) -> None:
    """
    Writes a deterministic sha256sums.txt
    """
    lines = [f"{entries[name]}  {name}\n" for name in sorted(entries.keys())]
    path.write_text("".join(lines), encoding="utf-8")
