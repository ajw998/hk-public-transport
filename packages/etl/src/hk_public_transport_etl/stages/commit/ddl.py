from __future__ import annotations

import re
from importlib import resources

_CONTRACTS_PKG = "hk_public_transport_contracts"


def load_canonical_ddl() -> str:
    root = resources.files(_CONTRACTS_PKG)
    matches = [p for p in root.rglob("canonical.sql") if p.is_file()]
    if not matches:
        raise FileNotFoundError(
            f"Could not find canonical.sql in {_CONTRACTS_PKG} package resources."
        )
    if len(matches) > 1:
        raise RuntimeError(
            "Found multiple canonical.sql files:\n" + "\n".join(str(m) for m in matches)
        )
    return matches[0].read_text(encoding="utf-8")


def load_schema_version() -> int:
    # prefer python constant
    try:
        mod = __import__(_CONTRACTS_PKG, fromlist=["SCHEMA_VERSION"])
        v = getattr(mod, "SCHEMA_VERSION", None)
        if isinstance(v, int):
            return v
    except Exception:
        pass

    root = resources.files(_CONTRACTS_PKG)
    candidates = []
    for name in ("SCHEMA_VERSION", "schema_version.txt", "VERSION"):
        candidates.extend([p for p in root.rglob(name) if p.is_file()])

    for p in candidates:
        txt = p.read_text(encoding="utf-8")
        m = re.search(r"(\d+)", txt)
        if m:
            return int(m.group(1))

    raise FileNotFoundError(
        f"Could not resolve schema version from {_CONTRACTS_PKG}. "
        "Provide SCHEMA_VERSION = <int> or a resource file with an integer."
    )
