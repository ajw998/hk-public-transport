from __future__ import annotations

from importlib import resources

_CONTRACTS_PKG = "hk_public_transport_contracts"


def load_app_ddl() -> str:
    root = resources.files(_CONTRACTS_PKG)
    matches = [p for p in root.rglob("app.sql") if p.is_file()]
    if not matches:
        raise FileNotFoundError(
            f"Could not find app.sql in {_CONTRACTS_PKG} package resources."
        )
    if len(matches) > 1:
        raise RuntimeError(
            "Found multiple app.sql files:\n" + "\n".join(str(m) for m in matches)
        )
    return matches[0].read_text(encoding="utf-8")
