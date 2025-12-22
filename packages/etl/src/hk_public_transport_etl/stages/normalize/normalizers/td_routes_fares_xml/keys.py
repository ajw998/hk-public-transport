from __future__ import annotations

import hashlib


def operator_id(company_code: str) -> str:
    cc = company_code.strip().upper()
    return f"td:operator:{cc}"


def route_key(*, mode: str, upstream_route_id: str) -> str:
    return f"td:{mode}:{upstream_route_id.strip()}"


def stop_key(*, mode: str, upstream_stop_id: str) -> str:
    return f"td:{mode}:{upstream_stop_id.strip()}"


def sequence_fingerprint(stop_keys: list[str], *, n: int = 12) -> str:
    s = "|".join(stop_keys).encode("utf-8")
    return hashlib.sha256(s).hexdigest()[: max(4, int(n))]


def pattern_key(*, route_key: str, route_seq: int, fingerprint: str) -> str:
    return f"{route_key}:{int(route_seq)}:{fingerprint}"


def direction_id_from_route_seq(route_seq: int, *, outbound_is_1: bool = True) -> int:
    if route_seq <= 0:
        return 0
    if outbound_is_1:
        return 1 if route_seq == 1 else 2 if route_seq == 2 else 0
    return 2 if route_seq == 1 else 1 if route_seq == 2 else 0
