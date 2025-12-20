from __future__ import annotations

import re
from typing import Mapping

from hk_public_transport_etl.registry import EndpointSpec

_safe_re = re.compile(r"[^a-zA-Z0-9._\-]+")


def sanitize(name: str) -> str:
    name = name.strip().strip("/")
    name = _safe_re.sub("_", name)
    return name or "artifact"


def cd_filename(content_disposition: str | None) -> str | None:
    if not content_disposition:
        return None
    m = re.search(
        r'filename\*?=(?:"([^"]+)"|([^;]+))', content_disposition, flags=re.IGNORECASE
    )
    if not m:
        return None
    v = (m.group(1) or m.group(2) or "").strip()
    return v or None


def resolve_artifact_filename(
    *,
    endpoint: EndpointSpec,
    uri: str,
    response_headers: Mapping[str, str | None],
    used_names: set[str],
) -> str:
    if endpoint.filename:
        base = sanitize(endpoint.filename)
    else:
        cd = cd_filename(response_headers.get("Content-Disposition"))
        if cd:
            base = sanitize(cd)
        else:
            base = sanitize(uri.rstrip("/").split("/")[-1])

    name = base
    i = 2
    while name in used_names:
        stem, dot, ext = base.partition(".")
        name = f"{stem}_{i}{dot}{ext}" if dot else f"{base}_{i}"
        i += 1
    return name
