from __future__ import annotations

import re

_ws = re.compile(r"\s+", flags=re.UNICODE)


def clean_name(s: str | None) -> str | None:
    """
    Performs strip, collapses whitespace, and not invent
    spacing between CJK characters
    """
    if s is None:
        return None
    x = s.replace("\u3000", " ").strip()
    x = _ws.sub(" ", x).strip()
    return x or None
