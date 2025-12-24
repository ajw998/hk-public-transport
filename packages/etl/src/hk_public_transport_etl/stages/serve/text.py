from __future__ import annotations

import re

_WS_RE = re.compile(r"\s+")
_EN_PUNCT_RE = re.compile(r"[(),]+")


def normalize_en(s: str | None) -> str:
    if not s:
        return ""
    s = _EN_PUNCT_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


def segment_cjk(s: str | None) -> str:
    if not s:
        return ""
    out: list[str] = []
    for ch in s:
        o = ord(ch)
        if 0x4E00 <= o <= 0x9FFF:
            out.append(ch)
            out.append(" ")
        elif ch.isascii() and (ch.isalnum() or ch in "+-/#&'"):
            out.append(ch)
        elif ch.isspace():
            out.append(" ")
        else:
            out.append(" ")
    return _WS_RE.sub(" ", "".join(out)).strip()
