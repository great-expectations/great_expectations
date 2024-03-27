from __future__ import annotations

from hashlib import md5


def anonymize(string: str) -> str:
    return md5(string.encode("utf-8")).hexdigest()
