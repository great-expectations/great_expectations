from __future__ import annotations

import secrets
from hashlib import md5


class Anonymizer:
    def __init__(self, salt: str | None = None) -> None:
        self._salt = salt or secrets.token_hex(8)

    def anonymize(self, string_: str) -> str:
        salted = self._salt + string_
        return md5(salted.encode("utf-8")).hexdigest()
