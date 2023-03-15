from __future__ import annotations

from typing import Any, Callable

_DYNAMIC_DEFINITIONS: dict[str, Callable[..., Any]] = {}
