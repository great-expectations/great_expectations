from __future__ import annotations

from typing import Any, Callable, TypeVar

try:
    from typing_extensions import Annotated, override
except ImportError:
    from typing import Annotated

    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


__all__ = ["Annotated", "override"]
