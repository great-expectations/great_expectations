from __future__ import annotations

from typing import Any, Callable, TypeVar

try:
    from typing import Annotated

    from typing_extensions import override
except ImportError:
    from typing_extensions import Annotated

    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


__all__ = ["Annotated", "override"]
