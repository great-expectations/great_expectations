from __future__ import annotations

from typing import Any, Callable, TypeVar

try:
    from typing import (
        Annotated,  # type: ignore[attr-defined]  # only exists in some python versions
    )
except ImportError:
    from typing_extensions import Annotated

try:
    from typing_extensions import override
except ImportError:
    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


__all__ = ["Annotated", "override"]
