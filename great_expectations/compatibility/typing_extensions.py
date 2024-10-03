from __future__ import annotations

from typing import Any, Callable, TypeVar

try:
    # default to the typing_extensions version if available as it contains bug fixes & improvements
    from typing_extensions import Annotated
except ImportError:
    from typing import Annotated  # type: ignore[assignment]

try:
    from typing_extensions import override
except ImportError:
    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


__all__ = ["Annotated", "override"]
