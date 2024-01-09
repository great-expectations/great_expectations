from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

try:
    from typing_extensions import override
except ImportError:
    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


try:
    from typing_extensions import dataclass_transform
except ImportError:
    if TYPE_CHECKING:
        # This branching is required to help mypy show errors correctly
        from typing_extensions import dataclass_transform
    else:
        _T = TypeVar("_T")

        # This is only for static analysis, so it's okay for it to be a no-op
        def dataclass_transform(  # type: ignore[misc]
            **kwargs: Any,
        ) -> Callable[[_T], _T]:
            return lambda x: x


__all__ = [
    "dataclass_transform",
    "override",
]
