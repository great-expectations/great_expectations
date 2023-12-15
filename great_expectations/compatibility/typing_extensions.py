from typing import Any, Callable, TypeVar

try:
    from typing_extensions import override
except ImportError:
    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


__all__ = ["override"]
