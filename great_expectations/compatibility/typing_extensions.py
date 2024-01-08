from __future__ import annotations

from typing import Any, Callable, Tuple, TypeVar, Union

try:
    from typing_extensions import override
except ImportError:
    F = TypeVar("F", bound=Callable[..., Any])

    def override(__arg: F, /) -> F:
        return __arg


try:
    from typing_extensions import dataclass_transform
except ImportError:
    # borrowed from pydantic 1.9
    _T = TypeVar("_T")

    def __dataclass_transform__(
        *,
        eq_default: bool = True,
        order_default: bool = False,
        kw_only_default: bool = False,
        field_descriptors: Tuple[Union[type, Callable[..., Any]], ...] = (()),
    ) -> Callable[[_T], _T]:
        return lambda a: a


__all__ = [
    "dataclass_transform",
    "override",
]
