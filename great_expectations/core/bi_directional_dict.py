import pprint
from collections import UserDict
from typing import Hashable, Mapping, Optional, TypeVar

T = TypeVar("T", bound=Hashable)


class BiDict(
    UserDict,
    Mapping[T, T],
):
    def __init__(
        self,
        __dict: Optional[Mapping[Hashable, Hashable]] = None,
        /,
        **kwargs: Hashable,
    ):
        if isinstance(__dict, Mapping):
            kwargs: dict = {**__dict, **kwargs}  # type: ignore[no-redef]
            __dict = None
        super().__init__(__dict, **kwargs)

    def __delitem__(self, key: Hashable):
        value = self.data.pop(key)
        super().pop(value, None)

    def __setitem__(self, key: Hashable, value: Hashable):
        if key in self:
            del self[self[key]]
        if value in self:
            del self[value]
        super().__setitem__(key, value)
        super().__setitem__(value, key)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return str(self.data)


# This makes BiDict pretty printable like a regular dict
if isinstance(getattr(pprint.PrettyPrinter, "_dispatch"), dict):
    # we are relying on a private implementation detail here, so first check that the
    # attribute still exists

    def pprint_BiDict(printer, object, stream, indent, allowance, context, level):
        return printer._pprint_dict(
            object.data, stream, indent, allowance, context, level
        )

    pprint.PrettyPrinter._dispatch[BiDict.__repr__] = pprint_BiDict  # type: ignore[attr-defined]
