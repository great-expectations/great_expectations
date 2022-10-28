from collections import UserDict
from typing import Hashable, Iterable, Mapping, Optional


class TypeLookupError(ValueError):
    pass


class TypeLookup(
    UserDict,
    Mapping[Hashable, Hashable],
):
    """
    Dict-like Mapping object that creates keys from values and values from keys.
    Because of this, all values must be Hashable.

    If a Mapping-like object is passed as the first parameter, its key/values will be
    unpacked (and combined with kwargs) into the new `TypeDict` object.

    Once set, values/keys cannot be overwritten.
    """

    def __init__(
        self,
        __dict: Optional[Mapping[Hashable, Hashable]] = None,
        **kwargs: Hashable,
    ):
        __dict = __dict or {}
        super().__init__(__dict, **kwargs)

    def __getitem__(self, key: Hashable) -> Hashable:
        return super().__getitem__(key)

    def __delitem__(self, key: Hashable):
        value = self.data.pop(key)
        super().pop(value, None)

    def __setitem__(self, key: Hashable, value: Hashable):
        if key in self:
            raise TypeLookupError(f"`{key}` already set - key")
        if value in self:
            raise TypeLookupError(f"`{value}` already set - value")
        super().__setitem__(key, value)
        super().__setitem__(value, key)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return str(self.data)

    def contains_anyof(self, collection_: Iterable[Hashable]) -> bool:
        return bool(set(collection_).intersection(self.keys()))
