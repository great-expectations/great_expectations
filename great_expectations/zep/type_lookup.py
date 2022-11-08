from collections import UserDict
from typing import Hashable, Iterable, Mapping, Optional, Set, Type, Union, overload

from typing_extensions import TypeAlias


class TypeLookupError(ValueError):
    pass


ValidTypes: TypeAlias = Union[Hashable, Type]


class TypeLookup(
    UserDict,
    Mapping[ValidTypes, ValidTypes],
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
        __dict: Optional[Mapping[ValidTypes, ValidTypes]] = None,
        **kwargs: Hashable,
    ):
        __dict = __dict or {}
        super().__init__(__dict, **kwargs)

    @overload
    def __getitem__(self, key: str) -> Type:
        ...

    @overload
    def __getitem__(self, key: Type) -> str:
        ...

    def __getitem__(self, key: ValidTypes) -> ValidTypes:
        return super().__getitem__(key)

    def __delitem__(self, key: ValidTypes):
        value = self.data.pop(key)
        super().pop(value, None)

    def __setitem__(self, key: ValidTypes, value: ValidTypes):
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

    def intersection(self, collection_: Iterable[ValidTypes]) -> Set[ValidTypes]:
        """
        Returns the intersection of a list (or other iterable) and the keys/values of
        the `TypeLookup` instance.
        """
        return set(collection_).intersection(self.keys())

    def raise_if_contains(self, collection_: Iterable[ValidTypes]):
        """Raise a TypeLookup error if the passed iterable contains any overlapping items."""
        intersection = self.intersection(collection_)
        if intersection:
            raise TypeLookupError(f"Items are already present - {intersection}")
