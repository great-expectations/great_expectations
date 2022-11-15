from __future__ import annotations

import contextlib
import copy
import logging
from collections import UserDict
from typing import (
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    overload,
)

from typing_extensions import TypeAlias

LOGGER = logging.getLogger(__name__)


class TypeLookupError(ValueError):
    pass


ValidTypes: TypeAlias = Union[str, Type]


class TypeLookup(
    UserDict,
    Mapping[ValidTypes, ValidTypes],
):
    """
    Dict-like Mapping object that creates keys from values and values from keys.
    Because of this, all values must be Hashable.
    `NoneType` / `None` is not allowed.

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
        self._original_keys: Set[ValidTypes] = set()
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
        self._original_keys.remove(key)

    def __setitem__(self, key: ValidTypes, value: ValidTypes):
        if key is None:
            raise TypeLookupError(
                f"`{type(key).__name__}` for {value} is not allowed - bad key"
            )
        if value is None:
            raise TypeLookupError(
                f"`{type(value).__name__}` for {key} is not allowed - bad value"
            )
        if key in self:
            raise TypeLookupError(f"`{key}` already set - bad key")
        if value in self:
            raise TypeLookupError(f"`{value}` already set - bad value")
        self._original_keys.add(key)
        super().__setitem__(key, value)
        super().__setitem__(value, key)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return str(self.data)

    def original_keys(self) -> Iterator[ValidTypes]:
        """Iterate over only the original key values."""
        for key in self._original_keys:
            yield key

    def original_items(self) -> Iterator[Tuple[ValidTypes, ValidTypes]]:
        """
        Iterate over items as if it were a traditional dict.
        Use this to prevent iterating over items twice (as both a key & value).
        """
        for key in self.original_keys():
            yield key, self[key]  # type: ignore[index]

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

    def clear(self) -> None:
        """Clear all data. Deletes all keys and values."""
        return self.data.clear()

    @contextlib.contextmanager
    def transaction(self) -> Generator[TypeLookup, None, None]:
        """
        Context manager that waits until end of transaction to commit changes.
        Any exceptions that happen within the context will prevent any of the changes from being committed.

        Any exceptions encountered will be re-raised on exit.

        Example
        -------
        >>> t = TypeLookup()
        >>> with t.transaction() as txn_t:
        ...     txn_t["my_type"] = tuple
        ...     print(tuple in txn_t)
        ...     print(tuple in t)
        True
        False
        >>> print(tuple in t)
        True
        """
        initial_keys: Set[ValidTypes] = set(self.keys())
        txn_exc: Union[Exception, None] = None

        txn_lookup_copy = self.__class__()
        txn_lookup_copy.data = copy.copy(self.data)
        LOGGER.info("Beginning TypeLookup transaction")
        try:
            yield txn_lookup_copy
        except Exception as exc:
            txn_exc = exc
            LOGGER.info(
                f"{exc.__class__.__name__}:{exc} - encountered during TypeLookup transaction"
            )
            raise
        finally:
            net_new_items = {
                k: v
                for k, v in txn_lookup_copy.original_items()
                if k not in initial_keys
            }
            if txn_exc:
                LOGGER.info(f"Transaction of {len(net_new_items)} items rolled back")
            else:
                LOGGER.info(f"Transaction committing {len(net_new_items)} items")
                self.update(net_new_items)
            LOGGER.info("Completed TypeLookup transaction")


if __name__ == "__main__":
    import doctest

    doctest.testmod(report=True, verbose=True)
