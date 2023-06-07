from __future__ import annotations

import contextlib
import copy
import logging
from collections import UserDict
from typing import (
    TYPE_CHECKING,
    Generator,
    Hashable,
    Iterable,
    Mapping,
    Optional,
    Set,
    Type,
    Union,
    overload,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

logger = logging.getLogger(__name__)


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
        super().__init__(__dict, **kwargs)

    def type_names(self) -> Generator[str, None, None]:
        """Yields only the type `str` names of the TypeLookup."""
        for k in self:
            if isinstance(k, str):
                yield k
            continue

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
        if key in self and value in self and self[key] == value and self[value] == key:
            # This key, value pair has already been registered so we return
            return
        if key is None:
            raise TypeLookupError(f"`NoneType` for {value} is not allowed - bad key")
        if value is None:
            raise TypeLookupError(f"`NoneType` for {key} is not allowed - bad value")
        if key in self:
            raise TypeLookupError(f"`{key}` already set - bad key")
        if value in self:
            raise TypeLookupError(f"`{value}` already set - bad value")
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
        >>> with t.transaction():
        ...     t["my_type"] = tuple
        ...     assert tuple in t, "Should not fail"
        ...     assert True is False, "Should fail"
        Traceback (most recent call last):
        ...
        AssertionError: Should fail
        >>> print(tuple in t)
        False
        """
        txn_exc: Union[Exception, None] = None

        backup_data = copy.copy(self.data)
        logger.debug("Beginning TypeLookup transaction")
        try:
            yield self
        except Exception as exc:
            txn_exc = exc
            raise
        finally:
            if txn_exc:
                logger.debug("Transaction of items rolled back")
                self.data = backup_data
            else:
                logger.debug("Transaction committing items")
            logger.debug("Completed TypeLookup transaction")


if __name__ == "__main__":
    import doctest

    doctest.testmod(report=True, verbose=True)
