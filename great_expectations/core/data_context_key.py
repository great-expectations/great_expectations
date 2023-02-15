from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple, Union


class DataContextKey(metaclass=ABCMeta):
    """DataContextKey objects are used to uniquely identify resources used by the DataContext.

    A DataContextKey is designed to support clear naming with multiple representations including a hashable
    version making it suitable for use as the key in a dictionary.
    """

    @abstractmethod
    def to_tuple(self) -> tuple:
        raise NotImplementedError

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(*tuple_)

    def to_fixed_length_tuple(self) -> tuple:
        raise NotImplementedError

    @classmethod
    def from_fixed_length_tuple(cls, tuple_) -> DataContextKey:
        raise NotImplementedError

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.to_tuple() == other.to_tuple()

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.to_tuple() < other.to_tuple()

    def __le__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.to_tuple() <= other.to_tuple()

    def __gt__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.to_tuple() > other.to_tuple()

    def __ge__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.to_tuple() >= other.to_tuple()

    def __hash__(self):
        return hash(self.to_tuple())

    def __repr__(self):
        return f"{self.__class__.__name__}::{'/'.join(self.to_tuple())}"


class StringKey(DataContextKey):
    """A simple DataContextKey with just a single string value"""

    def __init__(self, key) -> None:
        self._key = key

    def to_tuple(self):
        return (self._key,)

    def to_fixed_length_tuple(self):
        return self.to_tuple()

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls.from_tuple(tuple_)


class DataContextVariableKey(DataContextKey):
    def __init__(
        self,
        resource_name: Optional[str] = None,
    ) -> None:
        self._resource_name = resource_name

    @property
    def resource_name(self) -> Union[str, None]:
        return self._resource_name

    def to_tuple(self) -> Tuple[str]:
        """
        See parent `DataContextKey.to_tuple` for more information.
        """
        return (self._resource_name or "",)

    def to_fixed_length_tuple(self) -> Tuple[str]:
        """
        See parent `DataContextKey.to_fixed_length_tuple` for more information.
        """
        return self.to_tuple()

    @classmethod
    def from_fixed_length_tuple(cls, tuple_: tuple) -> DataContextVariableKey:
        """
        See parent `DataContextKey.from_fixed_length_tuple` for more information.
        """
        return cls.from_tuple(tuple_)
