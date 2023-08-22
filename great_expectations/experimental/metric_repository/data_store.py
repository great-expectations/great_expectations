import abc
from typing import Generic, TypeVar

from great_expectations.data_context import AbstractDataContext

T = TypeVar("T")


class DataStore(abc.ABC, Generic[T]):
    """Abstract base class for all DataStore implementations."""

    def __init__(self, context: AbstractDataContext):
        self._context = context

    @abc.abstractmethod
    def add(self, value: T) -> T:
        """Add a value to the DataStore.

        Args:
            value: Value to add to the DataStore.

        Returns:
            `value` passed in.
        """
        raise NotImplementedError
