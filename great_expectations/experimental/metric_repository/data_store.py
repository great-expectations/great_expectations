import abc
from typing import Generic, TypeVar

from great_expectations import DataContext

T = TypeVar("T")


class DataStore(abc.ABC):
    """Abstract base class for all DataStore implementations."""

    def __init__(self, context: DataContext):
        self._context = context

    @abc.abstractmethod
    def add(self, value: Generic[T]) -> T:
        """Add a value to the DataStore.

        Args:
            value: Value to add to the DataStore.

        Returns:
            `value` passed in.
        """
        raise NotImplementedError
