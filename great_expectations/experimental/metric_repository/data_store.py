from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

T = TypeVar("T")


class DataStore(abc.ABC, Generic[T]):
    """Abstract base class for all DataStore implementations."""

    def __init__(self, context: CloudDataContext):
        self._context = context

    @abc.abstractmethod
    def add(self, value: T) -> uuid.UUID:
        """Add a value to the DataStore.

        Args:
            value: Value to add to the DataStore.

        Returns:
            id of the created resource.
        """
        raise NotImplementedError
