import abc
from typing import Generic, TypeVar

T = TypeVar("T")


class Serializer(abc.ABC, Generic[T]):
    """Abstract base class for all Serializer implementations."""

    def serialize(self, value: T) -> str:
        """Serialize a value to json str.

        Args:
            value: Value to serialize.

        Returns:
            Bytes representation of `value`.
        """
        raise NotImplementedError

    def deserialize(self, config: dict) -> T:
        """Deserialize a value from bytes.

        Args:
            value: Bytes representation of a value.

        Returns:
            Deserialized value.
        """
        raise NotImplementedError
