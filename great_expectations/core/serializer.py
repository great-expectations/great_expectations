"""Serializer class interface definition.

Serializers determine how to write an object to disk, json, etc.
A serializer comprises the object destination and name e.g. YamlDatasourceSerializer.
A base implementation is provided if no modification needs to be included for the specific object / destination pair.

Typical usage example:

# TODO: AJB 20220822 Add typical usage example
"""

import abc
from typing import Any


class AbstractSerializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, obj: Any) -> Any:
        """Serialize to serializer specific data type.

        Note, specific parameter and return types to be implemented in subclasses.

        Args:
            obj: object to serialize.

        Returns:
            representation of object in serializer specific data type.
        """
        raise NotImplementedError


class BaseSerializer(AbstractSerializer):
    # TODO: This will contain a default implementation to be overridden in subclasses
    pass
