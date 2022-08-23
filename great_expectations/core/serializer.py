"""Serializer class interface definition.

Serializers determine how to write an object to disk, json, etc.
A serializer comprises the object destination and name e.g. YAMLReadyDictDatasourceConfigSerializer.
A base implementation (DictConfigSerializer) is provided if no modification needs to be included for the specific object / destination pair.

Typical usage example:

datasource_config = DatasourceConfig(...)
serializer = DictConfigSerializer(schema=datasourceConfigSchema)
serialized_value = serializer.serialize(datasource_config)
"""

import abc
from typing import Union

from great_expectations.core.configuration import AbstractConfig
from great_expectations.marshmallow__shade import Schema


class AbstractConfigSerializer(abc.ABC):
    """Serializer interface.

    Note: When mypy coverage is enhanced further, this Abstract class can be replaced with a Protocol.
    """

    def __init__(self, schema: Schema) -> None:
        """
        Args:
            schema: Marshmallow schema defining raw serialized version of object.
        """
        self.schema = schema

    @abc.abstractmethod
    def serialize(self, obj: AbstractConfig) -> Union[str, dict, AbstractConfig]:
        """Serialize to serializer specific data type.

        Note, specific return type to be implemented in subclasses.

        Args:
            obj: Object to serialize.

        Returns:
            Representation of object in serializer specific data type.
        """
        raise NotImplementedError


class DictConfigSerializer(AbstractConfigSerializer):
    def serialize(self, obj: AbstractConfig) -> dict:
        """Serialize to Python dictionary.

        This is typically the default implementation used in can be overridden in subclasses.

        Args:
            obj: Object to serialize.

        Returns:
            Representation of object as a Python dictionary using the defined Marshmallow schema.
        """
        return self.schema.dump(obj)
