"""Serialize Datasource Configurations.

Serializers determine how to write an object to disk, json, etc.
A serializer comprises the object destination and name e.g. YAMLReadyDictDatasourceConfigSerializer.

Typical usage example:

datasource_config = DatasourceConfig(...)
serializer = YAMLReadyDictDatasourceConfigSerializer()
serialized_value = serializer.serialize(datasource_config)

# TODO: AJB 20220822 Add typical usage example
"""
from typing import Optional

from great_expectations.core.serializer import AbstractConfigSerializer
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.marshmallow__shade import Schema


class YAMLReadyDictDatasourceConfigSerializer(AbstractConfigSerializer):
    def __init__(self, schema: Optional[Schema] = None) -> None:
        """
        Args:
            schema: marshmallow schema defining raw serialized version of object.
        """
        super().__init__(schema=schema)

        # Override schema
        self._schema = datasourceConfigSchema

    def serialize(self, obj: DatasourceConfig) -> dict:
        """Serialize DatasourceConfig to dict appropriate for writing to yaml.

        Args:
            obj: DatasourceConfig object to serialize.

        Returns:
            Representation of object as a dict appropriate for writing to yaml.
        """

        config: dict = self._schema.dump(obj)

        # Remove datasource name fields
        config.pop("name", None)

        # Remove data connector name fields
        for data_connector_name, data_connector_config in config.get(
            "data_connectors", {}
        ).items():
            data_connector_config.pop("name", None)

        return config


class JsonDatasourceConfigSerializer(AbstractConfigSerializer):
    def __init__(self, schema: Optional[Schema] = None) -> None:
        """
        Args:
            schema: marshmallow schema defining raw serialized version of object.
        """
        super().__init__(schema=schema)

        # Override schema
        self._schema = datasourceConfigSchema

    def serialize(self, obj: DatasourceConfig) -> dict:
        """Serialize DatasourceConfig to json dict.

        Args:
            obj: DatasourceConfig object to serialize.

        Returns:
            Representation of object as a dict suitable for serializing to json.
        """

        config: dict = self._schema.dump(obj)

        json_serializable_dict: dict = convert_to_json_serializable(data=config)

        return json_serializable_dict
