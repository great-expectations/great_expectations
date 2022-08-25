"""Serialize Datasource Configurations.

Serializers determine how to write an object to disk, json, etc.
A serializer comprises the object destination and name e.g. YAMLReadyDictDatasourceConfigSerializer.

Typical usage example:

datasource_config = DatasourceConfig(...)
serializer = YAMLReadyDictDatasourceConfigSerializer()
serialized_value = serializer.serialize(datasource_config)
"""
from great_expectations.core.serializer import AbstractConfigSerializer
from great_expectations.data_context.types.base import DatasourceConfig


class YAMLReadyDictDatasourceConfigSerializer(AbstractConfigSerializer):
    def serialize(self, obj: DatasourceConfig) -> dict:
        """Serialize DatasourceConfig to dict appropriate for writing to yaml.

        Args:
            obj: DatasourceConfig object to serialize.

        Returns:
            Representation of object as a dict appropriate for writing to yaml.
        """

        config: dict = self.schema.dump(obj)

        # Remove datasource name fields
        config.pop("name", None)

        # Remove data connector name fields
        for data_connector_name, data_connector_config in config.get(
            "data_connectors", {}
        ).items():
            data_connector_config.pop("name", None)

        return config


class ListDatasourceSerializer(AbstractConfigSerializer):
    def serialize(self, obj: DatasourceConfig) -> dict:
        """Serialize DatasourceConfig with datasource name but not data connector name to match existing context.list_datasources() functionality.

        Args:
            obj: DatasourceConfig object to serialize.

        Returns:
            Representation of object as a dict suitable for return in list_datasources().
        """

        config: dict = self.schema.dump(obj)

        # Remove data connector config names
        for data_connector_name, data_connector_config in config.get(
            "data_connectors", {}
        ).items():
            data_connector_config.pop("name", None)

        return config
