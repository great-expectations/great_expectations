"""Serialize Datasource Configurations.

Serializers determine how to write an object to disk, json, etc.
A serializer comprises the object destination and name e.g. YamlDatasourceSerializer.

Typical usage example:

# TODO: AJB 20220822 Add typical usage example
"""
from typing import Union

from great_expectations.core.serializer import BaseSerializer
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource import Datasource
from great_expectations.marshmallow__shade import Schema


class YamlDatasourceSerializer(BaseSerializer):

    _schema: Schema = datasourceConfigSchema

    def serialize(self, obj: Union[Datasource, DatasourceConfig]) -> dict:
        """Serialize Datasource or DatasourceConfig to yaml.

        Args:
            obj: object to serialize.

        Returns:
            representation of object in serializer specific data type.
        """
        if isinstance(obj, Datasource):
            obj = DatasourceConfig(**obj.config)

        config: dict = self._schema.dump(obj)

        # Remove datasource name fields
        config.pop("name", None)

        # Remove data connector name fields
        for data_connector_name, data_connector_config in config.get(
            "data_connectors", {}
        ).items():
            data_connector_config.pop("name", None)

        return config
