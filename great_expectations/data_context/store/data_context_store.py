from typing import Tuple, Union

from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
)


class DataContextStore(ConfigurationStore):
    """
    A DataContextStore manages persistence around DataContextConfigs.
    """

    _configuration_class = DataContextConfig

    def serialize(
        self, key: Tuple[str, ...], value: DataContextConfig
    ) -> Union[dict, str]:
        if self.ge_cloud_mode:
            return self._serialize_variables(key=key, value=value)
        return value.to_yaml_str()

    def _serialize_variables(
        self, key: Tuple[str, ...], value: DataContextConfig
    ) -> dict:
        config_schema: DataContextConfigSchema = value.get_schema_class()()
        payload: dict = config_schema.dump(value)

        for attr in (
            DataContextVariableSchema.DATASOURCES,
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
        ):
            payload.pop(attr)

        return payload
