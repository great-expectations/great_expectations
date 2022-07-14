from typing import Set, Tuple, Union

from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import DataContextConfig


class DataContextStore(ConfigurationStore):
    """
    A DataContextStore manages persistence around DataContextConfigs.
    """

    _configuration_class = DataContextConfig

    ge_cloud_exclude_field_names: Set[str] = {
        DataContextVariableSchema.DATASOURCES,
        DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
    }

    def serialize(
        self, key: Tuple[str, ...], value: DataContextConfig
    ) -> Union[dict, str]:
        payload: Union[str, dict] = super().serialize(key=key, value=value)

        # Cloud requires a subset of the DataContextConfig
        if self.ge_cloud_mode:
            assert isinstance(payload, dict)
            for attr in self.ge_cloud_exclude_field_names:
                payload.pop(attr)

        return payload
