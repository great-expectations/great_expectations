import logging
from typing import Set, Union

from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import DataContextConfig

logger = logging.getLogger(__name__)


class DataContextStore(ConfigurationStore):
    """
    A DataContextStore manages persistence around DataContextConfigs.
    """

    _configuration_class = DataContextConfig

    ge_cloud_exclude_field_names: Set[DataContextVariableSchema] = {
        DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
        DataContextVariableSchema.DATASOURCES,
        DataContextVariableSchema.VALIDATION_OPERATORS,
    }

    def serialize(self, value: DataContextConfig) -> Union[dict, str]:
        """
        Please see `ConfigurationStore.serialize` for more information.

        Note that GE Cloud utilizes a subset of the config; as such, an explicit
        step to remove unnecessary keys is a required part of the serialization process.

        Args:
            value: DataContextConfig to serialize utilizing the configured StoreBackend.

        Returns:
            Either a string or dictionary representation of the serialized config.
        """
        payload: Union[str, dict] = super().serialize(value=value)

        # Cloud requires a subset of the DataContextConfig
        if self.ge_cloud_mode:
            assert isinstance(payload, dict)
            for attr in self.ge_cloud_exclude_field_names:
                if attr in payload:
                    payload.pop(attr)
                    logger.debug(
                        f"Removed {attr} from DataContextConfig while serializing to JSON"
                    )

        return payload
