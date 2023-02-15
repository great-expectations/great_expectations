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

    cloud_exclude_field_names: Set[DataContextVariableSchema] = {
        DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
        DataContextVariableSchema.CHECKPOINT_STORE_NAME,
        DataContextVariableSchema.DATASOURCES,
        DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
        DataContextVariableSchema.EXPECTATIONS_STORE_NAME,
        DataContextVariableSchema.PROFILER_STORE_NAME,
        DataContextVariableSchema.VALIDATIONS_STORE_NAME,
        DataContextVariableSchema.VALIDATION_OPERATORS,
    }

    def serialize(self, value: DataContextConfig) -> Union[dict, str]:
        """
        Please see `ConfigurationStore.serialize` for more information.

        Note that GX Cloud utilizes a subset of the config; as such, an explicit
        step to remove unnecessary keys is a required part of the serialization process.

        Args:
            value: DataContextConfig to serialize utilizing the configured StoreBackend.

        Returns:
            Either a string or dictionary representation of the serialized config.
        """
        payload: Union[str, dict] = super().serialize(value=value)

        # Cloud requires a subset of the DataContextConfig
        if self.cloud_mode:
            assert isinstance(payload, dict)
            for attr in self.cloud_exclude_field_names:
                if attr in payload:
                    payload.pop(attr)
                    logger.debug(
                        f"Removed {attr} from DataContextConfig while serializing to JSON"
                    )

        return payload
