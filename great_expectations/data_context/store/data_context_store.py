from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import DataContextConfig


class DataContextStore(ConfigurationStore):
    """
    A DataContextStore manages persistence around DataContextConfigs.
    """

    _configuration_class = DataContextConfig
