from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import DataContextConfig


class DataContextStore(ConfigurationStore):
    """
    A GeConfigStore manages Data Context configuration
    """

    _configuration_class = DataContextConfig
