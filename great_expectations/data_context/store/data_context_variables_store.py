from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import DataContextConfig


class DataContextVariablesStore(ConfigurationStore):
    """
    A DataContextVariablesStore manages config variables for the DataContext.
    """

    _configuration_class = DataContextConfig
