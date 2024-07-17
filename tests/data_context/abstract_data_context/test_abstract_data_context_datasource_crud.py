from typing import Final, Optional

import pytest

from great_expectations.core.config_provider import _ConfigurationProvider
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,
    EphemeralDataContextVariables,
)
from great_expectations.data_context.store import DatasourceStore, InMemoryStoreBackend
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    DatasourceConfigSchema,
)


class StubDatasourceStore(DatasourceStore):
    """Used for mocking the set() call."""

    def __init__(self):
        datasourceConfigSchema = DatasourceConfigSchema()
        self._store_backend = InMemoryStoreBackend()
        self._use_fixed_length_key = self._store_backend.fixed_length_key
        self._schema = datasourceConfigSchema
        self._serializer = DictConfigSerializer(schema=datasourceConfigSchema)
        pass

    def set(self, key, value):
        return value


class StubConfigurationProvider(_ConfigurationProvider):
    def __init__(self, config_values=None) -> None:
        self._config_values = config_values or {}
        super().__init__()

    def get_values(self):
        return self._config_values


_STUB_CONFIG_PROVIDER: Final = StubConfigurationProvider()


class FakeAbstractDataContext(AbstractDataContext):
    def __init__(
        self, config_provider: StubConfigurationProvider = _STUB_CONFIG_PROVIDER
    ) -> None:
        """Override __init__ with only the needed attributes."""
        self._datasource_store = StubDatasourceStore()
        self._variables: Optional[DataContextVariables] = None
        self._datasources: dict = {}
        self._usage_statistics_handler = None
        self._config_provider = config_provider

    def _init_variables(self):
        """Using EphemeralDataContextVariables to store in memory."""
        return EphemeralDataContextVariables(
            config=DataContextConfig(), config_provider=self.config_provider
        )

    def save_expectation_suite(self):
        """Abstract method. Only a stub is needed."""
        pass

    def _init_datasource_store(self):
        """Abstract method. Only a stub is needed."""
        pass

    def _init_project_config(self, project_config):
        """Abstract method. Only a stub is needed."""
        pass


@pytest.mark.unit
def test_add_datasource_sanitizes_instantiated_objs_config(
    datasource_config_with_names: DatasourceConfig,
):
    # Set up fake with desired env var
    variable = "DATA_DIR"
    value_associated_with_variable = "a/b/c"
    config_values = {variable: value_associated_with_variable}
    context = FakeAbstractDataContext(
        config_provider=StubConfigurationProvider(config_values=config_values)
    )

    # Ensure that config references the above env var
    data_connector_name = tuple(datasource_config_with_names.data_connectors.keys())[0]
    datasource_config_dict = datasource_config_with_names.to_json_dict()
    datasource_config_dict["data_connectors"][data_connector_name][
        "base_directory"
    ] = f"${variable}"

    instantiated_datasource = context.add_datasource(**datasource_config_dict)

    # Runtime object should have the substituted value for downstream usage
    assert instantiated_datasource.data_connectors[
        data_connector_name
    ].base_directory.endswith(value_associated_with_variable)

    # Config attached to object should mirror the runtime object
    assert instantiated_datasource.config["data_connectors"][data_connector_name][
        "base_directory"
    ].endswith(value_associated_with_variable)

    # Raw config attached to object should reflect what needs to be persisted (no sensitive credentials!)
    assert (
        instantiated_datasource._raw_config["data_connectors"][data_connector_name][
            "base_directory"
        ]
        == f"${variable}"
    )
