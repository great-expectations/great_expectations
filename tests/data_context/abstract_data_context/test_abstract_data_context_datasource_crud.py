from typing import Optional, Union
from unittest import mock

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,
    EphemeralDataContextVariables,
)
from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
)
from great_expectations.datasource import BaseDatasource, LegacyDatasource


class StubDatasourceStore(DatasourceStore):
    def __init__(self):
        pass


class FakeAbstractDataContext(AbstractDataContext):
    def __init__(self):
        self._datasource_store = StubDatasourceStore()
        self._variables: Optional[DataContextVariables] = None
        self._cached_datasources: dict = {}

    def _init_variables(self):
        return EphemeralDataContextVariables(config=DataContextConfig())

    def _determine_substitutions(self):
        return {}

    def save_expectation_suite(self):
        """Abstract method."""
        pass

    def _init_datasource_store(self):
        """Abstract method."""
        pass


@pytest.mark.unit
def test_save_datasource_empty_store(datasource_config_with_names: DatasourceConfig):

    context = FakeAbstractDataContext()
    # Make sure the fixture has the right configuration
    assert len(context.list_datasources()) == 0

    # add_datasource used to create a datasource object for use in save_datasource
    datasource_to_save = context.add_datasource(
        **datasource_config_with_names.to_json_dict(), save_changes=False
    )

    with mock.patch(
        "great_expectations.data_context.store.datasource_store.DatasourceStore.set",
        autospec=True,
        return_value=datasource_config_with_names,
    ) as mock_set:

        saved_datasource: Union[
            LegacyDatasource, BaseDatasource
        ] = context.save_datasource(datasource_to_save)

    mock_set.assert_called_once()

    # Make sure the datasource config got into the context config
    assert len(context.config.datasources) == 1  # type: ignore[arg-type]
    assert context.config.datasources[datasource_to_save.name] == datasource_config_with_names  # type: ignore[index]

    # Make sure the datasource got into the cache
    assert len(context._cached_datasources) == 1

    # Make sure the stored and returned datasource is the same one as the cached datasource
    cached_datasource = context._cached_datasources[datasource_to_save.name]
    assert saved_datasource == cached_datasource


@pytest.mark.unit
def test_save_datasource_overwrites_on_name_collision(
    datasource_config_with_names: DatasourceConfig,
):
    pass
