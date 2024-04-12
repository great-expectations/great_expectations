import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import (
    PartitionerYear,
    PartitionerYearAndMonth,
    PartitionerYearAndMonthAndDay,
)
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import _SQLAsset


@pytest.fixture
def datasource(mocker):
    return mocker.Mock(spec=SQLDatasource)


@pytest.fixture
def asset(datasource) -> _SQLAsset:
    asset = _SQLAsset(
        name="test_asset",
    )
    asset._datasource = datasource  # same pattern Datasource uses to init Asset
    return asset


@pytest.mark.unit
def test_add_batch_definition_fluent_sql__add_batch_definition_whole_table(datasource, asset):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(name=name, partitioner=None, batching_regex=None)
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_whole_table(name=name)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_sql__add_batch_definition_yearly(datasource, asset):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=PartitionerYear(column_name=column), batching_regex=None
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_sql__add_batch_definition_monthly(datasource, asset):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=PartitionerYearAndMonth(column_name=column), batching_regex=None
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_sql__add_batch_definition_daily(datasource, asset):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=PartitionerYearAndMonthAndDay(column_name=column),
        batching_regex=None,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)
