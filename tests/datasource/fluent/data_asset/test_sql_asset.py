import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import (
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
)
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset, _SQLAsset
from tests.datasource.fluent.conftest import CreateSourceFixture


@pytest.fixture
def postgres_asset(empty_data_context, create_source: CreateSourceFixture, monkeypatch):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    monkeypatch.setattr(TableAsset, "test_connection", lambda _: None)
    years = [2021, 2022]
    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_table_asset(
            name="query_asset",
            table_name="my_table",
            batch_metadata=asset_specified_metadata,
        )
        assert asset.batch_metadata == asset_specified_metadata

        yield asset


@pytest.mark.postgresql
def test_get_batch_identifiers_list__sort_ascending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_identifiers_list(
        postgres_asset.build_batch_request(
            partitioner=ColumnPartitionerYearly(column_name="year", sort_ascending=True)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2021, 2022]):
        batches[i]["year"] = year


@pytest.mark.postgresql
def test_get_batch_identifiers_list__sort_descending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_identifiers_list(
        postgres_asset.build_batch_request(
            partitioner=ColumnPartitionerYearly(column_name="year", sort_ascending=False)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2022, 2021]):
        batches[i]["year"] = year


@pytest.fixture
def datasource(mocker):
    return mocker.Mock(spec=SQLDatasource)


@pytest.fixture
def asset(datasource) -> _SQLAsset:
    asset = _SQLAsset[SQLDatasource](name="test_asset", type="_sql_asset")
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
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_yearly(
    datasource, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerYearly(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_monthly(
    datasource, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerMonthly(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("sort_ascending", (True, False))
def test_add_batch_definition_fluent_sql__add_batch_definition_daily(
    datasource, asset, sort_ascending
):
    # arrange
    name = "batch_def_name"
    column = "test_column"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerDaily(column_name=column, sort_ascending=sort_ascending),
        batching_regex=None,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(
        name=name, column=column, sort_ascending=sort_ascending
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)
