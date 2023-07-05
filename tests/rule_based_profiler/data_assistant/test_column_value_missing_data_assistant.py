import pathlib
import pytest
import os

import great_expectations as gx

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource


@pytest.fixture
def sqlite_datasource_name() -> str:
    return "sqlite_datasource"


@pytest.fixture
def sqlite_database_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    return pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)


@pytest.fixture
def sqlite_datasource(
    empty_data_context, sqlite_database_path, sqlite_datasource_name
) -> SqliteDatasource:
    connection_string = f"sqlite:///{sqlite_database_path}"
    datasource = SqliteDatasource(
        name=sqlite_datasource_name,
        connection_string=connection_string,  # type: ignore[arg-type]  # pydantic will coerce
    )
    empty_data_context.add_datasource(
        datasource=datasource,
    )
    return datasource


@pytest.mark.integration
@pytest.mark.slow
def test_single_batch_not_null_expectation(sqlite_datasource):
    asset = sqlite_datasource.add_query_asset(
        name="my_asset", query="select vendor_id from yellow_tripdata_sample_2019_01"
    )
    data_context = sqlite_datasource._data_context

    # Running onboarding data assistant
    result = data_context.assistants.column_value_missing.run(
        batch_request=asset.build_batch_request()
    )

    assert len(result.expectation_configurations) == 1
    assert (
        result.expectation_configurations[0].expectation_type
        == "expect_column_values_to_not_be_null"
    )
    assert result.expectation_configurations[0].kwargs["column"] == "vendor_id"
    assert result.expectation_configurations[0].kwargs["mostly"] == 1.0


@pytest.mark.integration
@pytest.mark.slow
def test_single_batch_null_expectation(sqlite_datasource):
    asset = sqlite_datasource.add_query_asset(
        name="my_asset",
        query="select congestion_surcharge from yellow_tripdata_sample_2019_01 where congestion_surcharge is null",
    )
    data_context = sqlite_datasource._data_context

    # Running onboarding data assistant
    result = data_context.assistants.column_value_missing.run(
        batch_request=asset.build_batch_request()
    )

    assert len(result.expectation_configurations) == 1
    assert (
        result.expectation_configurations[0].expectation_type
        == "expect_column_values_to_be_null"
    )
    assert (
        result.expectation_configurations[0].kwargs["column"] == "congestion_surcharge"
    )
    assert result.expectation_configurations[0].kwargs["mostly"] == 1.0


@pytest.mark.integration
@pytest.mark.slow
def test_single_batch_no_expectation(sqlite_datasource):
    asset = sqlite_datasource.add_query_asset(
        name="my_asset",
        query="""
        select vendor_id from (
            select vendor_id from yellow_tripdata_sample_2019_01
            union
            select null as vendor_id from yellow_tripdata_sample_2019_01
        )
        """,
    )
    data_context = sqlite_datasource._data_context

    # Running onboarding data assistant
    result = data_context.assistants.column_value_missing.run(
        batch_request=asset.build_batch_request()
    )

    assert len(result.expectation_configurations) == 0
