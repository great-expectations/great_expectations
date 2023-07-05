import pathlib
import pytest
import os

import great_expectations as gx
from typing import List

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource
from great_expectations import DataContext
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.data_assistant_result.plot_result import (
    PlotResult,
)

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


@pytest.mark.integration
@pytest.mark.slow  # 1.67s
def test_column_value_missing_data_assistant_plot_expectations_and_metrics_correctly_handle_empty_plot_data(
    bobby_columnar_table_multi_batch_probabilistic_data_context: DataContext,
) -> None:
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    include_column_names: List[str] = [
        "congestion_surcharge",
    ]

    data_assistant_result: DataAssistantResult = (
        context.assistants.column_value_missing.run(
            batch_request=batch_request,
            include_column_names=include_column_names,
        )
    )
    plot_result: PlotResult = data_assistant_result.plot_expectations_and_metrics(
        include_column_names=include_column_names
    )

    # This test passes only if absense of any metrics and expectations to plot does not cause exceptions to be raised.
    column_domain_charts: List[dict] = [p.to_dict() for p in plot_result.charts[2:]]
    assert len(column_domain_charts) == 0
