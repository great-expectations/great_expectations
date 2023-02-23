from __future__ import annotations

import pathlib

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.experimental.datasources import (
    PandasFilesystemDatasource,
    SparkFilesystemDatasource,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
    TestConnectionError,
)
from tests.experimental.datasources.integration.conftest import sqlite_datasource
from tests.experimental.datasources.integration.integration_test_utils import (
    run_batch_head,
    run_checkpoint_and_data_doc,
    run_data_assistant_and_checkpoint,
    run_multibatch_data_assistant_and_checkpoint,
)


@pytest.mark.integration
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    include_rendered_content: bool,
):
    run_checkpoint_and_data_doc(
        datasource_test_data=datasource_test_data,
        include_rendered_content=include_rendered_content,
    )


@pytest.mark.integration
@pytest.mark.slow  # sql: 7s  # pandas: 4s
def test_run_data_assistant_and_checkpoint(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
):
    run_data_assistant_and_checkpoint(datasource_test_data=datasource_test_data)


@pytest.mark.integration
@pytest.mark.slow  # sql: 33s  # pandas: 9s
def test_run_multibatch_data_assistant_and_checkpoint(multibatch_datasource_test_data):
    """Test using data assistants to create expectation suite using multiple batches and to run checkpoint"""
    run_multibatch_data_assistant_and_checkpoint(
        multibatch_datasource_test_data=multibatch_datasource_test_data
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ["n_rows", "fetch_all", "success"],
    [
        (None, False, True),
        (3, False, True),
        (7, False, True),
        (-100, False, True),
        ("invalid_value", False, False),
        (1.5, False, False),
        (True, False, False),
        (0, False, True),
        (200000, False, True),
        (1, False, True),
        (-50000, False, True),
        (-5, True, True),
        (0, True, True),
        (3, True, True),
        (50000, True, True),
        (-20000, True, True),
        (None, True, True),
        (15, "invalid_value", False),
    ],
)
def test_batch_head(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    fetch_all: bool | str,
    n_rows: int | float | str | None,
    success: bool,
) -> None:
    run_batch_head(
        datasource_test_data=datasource_test_data,
        fetch_all=fetch_all,
        n_rows=n_rows,
        success=success,
    )


@pytest.mark.integration
def test_sql_query_data_asset(empty_data_context):
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    passenger_count_value = 5
    asset = (
        datasource.add_query_asset(
            name="query_asset",
            query=f"   SELECT * from yellow_tripdata_sample_2019_02 WHERE passenger_count = {passenger_count_value}",
        )
        .add_splitter_year_and_month(column_name="pickup_datetime")
        .add_sorters(["year"])
    )
    validator = context.get_validator(
        batch_request=asset.build_batch_request({"year": 2019})
    )
    result = validator.expect_column_distinct_values_to_equal_set(
        column="passenger_count",
        value_set=[passenger_count_value],
        result_format={"result_format": "BOOLEAN_ONLY"},
    )
    assert result.success


@pytest.mark.integration
@pytest.mark.parametrize(
    ["base_directory", "batching_regex", "raises_test_connection_error"],
    [
        pytest.param(
            pathlib.Path(__file__).parent.joinpath(
                pathlib.Path(
                    "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
                )
            ),
            r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
            False,
            id="good filename",
        ),
        pytest.param(
            pathlib.Path(__file__).parent.joinpath(
                pathlib.Path(
                    "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
                )
            ),
            r"bad_yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
            True,
            id="bad filename",
        ),
        pytest.param(
            pathlib.Path(__file__).parent.joinpath(
                pathlib.Path("..", "..", "..", "test_sets")
            ),
            r"taxi_yellow_tripdata_samples/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
            False,
            id="good path",
        ),
        pytest.param(
            pathlib.Path(__file__).parent.joinpath(
                pathlib.Path("..", "..", "..", "test_sets")
            ),
            r"bad_taxi_yellow_tripdata_samples/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
            True,
            id="bad path",
        ),
        pytest.param(
            pathlib.Path(__file__).parent.joinpath(
                pathlib.Path(
                    "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
                )
            ),
            None,
            False,
            id="default regex",
        ),
    ],
)
def test_filesystem_data_asset_batching_regex(
    filesystem_datasource: PandasFilesystemDatasource | SparkFilesystemDatasource,
    base_directory: pathlib.Path,
    batching_regex: str | None,
    raises_test_connection_error: bool,
):
    filesystem_datasource.base_directory = base_directory
    if raises_test_connection_error:
        with pytest.raises(TestConnectionError):
            filesystem_datasource.add_csv_asset(
                name="csv_asset", batching_regex=batching_regex
            )
    else:
        filesystem_datasource.add_csv_asset(
            name="csv_asset", batching_regex=batching_regex
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    [
        "database",
        "table_name",
        "splitter_name",
        "splitter_kwargs",
        "sorter_args",
        "all_batches_cnt",
        "specified_batch_request",
        "specified_batch_cnt",
        "last_specified_batch_metadata",
    ],
    [
        pytest.param(
            "yellow_tripdata_sample_2020_all_months_combined.db",
            "yellow_tripdata_sample_2020",
            "add_splitter_year",
            {"column_name": "pickup_datetime"},
            ["year"],
            1,
            {"year": 2020},
            1,
            {"year": 2020},
            id="year",
        ),
        pytest.param(
            "yellow_tripdata_sample_2020_all_months_combined.db",
            "yellow_tripdata_sample_2020",
            "add_splitter_year_and_month",
            {"column_name": "pickup_datetime"},
            ["year", "month"],
            12,
            {"year": 2020, "month": 6},
            1,
            {"year": 2020, "month": 6},
            id="year_and_month",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_year_and_month_and_day",
            {"column_name": "pickup_datetime"},
            ["year", "month", "day"],
            28,
            {"year": 2019, "month": 2, "day": 10},
            1,
            {"year": 2019, "month": 2, "day": 10},
            id="year_and_month_and_day",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_datetime_part",
            {
                "column_name": "pickup_datetime",
                "datetime_parts": ["year", "month", "day"],
            },
            ["year", "month", "day"],
            28,
            {"year": 2019, "month": 2},
            28,
            {"year": 2019, "month": 2, "day": 28},
            id="datetime_part",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_column_value",
            {"column_name": "passenger_count"},
            ["passenger_count"],
            7,
            {"passenger_count": 3},
            1,
            {"passenger_count": 3},
            id="column_value",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_divided_integer",
            {"column_name": "passenger_count", "divisor": 3},
            ["quotient"],
            3,
            {"quotient": 2},
            1,
            {"quotient": 2},
            id="divisor",
        ),
    ],
)
def test_column_splitter(
    empty_data_context,
    database,
    table_name,
    splitter_name,
    splitter_kwargs,
    sorter_args,
    all_batches_cnt,
    specified_batch_request,
    specified_batch_cnt,
    last_specified_batch_metadata,
):
    context = empty_data_context
    datasource = sqlite_datasource(context, database)
    asset = datasource.add_table_asset(
        name="table_asset",
        table_name=table_name,
    )
    getattr(asset, splitter_name)(**splitter_kwargs)
    asset.add_sorters(sorter_args)
    # Test getting all batches
    all_batches = asset.get_batch_list_from_batch_request(asset.build_batch_request())
    assert len(all_batches) == all_batches_cnt
    # Test getting specified batches
    specified_batches = asset.get_batch_list_from_batch_request(
        asset.build_batch_request(specified_batch_request)
    )
    assert len(specified_batches) == specified_batch_cnt
    assert specified_batches[-1].metadata == last_specified_batch_metadata
