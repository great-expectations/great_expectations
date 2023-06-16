from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import pandas as pd
import pydantic
import pytest

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context import (
    AbstractDataContext,
    CloudDataContext,
    FileDataContext,
)
from great_expectations.datasource.fluent import (
    BatchRequest,
    PandasFilesystemDatasource,
    SparkFilesystemDatasource,
)
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
    TestConnectionError,
)
from tests.datasource.fluent.integration.conftest import sqlite_datasource
from tests.datasource.fluent.integration.integration_test_utils import (
    run_batch_head,
    run_checkpoint_and_data_doc,
    run_data_assistant_and_checkpoint,
    run_multibatch_data_assistant_and_checkpoint,
)

if TYPE_CHECKING:
    from responses import RequestsMock


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
            MATCH_ALL_PATTERN,
            False,
            id="default regex",
        ),
    ],
)
def test_filesystem_data_asset_batching_regex(
    filesystem_datasource: PandasFilesystemDatasource | SparkFilesystemDatasource,
    base_directory: pathlib.Path,
    batching_regex: str,
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
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_mod_integer",
            {"column_name": "passenger_count", "mod": 3},
            ["remainder"],
            3,
            {"remainder": 2},
            1,
            {"remainder": 2},
            id="mod_integer",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_hashed_column",
            {"column_name": "passenger_count", "hash_digits": 3},
            ["hash"],
            7,
            {"hash": "af3"},
            1,
            {"hash": "af3"},
            id="hash",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_converted_datetime",
            {"column_name": "pickup_datetime", "date_format_string": "%Y-%m-%d"},
            ["datetime"],
            28,
            {"datetime": "2019-02-23"},
            1,
            {"datetime": "2019-02-23"},
            id="converted_datetime",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            "add_splitter_multi_column_values",
            {"column_names": ["passenger_count", "payment_type"]},
            ["passenger_count", "payment_type"],
            23,
            {"passenger_count": 1, "payment_type": 1},
            1,
            {"passenger_count": 1, "payment_type": 1},
            id="multi_column_values",
        ),
    ],
)
def test_splitter(
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


@pytest.mark.integration
def test_simple_checkpoint_run(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ]
):
    context, datasource, data_asset, batch_request = datasource_test_data
    expectation_suite_name = "my_expectation_suite"
    context.add_expectation_suite(expectation_suite_name)

    checkpoint = SimpleCheckpoint(
        "my_checkpoint",
        data_context=context,
        expectation_suite_name=expectation_suite_name,
        batch_request=batch_request,
    )
    result = checkpoint.run()
    assert result["success"]
    assert result["checkpoint_config"]["class_name"] == "SimpleCheckpoint"

    checkpoint = SimpleCheckpoint(
        "my_checkpoint",
        data_context=context,
        validations=[
            {
                "expectation_suite_name": expectation_suite_name,
                "batch_request": batch_request,
            }
        ],
    )
    result = checkpoint.run()
    assert result["success"]
    assert result["checkpoint_config"]["class_name"] == "SimpleCheckpoint"


@pytest.mark.integration
def test_simple_checkpoint_run_with_nonstring_path_option(empty_data_context):
    context = empty_data_context
    path = pathlib.Path(
        __file__,
        "..",
        "..",
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
    ).resolve(strict=True)
    datasource = context.sources.add_pandas_filesystem(name="name", base_directory=path)
    data_asset = datasource.add_csv_asset(name="csv_asset")
    batch_request = data_asset.build_batch_request(
        {"path": pathlib.Path("yellow_tripdata_sample_2019-02.csv")}
    )
    expectation_suite_name = "my_expectation_suite"
    context.add_expectation_suite(expectation_suite_name)
    checkpoint = SimpleCheckpoint(
        "my_checkpoint",
        data_context=context,
        expectation_suite_name=expectation_suite_name,
        batch_request=batch_request,
    )
    result = checkpoint.run()
    assert result["success"]
    assert result["checkpoint_config"]["class_name"] == "SimpleCheckpoint"


@pytest.mark.parametrize(
    ["add_asset_method", "add_asset_kwarg"],
    [
        pytest.param(
            "add_table_asset",
            {"table_name": "yellow_tripdata_sample_2019_02"},
            id="table_asset",
        ),
        pytest.param(
            "add_query_asset",
            {"query": "select * from yellow_tripdata_sample_2019_02"},
            id="query_asset",
        ),
    ],
)
@pytest.mark.integration
def test_asset_specified_metadata(
    empty_data_context, add_asset_method, add_asset_kwarg
):
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    asset_specified_metadata = {"pipeline_name": "my_pipeline"}
    asset = getattr(datasource, add_asset_method)(
        name="asset",
        batch_metadata=asset_specified_metadata,
        **add_asset_kwarg,
    )
    asset.add_splitter_year_and_month(column_name="pickup_datetime")
    asset.add_sorters(["year", "month"])
    # Test getting all batches
    batches = asset.get_batch_list_from_batch_request(asset.build_batch_request())
    assert len(batches) == 1
    # Update the batch_metadata from the request with the metadata inherited from the asset
    assert batches[0].metadata == {**asset_specified_metadata, "year": 2019, "month": 2}


@pytest.mark.integration
def test_batch_request_error_messages(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ]
) -> None:
    _, _, _, batch_request = datasource_test_data
    # DataAsset.build_batch_request() infers datasource_name and data_asset_name
    # which have already been confirmed as functional via test_connection() methods.
    with pytest.raises(TypeError):
        batch_request.datasource_name = "untested_datasource_name"

    with pytest.raises(TypeError):
        batch_request.data_asset_name = "untested_data_asset_name"

    # options can be added/updated if they take the correct form
    batch_request.options["new_option"] = 42
    assert "new_option" in batch_request.options

    with pytest.raises(pydantic.ValidationError):
        batch_request.options = {10: "value for non-string key"}  # type: ignore[dict-item]

    with pytest.raises(pydantic.ValidationError):
        batch_request.options = "not a dictionary"  # type: ignore[assignment]

    # batch_slice can be updated if it takes the correct form
    batch_request.batch_slice = "[5:10]"  # type: ignore[assignment]
    assert batch_request.batch_slice == slice(5, 10, None)

    # batch_slice can be updated via update method
    batch_request.update_batch_slice("[2:10:2]")
    assert batch_request.batch_slice == slice(2, 10, 2)

    with pytest.raises(ValueError):
        batch_request.batch_slice = "nonsense slice"  # type: ignore[assignment]

    with pytest.raises(ValueError):
        batch_request.batch_slice = True  # type: ignore[assignment]


@pytest.mark.integration
def test_pandas_data_adding_dataframe_in_cloud_context(
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})

    context = empty_cloud_context_fluent

    dataframe_asset = context.sources.add_or_update_pandas(
        name="fluent_pandas_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=df)
    assert dataframe_asset.dataframe.equals(df)


@pytest.mark.integration
def test_pandas_data_adding_dataframe_in_file_reloaded_context(
    empty_file_context: FileDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})

    context = empty_file_context

    datasource = context.sources.add_or_update_pandas(name="fluent_pandas_datasource")
    dataframe_asset = datasource.add_dataframe_asset(name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=df)
    assert dataframe_asset.dataframe.equals(df)

    context = gx.get_context(context_root_dir=context.root_directory, cloud_mode=False)
    dataframe_asset = context.get_datasource(  # type: ignore[union-attr]
        datasource_name="fluent_pandas_datasource"
    ).get_asset(asset_name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=df)
    assert dataframe_asset.dataframe.equals(df)


@pytest.mark.integration
def test_spark_data_adding_dataframe_in_cloud_context(
    spark_session,
    spark_df_from_pandas_df,
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    spark_df = spark_df_from_pandas_df(spark_session, df)

    context = empty_cloud_context_fluent

    dataframe_asset = context.sources.add_or_update_spark(
        name="fluent_pandas_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=spark_df)
    assert dataframe_asset.dataframe.toPandas().equals(df)


@pytest.mark.integration
def test_spark_data_adding_dataframe_in_file_reloaded_context(
    spark_session,
    spark_df_from_pandas_df,
    empty_file_context: FileDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    spark_df = spark_df_from_pandas_df(spark_session, df)

    context = empty_file_context

    dataframe_asset = context.sources.add_or_update_spark(
        name="fluent_pandas_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=spark_df)
    assert dataframe_asset.dataframe.toPandas().equals(df)

    datasource = context.sources.add_or_update_spark(name="fluent_pandas_datasource")
    dataframe_asset = datasource.add_dataframe_asset(name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=spark_df)
    assert dataframe_asset.dataframe.toPandas().equals(df)

    context = gx.get_context(context_root_dir=context.root_directory, cloud_mode=False)
    dataframe_asset = context.get_datasource(  # type: ignore[union-attr]
        datasource_name="fluent_pandas_datasource"
    ).get_asset(asset_name="my_df_asset")
    _ = dataframe_asset.build_batch_request(dataframe=spark_df)
    assert dataframe_asset.dataframe.toPandas().equals(df)
