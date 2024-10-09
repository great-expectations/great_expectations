from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable
from unittest import mock

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.core.partitioners import (
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    PartitionerColumnValue,
    PartitionerConvertedDatetime,
    PartitionerDatetimePart,
    PartitionerDividedInteger,
    PartitionerModInteger,
    PartitionerMultiColumnValue,
)
from great_expectations.data_context import (
    AbstractDataContext,
    CloudDataContext,
    EphemeralDataContext,
    FileDataContext,
)
from great_expectations.datasource.fluent import (
    BatchRequest,
)
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
)
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.validator.v1_validator import Validator
from tests.datasource.fluent.integration.conftest import sqlite_datasource
from tests.datasource.fluent.integration.integration_test_utils import (
    run_batch_head,
    run_checkpoint_and_data_doc,
)

if TYPE_CHECKING:
    from responses import RequestsMock

    from great_expectations.compatibility.pyspark import DataFrame as SparkDataFrame
    from great_expectations.compatibility.pyspark import SparkSession
    from great_expectations.datasource.fluent.pandas_datasource import (
        DataFrameAsset as PandasDataFrameAsset,
    )
    from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
    from great_expectations.datasource.fluent.spark_datasource import (
        DataFrameAsset as SparkDataFrameAsset,
    )
    from great_expectations.datasource.fluent.spark_datasource import SparkDatasource


# This is marked by the various backend used in testing in the datasource_test_data fixture.
def test_run_checkpoint_and_data_doc(
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
):
    run_checkpoint_and_data_doc(
        datasource_test_data=datasource_test_data,
    )


# This is marked by the various backend used in testing in the datasource_test_data fixture.
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
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
    fetch_all: bool | str,
    n_rows: int | float | str | None,  # noqa: PYI041
    success: bool,
) -> None:
    run_batch_head(
        datasource_test_data=datasource_test_data,
        fetch_all=fetch_all,
        n_rows=n_rows,
        success=success,
    )


@pytest.mark.sqlite
class TestQueryAssets:
    def test_success_with_partitioners(self, empty_data_context):
        context = empty_data_context
        datasource = sqlite_datasource(context, "yellow_tripdata.db")
        passenger_count_value = 5
        asset = datasource.add_query_asset(
            name="query_asset",
            query=f"   SELECT * from yellow_tripdata_sample_2019_02 WHERE passenger_count = {passenger_count_value}",  # noqa: E501
        )
        validator = context.get_validator(
            batch_request=asset.build_batch_request(
                options={"year": 2019},
                partitioner=ColumnPartitionerMonthly(column_name="pickup_datetime"),
            )
        )
        result = validator.expect_column_distinct_values_to_equal_set(
            column="passenger_count",
            value_set=[passenger_count_value],
            result_format={"result_format": "BOOLEAN_ONLY"},
        )
        assert result.success

    def test_partitioner_filtering(self, empty_data_context):
        context = empty_data_context
        datasource = sqlite_datasource(context, "../../test_cases_for_sql_data_connector.db")

        asset = datasource.add_query_asset(
            name="trip_asset_partition_by_event_type",
            query="SELECT * FROM table_partitioned_by_date_column__A",
        )
        batch_request = asset.build_batch_request(
            options={"event_type": "start"},
            partitioner=PartitionerColumnValue(column_name="event_type"),
        )
        validator = context.get_validator(batch_request=batch_request)

        # All rows returned by head have the start event_type.
        result = validator.execution_engine.batch_manager.active_batch.head(n_rows=50)
        unique_event_types = set(result.data["event_type"].unique())
        print(f"{unique_event_types=}")
        assert unique_event_types == {"start"}


@pytest.mark.sqlite
@pytest.mark.parametrize(
    [
        "database",
        "table_name",
        "partitioner_class",
        "partitioner_kwargs",
        "all_batches_cnt",
        "specified_batch_request",
        "specified_batch_cnt",
        "last_specified_batch_metadata",
    ],
    [
        pytest.param(
            "yellow_tripdata_sample_2020_all_months_combined.db",
            "yellow_tripdata_sample_2020",
            ColumnPartitionerYearly,
            {"column_name": "pickup_datetime"},
            1,
            {"year": 2020},
            1,
            {"year": 2020},
            id="year",
        ),
        pytest.param(
            "yellow_tripdata_sample_2020_all_months_combined.db",
            "yellow_tripdata_sample_2020",
            ColumnPartitionerMonthly,
            {"column_name": "pickup_datetime"},
            12,
            {"year": 2020, "month": 6},
            1,
            {"year": 2020, "month": 6},
            id="year_and_month",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            ColumnPartitionerDaily,
            {"column_name": "pickup_datetime"},
            28,
            {"year": 2019, "month": 2, "day": 10},
            1,
            {"year": 2019, "month": 2, "day": 10},
            id="year_and_month_and_day",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerDatetimePart,
            {
                "column_name": "pickup_datetime",
                "datetime_parts": ["year", "month", "day"],
            },
            28,
            {"year": 2019, "month": 2},
            28,
            {"year": 2019, "month": 2, "day": 28},
            id="datetime_part",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerColumnValue,
            {"column_name": "passenger_count"},
            7,
            {"passenger_count": 3},
            1,
            {"passenger_count": 3},
            id="column_value",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerColumnValue,
            {"column_name": "pickup_datetime"},
            9977,
            {"pickup_datetime": "2019-02-07 15:48:06"},
            1,
            {"pickup_datetime": "2019-02-07 15:48:06"},
            id="column_value_datetime",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerDividedInteger,
            {"column_name": "passenger_count", "divisor": 3},
            3,
            {"quotient": 2},
            1,
            {"quotient": 2},
            id="divisor",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerModInteger,
            {"column_name": "passenger_count", "mod": 3},
            3,
            {"remainder": 2},
            1,
            {"remainder": 2},
            id="mod_integer",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerConvertedDatetime,
            {"column_name": "pickup_datetime", "date_format_string": "%Y-%m-%d"},
            28,
            {"datetime": "2019-02-23"},
            1,
            {"datetime": "2019-02-23"},
            id="converted_datetime",
        ),
        pytest.param(
            "yellow_tripdata.db",
            "yellow_tripdata_sample_2019_02",
            PartitionerMultiColumnValue,
            {"column_names": ["passenger_count", "payment_type"]},
            23,
            {"passenger_count": 1, "payment_type": 1},
            1,
            {"passenger_count": 1, "payment_type": 1},
            id="multi_column_values",
        ),
    ],
)
def test_partitioner(
    empty_data_context,
    database,
    table_name,
    partitioner_class,
    partitioner_kwargs,
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
    partitioner = partitioner_class(**partitioner_kwargs)
    # Test getting all batches
    all_batches = asset.get_batch_identifiers_list(
        asset.build_batch_request(partitioner=partitioner)
    )
    assert len(all_batches) == all_batches_cnt
    # Test getting specified batches
    specified_batches = asset.get_batch_identifiers_list(
        asset.build_batch_request(specified_batch_request, partitioner=partitioner)
    )
    assert len(specified_batches) == specified_batch_cnt
    assert specified_batches[-1] == last_specified_batch_metadata


@pytest.mark.sqlite
def test_partitioner_build_batch_request_allows_selecting_by_date_and_datetime_as_string(
    empty_data_context,
):
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata.db")

    asset = datasource.add_query_asset(
        "query_asset",
        "SELECT date(pickup_datetime) as pickup_date, passenger_count FROM yellow_tripdata_sample_2019_02",  # noqa: E501
    )
    partitioner = PartitionerColumnValue(column_name="pickup_date")
    # Test getting all batches
    all_batches = asset.get_batch_identifiers_list(
        asset.build_batch_request(partitioner=partitioner)
    )
    assert len(all_batches) == 28

    with mock.patch(
        "great_expectations.datasource.fluent.sql_datasource._partitioner_and_sql_asset_to_batch_identifier_data"
    ) as mock_batch_identifiers:
        mock_batch_identifiers.return_value = [
            {"pickup_date": datetime.date(2019, 2, 1)},
            {"pickup_date": datetime.date(2019, 2, 2)},
        ]
        specified_batches = asset.get_batch_identifiers_list(
            asset.build_batch_request(
                options={"pickup_date": "2019-02-01"}, partitioner=partitioner
            )
        )
        assert len(specified_batches) == 1

    with mock.patch(
        "great_expectations.datasource.fluent.sql_datasource._partitioner_and_sql_asset_to_batch_identifier_data"
    ) as mock_batch_identifiers:
        mock_batch_identifiers.return_value = [
            {"pickup_date": datetime.datetime(2019, 2, 1)},  # noqa: DTZ001
            {"pickup_date": datetime.datetime(2019, 2, 2)},  # noqa: DTZ001
        ]
        specified_batches = asset.get_batch_identifiers_list(
            asset.build_batch_request(
                options={"pickup_date": "2019-02-01 00:00:00"}, partitioner=partitioner
            )
        )
        assert len(specified_batches) == 1


@pytest.mark.parametrize(
    ["month", "expected"],
    [
        (1, 364),
        (2, 342),
    ],
)
@pytest.mark.sqlite
def test_success_with_partitioners_from_batch_definitions(
    empty_data_context,
    month: int,
    expected: int,
):
    """Integration test to ensure partitions from batch configs are used.

    The test is parameterized just to ensure that the partitioner is actually doing something.
    """
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata_sample_2020_all_months_combined.db")
    passenger_count_value = 5
    asset = datasource.add_query_asset(
        name="query_asset",
        query=f"SELECT * from yellow_tripdata_sample_2020 WHERE passenger_count = {passenger_count_value}",  # noqa: E501
    )
    batch_definition = asset.add_batch_definition(
        name="whatevs",
        partitioner=ColumnPartitionerMonthly(column_name="pickup_datetime"),
    )
    validator = Validator(
        batch_definition=batch_definition,
        batch_parameters={"year": 2020, "month": month},
    )
    result = validator.validate_expectation(gxe.ExpectTableRowCountToEqual(value=expected))
    assert result.success


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
@pytest.mark.sqlite
def test_asset_specified_metadata(empty_data_context, add_asset_method, add_asset_kwarg):
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    asset_specified_metadata = {"pipeline_name": "my_pipeline"}
    asset = getattr(datasource, add_asset_method)(
        name="asset",
        batch_metadata=asset_specified_metadata,
        **add_asset_kwarg,
    )
    partitioner = ColumnPartitionerMonthly(column_name="pickup_datetime")
    # Test getting all batches
    batch = asset.get_batch(asset.build_batch_request(partitioner=partitioner))
    # Update the batch_metadata from the request with the metadata inherited from the asset
    assert batch.metadata == {**asset_specified_metadata, "year": 2019, "month": 2}


# This is marked by the various backend used in testing in the datasource_test_data fixture.
def test_batch_request_error_messages(
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
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


@pytest.mark.cloud
def test_pandas_data_adding_dataframe_in_cloud_context(
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})

    context = empty_cloud_context_fluent

    dataframe_asset: PandasDataFrameAsset = context.data_sources.add_or_update_pandas(
        name="fluent_pandas_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    batch_def = dataframe_asset.add_batch_definition_whole_dataframe(name="bd")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    assert isinstance(batch.data, PandasBatchData)
    assert batch.data.dataframe.equals(df)


@pytest.mark.filesystem
def test_pandas_data_adding_dataframe_in_file_reloaded_context(
    empty_file_context: FileDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})

    context = empty_file_context

    datasource = context.data_sources.add_or_update_pandas(name="fluent_pandas_datasource")
    dataframe_asset: PandasDataFrameAsset = datasource.add_dataframe_asset(name="my_df_asset")
    batch_def = dataframe_asset.add_batch_definition_whole_dataframe(name="bd")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    assert isinstance(batch.data, PandasBatchData)
    assert batch.data.dataframe.equals(df)

    # Reload the asset and see that we can re-add the df to the batch definition
    context = gx.get_context(context_root_dir=context.root_directory, cloud_mode=False)
    dataframe_asset = context.data_sources.get(name="fluent_pandas_datasource").get_asset(
        name="my_df_asset"
    )
    reloaded_batch_def = dataframe_asset.get_batch_definition(name="bd")
    batch = reloaded_batch_def.get_batch(batch_parameters={"dataframe": df})
    assert isinstance(batch.data, PandasBatchData)
    assert batch.data.dataframe.equals(df)


@pytest.mark.spark
def test_spark_data_adding_dataframe_in_cloud_context(
    spark_session,
    spark_df_from_pandas_df,
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    spark_df = spark_df_from_pandas_df(spark_session, df)

    context = empty_cloud_context_fluent

    dataframe_asset: SparkDataFrameAsset = context.data_sources.add_or_update_spark(
        name="fluent_spark_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    batch_def = dataframe_asset.add_batch_definition_whole_dataframe(name="bd")
    batch = batch_def.get_batch(batch_parameters={"dataframe": spark_df})
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.toPandas().equals(df)


@pytest.mark.spark
def test_spark_data_adding_dataframe_in_file_reloaded_context(
    spark_session,
    spark_df_from_pandas_df,
    empty_file_context: FileDataContext,
):
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    spark_df = spark_df_from_pandas_df(spark_session, df)

    context = empty_file_context

    dataframe_asset: SparkDataFrameAsset = context.data_sources.add_or_update_spark(
        name="fluent_spark_datasource"
    ).add_dataframe_asset(name="my_df_asset")
    batch_def = dataframe_asset.add_batch_definition_whole_dataframe(name="bd")
    batch = batch_def.get_batch(batch_parameters={"dataframe": spark_df})
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.toPandas().equals(df)

    context = gx.get_context(context_root_dir=context.root_directory, cloud_mode=False)
    retrieved_bd = (
        context.data_sources.get(name="fluent_spark_datasource")
        .get_asset(name="my_df_asset")
        .get_batch_definition(name="bd")
    )
    new_batch = retrieved_bd.get_batch(batch_parameters={"dataframe": spark_df})
    assert isinstance(new_batch.data, SparkDFBatchData)
    assert new_batch.data.dataframe.toPandas().equals(df)


@dataclass
class PandasDataSourceAndFrame:
    datasource: PandasDatasource
    dataframe: pd.DataFrame


@dataclass
class SparkDataSourceAndFrame:
    datasource: SparkDatasource
    dataframe: SparkDataFrame


def _validate_whole_dataframe_batch(
    source_and_frame: PandasDataSourceAndFrame | SparkDataSourceAndFrame,
):
    my_expectation = gxe.ExpectColumnMeanToBeBetween(
        column="column_name", min_value=2.5, max_value=3.5
    )
    asset = source_and_frame.datasource.add_dataframe_asset(name="asset")
    bd = asset.add_batch_definition_whole_dataframe(name="bd")
    batch = bd.get_batch(batch_parameters={"dataframe": source_and_frame.dataframe})
    result = batch.validate(my_expectation)
    assert result.success


@pytest.mark.unit
def test_validate_pandas_batch():
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name="ds")
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    _validate_whole_dataframe_batch(PandasDataSourceAndFrame(datasource=datasource, dataframe=df))


@pytest.mark.spark
def test_validate_spark_batch(
    spark_session: SparkSession,
    spark_df_from_pandas_df: Callable[[SparkSession, pd.DataFrame], SparkDataFrame],
):
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name="ds")
    spark_df = spark_df_from_pandas_df(
        spark_session, pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    )
    _validate_whole_dataframe_batch(
        SparkDataSourceAndFrame(datasource=datasource, dataframe=spark_df)
    )


@dataclass
class ContextPandasDataSourceAndFrame:
    context: EphemeralDataContext
    datasource: PandasDatasource
    dataframe: pd.DataFrame


@dataclass
class ContextSparkDataSourceAndFrame:
    context: EphemeralDataContext
    datasource: SparkDatasource
    dataframe: SparkDataFrame


def _validate_whole_dataframe_batch_definition(
    context_source_frame: ContextPandasDataSourceAndFrame | ContextSparkDataSourceAndFrame,
):
    asset = context_source_frame.datasource.add_dataframe_asset(name="asset")
    bd = asset.add_batch_definition_whole_dataframe(name="bd")
    suite = context_source_frame.context.suites.add(gx.ExpectationSuite(name="suite"))
    suite.add_expectation(
        gxe.ExpectColumnMeanToBeBetween(column="column_name", min_value=2.5, max_value=3.5)
    )
    validation_def = context_source_frame.context.validation_definitions.add(
        gx.ValidationDefinition(
            name="vd",
            data=bd,
            suite=suite,
        )
    )
    result = validation_def.run(batch_parameters={"dataframe": context_source_frame.dataframe})
    assert result.success


@pytest.mark.unit
def test_validate_pandas_batch_definition():
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name="ds")
    df = pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    _validate_whole_dataframe_batch_definition(
        ContextPandasDataSourceAndFrame(context=context, datasource=datasource, dataframe=df)
    )


@pytest.mark.spark
def test_validate_spark_batch_definition(
    spark_session: SparkSession,
    spark_df_from_pandas_df: Callable[[SparkSession, pd.DataFrame], SparkDataFrame],
):
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name="ds")
    spark_df = spark_df_from_pandas_df(
        spark_session, pd.DataFrame({"column_name": [1, 2, 3, 4, 5]})
    )
    _validate_whole_dataframe_batch_definition(
        ContextSparkDataSourceAndFrame(context=context, datasource=datasource, dataframe=spark_df)
    )
