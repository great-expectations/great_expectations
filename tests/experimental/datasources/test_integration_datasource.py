from __future__ import annotations

import pathlib

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
)
from tests.experimental.datasources.test_utils import (
    run_batch_head,
    run_checkpoint_and_data_doc,
    run_data_assistant_and_checkpoint,
    run_multibatch_data_assistant_and_checkpoint,
)


def pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    pandas_ds = context.sources.add_pandas(name="my_pandas")
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    return context, pandas_ds, asset, batch_request


def sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    db_file = (
        pathlib.Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    ).resolve()
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2019_01",
        )
        .add_year_and_month_splitter(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.get_batch_request({"year": 2019, "month": 1})
    return context, datasource, asset, batch_request


def spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    spark_ds = context.sources.add_spark(name="my_spark")
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    return context, spark_ds, asset, batch_request


def multibatch_pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    pandas_ds = context.sources.add_pandas(name="my_pandas")
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2020"})
    return context, pandas_ds, asset, batch_request


def multibatch_sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    db_file = (
        pathlib.Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata_sample_2020_all_months_combined.db"
    ).resolve()
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2020",
        )
        .add_year_and_month_splitter(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.get_batch_request({"year": 2020})
    return context, datasource, asset, batch_request


def multibatch_spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    spark_ds = context.sources.add_spark(name="my_spark")
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2020"})
    return context, spark_ds, asset, batch_request


@pytest.fixture(params=[pandas_data, sql_data, spark_data])
def datasource_test_data(
    empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(empty_data_context)


@pytest.fixture(
    params=[multibatch_pandas_data, multibatch_sql_data, multibatch_spark_data]
)
def multibatch_datasource_test_data(
    empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(empty_data_context)


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
