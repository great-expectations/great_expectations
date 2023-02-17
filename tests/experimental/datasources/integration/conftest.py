from __future__ import annotations

import logging
import pathlib

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.experimental.datasources import SqliteDatasource
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
)

logger = logging.getLogger(__name__)


def pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    pandas_ds = context.sources.add_pandas_filesystem(
        name="my_pandas",
        base_directory=csv_path,
    )
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.build_batch_request({"year": "2019", "month": "01"})
    return context, pandas_ds, asset, batch_request


def sqlite_datasource(
    context: AbstractDataContext, db_filename: str
) -> SqliteDatasource:
    relative_path = pathlib.Path(
        "..",
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        db_filename,
    )
    db_file = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    return datasource


def sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2019_01",
        )
        .add_splitter_year_and_month(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.build_batch_request({"year": 2019, "month": 1})
    return context, datasource, asset, batch_request


def spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    spark_ds = context.sources.add_spark(
        name="my_spark",
        base_directory=csv_path,
    )
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
        header=True,
        infer_schema=True,
    )
    batch_request = asset.build_batch_request({"year": "2019", "month": "01"})
    return context, spark_ds, asset, batch_request


def multibatch_pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    pandas_ds = context.sources.add_pandas_filesystem(
        name="my_pandas",
        base_directory=csv_path,
    )
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.build_batch_request({"year": "2020"})
    return context, pandas_ds, asset, batch_request


def multibatch_sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    datasource = sqlite_datasource(
        context, "yellow_tripdata_sample_2020_all_months_combined.db"
    )
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2020",
        )
        .add_splitter_year_and_month(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.build_batch_request({"year": 2020})
    return context, datasource, asset, batch_request


def multibatch_spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    spark_ds = context.sources.add_spark(
        name="my_spark",
        base_directory=csv_path,
    )
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
        header=True,
        infer_schema=True,
    )
    batch_request = asset.build_batch_request({"year": "2020"})
    return context, spark_ds, asset, batch_request


@pytest.fixture(params=[pandas_data, sql_data, spark_data])
def datasource_test_data(
    test_backends, empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    if request.param.__name__ == "spark_data" and "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    return request.param(empty_data_context)


@pytest.fixture(
    params=[multibatch_pandas_data, multibatch_sql_data, multibatch_spark_data]
)
def multibatch_datasource_test_data(
    test_backends, empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    if (
        request.param.__name__ == "multibatch_spark_data"
        and "SparkDFDataset" not in test_backends
    ):
        pytest.skip("No spark backend selected.")

    return request.param(empty_data_context)
