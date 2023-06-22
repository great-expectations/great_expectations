from __future__ import annotations

import logging
import pathlib

import numpy as np
import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import (
    BatchRequest,
    PandasFilesystemDatasource,
    SparkFilesystemDatasource,
    SqliteDatasource,
)
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
)

logger = logging.getLogger(__name__)


def default_pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    pandas_ds = context.sources.pandas_default
    pandas_ds.read_csv(
        filepath_or_buffer=csv_path / "yellow_tripdata_sample_2019-02.csv",
    )
    asset = pandas_ds.get_asset(asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME)
    batch_request = asset.build_batch_request()
    return context, pandas_ds, asset, batch_request


def pandas_sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    passenger_count = np.repeat([1, 1, 1, 2, 6], 2000)
    df = pd.DataFrame(
        data={
            "passenger_count": passenger_count,
        }
    )
    con = sa.create_engine("sqlite://")
    add_dataframe_to_db(df=df, name="my_table", con=con)
    pandas_ds = context.sources.add_pandas("my_pandas")
    pandas_ds.read_sql(
        sql=sa.text("SELECT * FROM my_table"),
        con=con,
    )
    asset = pandas_ds.get_asset(asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME)
    batch_request = asset.build_batch_request()
    return context, pandas_ds, asset, batch_request


def pandas_filesystem_datasource(
    context: AbstractDataContext,
) -> PandasFilesystemDatasource:
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
    return pandas_ds


def pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, PandasFilesystemDatasource, DataAsset, BatchRequest]:
    context.config_variables.update({"pipeline_filename": __file__})
    pandas_ds = pandas_filesystem_datasource(context=context)
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
        batch_metadata={"my_pipeline": "${pipeline_filename}"},
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


def spark_filesystem_datasource(
    context: AbstractDataContext,
) -> SparkFilesystemDatasource:
    relative_path = pathlib.Path(
        "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    spark_ds = context.sources.add_spark_filesystem(
        name="my_spark",
        base_directory=csv_path,
    )
    return spark_ds


def spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, SparkFilesystemDatasource, DataAsset, BatchRequest]:
    spark_ds = spark_filesystem_datasource(context=context)
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        header=True,
        infer_schema=True,
        order_by=["year", "month"],
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
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
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
    spark_ds = context.sources.add_spark_filesystem(
        name="my_spark",
        base_directory=csv_path,
    )
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        header=True,
        infer_schema=True,
        order_by=["year", "month"],
    )
    batch_request = asset.build_batch_request({"year": "2020"})
    return context, spark_ds, asset, batch_request


@pytest.fixture(
    params=[pandas_data, sql_data, spark_data, default_pandas_data, pandas_sql_data]
)
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


@pytest.fixture(params=[pandas_filesystem_datasource, spark_filesystem_datasource])
def filesystem_datasource(test_backends, empty_data_context, request) -> Datasource:
    if request.param.__name__ == "spark_data" and "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    return request.param(empty_data_context)
