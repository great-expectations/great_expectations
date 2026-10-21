from __future__ import annotations

import logging
import pathlib
import uuid
from typing import TYPE_CHECKING, Final

import numpy as np
import pandas as pd
import pytest

import great_expectations as gx
from great_expectations import ValidationDefinition, get_context
from great_expectations.checkpoint import Checkpoint
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core import ExpectationSuite
from great_expectations.core.partitioners import (
    ColumnPartitionerMonthly,
)
from great_expectations.data_context import (
    AbstractDataContext,
    EphemeralDataContext,
    FileDataContext,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
)
from great_expectations.execution_engine.sqlalchemy_engine_lifecycle import (
    close_connections_when_collected,
)
from great_expectations.expectations import ExpectColumnValuesToNotBeNull
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
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

logger = logging.getLogger(__name__)

TEST_TABLE_NAME: Final[str] = "test_table"


@pytest.fixture
def file_dc_config_dir_init(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Initialize an regular/old-style FileDataContext project config directory.
    Removed on teardown.
    """
    gx_yml = tmp_path / FileDataContext.GX_DIR / FileDataContext.GX_YML
    assert gx_yml.exists() is False
    gx.get_context(mode="file", project_root_dir=tmp_path)
    assert gx_yml.exists()

    tmp_gx_dir = gx_yml.parent.absolute()
    logger.info(f"tmp_gx_dir -> {tmp_gx_dir}")
    return tmp_gx_dir


@pytest.fixture
def empty_file_context(file_dc_config_dir_init) -> FileDataContext:
    context = gx.get_context(context_root_dir=file_dc_config_dir_init, cloud_mode=False)
    return context


def default_pandas_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    pandas_ds = context.data_sources.pandas_default
    pandas_ds.read_csv(
        filepath_or_buffer=csv_path / "yellow_tripdata_sample_2019-02.csv",
    )
    asset = pandas_ds.get_asset(name=DEFAULT_PANDAS_DATA_ASSET_NAME)
    batch_request = asset.build_batch_request()
    return context, pandas_ds, asset, batch_request


def pandas_sql_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    passenger_count = np.repeat([1, 1, 1, 2, 6], 2000)
    df = pd.DataFrame(
        data={
            "passenger_count": passenger_count,
        }
    )
    # The asset reads through this engine for as long as the datasource lives.
    con = close_connections_when_collected(sa.create_engine("sqlite://"))
    add_dataframe_to_db(df=df, name="my_table", con=con)
    pandas_ds = context.data_sources.add_pandas("my_pandas")
    pandas_ds.read_sql(
        sql=sa.text("SELECT * FROM my_table"),
        con=con,
    )
    asset = pandas_ds.get_asset(name=DEFAULT_PANDAS_DATA_ASSET_NAME)
    batch_request = asset.build_batch_request()
    return context, pandas_ds, asset, batch_request


def pandas_filesystem_datasource(
    test_backends,
    context: AbstractDataContext,
) -> PandasFilesystemDatasource:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "first_3_files",
    )
    csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    pandas_ds = context.data_sources.add_pandas_filesystem(
        name="my_pandas",
        base_directory=csv_path,
    )
    return pandas_ds


def pandas_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, PandasFilesystemDatasource, DataAsset, BatchRequest]:
    context.config_variables.update({"pipeline_filename": __file__})
    pandas_ds = pandas_filesystem_datasource(test_backends=test_backends, context=context)
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        batch_metadata={"my_pipeline": "${pipeline_filename}"},
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="monthly_batch_def", regex=batching_regex)
    batch_request = batch_def.build_batch_request(batch_parameters={"year": "2019", "month": "01"})
    return context, pandas_ds, asset, batch_request


def sqlite_datasource(
    context: AbstractDataContext, db_filename: str | pathlib.Path
) -> SqliteDatasource:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        db_filename,
    )
    db_file = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    datasource = context.data_sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
        # don't set `create_temp_table` so that we can test the default behavior
    )
    return datasource


def sql_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    asset = datasource.add_table_asset(
        name="my_asset",
        table_name="yellow_tripdata_sample_2019_01",
    )
    batch_request = asset.build_batch_request(
        options={"year": 2019, "month": 1},
        partitioner=ColumnPartitionerMonthly(column_name="pickup_datetime"),
    )
    return context, datasource, asset, batch_request


def spark_filesystem_datasource(
    test_backends,
    context: AbstractDataContext,
) -> SparkFilesystemDatasource:
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    relative_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    spark_ds = context.data_sources.add_spark_filesystem(
        name="my_spark",
        base_directory=csv_path,
    )
    return spark_ds


def spark_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, SparkFilesystemDatasource, DataAsset, BatchRequest]:
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    spark_ds = spark_filesystem_datasource(test_backends=test_backends, context=context)
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_definition = asset.add_batch_definition_monthly("my_batch_def", regex=batching_regex)
    batch_request = batch_definition.build_batch_request({"year": "2019", "month": "01"})
    return context, spark_ds, asset, batch_request


def multibatch_pandas_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    relative_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    pandas_ds = context.data_sources.add_pandas_filesystem(
        name="my_pandas",
        base_directory=csv_path,
    )
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_definition = asset.add_batch_definition_monthly("monthly_batch_def", regex=batching_regex)
    batch_request = batch_definition.build_batch_request({"year": "2020"})
    return context, pandas_ds, asset, batch_request


def multibatch_sql_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    datasource = sqlite_datasource(context, "yellow_tripdata_sample_2020_all_months_combined.db")
    asset = datasource.add_table_asset(
        name="my_asset",
        table_name="yellow_tripdata_sample_2020",
    )
    batch_request = asset.build_batch_request(
        options={"year": 2020},
        partitioner=ColumnPartitionerMonthly(column_name="pickup_datetime"),
    )
    return context, datasource, asset, batch_request


def multibatch_spark_data(
    test_backends,
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    relative_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    csv_path = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    spark_ds = context.data_sources.add_spark_filesystem(
        name="my_spark",
        base_directory=csv_path,
    )
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_definition = asset.add_batch_definition_monthly("monthly_batch_def", regex=batching_regex)
    batch_request = batch_definition.build_batch_request({"year": "2020"})
    return context, spark_ds, asset, batch_request


@pytest.fixture(
    params=[
        pytest.param(pandas_data, marks=pytest.mark.filesystem),
        pytest.param(sql_data, marks=pytest.mark.sqlite),
        pytest.param(spark_data, marks=pytest.mark.spark),
        pytest.param(default_pandas_data, marks=pytest.mark.filesystem),
        pytest.param(pandas_sql_data, marks=pytest.mark.filesystem),
    ]
)
def datasource_test_data(
    test_backends,
    empty_data_context,
    request,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(test_backends=test_backends, context=empty_data_context)


@pytest.fixture(
    params=[
        pytest.param(multibatch_pandas_data, marks=pytest.mark.filesystem),
        pytest.param(multibatch_sql_data, marks=pytest.mark.sqlite),
        pytest.param(multibatch_spark_data, marks=pytest.mark.spark),
    ]
)
def multibatch_datasource_test_data(
    test_backends, empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(test_backends=test_backends, context=empty_data_context)


@pytest.fixture(params=[pandas_filesystem_datasource, spark_filesystem_datasource])
def filesystem_datasource(test_backends, empty_data_context, request) -> Datasource:
    if request.param.__name__ == "spark_data" and "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    return request.param(test_backends, empty_data_context)


@pytest.fixture
def context() -> EphemeralDataContext:
    """Return an ephemeral data context for testing."""
    ctx = get_context(mode="ephemeral")
    assert isinstance(ctx, EphemeralDataContext)
    return ctx


def _run_checkpoint_test(batch_for_datasource, datasource_type: str) -> None:
    """Helper function to run checkpoint validation test"""
    context = batch_for_datasource.datasource.data_context
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name=f"{datasource_type}_es_{uuid.uuid4().hex}",
            expectations=[ExpectColumnValuesToNotBeNull(column="test_column", mostly=1)],
        )
    )
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name=f"{datasource_type}_val_def_{uuid.uuid4().hex}",
            data=batch_for_datasource.data_asset.batch_definitions[0],
            suite=expectation_suite,
        )
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name=f"{datasource_type.title()} Test Checkpoint {uuid.uuid4().hex}",
            validation_definitions=[validation_definition],
        )
    )
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success


def _run_column_expectation_test(
    batch_for_datasource, datasource_type: str, column_name: str
) -> None:
    """Helper function to run column expectation validation test"""
    context = batch_for_datasource.datasource.data_context
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name=f"{datasource_type}_column_es_{uuid.uuid4().hex}",
        )
    )
    expectation_suite.add_expectation_configuration(
        expectation_configuration=ExpectationConfiguration(
            type="expect_column_values_to_match_regex",
            kwargs={"column": column_name, "regex": r".*"},
        )
    )
    expectation_suite.save()
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name=f"{datasource_type}_column_val_def_{uuid.uuid4().hex}",
            data=batch_for_datasource.data_asset.batch_definitions[0],
            suite=expectation_suite,
        )
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name=f"{datasource_type.title()} Column Test Checkpoint {uuid.uuid4().hex}",
            validation_definitions=[validation_definition],
        )
    )
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
