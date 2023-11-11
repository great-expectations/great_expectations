from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.compatibility import pyspark
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        Datasource,
        SparkDatasource,
    )
    from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset


@pytest.fixture(scope="module")
def spark_datasource(
    context: CloudDataContext,
    datasource: Datasource,
) -> SparkDatasource:
    datasource = context.sources.add_spark(
        name=datasource.name,
        persist=True,
    )
    datasource.persist = False
    datasource = context.sources.add_or_update_spark(datasource=datasource)  # type: ignore[call-arg]
    assert (
        datasource.persist is False
    ), "The datasource was not updated in the previous method call."
    datasource.persist = True
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.persist is True
    ), "The datasource was not updated in the previous method call."
    datasource.persist = False
    datasource_dict = datasource.dict()
    datasource = context.sources.add_or_update_spark(**datasource_dict)
    assert (
        datasource.persist is False
    ), "The datasource was not updated in the previous method call."
    datasource.persist = True
    datasource_dict = datasource.dict()
    _ = context.add_or_update_datasource(**datasource_dict)
    datasource = context.get_datasource(datasource_name=datasource.name)  # type: ignore[assignment]
    assert (
        datasource.persist is True
    ), "The datasource was not updated in the previous method call."
    return datasource


@pytest.fixture(scope="module")
def data_asset(
    spark_datasource: SparkDatasource,
    data_asset: DataAsset,
) -> DataFrameAsset:
    _ = spark_datasource.add_dataframe_asset(name=data_asset.name)
    return spark_datasource.get_asset(asset_name=data_asset.name)


@pytest.fixture(scope="module")
def batch_request(
    data_asset: DataFrameAsset,
    spark_session: pyspark.SparkSession,
    spark_df_from_pandas_df: Callable[
        [pyspark.SparkSession, pd.DataFrame], pyspark.DataFrame
    ],
    in_memory_batch_request_missing_dataframe_error_type: type[Exception],
) -> BatchRequest:
    with pytest.raises(in_memory_batch_request_missing_dataframe_error_type):
        data_asset.build_batch_request()
    pandas_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": [1, 2, 3, 4],
        },
    )
    spark_df: pyspark.DataFrame = spark_df_from_pandas_df(spark_session, pandas_df)
    return data_asset.build_batch_request(dataframe=spark_df)


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
) -> ExpectationSuite:
    """Test adding Expectations and updating the Expectation Suite for the Data Asset
    defined in this module. The package-level expectation_suite fixture handles add and delete.
    """
    expectation_suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "name",
                "mostly": 1,
            },
        )
    )
    _ = context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite.name
    )
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    return expectation_suite


@pytest.mark.cloud
def test_interactive_validator(
    context: CloudDataContext,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
):
    expectation_count = len(expectation_suite.expectations)
    expectation_suite_name = expectation_suite.expectation_suite_name
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    validator.head()
    validator.expect_column_values_to_not_be_null(
        column="id",
        mostly=1,
    )
    validator.save_expectation_suite()
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert len(expectation_suite.expectations) == expectation_count + 1


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.success is True
