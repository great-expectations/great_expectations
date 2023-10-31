from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest, SparkDatasource
    from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset


@pytest.fixture
def datasource(
    context: CloudDataContext,
) -> Iterator[SparkDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_spark(
        name=datasource_name,
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
    datasource = context.get_datasource(datasource_name=datasource_name)  # type: ignore[assignment]
    assert (
        datasource.persist is True
    ), "The datasource was not updated in the previous method call."
    yield datasource
    # PP-692: this doesn't work due to a bug
    # calling delete_datasource() will fail with:
    # Datasource is used by Checkpoint <LONG HASH>
    # This is confirmed to be the default Checkpoint,
    # but error message is not specific enough to know without additional inspection
    # context.delete_datasource(datasource_name=datasource_name)


@pytest.fixture
def data_asset(datasource: SparkDatasource, table_factory) -> Iterator[DataFrameAsset]:
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_dataframe_asset(name=asset_name)
    dataframe_asset = datasource.get_asset(asset_name=asset_name)
    yield dataframe_asset
    # PP-692: this doesn't work due to a bug
    # calling delete_asset() will fail with:
    # Cannot perform action because Asset is used by Checkpoint:
    # end-to-end_snowflake_asset <SHORT HASH> - Default Checkpoint
    # datasource.delete_asset(asset_name=asset_name)


@pytest.fixture
def batch_request(
    data_asset: DataFrameAsset,
    spark_session,
    spark_df_from_pandas_df,
) -> BatchRequest:
    pandas_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": [1, 2, 3, 4],
        },
    )
    spark_df = spark_df_from_pandas_df(spark_session, pandas_df)
    return data_asset.build_batch_request(dataframe=spark_df)


@pytest.fixture
def expectation_suite(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
) -> Iterator[ExpectationSuite]:
    expectation_suite_name = f"{data_asset.datasource.name} | {data_asset.name}"
    expectation_suite = context.add_expectation_suite(
        expectation_suite_name=expectation_suite_name,
    )
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
        expectation_suite_name=expectation_suite_name
    )
    yield expectation_suite
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)


@pytest.fixture
def checkpoint(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
) -> Iterator[Checkpoint]:
    checkpoint_name = f"{data_asset.datasource.name} | {data_asset.name}"
    _ = context.add_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            },
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            },
        ],
    )
    _ = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            }
        ],
    )
    checkpoint = context.get_checkpoint(name=checkpoint_name)
    yield checkpoint
    # PP-691: this is a bug
    # you should only have to pass name
    context.delete_checkpoint(
        # name=checkpoint_name,
        id=checkpoint.ge_cloud_id,
    )


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
