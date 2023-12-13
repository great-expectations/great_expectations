from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Callable, Iterator

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration
from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.compatibility import pyspark
    from great_expectations.core import ExpectationSuite, ExpectationValidationResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        SparkDatasource,
    )
    from great_expectations.validator.validator import Validator


@pytest.fixture(scope="module")
def spark_test_df(
    spark_session: pyspark.SparkSession,
    spark_df_from_pandas_df: Callable[
        [pyspark.SparkSession, pd.DataFrame], pyspark.DataFrame
    ],
) -> pyspark.DataFrame:
    pandas_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": [1, 2, 3, 4],
        },
    )
    return spark_df_from_pandas_df(spark_session, pandas_df)


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
) -> SparkDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
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
    datasource = context.add_or_update_datasource(**datasource_dict)  # type: ignore[assignment]
    assert (
        datasource.persist is True
    ), "The datasource was not updated in the previous method call."
    return datasource


def dataframe_asset(
    datasource: SparkDatasource,
    asset_name: str,
) -> DataFrameAsset:
    return datasource.add_dataframe_asset(
        name=asset_name,
    )


@pytest.fixture(scope="module", params=[dataframe_asset])
def data_asset(
    datasource: SparkDatasource,
    get_missing_data_asset_error_type: type[Exception],
    request,
) -> Iterator[DataAsset]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"da_{uuid.uuid4().hex}"
    yield request.param(
        datasource=datasource,
        asset_name=asset_name,
    )
    datasource.delete_asset(asset_name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(asset_name=asset_name)


@pytest.fixture(scope="module")
def batch_request(
    data_asset: DataAsset,
    in_memory_batch_request_missing_dataframe_error_type: type[Exception],
    spark_test_df: pyspark.DataFrame,
) -> BatchRequest:
    """Build a BatchRequest depending on the types of Data Assets tested in the module."""
    if isinstance(data_asset, DataFrameAsset):
        with pytest.raises(in_memory_batch_request_missing_dataframe_error_type):
            data_asset.build_batch_request()
        batch_request = data_asset.build_batch_request(dataframe=spark_test_df)
    else:
        batch_request = data_asset.build_batch_request()
    return batch_request


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
) -> ExpectationSuite:
    """Add Expectations for the Data Assets defined in this module, and update the Expectation Suite.
    Note: There is no need to test Expectation Suite create, get, or delete in this module.
    Those assertions can be found in the expectation_suite fixture.
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
    expectation_suite = context.add_or_update_expectation_suite(
        expectation_suite=expectation_suite
    )
    return expectation_suite


@pytest.mark.cloud
def test_interactive_validator(
    context: CloudDataContext,
    validator: Validator,
):
    """Test interactive evaluation of the Data Assets in this module using an existing Validator.
    Note: There is no need to test getting a Validator or using Validator.head(). That is already
    tested in the validator fixture.
    """
    expectation_validation_result: ExpectationValidationResult = (
        validator.expect_column_values_to_not_be_null(
            column="id",
            mostly=1,
        )
    )
    assert expectation_validation_result.success


@pytest.mark.cloud
def test_checkpoint_run(
    checkpoint: Checkpoint,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success


@pytest.mark.cloud
def test_checkpoint_run_runtime_validations(
    context: CloudDataContext,
    checkpoint: Checkpoint,
    spark_test_df: pyspark.DataFrame,
):
    """This Checkpoint only has one in-memory validation configured.
    This means if we don't pass runtime validations we should get an error,
    because nothing is actually going to be validated.
    """
    # in-memory DataFrame referenced by BatchRequest isn't serialized in Checkpoint config
    # the Checkpoint from the fixture hasn't been round-tripped,
    # so it will work as long as it stays in memory
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success

    # destroy and retrieve the Data Context, losing the in-memory DataFrame
    del context
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    checkpoint: Checkpoint = context.get_checkpoint(name=checkpoint.name)
    # failure to pass runtime validations results in error
    with pytest.raises(RuntimeError):
        _ = checkpoint.run()
