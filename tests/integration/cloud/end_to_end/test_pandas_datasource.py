from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core import ExpectationSuite, ExpectationValidationResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        PandasDatasource,
    )
    from great_expectations.validator.validator import Validator


@pytest.fixture(scope="module")
def pandas_test_df() -> pd.DataFrame:
    d = {
        "string": ["a", "b", "c"],
        "datetime": [
            pd.to_datetime("2020-01-01"),
            pd.to_datetime("2020-01-02"),
            pd.to_datetime("2020-01-03"),
        ],
    }
    df = pd.DataFrame(data=d)
    return df


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
) -> PandasDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
    datasource = context.sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    new_datasource_name = f"ds{uuid.uuid4().hex}"
    datasource.name = new_datasource_name
    datasource = context.sources.add_or_update_pandas(
        datasource=datasource,
    )
    assert (
        datasource.name == new_datasource_name
    ), "The datasource was not updated in the previous method call."
    datasource.name = datasource_name
    datasource = context.add_or_update_datasource(  # type: ignore[assignment]
        datasource=datasource,
    )
    assert (
        datasource.name == datasource_name
    ), "The datasource was not updated in the previous method call."
    return datasource


def dataframe_asset(
    datasource: PandasDatasource,
    asset_name: str,
) -> DataFrameAsset:
    return datasource.add_dataframe_asset(name=asset_name)


@pytest.fixture(scope="module", params=[dataframe_asset])
def data_asset(
    datasource: PandasDatasource,
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
    pandas_test_df: pd.DataFrame,
    in_memory_batch_request_missing_dataframe_error_type: type[Exception],
) -> BatchRequest:
    """Build a BatchRequest depending on the types of Data Assets tested in the module."""
    if isinstance(data_asset, DataFrameAsset):
        with pytest.raises(in_memory_batch_request_missing_dataframe_error_type):
            data_asset.build_batch_request()
        batch_request = data_asset.build_batch_request(dataframe=pandas_test_df)
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
                "column": "string",
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
            column="datetime",
            mostly=1,
        )
    )
    assert expectation_validation_result.success


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success


@pytest.fixture(scope="module", params=[dataframe_asset])
def in_memory_asset(
    datasource: PandasDatasource,
    request,
) -> DataFrameAsset:
    asset_name = f"da_{uuid.uuid4().hex}"
    return request.param(
        datasource=datasource,
        asset_name=asset_name,
    )


@pytest.mark.cloud
def test_checkpoint_run_in_memory_runtime_validations(
    cloud_base_url: str,
    cloud_organization_id: str,
    cloud_access_token: str,
    context: CloudDataContext,
    in_memory_asset: DataFrameAsset,
    pandas_test_df: pd.DataFrame,
    datasource: PandasDatasource,
    expectation_suite: ExpectationSuite,
):
    """This Checkpoint only has one in-memory validation configured.
    This means with a deserialized Checkpoint, if we don't pass runtime validations,
    we should get an error because nothing is there to be validated.
    """
    batch_request = in_memory_asset.build_batch_request(dataframe=pandas_test_df)
    checkpoint_name = (
        f"{in_memory_asset.name} | {expectation_suite.expectation_suite_name}"
    )
    validations = [
        {
            "expectation_suite_name": expectation_suite.expectation_suite_name,
            "batch_request": batch_request,
        },
    ]
    checkpoint: Checkpoint = context.add_checkpoint(
        name=checkpoint_name,
        validations=validations,
    )

    # the Checkpoint from the fixture hasn't been round-tripped,
    # so it will work as long as everything stays in memory
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success

    # re-retrieve the Data Context, losing the in-memory DataFrame
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=cloud_base_url,
        cloud_organization_id=cloud_organization_id,
        cloud_access_token=cloud_access_token,
    )
    checkpoint = context.get_checkpoint(name=checkpoint.name)
    # failure to pass runtime validations results in error
    with pytest.raises(RuntimeError):
        _ = checkpoint.run()

    # now that the Data Context is unaware of the Dataframe,
    # we need to re-associate the DataFrame with the DataFrameAsset
    # one way to do this is to assign to the attribute directly
    # the fixtures came from the old Data Context
    # we have to get them again, because they exist in a new place in memory
    datasource = context.get_datasource(datasource_name=datasource.name)
    in_memory_asset = datasource.get_asset(asset_name=in_memory_asset.name)
    in_memory_asset.dataframe = pandas_test_df
    checkpoint_result = checkpoint.run(validations=validations)
    assert checkpoint_result.success

    # building a new Batch Request also associates the DataFrame with the DataFrameAsset again
    # users might choose to pass this Batch Request as a runtime validation
    # although that step is not required
    datasource = context.get_datasource(datasource_name=datasource.name)
    in_memory_asset = datasource.get_asset(asset_name=in_memory_asset.name)
    assert in_memory_asset.dataframe is None
    batch_request = in_memory_asset.build_batch_request(dataframe=pandas_test_df)
    validations[0]["batch_request"] = batch_request
    checkpoint_result = checkpoint.run(validations=validations)
    assert checkpoint_result.success
    # ensure the runtime validation wasn't additive since it
    assert len(validations) == len(checkpoint_result.run_results)

    # clean up Checkpoint so associated entities can also be deleted in fixtures
    context.delete_checkpoint(name=checkpoint_name)
