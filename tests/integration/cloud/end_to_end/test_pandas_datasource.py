from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

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
    return datasource.add_dataframe_asset(
        name=asset_name,
    )


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
    checkpoint: Checkpoint,
    reloaded_context: CloudDataContext,
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

    # re-retrieve the Data Context, losing the in-memory DataFrame
    checkpoint: Checkpoint = reloaded_context.get_checkpoint(name=checkpoint.name)
    # failure to pass runtime validations results in error
    with pytest.raises(RuntimeError):
        _ = checkpoint.run()
