from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
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
    datasource = context.data_sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    new_datasource_name = f"ds{uuid.uuid4().hex}"
    datasource.name = new_datasource_name
    datasource = context.data_sources.add_or_update_pandas(
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
    datasource.delete_asset(name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(name=asset_name)


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
        batch_request = data_asset.build_batch_request(options={"dataframe": pandas_test_df})
    else:
        batch_request = data_asset.build_batch_request()
    return batch_request


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
) -> ExpectationSuite:
    """Add Expectations for the Data Assets defined in this module.
    Note: There is no need to test Expectation Suite CRUD.
    Those assertions can be found in the expectation_suite fixture.
    """
    expectation_suite.add_expectation_configuration(
        expectation_configuration=ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "string",
                "mostly": 1,
            },
        )
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


@pytest.mark.xfail(reason="Fails due to a V1 change in the Checkpoint shape.", strict=True)
@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
