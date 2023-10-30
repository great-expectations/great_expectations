from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest, PandasDatasource


@pytest.fixture
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


@pytest.fixture
def datasource(
    context: CloudDataContext,
) -> Iterator[PandasDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    yield datasource


@pytest.fixture
def data_asset(datasource: PandasDatasource) -> Iterator[DataFrameAsset]:
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_dataframe_asset(
        name=asset_name,
    )
    data_asset = datasource.get_asset(asset_name=asset_name)
    yield data_asset


@pytest.fixture
def batch_request(
    data_asset: DataFrameAsset, pandas_test_df: pd.DataFrame
) -> BatchRequest:
    return data_asset.build_batch_request(dataframe=pandas_test_df)


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
                "column": "string",
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
def test_datasource_crud(
    context: CloudDataContext,
):
    datasource_name = f"i{uuid.uuid4().hex}"
    # add_or_update
    datasource = context.sources.add_or_update_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name

    # delete
    _ = context.delete_datasource(datasource_name=datasource_name)

    # get after delete
    with pytest.raises(ValueError):
        context.get_datasource(datasource_name=datasource_name)


@pytest.mark.cloud
def test_dataasset_crud(
    context: CloudDataContext,
):
    # start with fresh datasource
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_or_update_pandas(
        name=datasource_name,
    )
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_dataframe_asset(
        name=asset_name,
    )

    # PP-692: this doesn't work due to a bug
    # calling delete_datasource() will fail with:
    # Datasource is used by Checkpoint <LONG HASH>
    # This is confirmed to be the default Checkpoint,
    # but error message is not specific enough to know without additional inspection
    # delete
    # datasource.delete_asset(asset_name=asset_name)
    # get after delete
    # with pytest.raises(ValueError):
    #    _ = datasource.get_asset(asset_name=asset_name)


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
        column="datetime",
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
