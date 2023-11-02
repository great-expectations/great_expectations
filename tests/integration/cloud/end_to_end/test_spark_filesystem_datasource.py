from __future__ import annotations

import pathlib
import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.datasource.data_connector.util import normalize_directory_path

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        SparkFilesystemDatasource,
    )
    from great_expectations.datasource.fluent.pandas_file_path_datasource import (
        CSVAsset,
    )


@pytest.fixture
def base_dir(tmpdir) -> Iterator[pathlib.Path]:
    dir_path = tmpdir / "data"
    dir_path.mkdir()
    df = pd.DataFrame(
        {"name": [1, 2, 3, 4], "id": ["one", "two", "three", "four"]},
    )
    csv_path = dir_path / "data.csv"
    df.to_csv(csv_path)
    yield dir_path


@pytest.fixture
def updated_base_dir(tmpdir) -> Iterator[pathlib.Path]:
    dir_path = tmpdir / "other_data"
    dir_path.mkdir()
    df = pd.DataFrame(
        {"name": [1, 2, 3, 4], "id": ["one", "two", "three", "four"]},
    )
    csv_path = dir_path / "data.csv"
    df.to_csv(csv_path)
    yield dir_path


@pytest.fixture
def datasource(
    context: CloudDataContext,
    base_dir: pathlib.Path,
    updated_base_dir: pathlib.Path,
) -> Iterator[SparkFilesystemDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    original_base_dir = base_dir

    datasource = context.sources.add_spark_filesystem(
        name=datasource_name, base_directory=original_base_dir
    )

    datasource.base_directory = normalize_directory_path(
        updated_base_dir, context.root_directory
    )
    datasource = context.sources.add_or_update_spark_filesystem(datasource=datasource)
    assert (
        datasource.base_directory == updated_base_dir
    ), "The datasource was not updated in the previous method call."

    datasource.base_directory = normalize_directory_path(
        original_base_dir, context.root_directory
    )
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.base_directory == original_base_dir
    ), "The datasource was not updated in the previous method call."

    datasource = context.get_datasource(datasource_name=datasource_name)  # type: ignore[assignment]
    assert (
        datasource.base_directory == original_base_dir
    ), "The datasource was not updated in the previous method call."

    yield datasource
    # PP-692: this doesn't work due to a bug
    # calling delete_datasource() will fail with:
    # Datasource is used by Checkpoint <LONG HASH>
    # This is confirmed to be the default Checkpoint,
    # but error message is not specific enough to know without additional inspection
    # context.delete_datasource(datasource_name=datasource_name)


@pytest.fixture
def data_asset(datasource: SparkFilesystemDatasource) -> Iterator[CSVAsset]:
    asset_name = f"i{uuid.uuid4().hex}"

    _ = datasource.add_csv_asset(name=asset_name, header=True, infer_schema=True)
    csv_asset = datasource.get_asset(asset_name=asset_name)

    yield csv_asset
    # PP-692: this doesn't work due to a bug
    # calling delete_asset() will fail with:
    # Cannot perform action because Asset is used by Checkpoint:
    # end-to-end_pandas_filesystem_asset <SHORT HASH> - Default Checkpoint
    # datasource.delete_asset(asset_name=asset_name)


@pytest.fixture
def batch_request(data_asset: CSVAsset) -> BatchRequest:
    return data_asset.build_batch_request()


@pytest.fixture
def expectation_suite(
    context: CloudDataContext,
    data_asset: CSVAsset,
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
    data_asset: CSVAsset,
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

    print(validator.head())
    validator.expect_column_mean_to_be_between(column="name", min_value=0, max_value=4)
    validator.save_expectation_suite()
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert len(expectation_suite.expectations) == expectation_count + 1


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.success is True
