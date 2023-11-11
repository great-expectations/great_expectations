from __future__ import annotations

import pathlib
import uuid
from typing import TYPE_CHECKING, Iterator

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.datasource.data_connector.util import normalize_directory_path

if TYPE_CHECKING:
    import py

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


@pytest.fixture(scope="module")
def base_dir(tmp_dir: py.path) -> pathlib.Path:
    dir_path = tmp_dir / "data"
    dir_path.mkdir()
    df = pd.DataFrame(
        {"name": [1, 2, 3, 4], "id": ["one", "two", "three", "four"]},
    )
    csv_path = dir_path / "data.csv"
    df.to_csv(csv_path)
    return dir_path


@pytest.fixture(scope="module")
def updated_base_dir(tmp_dir: py.path) -> pathlib.Path:
    dir_path = tmp_dir / "other_data"
    dir_path.mkdir()
    df = pd.DataFrame(
        {"name": [1, 2, 3, 4], "id": ["one", "two", "three", "four"]},
    )
    csv_path = dir_path / "data.csv"
    df.to_csv(csv_path)
    return dir_path


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    base_dir: pathlib.Path,
    updated_base_dir: pathlib.Path,
    get_missing_datasource_error_type: type[Exception],
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
    context.delete_datasource(datasource_name=datasource_name)
    with pytest.raises(get_missing_datasource_error_type):
        context.get_datasource(datasource_name=datasource_name)


@pytest.fixture(scope="module")
def data_asset(
    datasource: SparkFilesystemDatasource,
    get_missing_data_asset_error_type: type[Exception],
) -> Iterator[CSVAsset]:
    asset_name = f"i{uuid.uuid4().hex}"

    _ = datasource.add_csv_asset(name=asset_name, header=True, infer_schema=True)
    csv_asset = datasource.get_asset(asset_name=asset_name)
    yield csv_asset
    datasource.delete_asset(asset_name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(asset_name=asset_name)


@pytest.fixture(scope="module")
def batch_request(data_asset: CSVAsset) -> BatchRequest:
    return data_asset.build_batch_request()


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    data_asset: CSVAsset,
    get_missing_expectation_suite_error_type: type[Exception],
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
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    yield expectation_suite
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
    with pytest.raises(get_missing_expectation_suite_error_type):
        context.get_expectation_suite(expectation_suite_name=expectation_suite_name)


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
    data_asset: CSVAsset,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
    get_missing_checkpoint_error_type: type[Exception],
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
    assert (
        len(checkpoint.validations) == 1
    ), "Checkpoint was not updated in the previous method call."
    yield checkpoint
    # PP-691: this is a bug
    # you should only have to pass name
    context.delete_checkpoint(
        # name=checkpoint_name,
        id=checkpoint.ge_cloud_id,
    )
    with pytest.raises(get_missing_checkpoint_error_type):
        context.get_checkpoint(name=checkpoint_name)


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
