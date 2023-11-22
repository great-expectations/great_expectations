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
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core import ExpectationSuite, ExpectationValidationResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        SparkFilesystemDatasource,
    )
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
    )
    from great_expectations.validator.validator import Validator


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
    return dir_path


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
    base_dir: pathlib.Path,
    updated_base_dir: pathlib.Path,
) -> SparkFilesystemDatasource:
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
    return datasource


def csv_asset(
    datasource: SparkFilesystemDatasource,
    asset_name: str,
) -> CSVAsset:
    return datasource.add_csv_asset(name=asset_name, header=True, infer_schema=True)


@pytest.fixture(scope="module", params=[csv_asset])
def data_asset(
    datasource: SparkFilesystemDatasource,
    request,
) -> Iterator[DataAsset]:
    asset_name = f"da_{uuid.uuid4().hex}"
    yield request.param(
        datasource=datasource,
        asset_name=asset_name,
    )
    datasource.delete_asset(asset_name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(asset_name=asset_name)


@pytest.fixture(scope="module")
def batch_request(data_asset: DataAsset) -> BatchRequest:
    """Build a BatchRequest depending on the types of Data Assets tested in the module."""
    return data_asset.build_batch_request()


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
) -> ExpectationSuite:
    """Add Expectations for the Data Assets defined in this module."""
    expectation_suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "name",
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
    expectation_validation_result: ExpectationValidationResult = (
        validator.expect_column_mean_to_be_between(
            column="name",
            min_value=0,
            max_value=4,
        )
    )
    assert expectation_validation_result.success


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
