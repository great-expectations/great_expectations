from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Generator

import pytest

from great_expectations import ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.data_connector.filesystem_data_connector import (
    normalize_directory_path,
)

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        SparkFilesystemDatasource,
    )
    from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import CSVAsset


@pytest.fixture(scope="module")
def csv_path() -> Path:
    return Path("data.csv")


@pytest.fixture(scope="module")
def base_dir(tmp_path: Path, csv_path: Path, test_data: pd.DataFrame) -> Path:
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    csv_path = dir_path / csv_path
    test_data.to_csv(csv_path)
    return dir_path


@pytest.fixture(scope="module")
def updated_base_dir(tmp_path: Path) -> Path:
    dir_path = tmp_path / "other_data"
    dir_path.mkdir()
    return dir_path


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
    base_dir: Path,
    updated_base_dir: Path,
) -> SparkFilesystemDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
    original_base_dir = base_dir

    datasource = context.data_sources.add_spark_filesystem(
        name=datasource_name, base_directory=original_base_dir
    )

    datasource.base_directory = normalize_directory_path(updated_base_dir, context.root_directory)
    datasource = context.data_sources.add_or_update_spark_filesystem(datasource=datasource)
    assert (
        datasource.base_directory == updated_base_dir
    ), "The datasource was not updated in the previous method call."

    datasource.base_directory = normalize_directory_path(original_base_dir, context.root_directory)
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.base_directory == original_base_dir
    ), "The datasource was not updated in the previous method call."
    return datasource


@pytest.fixture(scope="module")
def csv_asset(
    datasource: SparkFilesystemDatasource,
) -> Generator[CSVAsset, None, None]:
    """Test CSVAsset lifecycle"""
    asset_name = f"asset_{uuid.uuid4().hex}"
    yield datasource.add_csv_asset(name=asset_name, header=True, infer_schema=True)
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def batch_definition(csv_asset: CSVAsset, csv_path: Path) -> BatchDefinition:
    batch_def_name = f"batch_def_{uuid.uuid4().hex}"
    return csv_asset.add_batch_definition_path(
        name=batch_def_name,
        path=csv_path,
    )


@pytest.fixture(scope="module")
def validation_definition(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
    batch_definition: BatchDefinition,
) -> Generator[ValidationDefinition, None, None]:
    validation_def_name = f"val_def_{uuid.uuid4().hex}"
    yield context.validation_definitions.add(
        ValidationDefinition(
            name=validation_def_name,
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    context.validation_definitions.delete(name=validation_def_name)


@pytest.fixture(scope="module")
def checkpoint(
    checkpoint: Checkpoint,
    validation_definition: ValidationDefinition,
) -> Checkpoint:
    checkpoint.validation_definitions = [validation_definition]
    checkpoint.save()
    return checkpoint


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
