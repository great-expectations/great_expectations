from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Iterator

import pytest

from great_expectations import ValidationDefinition
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent import PandasFilesystemDatasource

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        DataAsset,
    )
    from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
        CSVAsset,
        ParquetAsset,
    )


@pytest.fixture(scope="module")
def base_dir(tmp_path: Path, csv_path: Path, parquet_path: Path, test_data: pd.DataFrame) -> Path:
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    # write data for all asset types
    test_data.to_csv(dir_path / csv_path)
    test_data.to_parquet(dir_path / parquet_path)
    return Path(dir_path)


@pytest.fixture(scope="module")
def csv_path() -> Path:
    return Path("data.csv")


@pytest.fixture(scope="module")
def parquet_path() -> Path:
    return Path("data.parquet")


@pytest.fixture(scope="module")
def updated_base_dir(tmp_path: Path) -> Path:
    dir_path = tmp_path / "other_data"
    dir_path.mkdir()
    return Path(dir_path)


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
    base_dir: Path,
    updated_base_dir: Path,
) -> PandasFilesystemDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
    original_base_dir = base_dir

    datasource = context.data_sources.add_pandas_filesystem(
        name=datasource_name, base_directory=original_base_dir
    )
    datasource.base_directory = updated_base_dir
    datasource = context.data_sources.add_or_update_pandas_filesystem(datasource=datasource)
    assert (
        datasource.base_directory == updated_base_dir
    ), "The datasource was not updated in the previous method call."

    datasource.base_directory = original_base_dir
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.base_directory == original_base_dir
    ), "The datasource was not updated in the previous method call."
    return datasource


@pytest.fixture(scope="module")
def batch_definition_name() -> str:
    return f"batch_def_{uuid.uuid4().hex}"


@pytest.fixture(scope="module")
def csv_asset(
    datasource: PandasFilesystemDatasource,
) -> Generator[CSVAsset, None, None]:
    """Test CSVAsset lifecycle"""
    asset_name = f"asset_{uuid.uuid4().hex}"
    yield datasource.add_csv_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def csv_batch_definition(
    batch_definition_name: str, csv_asset: CSVAsset, csv_path: Path
) -> BatchDefinition:
    return csv_asset.add_batch_definition_path(
        name=batch_definition_name,
        path=csv_path,
    )


@pytest.fixture(scope="module")
def parquet_asset(
    context: CloudDataContext,
    datasource_name: str,
) -> Generator[ParquetAsset, None, None]:
    """Test ParquetAsset lifecycle"""
    asset_name = f"asset_{uuid.uuid4().hex}"
    datasource = context.data_sources.get(name=datasource_name)
    assert isinstance(datasource, PandasFilesystemDatasource)
    yield datasource.add_parquet_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def parquet_batch_definition(
    batch_definition_name: str, parquet_asset: CSVAsset, parquet_path: Path
) -> BatchDefinition:
    return parquet_asset.add_batch_definition_path(
        name=batch_definition_name,
        path=parquet_path,
    )


@pytest.fixture(scope="module", params=["csv_batch_definition", "parquet_batch_definition"])
def batch_definition(
    request,
) -> Iterator[DataAsset]:
    """Parametrize the BatchDefinition fixtures under test"""
    return request.getfixturevalue(request.param)


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
