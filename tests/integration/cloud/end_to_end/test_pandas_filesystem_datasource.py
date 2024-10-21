from __future__ import annotations

import pathlib
import uuid
from typing import TYPE_CHECKING, Generator, Iterator

import pandas as pd
import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
    from great_expectations.core import ExpectationSuite, ExpectationValidationResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        DataAsset,
        PandasFilesystemDatasource,
    )
    from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
        CSVAsset,
        ParquetAsset,
    )
    from great_expectations.validator.validator import Validator


@pytest.fixture(scope="module")
def base_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    df = pd.DataFrame({"name": ["bob", "alice"]})
    df.to_csv(dir_path / "data.csv")
    df.to_parquet(dir_path / "data.parquet")
    return pathlib.Path(dir_path)


@pytest.fixture(scope="module")
def updated_base_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    dir_path = tmp_path / "other_data"
    dir_path.mkdir()
    return pathlib.Path(dir_path)


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
    base_dir: pathlib.Path,
    updated_base_dir: pathlib.Path,
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
def asset_name() -> str:
    return f"da_{uuid.uuid4().hex}"


@pytest.fixture(scope="module")
def batch_definition_name() -> str:
    return f"batch_def_{uuid.uuid4().hex}"


@pytest.fixture(scope="module")
def csv_asset(
    datasource: PandasFilesystemDatasource,
    asset_name: str,
) -> Generator[CSVAsset, None, None]:
    """Test CSVAsset lifecycle"""
    yield datasource.add_csv_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def csv_batch_definition(
    batch_definition_name: str, csv_asset: CSVAsset, csv_path: pathlib.Path
) -> BatchDefinition:
    return csv_asset.add_batch_definition_path(
        name=batch_definition_name,
        path=csv_path,
    )


@pytest.fixture(scope="module")
def parquet_asset(
    datasource: PandasFilesystemDatasource,
    asset_name: str,
) -> Generator[ParquetAsset, None, None]:
    """Test ParquetAsset lifecycle"""
    yield datasource.add_parquet_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def parquet_batch_definition(
    batch_definition_name: str, parquet_asset: CSVAsset, parquet_path: pathlib.Path
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
def expectation_suite(
    context: CloudDataContext,
    expectation_suite: ExpectationSuite,
) -> ExpectationSuite:
    """Add Expectations for the Data Assets defined in this module.
    Note: There is no need to test Expectation Suite CRUD.
    Those assertions can be found in the expectation_suite fixture."""
    expectation_suite.add_expectation_configuration(
        expectation_configuration=ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "name",
                "mostly": 1,
            },
        )
    )
    return expectation_suite


@pytest.mark.xfail(
    reason="1.0 API requires a backend change. Test should pass once #2623 is merged"
)
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
        validator.expect_column_values_to_be_in_set(
            column="name",
            value_set=["bob", "alice"],
        )
    )
    assert expectation_validation_result.success


@pytest.mark.xfail(
    reason="1.0 API requires a backend change. Test should pass once #2623 is merged"
)
@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
