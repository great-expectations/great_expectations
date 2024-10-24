from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Generator

import pytest

from great_expectations import ValidationDefinition
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        PandasDatasource,
    )


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
    new_datasource_name = f"data_source_{uuid.uuid4().hex}"
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


@pytest.fixture(scope="module")
def data_asset(
    datasource: PandasDatasource,
) -> Generator[DataFrameAsset, None, None]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"asset_{uuid.uuid4().hex}"
    yield datasource.add_dataframe_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def batch_definition(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
) -> BatchDefinition:
    batch_def_name = f"batch_def_{uuid.uuid4().hex}"
    return data_asset.add_batch_definition_whole_dataframe(
        name=batch_def_name,
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
def test_checkpoint_run(checkpoint: Checkpoint, test_data):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": test_data})
    assert checkpoint_result.success
