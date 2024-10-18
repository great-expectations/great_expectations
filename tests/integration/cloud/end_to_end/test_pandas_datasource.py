from __future__ import annotations

import uuid
import warnings
from typing import TYPE_CHECKING, Iterator, Generator

import pandas as pd
import pytest

from great_expectations import ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset
import great_expectations.expectations as gxe
from great_expectations.exceptions import DataContextError
from great_expectations.core import ExpectationSuite, ExpectationValidationResult
from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        PandasDatasource, GxInvalidDatasourceWarning,
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
    get_missing_data_asset_error_type: type[Exception],
) -> Generator[DataFrameAsset, None, None]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"asset_{uuid.uuid4().hex}"
    yield datasource.add_dataframe_asset(
        name=asset_name,
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
def batch_definition(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
) -> Generator[BatchDefinition, None, None]:
    batch_def_name = f"batch_def_{uuid.uuid4().hex}"
    yield data_asset.add_batch_definition_whole_dataframe(
        name=batch_def_name,
    )
    data_asset.delete_batch_definition(name=batch_def_name)
    with pytest.raises(KeyError):
        data_asset.get_batch_definition(name=batch_def_name)

@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
) -> Generator[ExpectationSuite, None, None]:
    """Add Expectations for the Data Assets defined in this module.
    Note: There is no need to test Expectation Suite CRUD.
    Those assertions can be found in the expectation_suite fixture.
    """
    expectation_suite_name = f"es_{uuid.uuid4().hex}"
    yield context.suites.add(
        ExpectationSuite(
            name=expectation_suite_name,
            expectations=[
                gxe.ExpectColumnValuesToNotBeNull(
                    column="string",
                    mostly=1
                )
            ]
        )
    )
    context.suites.delete(name=expectation_suite_name)


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
    context: CloudDataContext,
    validation_definition: ValidationDefinition,
) -> Iterator[Checkpoint]:
    checkpoint_name = f"{validation_definition.name} Checkpoint"

    checkpoint = Checkpoint(name=checkpoint_name, validation_definitions=[validation_definition])
    checkpoint = context.checkpoints.add(checkpoint=checkpoint)
    yield checkpoint
    context.checkpoints.delete(name=checkpoint_name)

    with pytest.raises(DataContextError):
        context.checkpoints.get(name=checkpoint_name)


@pytest.mark.cloud
def test_interactive_validator(
    context: CloudDataContext,
    validator: Validator,
):
    """Test interactive evaluation of the Data Assets in this module using an existing Validator.
    Note: There is no need to test getting a Validator or using Validator.head(). That is already
    tested in the validator fixture.
    """
    expectation_validation_result = (
        validator.expect_column_values_to_not_be_null(
            column="datetime",
            mostly=1,
        )
    )
    assert expectation_validation_result.success


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint, pandas_test_df):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result = checkpoint.run(batch_parameters={
        "dataframe": pandas_test_df
    })
    assert checkpoint_result.success
