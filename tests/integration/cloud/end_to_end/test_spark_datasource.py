from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Generator

import pytest

from great_expectations import ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset
from tests.integration.cloud.end_to_end.conftest import construct_spark_df_from_pandas

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
    from great_expectations.compatibility import pyspark
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        DataAsset,
        SparkDatasource,
    )


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
) -> SparkDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
    datasource = context.data_sources.add_spark(
        name=datasource_name,
        persist=True,
    )
    datasource.persist = False
    datasource = context.data_sources.add_or_update_spark(datasource=datasource)  # type: ignore[call-arg]
    assert (
        datasource.persist is False
    ), "The datasource was not updated in the previous method call."
    datasource.persist = True
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert datasource.persist is True, "The datasource was not updated in the previous method call."
    datasource.persist = False
    datasource_dict = datasource.dict()
    datasource = context.data_sources.add_or_update_spark(**datasource_dict)
    assert (
        datasource.persist is False
    ), "The datasource was not updated in the previous method call."
    datasource.persist = True
    datasource_dict = datasource.dict()
    datasource = context.add_or_update_datasource(**datasource_dict)  # type: ignore[assignment]
    assert datasource.persist is True, "The datasource was not updated in the previous method call."
    return datasource


@pytest.fixture(scope="module")
def data_asset(
    datasource: SparkDatasource,
) -> Generator[DataAsset, None, None]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"da_{uuid.uuid4().hex}"
    yield datasource.add_dataframe_asset(
        name=asset_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def spark_dataframe(
    spark_session: pyspark.SparkSession, test_data: pd.DataFrame
) -> pyspark.DataFrame:
    return construct_spark_df_from_pandas(spark_session, test_data)


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
def test_checkpoint_run(checkpoint: Checkpoint, spark_dataframe: pyspark.DataFrame):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run(
        batch_parameters={
            "dataframe": spark_dataframe,
        }
    )
    assert checkpoint_result.success
