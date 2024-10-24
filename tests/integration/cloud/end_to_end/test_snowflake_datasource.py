from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Final, Generator

import pytest

from great_expectations import ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        DataAsset,
        SnowflakeDatasource,
    )
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.cloud.end_to_end.conftest import TableFactory

RANDOM_SCHEMA: Final[str] = f"i{uuid.uuid4().hex}"


@pytest.fixture(scope="module")
def connection_string() -> str:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return (
            "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci"
            f"/{RANDOM_SCHEMA}?warehouse=ci&role=ci"
        )
    elif os.getenv("SNOWFLAKE_USER") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return (
            "snowflake://${SNOWFLAKE_USER}@oca29081.us-east-1/DEMO_DB"
            f"/{RANDOM_SCHEMA}?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser"
        )
    else:
        pytest.skip("no snowflake credentials")


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    datasource_name: str,
    connection_string: str,
) -> SnowflakeDatasource:
    """Test Adding and Updating the Datasource associated with this module.
    Note: There is no need to test Get or Delete Datasource.
    Those assertions can be found in the datasource_name fixture."""
    datasource = context.data_sources.add_snowflake(
        name=datasource_name,
        connection_string=connection_string,
        create_temp_table=False,
    )
    return datasource


@pytest.fixture(scope="module")
def data_asset(
    datasource: SnowflakeDatasource,
    table_factory: TableFactory,
) -> Generator[DataAsset, None, None]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"da_{uuid.uuid4().hex}"
    table_name = f"i{uuid.uuid4().hex}"
    table_factory(
        gx_engine=datasource.get_execution_engine(),
        table_names={table_name},
        schema_name=RANDOM_SCHEMA,
    )
    yield datasource.add_table_asset(
        name=asset_name,
        table_name=table_name,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(LookupError):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def batch_definition(
    context: CloudDataContext,
    data_asset: TableAsset,
) -> BatchDefinition:
    batch_def_name = f"batch_def_{uuid.uuid4().hex}"
    return data_asset.add_batch_definition_whole_table(
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
def test_checkpoint_run(checkpoint: Checkpoint):
    """Test running a Checkpoint that was created using the entities defined in this module."""
    checkpoint_result: CheckpointResult = checkpoint.run()
    assert checkpoint_result.success
