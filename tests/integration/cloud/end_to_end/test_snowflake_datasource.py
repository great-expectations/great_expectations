from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Iterator

import pytest

from great_expectations.core import ExpectationConfiguration

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest, SnowflakeDatasource
    from great_expectations.datasource.fluent.sql_datasource import TableAsset


@pytest.fixture
def connection_string() -> str:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci&role=ci"
    elif os.getenv("SNOWFLAKE_USER") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return "snowflake://${SNOWFLAKE_USER}@${SNOWFLAKE_CI_ACCOUNT}/DEMO_DB?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser"
    else:
        pytest.skip("no snowflake credentials")


@pytest.fixture
def datasource(
    context: CloudDataContext,
    connection_string: str,
) -> Iterator[SnowflakeDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_snowflake(
        name=datasource_name,
        connection_string=connection_string,
        create_temp_table=False,
    )
    datasource.create_temp_table = True
    datasource = context.sources.add_or_update_snowflake(datasource=datasource)
    assert (
        datasource.create_temp_table is True
    ), "The datasource was not updated in the previous method call."
    datasource.create_temp_table = False
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.create_temp_table is False
    ), "The datasource was not updated in the previous method call."
    datasource.create_temp_table = True
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    datasource = context.sources.add_or_update_snowflake(**datasource_dict)
    assert (
        datasource.create_temp_table is True
    ), "The datasource was not updated in the previous method call."
    datasource.create_temp_table = False
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    _ = context.add_or_update_datasource(**datasource_dict)
    datasource = context.get_datasource(datasource_name=datasource_name)  # type: ignore[assignment]
    assert (
        datasource.create_temp_table is False
    ), "The datasource was not updated in the previous method call."
    yield datasource
    # PP-692: this doesn't work due to a bug
    # calling delete_datasource() will fail with:
    # Datasource is used by Checkpoint <LONG HASH>
    # This is confirmed to be the default Checkpoint,
    # but error message is not specific enough to know without additional inspection
    # context.delete_datasource(datasource_name=datasource_name)


@pytest.fixture
def data_asset(datasource: SnowflakeDatasource, table_factory) -> Iterator[TableAsset]:
    schema_name = f"i{uuid.uuid4().hex}"
    table_name = f"i{uuid.uuid4().hex}"
    table_factory(
        gx_engine=datasource.get_execution_engine(),
        table_names={table_name},
        schema_name=schema_name,
    )
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_table_asset(
        name=asset_name, table_name=table_name, schema_name=schema_name
    )
    table_asset = datasource.get_asset(asset_name=asset_name)
    yield table_asset
    # PP-692: this doesn't work due to a bug
    # calling delete_asset() will fail with:
    # Cannot perform action because Asset is used by Checkpoint:
    # end-to-end_snowflake_asset <SHORT HASH> - Default Checkpoint
    # datasource.delete_asset(asset_name=asset_name)


@pytest.fixture
def batch_request(data_asset: TableAsset) -> BatchRequest:
    return data_asset.build_batch_request()


@pytest.fixture
def expectation_suite(
    context: CloudDataContext,
    data_asset: TableAsset,
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
    data_asset: TableAsset,
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
    validator.head()
    validator.expect_column_values_to_not_be_null(
        column="id",
        mostly=1,
    )
    validator.save_expectation_suite()
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert len(expectation_suite.expectations) == expectation_count + 1


@pytest.mark.cloud
def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.success is True
