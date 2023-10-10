import os
import uuid

import pytest

from great_expectations.checkpoint import Checkpoint
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest, SnowflakeDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset


@pytest.fixture
def creds_populated() -> bool:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") or os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return True
    return False


@pytest.fixture
def datasource(
    context: CloudDataContext,
    creds_populated: bool,
) -> SnowflakeDatasource:
    if not creds_populated:
        pytest.skip("no snowflake credentials")

    datasource_name = "snowflake_ci_datasource"
    datasource = context.sources.add_snowflake(
        name=datasource_name,
        connection_string="snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci&role=ci",
        # NOTE: uncomment this and set SNOWFLAKE_USER to run tests against your own snowflake account
        # connection_string="snowflake://${SNOWFLAKE_USER}@${SNOWFLAKE_CI_ACCOUNT}/DEMO_DB?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser",
    )
    datasource.create_temp_table = False
    # PP-690: this doesn't work due to a bug
    # calling add_or_update_<datasource>() results in the datasource being deleted from the store
    # _ = context.sources.add_or_update_snowflake(datasource)
    # get_datasource() works, but we don't use the return object here,
    # because it won't have the create_temp_table attribute set to False
    # once the add_or_update bug above is fixed, we can use the return object
    _ = context.get_datasource(datasource_name=datasource_name)
    yield datasource
    # this doesn't work due to a bug
    # the checkpoint is already deleted, but an error is incorrectly raised
    # context.delete_datasource(datasource_name=datasource_name)


@pytest.fixture
def data_asset(
    context: CloudDataContext, datasource: SnowflakeDatasource, table_factory
) -> TableAsset:
    schema_name = f"i{uuid.uuid4().hex}"
    table_name = "test_table"
    table_factory(
        gx_engine=datasource.get_execution_engine(),
        table_names={table_name},
        schema_name=schema_name,
    )
    asset_name = "end-to-end_snowflake_asset"
    _ = datasource.add_table_asset(
        name=asset_name, table_name=table_name, schema_name=schema_name
    )
    table_asset = datasource.get_asset(asset_name=asset_name)
    yield table_asset
    # this doesn't work due to a bug
    # the checkpoint is already deleted, but an error is incorrectly raised
    # datasource.delete_asset(asset_name=asset_name)


@pytest.fixture
def batch_request(data_asset: TableAsset) -> BatchRequest:
    return data_asset.build_batch_request()


@pytest.fixture
def expectation_suite(
    context: CloudDataContext,
    data_asset: TableAsset,
) -> ExpectationSuite:
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
) -> Checkpoint:
    checkpoint_name = f"{data_asset.datasource.name} | {data_asset.name}"
    _ = context.add_checkpoint(
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
    # this is a bug - you should only have to pass name
    context.delete_checkpoint(
        # name=checkpoint_name,
        id=checkpoint.ge_cloud_id,
    )


@pytest.mark.mercury
def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint.run()
