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
    from great_expectations.datasource.fluent.config_str import ConfigStr
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.cloud.end_to_end.conftest import TableFactory


@pytest.fixture(scope="module")
def connection_string() -> str:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci&role=ci"
    elif os.getenv("SNOWFLAKE_USER") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return "snowflake://${SNOWFLAKE_USER}@${SNOWFLAKE_CI_ACCOUNT}/DEMO_DB?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser"
    else:
        pytest.skip("no snowflake credentials")


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    connection_string: str | ConfigStr,
    get_missing_datasource_error_type: type[Exception],
) -> Iterator[SnowflakeDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_snowflake(
        name=datasource_name,
        connection_string=connection_string,
        create_temp_table=False,
        # kwargs must set here otherwise the field will be 'unset' and never serialized even if updated
        kwargs={"echo": False},  # type: ignore[call-overload]
    )

    datasource.kwargs.update(echo=True)
    datasource = context.sources.add_or_update_snowflake(datasource=datasource)
    assert datasource.kwargs == {
        "echo": True
    }, "The datasource was not updated in the previous method call."
    datasource.kwargs["echo"] = False
    datasource = context.add_or_update_datasource(datasource=datasource)
    assert datasource.kwargs == {
        "echo": False
    }, "The datasource was not updated in the previous method call."
    datasource.kwargs["echo"] = True
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    datasource = context.sources.add_or_update_snowflake(**datasource_dict)
    assert datasource.kwargs == {
        "echo": True
    }, "The datasource was not updated in the previous method call."
    datasource.kwargs["echo"] = False
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    _ = context.add_or_update_datasource(**datasource_dict)
    datasource = context.get_datasource(datasource_name=datasource_name)
    assert datasource.kwargs == {
        "echo": False
    }, "The datasource was not updated in the previous method call."
    yield datasource
    context.delete_datasource(datasource_name=datasource_name)
    with pytest.raises(get_missing_datasource_error_type):
        context.get_datasource(datasource_name=datasource_name)


@pytest.fixture(scope="module")
def data_asset(
    datasource: SnowflakeDatasource,
    table_factory: TableFactory,
    get_missing_data_asset_error_type: type[Exception],
) -> Iterator[TableAsset]:
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
    datasource.delete_asset(asset_name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(asset_name=asset_name)


@pytest.fixture(scope="module")
def batch_request(data_asset: TableAsset) -> BatchRequest:
    return data_asset.build_batch_request()


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    data_asset: TableAsset,
    get_missing_expectation_suite_error_type: type[Exception],
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
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    yield expectation_suite
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
    with pytest.raises(get_missing_expectation_suite_error_type):
        context.get_expectation_suite(expectation_suite_name=expectation_suite_name)


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
    data_asset: TableAsset,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
    get_missing_checkpoint_error_type: type[Exception],
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
    assert (
        len(checkpoint.validations) == 1
    ), "Checkpoint was not updated in the previous method call."
    yield checkpoint
    # PP-691: this is a bug
    # you should only have to pass name
    context.delete_checkpoint(
        # name=checkpoint_name,
        id=checkpoint.ge_cloud_id,
    )
    with pytest.raises(get_missing_checkpoint_error_type):
        context.get_checkpoint(name=checkpoint_name)


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
