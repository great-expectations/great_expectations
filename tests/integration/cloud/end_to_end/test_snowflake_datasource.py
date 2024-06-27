from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Final, Iterator

import pytest
from typing_extensions import Literal

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import StoreBackendError

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest, SnowflakeDatasource
    from great_expectations.datasource.fluent.config_str import ConfigStr
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.cloud.end_to_end.conftest import TableFactory

pytestmark: Final = pytest.mark.cloud

RANDOM_SCHEMA: Final[str] = f"i{uuid.uuid4().hex}"

ConnectionDetailKeys = Literal[
    "account", "user", "password", "database", "schema", "warehouse", "role"
]


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
def connection_details() -> dict[ConnectionDetailKeys, str]:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") and os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return {
            "account": "oca29081.us-east-1",
            "user": "ci",
            "password": "${SNOWFLAKE_CI_USER_PASSWORD}",
            "database": "ci",
            "schema": RANDOM_SCHEMA,
            "warehouse": "ci",
            "role": "ci",
        }
    pytest.skip("no snowflake credentials")


@pytest.fixture(scope="module", params=["connection_string", "connection_details"])
def connections(
    request: pytest.FixtureRequest,
) -> str | dict[ConnectionDetailKeys, str]:
    """Parametrized fixture that returns both a connection string and connection details."""
    return request.getfixturevalue(request.param)


def test_create_datasource(
    context: CloudDataContext, connections: str | dict[str, str]
):
    datasource_name = f"i{uuid.uuid4().hex}"
    _: SnowflakeDatasource = context.sources.add_snowflake(
        name=datasource_name,
        connection_string=connections,
    )


@pytest.mark.parametrize(
    ["details_override", "expected_err_pattern"],
    [
        pytest.param(
            {"schema": None},
            r'.*"loc":\s*\["snowflake",\s*"connection_string",\s*"SnowflakeConnectionDetails",\s*"schema"\],'
            r'\s*"msg":\s*"Field required",\s*"type":\s*"missing".*',
            id="schema missing",
        ),
        pytest.param(
            {"database": None, "schema": None},
            r'.*"loc":\s*\["snowflake",\s*"connection_string",\s*"SnowflakeConnectionDetails",\s*"database"\],'
            r'\s*"msg":\s*"Field required",\s*"type":\s*"missing".*',
            id="database missing",
        ),
        pytest.param(
            {"warehouse": None},
            r'.*"loc":\s*\["snowflake",\s*"connection_string",\s*"SnowflakeConnectionDetails",\s*"warehouse"\],'
            r'\s*"msg":\s*"Field required",\s*"type":\s*"missing".*',
            id="warehouse missing",
        ),
        pytest.param(
            {"role": None},
            r'.*"loc":\s*\["snowflake",\s*"connection_string",\s*"SnowflakeConnectionDetails",\s*"role"\],'
            r'\s*"msg":\s*"Field required",\s*"type":\s*"missing".*',
            id="role missing",
        ),
    ],
)
def test_create_4xx_error_message_handling(
    context: CloudDataContext,
    connection_details: dict[str, str],
    details_override: dict[str, str | None],
    expected_err_pattern: str,
):
    connection = {**connection_details, **details_override}
    with pytest.raises(
        StoreBackendError,
        match=r"Unable to set object in GX Cloud Store Backend: "
        + expected_err_pattern,
    ):
        _: SnowflakeDatasource = context.sources.add_snowflake(
            name=f"i{uuid.uuid4().hex}",
            connection_string={  # filter out falsey values
                k: v for k, v in connection.items() if v
            },
        )


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    connection_string: str | ConfigStr,
    get_missing_datasource_error_type: type[Exception],
) -> Iterator[SnowflakeDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource: SnowflakeDatasource = context.sources.add_snowflake(
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
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment] # more specific type
    assert datasource.kwargs == {
        "echo": False
    }, "The datasource was not updated in the previous method call."
    datasource.kwargs["echo"] = True

    datasource = context.sources.add_or_update_snowflake(**datasource.dict())
    assert datasource.kwargs == {
        "echo": True
    }, "The datasource was not updated in the previous method call."
    datasource.kwargs["echo"] = False

    _ = context.add_or_update_datasource(**datasource.dict())
    datasource = context.get_datasource(datasource_name=datasource_name)  # type: ignore[assignment] # more specific type
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
    table_name = f"i{uuid.uuid4().hex}"
    table_factory(
        gx_engine=datasource.get_execution_engine(),
        table_names={table_name},
        schema_name=RANDOM_SCHEMA,
    )
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_table_asset(name=asset_name, table_name=table_name)
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


def test_checkpoint_run(checkpoint: Checkpoint):
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.success is True
