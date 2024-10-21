from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Final, Iterator

import pytest

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
    from great_expectations.core import ExpectationSuite, ExpectationValidationResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import (
        BatchRequest,
        DataAsset,
        SnowflakeDatasource,
    )
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from great_expectations.validator.validator import Validator
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
    updated_connection_string = f"{connection_string}&foo=bar"

    datasource.connection_string = updated_connection_string  # type: ignore[assignment] # is a str
    datasource = context.data_sources.add_or_update_snowflake(datasource=datasource)
    assert (
        datasource.connection_string == updated_connection_string
    ), "The datasource was not updated in the previous method call."
    datasource.connection_string = connection_string  # type: ignore[assignment] # is a str
    datasource = context.add_or_update_datasource(datasource=datasource)  # type: ignore[assignment]
    assert (
        datasource.connection_string == connection_string
    ), "The datasource was not updated in the previous method call."
    datasource.connection_string = updated_connection_string  # type: ignore[assignment] # is a str
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    datasource = context.data_sources.add_or_update_snowflake(**datasource_dict)
    assert (
        datasource.connection_string == updated_connection_string
    ), "The datasource was not updated in the previous method call."
    datasource.connection_string = connection_string  # type: ignore[assignment] # is a str
    datasource_dict = datasource.dict()
    # this is a bug - LATIKU-448
    # call to datasource.dict() results in a ConfigStr that fails pydantic
    # validation on SnowflakeDatasource
    datasource_dict["connection_string"] = str(datasource_dict["connection_string"])
    _ = context.add_or_update_datasource(**datasource_dict)
    datasource = context.data_sources.get(name=datasource_name)  # type: ignore[assignment]
    assert (
        datasource.connection_string == connection_string
    ), "The datasource was not updated in the previous method call."
    return datasource


def table_asset(
    datasource: SnowflakeDatasource,
    asset_name: str,
    table_factory: TableFactory,
) -> TableAsset:
    table_name = f"i{uuid.uuid4().hex}"
    table_factory(
        gx_engine=datasource.get_execution_engine(),
        table_names={table_name},
        schema_name=RANDOM_SCHEMA,
    )
    return datasource.add_table_asset(
        name=asset_name,
        schema_name=RANDOM_SCHEMA,
        table_name=table_name,
    )


@pytest.fixture(scope="module", params=[table_asset])
def data_asset(
    datasource: SnowflakeDatasource,
    table_factory: TableFactory,
    get_missing_data_asset_error_type: type[Exception],
    request,
) -> Iterator[DataAsset]:
    """Test the entire Data Asset CRUD lifecycle here and in Data Asset-specific fixtures."""
    asset_name = f"da_{uuid.uuid4().hex}"
    yield request.param(
        datasource=datasource,
        asset_name=asset_name,
        table_factory=table_factory,
    )
    datasource.delete_asset(name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def batch_request(data_asset: DataAsset) -> BatchRequest:
    """Build a BatchRequest depending on the types of Data Assets tested in the module."""
    return data_asset.build_batch_request()


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
    reason="Expectation suites in 1.0.0 now have a name attribute "
    "instead of expectation_suite_name which mercury currently doesn't support",
    strict=True,
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
        validator.expect_column_values_to_not_be_null(
            column="id",
            mostly=1,
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
