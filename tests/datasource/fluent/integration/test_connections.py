from __future__ import annotations

import random
from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa
from pytest import param

from great_expectations.datasource.fluent import (
    SnowflakeDatasource,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext as DataContext


@pytest.mark.snowflake
class TestSnowflake:
    @pytest.mark.parametrize(
        "connection_string",
        [
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci",
                id="missing role",
                # marks=pytest.mark.skip,
            ),
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}?warehouse=ci&role=ci",
                id="missing database + schema",
                # marks=pytest.mark.skip(reason="taking too long"),
            ),
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci?warehouse=ci&role=ci",
                id="missing schema",
            ),
        ],
    )
    def test_un_queryable_asset_should_raise_error(
        self, context: DataContext, connection_string: str
    ):
        """
        A SnowflakeDatasource can successfully connect even if things like database, schema, warehouse, and role are omitted.
        However, if we try to add an asset that is not queryable with the current datasource connection details,
        then we should expect a TestConnectionError.
        https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#connection-parameters
        """
        snowflake_ds: SnowflakeDatasource = context.sources.add_snowflake(
            "my_ds", connection_string=connection_string
        )

        inspector_schemas = sa.inspection.inspect(
            snowflake_ds.get_engine()
        ).get_schema_names()
        inspector_tables = sa.inspection.inspect(
            snowflake_ds.get_engine()
        ).get_table_names()
        print(f"schemas: {len(inspector_schemas)}")
        print(f"tables: {len(inspector_tables)}")

        table_name = random.choice(inspector_tables)

        # query the asset, if it fails then we should expect a TestConnectionError
        # expect the sql ProgrammingError to be raised
        # we are only testing the failure case here
        with pytest.raises(sa.exc.ProgrammingError):
            snowflake_ds.get_engine().execute(f"SELECT * FROM {table_name} LIMIT 1;")
            print(f"\n  {table_name} is queryable")

        with pytest.raises(TestConnectionError) as exc_info:
            # TODO: check specific error message
            asset = snowflake_ds.add_table_asset(
                name="un-reachable asset", table_name=table_name
            )
            print(f"\n  Uh oh, asset should not have been created...\n{asset!r}")
        print(f"\n  TestConnectionError was raised as expected.\n{exc_info.exconly()}")

    @pytest.mark.parametrize(
        "connection_string",
        [
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci&role=ci",
                id="full connection string",
            ),
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?role=ci",
                id="missing warehouse",
            ),
        ],
    )
    def test_queryable_asset_should_pass_test_connection(
        self, context: DataContext, connection_string: str
    ):
        snowflake_ds: SnowflakeDatasource = context.sources.add_snowflake(
            "my_ds", connection_string=connection_string
        )

        inspector_schemas = sa.inspection.inspect(
            snowflake_ds.get_engine()
        ).get_schema_names()
        inspector_tables = sa.inspection.inspect(
            snowflake_ds.get_engine()
        ).get_table_names()
        print(f"schemas: {len(inspector_schemas)}")
        print(f"tables: {len(inspector_tables)}")

        table_name = random.choice(inspector_tables)

        # query the table to make sure it is queryable
        snowflake_ds.get_engine().execute(f"SELECT * FROM {table_name} LIMIT 1;")

        # the table is queryable so the `add_table_asset()` should pass the test_connection step
        asset = snowflake_ds.add_table_asset(
            name="reachable asset", table_name=table_name
        )
        print(f"\n  Yay, asset was created!\n{asset!r}")


if __name__ == "__main__":
    pytest.main()
