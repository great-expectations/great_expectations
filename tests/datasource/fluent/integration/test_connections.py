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
    from sqlalchemy.engine.reflection import Inspector

    from great_expectations.data_context import AbstractDataContext as DataContext


@pytest.mark.snowflake
class TestSnowflake:
    @pytest.mark.parametrize(
        "connection_string",
        [
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci/public?warehouse=ci",
                id="missing role",
            ),
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci/public?warehouse=ci&role=ci_no_select",
                id="role wo select",
            ),
        ],
    )
    def test_un_queryable_asset_should_raise_error(
        self, context: DataContext, connection_string: str
    ):
        """
        A SnowflakeDatasource can successfully connect even if things like warehouse, and role are omitted.
        However, if we try to add an asset that is not queryable with the current datasource connection details,
        then we should expect a TestConnectionError.
        https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#connection-parameters
        """
        snowflake_ds: SnowflakeDatasource = context.sources.add_snowflake(
            "my_ds", connection_string=connection_string
        )

        inspector: Inspector = sa.inspection.inspect(snowflake_ds.get_engine())
        inspector_tables: list[str] = inspector.get_table_names()
        print(f"tables: {len(inspector_tables)}\n{inspector_tables}")
        random.shuffle(inspector_tables)

        unqueryable_table: str = ""
        for table_name in inspector_tables:
            try:
                # query the asset, if it fails then we should expect a TestConnectionError
                # expect the sql ProgrammingError to be raised
                # we are only testing the failure case here
                snowflake_ds.get_engine().execute(
                    f"SELECT * FROM {table_name} LIMIT 1;"
                )
                print(f"{table_name} is queryable")
            except sa.exc.ProgrammingError:
                print(f"{table_name} is not queryable")
                unqueryable_table = table_name
                break
        assert unqueryable_table, "no unqueryable tables found, cannot run test"

        with pytest.raises(TestConnectionError) as exc_info:
            asset = snowflake_ds.add_table_asset(
                name="un-reachable asset", table_name=unqueryable_table
            )
            print(f"\n  Uh oh, asset should not have been created...\n{asset!r}")
        print(f"\n  TestConnectionError was raised as expected.\n{exc_info.exconly()}")

    @pytest.mark.parametrize(
        "connection_string",
        [
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci/public?warehouse=ci&role=ci&database=ci&schema=public",
                id="full connection string",
            ),
            param(
                "snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci/public?role=ci&database=ci&schema=public",
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

        inspector: Inspector = sa.inspection.inspect(snowflake_ds.get_engine())
        inspector_tables = inspector.get_table_names()
        print(f"tables: {len(inspector_tables)}\n{inspector_tables}")

        table_name = random.choice(inspector_tables)

        # query the table to make sure it is queryable
        snowflake_ds.get_engine().execute(f"SELECT * FROM {table_name} LIMIT 1;")

        # the table is queryable so the `add_table_asset()` should pass the test_connection step
        asset = snowflake_ds.add_table_asset(
            name="reachable asset", table_name=table_name
        )
        print(f"\n  Yay, asset was created!\n{asset!r}")


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
