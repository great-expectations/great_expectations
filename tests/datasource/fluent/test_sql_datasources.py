from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_kwargs_are_passed_to_create_engine(mocker: MockerFixture):
    create_engine_spy = mocker.spy(sa, "create_engine")

    ds = SQLDatasource(
        name="my_datasource",
        connection_string="sqlite:///",
        kwargs={"isolation_level": "SERIALIZABLE"},
    )
    print(ds)
    ds.test_connection()

    create_engine_spy.assert_called_once_with(
        "sqlite:///", **{"isolation_level": "SERIALIZABLE"}
    )


@pytest.mark.unit
def test_table_quoted_name_type_does_not_exist(
    mocker,
):
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that mechanism for detection of non-existent table_nam" works correctly.
    """
    table_names_in_dbms_schema: list[str] = [
        "table_name_0",
        "table_name_1",
        "table_name_2",
        "table_name_3",
    ]

    with mock.patch(
        "great_expectations.datasource.fluent.sql_datasource.TableAsset.datasource",
        new_callable=mock.PropertyMock,
        return_value=SQLDatasource(
            name="my_snowflake_datasource",
            connection_string="snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>",
        ),
    ):
        table_asset = TableAsset(
            name="my_table_asset",
            table_name="nonexistent_table_name",
            schema_name="my_schema",
        )
        assert table_asset.table_name not in table_names_in_dbms_schema


@pytest.mark.unit
def test_table_quoted_name_type_all_upper_case_normalizion_is_noop():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that all upper case entity usage does not undergo any conversion.
    """
    table_names_in_dbms_schema: list[str] = [
        "ACTORS",
        "ARTISTS",
        "ATHLETES",
        "BUSINESS_PEOPLE",
        "HEALTHCARE_WORKERS",
        "ENGINEERS",
        "LAWYERS",
        "MUSICIANS",
        "SCIENTISTS",
        "LITERARY_PROFESSIONALS",
    ]

    asset_name: str
    table_name: str

    with mock.patch(
        "great_expectations.datasource.fluent.sql_datasource.TableAsset.datasource",
        new_callable=mock.PropertyMock,
        return_value=SQLDatasource(
            name="my_snowflake_datasource",
            connection_string="snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>",
        ),
    ):
        for table_name in table_names_in_dbms_schema:
            asset_name = f"{table_name}_asset"
            table_asset = TableAsset(
                name=asset_name,
                table_name=table_name,
                schema_name="my_schema",
            )
            assert str(table_asset.table_name) == table_name
            assert str(table_asset.table_name.casefold()) != table_name
            assert isinstance(table_asset.table_name, sqlalchemy.quoted_name)
            assert table_asset.table_name.quote is True
            assert table_asset.table_name in table_names_in_dbms_schema


@pytest.mark.unit
def test_table_quoted_name_type_all_lower_case_normalizion_full():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that all lower case entity usage undergo conversion to quoted literals.
    """
    table_names_in_dbms_schema: list[str] = [
        "actors",
        "artists",
        "athletes",
        "business_people",
        "healthcare_workers",
        "engineers",
        "lawyers",
        "musicians",
        "scientists",
        "literary_professionals",
    ]

    name: str

    quoted_table_names: list[sqlalchemy.quoted_name] = [
        sqlalchemy.quoted_name(value="actors", quote=True),
        sqlalchemy.quoted_name(value="artists", quote=True),
        sqlalchemy.quoted_name(value="athletes", quote=True),
        sqlalchemy.quoted_name(value="business_people", quote=True),
        sqlalchemy.quoted_name(value="healthcare_workers", quote=True),
        sqlalchemy.quoted_name(value="engineers", quote=True),
        sqlalchemy.quoted_name(value="lawyers", quote=True),
        sqlalchemy.quoted_name(value="musicians", quote=True),
        sqlalchemy.quoted_name(value="scientists", quote=True),
        sqlalchemy.quoted_name(value="literary_professionals", quote=True),
    ]

    asset_name: str
    table_name: str

    with mock.patch(
        "great_expectations.datasource.fluent.sql_datasource.TableAsset.datasource",
        new_callable=mock.PropertyMock,
        return_value=SQLDatasource(
            name="my_snowflake_datasource",
            connection_string="snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>",
        ),
    ):
        for table_name in table_names_in_dbms_schema:
            asset_name = f"{table_name}_asset"
            table_asset = TableAsset(
                name=asset_name,
                table_name=table_name,
                schema_name="my_schema",
            )
            assert str(table_asset.table_name) == table_name
            assert str(table_asset.table_name.casefold()) == table_name
            assert isinstance(table_asset.table_name, sqlalchemy.quoted_name)
            assert table_asset.table_name.quote is True
            assert table_asset.table_name in table_names_in_dbms_schema
            assert table_asset.table_name in quoted_table_names


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
