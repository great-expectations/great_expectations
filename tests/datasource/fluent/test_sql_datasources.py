from __future__ import annotations

import logging
import warnings
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Generator
from unittest import mock

import pytest
from pytest import param

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.datasource.fluent import GxDatasourceWarning, SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.data_context import EphemeralDataContext


LOGGER = logging.getLogger(__name__)


@pytest.fixture
def create_engine_spy(mocker: MockerFixture) -> Generator[mock.MagicMock, None, None]:
    spy = mocker.spy(sa, "create_engine")
    yield spy
    if not spy.call_count:
        LOGGER.warning("SQLAlchemy create_engine was not called")


@pytest.fixture
def gx_sqlalchemy_execution_engine_spy(
    mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> Generator[mock.MagicMock, None, None]:
    """
    Mock the SQLDatasource.execution_engine_type property to return a spy so that what would be passed to
    the GX SqlAlchemyExecutionEngine constructor can be inspected.

    NOTE: This is not exactly what gets passed to the sqlalchemy.engine.create_engine() function, but it is close.
    """
    spy = mocker.Mock(spec=SqlAlchemyExecutionEngine)
    monkeypatch.setattr(SQLDatasource, "execution_engine_type", spy)
    yield spy
    if not spy.call_count:
        LOGGER.warning("SqlAlchemyExecutionEngine.__init__() was not called")


@pytest.fixture
def create_engine_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch sqlalchemy.create_engine to always return a in-memory sqlite engine."""
    in_memory_sqlite_engine = sa.create_engine("sqlite:///")

    def _fake_create_engine(*args, **kwargs) -> sa.engine.Engine:
        LOGGER.info(f"Mock create_engine called with {args=} {kwargs=}")
        return in_memory_sqlite_engine

    monkeypatch.setattr(sa, "create_engine", _fake_create_engine, raising=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "ds_kwargs",
    [
        param(
            dict(
                connection_string="sqlite:///",
            ),
            id="connection_string only",
        ),
        param(
            dict(
                connection_string="sqlite:///",
                kwargs={"isolation_level": "SERIALIZABLE"},
            ),
            id="no subs + kwargs",
        ),
        param(
            dict(
                connection_string="${MY_CONN_STR}",
                kwargs={"isolation_level": "SERIALIZABLE"},
            ),
            id="subs + kwargs",
        ),
        param(
            dict(
                connection_string="sqlite:///",
                create_temp_table=True,
            ),
            id="create_temp_table=True",
        ),
        param(
            dict(
                connection_string="sqlite:///",
                create_temp_table=False,
            ),
            id="create_temp_table=False",
        ),
    ],
)
class TestConfigPassthrough:
    def test_kwargs_passed_to_create_engine(
        self,
        create_engine_spy: mock.MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.sources.add_or_update_sql(name="my_datasource", **ds_kwargs)
        print(ds)
        ds.test_connection()

        create_engine_spy.assert_called_once_with(
            "sqlite:///",
            **{
                **ds.dict(include={"kwargs"}, exclude_unset=False)["kwargs"],
                **ds_kwargs.get("kwargs", {}),
            },
        )

    def test_ds_config_passed_to_gx_sqlalchemy_execution_engine(
        self,
        gx_sqlalchemy_execution_engine_spy: mock.MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.sources.add_or_update_sql(name="my_datasource", **ds_kwargs)
        print(ds)
        gx_execution_engine: SqlAlchemyExecutionEngine = ds.get_execution_engine()
        print(f"{gx_execution_engine=}")

        expected_args: dict[str, Any] = {
            # kwargs that we expect are passed to SqlAlchemyExecutionEngine
            # including datasource field default values
            **ds.dict(
                exclude_unset=False,
                exclude={"kwargs", *ds_kwargs.keys(), *ds._get_exec_engine_excludes()},
            ),
            **{k: v for k, v in ds_kwargs.items() if k not in ["kwargs"]},
            **ds_kwargs.get("kwargs", {}),
            # config substitution should have been performed
            **ds.dict(
                include={"connection_string"}, config_provider=ds._config_provider
            ),
        }
        assert "create_temp_table" in expected_args

        print(f"\nExpected SqlAlchemyExecutionEngine arguments:\n{pf(expected_args)}")
        gx_sqlalchemy_execution_engine_spy.assert_called_once_with(**expected_args)


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
            assert table_asset.table_name in table_names_in_dbms_schema
            assert table_asset.table_name in quoted_table_names


@pytest.mark.big
@pytest.mark.parametrize(
    ["connection_string", "suggested_datasource_class"],
    [
        ("gregshift://", None),
        ("sqlite:///", "SqliteDatasource"),
        ("snowflake+pyodbc://", "SnowflakeDatasource"),
        ("postgresql+psycopg2://bob:secret@localhost:5432/my_db", "PostgresDatasource"),
        ("${MY_PG_CONN_STR}", "PostgresDatasource"),
        ("databricks://", "DatabricksSQLDatasource"),
    ],
)
def test_specific_datasource_warnings(
    create_engine_fake: None,
    ephemeral_context_with_defaults: EphemeralDataContext,
    monkeypatch: pytest.MonkeyPatch,
    connection_string: str,
    suggested_datasource_class: str | None,
):
    """
    This test ensures that a warning is raised when a specific datasource class is suggested.
    """
    context = ephemeral_context_with_defaults
    monkeypatch.setenv(
        "MY_PG_CONN_STR", "postgresql://bob:secret@localhost:5432/bobs_db"
    )

    if suggested_datasource_class:
        with pytest.warns(GxDatasourceWarning, match=suggested_datasource_class):
            context.sources.add_sql(
                name="my_datasource", connection_string=connection_string
            )
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # should already be the default
            context.sources.add_sql(
                name="my_datasource", connection_string=connection_string
            ).test_connection()


@pytest.mark.unit
@pytest.mark.parametrize(
    "config",
    [
        {
            "name": "connection_string only",
            "connection_string": "sqlite:///",
        },
        {
            "name": "no subs + kwargs",
            "connection_string": "sqlite:///",
            "kwargs": {"isolation_level": "SERIALIZABLE"},
        },
        {
            "name": "subs + kwargs",
            "connection_string": "sqlite:///${MY_VAR}",
            "kwargs": {"isolation_level": "SERIALIZABLE"},
        },
    ],
    ids=lambda x: x["name"],
)
def test_recreate_from_dict(
    monkeypatch: pytest.MonkeyPatch,
    create_engine_fake: None,
    ephemeral_context_with_defaults: EphemeralDataContext,
    config: dict,
):
    """
    Test that .dict() method of a datasource can be fed back into the constructor to recreate the datasource.
    """
    monkeypatch.setenv("MY_VAR", "my_var_value")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        d1 = ephemeral_context_with_defaults.sources.add_sql(**config)
        ephemeral_context_with_defaults.delete_datasource(d1.name)
        d2 = ephemeral_context_with_defaults.sources.add_sql(**d1.dict())
    assert d1 == d2


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
