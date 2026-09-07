from __future__ import annotations

import logging
import warnings
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Generator
from unittest import mock

import pytest
from pytest import param

from great_expectations.compatibility import sqlalchemy
from great_expectations.datasource.fluent import GxDatasourceWarning, SQLDatasource
from great_expectations.datasource.fluent.batch_parameter_normalization import (
    numeric_parameter_names_of,
)
from great_expectations.datasource.fluent.sql_datasource import (
    DEFAULT_INITIAL_QUOTE_CHARACTERS,
    SqlPartitionerColumnValue,
    SqlPartitionerDatetimePart,
    SqlPartitionerDividedInteger,
    SqlPartitionerModInteger,
    SqlPartitionerMultiColumnValue,
    SqlPartitionerYear,
    SqlPartitionerYearAndMonth,
    SqlPartitionerYearAndMonthAndDay,
    TableAsset,
    to_lower_if_not_quoted,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.data_context import (
        EphemeralDataContext,
    )


LOGGER = logging.getLogger(__name__)


@pytest.fixture
def gx_sqlalchemy_execution_engine_spy(
    mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> Generator[mock.MagicMock, None, None]:  # noqa: TID251 # FIXME CoP
    """
    Mock the SQLDatasource.execution_engine_type property to return a spy so that what would be passed to
    the GX SqlAlchemyExecutionEngine constructor can be inspected.

    NOTE: This is not exactly what gets passed to the sqlalchemy.engine.create_engine() function, but it is close.
    """  # noqa: E501 # FIXME CoP
    spy = mocker.Mock(spec=SqlAlchemyExecutionEngine)
    monkeypatch.setattr(SQLDatasource, "execution_engine_type", spy)
    yield spy
    if not spy.call_count:
        LOGGER.warning("SqlAlchemyExecutionEngine.__init__() was not called")


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
class TestConfigPasstrough:
    def test_kwargs_passed_to_create_engine(
        self,
        create_engine_spy: mock.MagicMock,  # noqa: TID251 # FIXME CoP
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.data_sources.add_or_update_sql(name="my_datasource", **ds_kwargs)
        print(ds)
        ds.test_connection()

        create_engine_spy.assert_called_once_with(
            "sqlite:///",
            **{
                **ds.dict(include={"kwargs"}, exclude_unset=False)["kwargs"],
                **ds_kwargs.get("kwargs", {}),
            },
        )

    def test_execution_engine_is_cached_across_calls(
        self,
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        """The execution engine, and the SQLAlchemy engine it owns, are built once.

        Every validation goes through ``get_execution_engine``. If the cache never hits, each
        validation builds a new SQLAlchemy engine and connection pool, and the pooled
        connection of every abandoned engine lingers on the server until garbage collection.
        """
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.data_sources.add_or_update_sql(name="my_datasource", **ds_kwargs)

        first = ds.get_execution_engine()
        second = ds.get_execution_engine()

        assert second is first

    def test_changed_config_rebuilds_the_execution_engine(
        self,
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        """Caching must not outlive the configuration the engine was built from.

        The kwargs comparison is the only thing separating a cache hit from a rebuild, so a
        cache that always hits is as wrong as one that never does: it would serve an engine
        bound to a connection string the datasource no longer carries.
        """
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.data_sources.add_or_update_sql(name="my_datasource", **ds_kwargs)

        first = ds.get_execution_engine()
        ds.create_temp_table = not ds.create_temp_table

        assert ds.get_execution_engine() is not first

    def test_failed_rebuild_does_not_suppress_the_next_one(
        self,
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        """A rebuild that raises must not leave the new kwargs cached against the old engine.

        Construction can fail on a configuration change alone - a rotated connection string
        naming a driver that is not installed, an unreachable host. If the kwargs were cached
        before the constructor ran, the next call would compare equal, find the previous
        engine still in place, and silently return an engine bound to the old configuration.
        """
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.data_sources.add_or_update_sql(name="my_datasource", **ds_kwargs)
        first = ds.get_execution_engine()

        def _raise_on_construction(*args, **kwargs):
            raise RuntimeError("could not connect")

        ds.create_temp_table = not ds.create_temp_table
        with mock.patch.object(SQLDatasource, "execution_engine_type", _raise_on_construction):
            with pytest.raises(RuntimeError, match="could not connect"):
                ds.get_execution_engine()

        assert ds.get_execution_engine() is not first

    def test_ds_config_passed_to_gx_sqlalchemy_execution_engine(
        self,
        gx_sqlalchemy_execution_engine_spy: mock.MagicMock,  # noqa: TID251 # FIXME CoP
        monkeypatch: pytest.MonkeyPatch,
        ephemeral_context_with_defaults: EphemeralDataContext,
        ds_kwargs: dict,
        filter_gx_datasource_warnings: None,
    ):
        monkeypatch.setenv("MY_CONN_STR", "sqlite:///")

        context = ephemeral_context_with_defaults
        ds = context.data_sources.add_or_update_sql(name="my_datasource", **ds_kwargs)
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
            **ds.dict(include={"connection_string"}, config_provider=ds._config_provider),
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
    """  # noqa: E501 # FIXME CoP
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
        )
        assert table_asset.table_name not in table_names_in_dbms_schema


@pytest.mark.unit
def test_table_quoted_name_type_all_upper_case_normalizion_is_noop():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that all upper case entity usage does not undergo any conversion.
    """  # noqa: E501 # FIXME CoP
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
    """  # noqa: E501 # FIXME CoP
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
    monkeypatch.setenv("MY_PG_CONN_STR", "postgresql://bob:secret@localhost:5432/bobs_db")

    if suggested_datasource_class:
        with pytest.warns(GxDatasourceWarning, match=suggested_datasource_class):
            context.data_sources.add_sql(name="my_datasource", connection_string=connection_string)
    else:
        with warnings.catch_warnings():
            context.data_sources.add_sql(
                name="my_datasource", connection_string=connection_string
            ).test_connection()


@pytest.mark.unit
@pytest.mark.parametrize(
    ["input_", "expected_output", "quote_characters"],
    [
        ("my_schema", "my_schema", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("MY_SCHEMA", "my_schema", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("My_Schema", "my_schema", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ('"my_schema"', '"my_schema"', DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ('"MY_SCHEMA"', '"MY_SCHEMA"', DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ('"My_Schema"', '"My_Schema"', DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("'my_schema'", "'my_schema'", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("'MY_SCHEMA'", "'MY_SCHEMA'", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("'My_Schema'", "'My_Schema'", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        (None, None, DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("", "", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("`My_Schema`", "`My_Schema`", DEFAULT_INITIAL_QUOTE_CHARACTERS),
        ("'My_Schema'", "'my_schema'", ("`")),
        ("[My_Schema]", "[My_Schema]", DEFAULT_INITIAL_QUOTE_CHARACTERS),
    ],
    ids=str,
)
def test_to_lower_if_not_quoted(
    input_: str | None, expected_output: str | None, quote_characters: tuple[str, ...]
):
    assert to_lower_if_not_quoted(input_, quote_characters=quote_characters) == expected_output


@pytest.fixture
def sql_datasource_with_schema(
    sql_datasource_table_asset_test_connection_noop: SQLDatasource,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> SQLDatasource:
    monkeypatch.setattr(
        type(sql_datasource_table_asset_test_connection_noop),
        "schema_",
        property(lambda self: request.param),
    )
    return sql_datasource_table_asset_test_connection_noop


@pytest.mark.unit
class TestTableAsset:
    @pytest.mark.parametrize(
        "sql_datasource_with_schema", ["my_schema", "MY_SCHEMA", "My_Schema"], indirect=True
    )
    def test_unquoted_schema_names_are_added_as_lowercase(
        self,
        sql_datasource_with_schema: SQLDatasource,
    ):
        table_asset = sql_datasource_with_schema.add_table_asset(
            name="my_table_asset",
            table_name="my_table",
        )
        schema = sql_datasource_with_schema.schema_
        assert schema is not None
        assert table_asset._effective_schema_name == schema.lower()

    @pytest.mark.parametrize("table_name", ["my_table", "MY_TABLE", "My_Table"])
    def test_unquoted_table_names_are_unquoted(
        self,
        sql_datasource_table_asset_test_connection_noop: SQLDatasource,
        table_name: str,
    ):
        my_datasource: SQLDatasource = sql_datasource_table_asset_test_connection_noop

        table_asset = my_datasource.add_table_asset(
            name="my_table_asset",
            table_name=table_name,
        )
        assert isinstance(table_asset.table_name, sqlalchemy.quoted_name)
        assert table_asset.table_name == table_name
        assert not table_asset.table_name.quote

    @pytest.mark.parametrize(
        "sql_datasource_with_schema",
        [
            '"my_schema"',
            '"MY_SCHEMA"',
            '"My_Schema"',
            "'my_schema'",
            "'MY_SCHEMA'",
            "'My_Schema'",
            "`My_Schema`",
            "[My_Schema]",
        ],
        indirect=True,
    )
    def test_quoted_schema_names_are_not_modified(
        self,
        sql_datasource_with_schema: SQLDatasource,
    ):
        table_asset = sql_datasource_with_schema.add_table_asset(
            name="my_table_asset",
            table_name="my_table",
        )
        assert table_asset._effective_schema_name == sql_datasource_with_schema.schema_

    @pytest.mark.parametrize(
        "table_name",
        [
            '"my_table"',
            '"MY_TABLE"',
            '"My_Table"',
            "'my_table'",
            "'MY_TABLE'",
            "'My_Table'",
            "`my_table`",
            "[my_table]",
        ],
    )
    def test_quoted_table_names_are_quoted(
        self,
        sql_datasource_table_asset_test_connection_noop: SQLDatasource,
        table_name: str,
    ):
        my_datasource: SQLDatasource = sql_datasource_table_asset_test_connection_noop

        table_asset = my_datasource.add_table_asset(
            name="my_table_asset",
            table_name=table_name,
        )
        assert isinstance(table_asset.table_name, sqlalchemy.quoted_name)
        assert table_asset.table_name == table_name[1:-1]
        assert table_asset.table_name.quote

    @pytest.mark.parametrize(
        "table_name,serialized_name",
        [
            pytest.param('"my_table"', '"my_table"'),
            pytest.param('"MY_TABLE"', '"MY_TABLE"'),
            pytest.param('"My_Table"', '"My_Table"'),
            pytest.param("'my_table'", "'my_table'"),
            pytest.param("'MY_TABLE'", "'MY_TABLE'"),
            pytest.param("'My_Table'", "'My_Table'"),
            pytest.param("[My_Table]", "[My_Table]"),
            pytest.param("`My_Table`", "`My_Table`"),
            pytest.param("my_table", "my_table"),
            pytest.param("MY_TABLE", "MY_TABLE"),
            pytest.param("My_Table", "My_Table"),
        ],
    )
    def test_table_name_serialization_preserves_quotes(
        self,
        table_name: str,
        serialized_name: str,
    ):
        table_asset = TableAsset(name="my_table_asset", table_name=table_name)

        with mock.patch(
            "great_expectations.datasource.fluent.sql_datasource.TableAsset.datasource",
            new_callable=mock.PropertyMock,
            return_value=SQLDatasource(
                name="my_snowflake_datasource",
                connection_string="snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>",
            ),
        ):
            serialized = table_asset.dict()
            assert serialized["table_name"] == serialized_name


@pytest.mark.unit
@pytest.mark.parametrize(
    "partitioner",
    [
        param(SqlPartitionerYear(column_name="ts"), id="year"),
        param(SqlPartitionerYearAndMonth(column_name="ts"), id="year-and-month"),
        param(SqlPartitionerYearAndMonthAndDay(column_name="ts"), id="year-and-month-and-day"),
        param(
            SqlPartitionerDatetimePart(column_name="ts", datetime_parts=["year", "month"]),
            id="datetime-part",
        ),
        param(SqlPartitionerDividedInteger(column_name="id", divisor=10), id="divided-integer"),
        param(SqlPartitionerModInteger(column_name="id", mod=10), id="mod-integer"),
    ],
)
def test_numeric_param_names_matches_param_names_for_numeric_sql_partitioners(partitioner):
    """Numeric SQL partitioner kinds declare exactly their own parameter names as numeric.

    This is what lets digit-string batch parameters (e.g. "2024") be coerced to int for
    these kinds without a partitioner-specific special case.
    """
    assert list(partitioner.numeric_param_names) == list(partitioner.param_names)
    assert numeric_parameter_names_of(partitioner) == frozenset(partitioner.param_names)


@pytest.mark.unit
def test_sql_column_value_partitioner_declares_no_numeric_param_names():
    """A column-value partitioner declares nothing, so a column literally named "year" is
    never coerced to int -- the exemption is closed by construction, not a special case.
    """
    partitioner = SqlPartitionerColumnValue(column_name="year")
    assert numeric_parameter_names_of(partitioner) == frozenset()


@pytest.mark.unit
def test_sql_multi_column_value_partitioner_declares_no_numeric_param_names():
    """Multi-column value partitioning is exempt for the same reason as the single-column
    kind: the parameters are named after columns, so any of them may legitimately carry a
    non-numeric value even when the column is named "year".
    """
    partitioner = SqlPartitionerMultiColumnValue(column_names=["year", "month"])
    assert numeric_parameter_names_of(partitioner) == frozenset()


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
