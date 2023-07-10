from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, overload

import numpy as np
from dateutil.parser import parse
from packaging import version

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import aws, sqlalchemy, trino
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,  # noqa: TCH001
    SqlAlchemyExecutionEngine,  # noqa: TCH001
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.execution_engine.util import check_sql_engine_dialect
from great_expectations.util import get_sqlalchemy_inspector

try:
    import psycopg2  # noqa: F401
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2  # noqa: TID251
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None

try:
    import snowflake
except ImportError:
    snowflake = None


logger = logging.getLogger(__name__)

try:
    import sqlalchemy_dremio.pyodbc

    sqlalchemy.registry.register("dremio", "sqlalchemy_dremio.pyodbc", "dialect")
except ImportError:
    sqlalchemy_dremio = None

try:
    import clickhouse_sqlalchemy
except ImportError:
    clickhouse_sqlalchemy = None

_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"

from great_expectations.compatibility import bigquery as sqla_bigquery
from great_expectations.compatibility.bigquery import bigquery_types_tuple

if TYPE_CHECKING:
    import pandas as pd

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None
    teradatatypes = None


def get_dialect_regex_expression(  # noqa: C901, PLR0911, PLR0912, PLR0915
    column, regex, dialect, positive=True
):
    try:
        # postgres
        if issubclass(dialect.dialect, sa.dialects.postgresql.dialect):
            if positive:
                return sqlalchemy.BinaryExpression(
                    column, sqlalchemy.literal(regex), sqlalchemy.custom_op("~")
                )
            else:
                return sqlalchemy.BinaryExpression(
                    column, sqlalchemy.literal(regex), sqlalchemy.custom_op("!~")
                )
    except AttributeError:
        pass

    # redshift
    # noinspection PyUnresolvedReferences
    if hasattr(dialect, "RedshiftDialect") or (
        aws.redshiftdialect
        and issubclass(dialect.dialect, aws.redshiftdialect.RedshiftDialect)
    ):
        if positive:
            return sqlalchemy.BinaryExpression(
                column, sqlalchemy.literal(regex), sqlalchemy.custom_op("~")
            )
        else:
            return sqlalchemy.BinaryExpression(
                column, sqlalchemy.literal(regex), sqlalchemy.custom_op("!~")
            )
    else:
        pass

    try:
        # MySQL
        if issubclass(dialect.dialect, sa.dialects.mysql.dialect):
            if positive:
                return sqlalchemy.BinaryExpression(
                    column, sqlalchemy.literal(regex), sqlalchemy.custom_op("REGEXP")
                )
            else:
                return sqlalchemy.BinaryExpression(
                    column,
                    sqlalchemy.literal(regex),
                    sqlalchemy.custom_op("NOT REGEXP"),
                )
    except AttributeError:
        pass

    try:
        # Snowflake
        if issubclass(
            dialect.dialect,
            snowflake.sqlalchemy.snowdialect.SnowflakeDialect,
        ):
            # if positive:
            #     return BinaryExpression(column, literal(regex), custom_op("RLIKE"))
            # else:
            #     return BinaryExpression(column, literal(regex), custom_op("NOT RLIKE"))

            # While the snowflake docs mention having regex-related functions, they don't
            # seem to work with the Python driver
            # https://docs.snowflake.com/en/sql-reference/functions/regexp.html
            return None
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    try:
        # Bigquery
        if hasattr(dialect, "BigQueryDialect"):
            if positive:
                return sa.func.REGEXP_CONTAINS(column, sqlalchemy.literal(regex))
            else:
                return sa.not_(
                    sa.func.REGEXP_CONTAINS(column, sqlalchemy.literal(regex))
                )
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        logger.debug(
            "Unable to load BigQueryDialect dialect while running get_dialect_regex_expression in expectations.metrics.util",
            exc_info=True,
        )
        pass

    try:
        # Trino
        # noinspection PyUnresolvedReferences
        if hasattr(dialect, "TrinoDialect") or (
            trino.trinodialect and isinstance(dialect, trino.trinodialect.TrinoDialect)
        ):
            if positive:
                return sa.func.regexp_like(column, sqlalchemy.literal(regex))
            else:
                return sa.not_(sa.func.regexp_like(column, sqlalchemy.literal(regex)))
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    try:
        # Clickhouse
        # noinspection PyUnresolvedReferences
        if hasattr(dialect, "ClickHouseDialect") or isinstance(
            dialect, clickhouse_sqlalchemy.drivers.base.ClickHouseDialect
        ):
            if positive:
                return sa.func.regexp_like(column, sqlalchemy.literal(regex))
            else:
                return sa.not_(sa.func.regexp_like(column, sqlalchemy.literal(regex)))
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass
    try:
        # Dremio
        if hasattr(dialect, "DremioDialect"):
            if positive:
                return sa.func.REGEXP_MATCHES(column, sqlalchemy.literal(regex))
            else:
                return sa.not_(
                    sa.func.REGEXP_MATCHES(column, sqlalchemy.literal(regex))
                )
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    try:
        # Teradata
        if issubclass(dialect.dialect, teradatasqlalchemy.dialect.TeradataDialect):
            if positive:
                return (
                    sa.func.REGEXP_SIMILAR(
                        column, sqlalchemy.literal(regex), sqlalchemy.literal("i")
                    )
                    == 1
                )
            else:
                return (
                    sa.func.REGEXP_SIMILAR(
                        column, sqlalchemy.literal(regex), sqlalchemy.literal("i")
                    )
                    == 0
                )
    except (AttributeError, TypeError):
        pass

    try:
        # sqlite
        # regex_match for sqlite introduced in sqlalchemy v1.4
        if issubclass(dialect.dialect, sa.dialects.sqlite.dialect) and version.parse(
            sa.__version__
        ) >= version.parse("1.4"):
            if positive:
                return column.regexp_match(sqlalchemy.literal(regex))
            else:
                return sa.not_(column.regexp_match(sqlalchemy.literal(regex)))
        else:
            logger.debug(
                "regex_match is only enabled for sqlite when SQLAlchemy version is >= 1.4",
                exc_info=True,
            )
            pass
    except AttributeError:
        pass

    return None


def _get_dialect_type_module(dialect=None):
    if dialect is None:
        logger.warning(
            "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
        )
        return sa

    # Redshift does not (yet) export types to top level; only recognize base SA types
    # noinspection PyUnresolvedReferences
    if aws.redshiftdialect and isinstance(
        dialect,
        aws.redshiftdialect.RedshiftDialect,
    ):
        return dialect.sa

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if (
            isinstance(
                dialect,
                sqla_bigquery.BigQueryDialect,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    # Teradata types module
    try:
        if (
            issubclass(
                dialect,
                teradatasqlalchemy.dialect.TeradataDialect,
            )
            and teradatatypes is not None
        ):
            return teradatatypes
    except (TypeError, AttributeError):
        pass

    return dialect


def attempt_allowing_relative_error(dialect):
    # noinspection PyUnresolvedReferences
    detected_redshift: bool = aws.redshiftdialect and check_sql_engine_dialect(
        actual_sql_engine_dialect=dialect,
        candidate_sql_engine_dialect=aws.redshiftdialect.RedshiftDialect,
    )
    # noinspection PyTypeChecker
    detected_psycopg2: bool = (
        sqlalchemy_psycopg2 is not None
        and check_sql_engine_dialect(
            actual_sql_engine_dialect=dialect,
            candidate_sql_engine_dialect=sqlalchemy_psycopg2.PGDialect_psycopg2,
        )
    )
    return detected_redshift or detected_psycopg2


def is_column_present_in_table(
    engine: sqlalchemy.Engine,
    table_selectable: sqlalchemy.Select,
    column_name: str,
    schema_name: Optional[str] = None,
) -> bool:
    all_columns_metadata: List[Dict[str, Any]] = (
        get_sqlalchemy_column_metadata(
            engine=engine, table_selectable=table_selectable, schema_name=schema_name
        )
        or []
    )
    # Purposefully do not check for a NULL "all_columns_metadata" to insure that it must never happen.
    column_names: List[str] = [col_md["name"] for col_md in all_columns_metadata]
    return column_name in column_names


def get_sqlalchemy_column_metadata(
    engine: sqlalchemy.Engine,
    table_selectable: sqlalchemy.Select,
    schema_name: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    try:
        columns: List[Dict[str, Any]]

        inspector: sqlalchemy.reflection.Inspector = get_sqlalchemy_inspector(engine)
        try:
            # if a custom query was passed
            if sqlalchemy.TextClause and isinstance(
                table_selectable, sqlalchemy.TextClause
            ):
                if hasattr(table_selectable, "selected_columns"):
                    columns = table_selectable.selected_columns.columns
                else:
                    columns = table_selectable.columns().columns
            else:
                columns = inspector.get_columns(
                    table_selectable,
                    schema=schema_name,
                )
        except (
            KeyError,
            AttributeError,
            sa.exc.NoSuchTableError,
            sa.exc.ProgrammingError,
        ):
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            columns = column_reflection_fallback(
                selectable=table_selectable,
                dialect=engine.dialect,
                sqlalchemy_engine=engine,
            )

        # Use fallback because for mssql and trino reflection mechanisms do not throw an error but return an empty list
        if len(columns) == 0:
            columns = column_reflection_fallback(
                selectable=table_selectable,
                dialect=engine.dialect,
                sqlalchemy_engine=engine,
            )

        return columns
    except AttributeError as e:
        logger.debug(f"Error while introspecting columns: {str(e)}")
        return None


def column_reflection_fallback(
    selectable: sqlalchemy.Select,
    dialect: sqlalchemy.Dialect,
    sqlalchemy_engine: sqlalchemy.Engine,
) -> List[Dict[str, str]]:
    """If we can't reflect the table, use a query to at least get column names."""

    if isinstance(sqlalchemy_engine.engine, sqlalchemy.Engine):
        connection = sqlalchemy_engine.engine.connect()
    else:
        connection = sqlalchemy_engine.engine

    # with sqlalchemy_engine.begin() as connection:
    with connection:
        col_info_dict_list: List[Dict[str, str]]
        # noinspection PyUnresolvedReferences
        if dialect.name.lower() == "mssql":
            # Get column names and types from the database
            # Reference: https://dataedo.com/kb/query/sql-server/list-table-columns-in-database
            tables_table_clause: sqlalchemy.TableClause = sa.table(
                "tables",
                sa.column("object_id"),
                sa.column("schema_id"),
                sa.column("name"),
                schema="sys",
            ).alias("sys_tables_table_clause")
            tables_table_query: sqlalchemy.Select = (
                sa.select(
                    tables_table_clause.columns.object_id.label("object_id"),
                    sa.func.schema_name(tables_table_clause.columns.schema_id).label(
                        "schema_name"
                    ),
                    tables_table_clause.columns.name.label("table_name"),
                )
                .select_from(tables_table_clause)
                .alias("sys_tables_table_subquery")
            )
            columns_table_clause: sqlalchemy.TableClause = sa.table(
                "columns",
                sa.column("object_id"),
                sa.column("user_type_id"),
                sa.column("column_id"),
                sa.column("name"),
                sa.column("max_length"),
                sa.column("precision"),
                schema="sys",
            ).alias("sys_columns_table_clause")
            columns_table_query: sqlalchemy.Select = (
                sa.select(
                    columns_table_clause.columns.object_id.label("object_id"),
                    columns_table_clause.columns.user_type_id.label("user_type_id"),
                    columns_table_clause.columns.column_id.label("column_id"),
                    columns_table_clause.columns.name.label("column_name"),
                    columns_table_clause.columns.max_length.label("column_max_length"),
                    columns_table_clause.columns.precision.label("column_precision"),
                )
                .select_from(columns_table_clause)
                .alias("sys_columns_table_subquery")
            )
            types_table_clause: sqlalchemy.TableClause = sa.table(
                "types",
                sa.column("user_type_id"),
                sa.column("name"),
                schema="sys",
            ).alias("sys_types_table_clause")
            types_table_query: sqlalchemy.Select = (
                sa.select(
                    types_table_clause.columns.user_type_id.label("user_type_id"),
                    types_table_clause.columns.name.label("column_data_type"),
                )
                .select_from(types_table_clause)
                .alias("sys_types_table_subquery")
            )
            inner_join_conditions: sqlalchemy.BinaryExpression = sa.and_(
                *(tables_table_query.c.object_id == columns_table_query.c.object_id,)
            )
            outer_join_conditions: sqlalchemy.BinaryExpression = sa.and_(
                *(
                    columns_table_query.columns.user_type_id
                    == types_table_query.columns.user_type_id,
                )
            )
            col_info_query = (
                sa.select(
                    tables_table_query.c.schema_name,
                    tables_table_query.c.table_name,
                    columns_table_query.c.column_id,
                    columns_table_query.c.column_name,
                    types_table_query.c.column_data_type,
                    columns_table_query.c.column_max_length,
                    columns_table_query.c.column_precision,
                )
                .select_from(
                    tables_table_query.join(
                        right=columns_table_query,
                        onclause=inner_join_conditions,
                        isouter=False,
                    ).join(
                        right=types_table_query,
                        onclause=outer_join_conditions,
                        isouter=True,
                    )
                )
                .where(tables_table_query.c.table_name == selectable.name)
                .order_by(
                    tables_table_query.c.schema_name.asc(),
                    tables_table_query.c.table_name.asc(),
                    columns_table_query.c.column_id.asc(),
                )
            )
            col_info_tuples_list: List[tuple] = connection.execute(
                col_info_query
            ).fetchall()
            # type_module = _get_dialect_type_module(dialect=dialect)
            col_info_dict_list = [
                {
                    "name": column_name,
                    # "type": getattr(type_module, column_data_type.upper())(),
                    "type": column_data_type.upper(),
                }
                for schema_name, table_name, column_id, column_name, column_data_type, column_max_length, column_precision in col_info_tuples_list
            ]
        elif dialect.name.lower() == "trino":
            try:
                table_name = selectable.name
            except AttributeError:
                table_name = selectable
                if str(table_name).lower().startswith("select"):
                    rx = re.compile(r"^.* from ([\S]+)", re.I)
                    match = rx.match(str(table_name).replace("\n", ""))
                    if match:
                        table_name = match.group(1)
            schema_name = sqlalchemy_engine.dialect.default_schema_name

            tables_table: sa.Table = sa.Table(
                "tables",
                sa.MetaData(),
                schema="information_schema",
            )
            tables_table_query = (
                sa.select(
                    sa.column("table_schema").label("schema_name"),
                    sa.column("table_name").label("table_name"),
                )
                .select_from(tables_table)
                .alias("information_schema_tables_table")
            )
            columns_table: sa.Table = sa.Table(
                "columns",
                sa.MetaData(),
                schema="information_schema",
            )
            columns_table_query = (
                sa.select(
                    sa.column("column_name").label("column_name"),
                    sa.column("table_name").label("table_name"),
                    sa.column("table_schema").label("schema_name"),
                    sa.column("data_type").label("column_data_type"),
                )
                .select_from(columns_table)
                .alias("information_schema_columns_table")
            )
            conditions = sa.and_(
                *(
                    tables_table_query.c.table_name == columns_table_query.c.table_name,
                    tables_table_query.c.schema_name
                    == columns_table_query.c.schema_name,
                )
            )
            col_info_query = (
                sa.select(
                    tables_table_query.c.schema_name,
                    tables_table_query.c.table_name,
                    columns_table_query.c.column_name,
                    columns_table_query.c.column_data_type,
                )
                .select_from(
                    tables_table_query.join(
                        right=columns_table_query, onclause=conditions, isouter=False
                    )
                )
                .where(
                    sa.and_(
                        *(
                            tables_table_query.c.table_name == table_name,
                            tables_table_query.c.schema_name == schema_name,
                        )
                    )
                )
                .order_by(
                    tables_table_query.c.schema_name.asc(),
                    tables_table_query.c.table_name.asc(),
                    columns_table_query.c.column_name.asc(),
                )
                .alias("column_info")
            )

            # in sqlalchemy > 2.0.0 this is a Subquery, which we need to convert into a Selectable
            if not col_info_query.supports_execution:
                col_info_query = sa.select(col_info_query)

            col_info_tuples_list = connection.execute(col_info_query).fetchall()
            # type_module = _get_dialect_type_module(dialect=dialect)
            col_info_dict_list = [
                {
                    "name": column_name,
                    "type": column_data_type.upper(),
                }
                for schema_name, table_name, column_name, column_data_type in col_info_tuples_list
            ]
        else:
            # if a custom query was passed
            if sqlalchemy.TextClause and isinstance(selectable, sqlalchemy.TextClause):
                query: sqlalchemy.TextClause = selectable
            else:
                # noinspection PyUnresolvedReferences
                if dialect.name.lower() == GXSqlDialect.REDSHIFT:  # noqa: PLR5501
                    # Redshift needs temp tables to be declared as text
                    query = (
                        sa.select(sa.text("*"))
                        .select_from(sa.text(selectable))
                        .limit(1)
                    )
                else:
                    query = sa.select(sa.text("*")).select_from(selectable).limit(1)

            result_object = connection.execute(query)
            # noinspection PyProtectedMember
            col_names: List[str] = result_object._metadata.keys
            col_info_dict_list = [{"name": col_name} for col_name in col_names]
        return col_info_dict_list


@overload
def get_dbms_compatible_column_names(
    column_names: str,
    batch_columns_list: List[str | sqlalchemy.quoted_name],
    error_message_template: str = ...,
) -> str | sqlalchemy.quoted_name:
    ...


@overload
def get_dbms_compatible_column_names(
    column_names: List[str],
    batch_columns_list: List[str | sqlalchemy.quoted_name],
    error_message_template: str = ...,
) -> List[str | sqlalchemy.quoted_name]:
    ...


def get_dbms_compatible_column_names(
    column_names: List[str] | str,
    batch_columns_list: List[str | sqlalchemy.quoted_name],
    error_message_template: str = 'Error: The column "{column_name:s}" in BatchData does not exist.',
) -> List[str | sqlalchemy.quoted_name] | str | sqlalchemy.quoted_name:
    """
    Case non-sensitivity is expressed in upper case by common DBMS backends and in lower case by SQLAlchemy, with any
    deviations enclosed with double quotes.

    SQLAlchemy enables correct translation to/from DBMS backends through "sqlalchemy.sql.elements.quoted_name" class
    where necessary by insuring that column names of correct type (i.e., "str" or "sqlalchemy.sql.elements.quoted_name")
    are returned by "sqlalchemy.inspect(sqlalchemy.engine.Engine).get_columns(table_name, schema)" ("table.columns"
    metric is based on this method).  Columns of precise type (string or "quoted_name" as appropriate) are returned.

    Args:
        column_names: Single string-valued column name or list of string-valued column names
        batch_columns_list: Properly typed column names (output of "table.columns" metric)
        error_message_template: String template to output error message if any column cannot be found in "Batch" object.

    Returns:
        Single property-typed column name object or list of property-typed column name objects (depending on input).
    """
    normalized_typed_batch_columns_mappings: List[
        Tuple[str, str | sqlalchemy.quoted_name]
    ] = (
        _verify_column_names_exist_and_get_normalized_typed_column_names_map(
            column_names=column_names,
            batch_columns_list=batch_columns_list,
            error_message_template=error_message_template,
        )
        or []
    )

    element: Tuple[str, str | sqlalchemy.quoted_name]
    typed_batch_column_names_list: List[str | sqlalchemy.quoted_name] = [
        element[1] for element in normalized_typed_batch_columns_mappings
    ]
    if isinstance(column_names, list):
        return typed_batch_column_names_list

    return typed_batch_column_names_list[0]


def verify_column_names_exist(
    column_names: List[str] | str,
    batch_columns_list: List[str | sqlalchemy.quoted_name],
    error_message_template: str = 'Error: The column "{column_name:s}" in BatchData does not exist.',
) -> None:
    _ = _verify_column_names_exist_and_get_normalized_typed_column_names_map(
        column_names=column_names,
        batch_columns_list=batch_columns_list,
        error_message_template=error_message_template,
        verify_only=True,
    )


def _verify_column_names_exist_and_get_normalized_typed_column_names_map(
    column_names: List[str] | str,
    batch_columns_list: List[str | sqlalchemy.quoted_name],
    error_message_template: str = 'Error: The column "{column_name:s}" in BatchData does not exist.',
    verify_only: bool = False,
) -> List[Tuple[str, str | sqlalchemy.quoted_name]] | None:
    """
    Insures that column name or column names (supplied as argument using "str" representation) exist in "Batch" object.

    Args:
        column_names: Single string-valued column name or list of string-valued column names
        batch_columns_list: Properly typed column names (output of "table.columns" metric)
        verify_only: Perform verification only (do not return normalized typed column names)
        error_message_template: String template to output error message if any column cannot be found in "Batch" object.

    Returns:
        List of tuples having mapping from string-valued column name to typed column name; None if "verify_only" is set.
    """
    column_names_list: List[str]
    if isinstance(column_names, list):
        column_names_list = column_names
    else:
        column_names_list = [column_names]

    def _get_normalized_column_name_mapping_if_exists(
        column_name: str,
    ) -> Tuple[str, str | sqlalchemy.quoted_name] | None:
        typed_column_name_cursor: str | sqlalchemy.quoted_name
        for typed_column_name_cursor in batch_columns_list:
            if (
                (type(typed_column_name_cursor) == str)
                and (column_name.casefold() == typed_column_name_cursor.casefold())
            ) or (column_name == str(typed_column_name_cursor)):
                return column_name, typed_column_name_cursor

        return None

    normalized_batch_columns_mappings: List[
        Tuple[str, str | sqlalchemy.quoted_name]
    ] = []

    normalized_column_name_mapping: Tuple[str, str | sqlalchemy.quoted_name] | None
    column_name: str
    for column_name in column_names_list:
        normalized_column_name_mapping = _get_normalized_column_name_mapping_if_exists(
            column_name=column_name
        )
        if normalized_column_name_mapping is None:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=error_message_template.format(column_name=column_name)
            )
        else:
            if not verify_only:  # noqa: PLR5501
                normalized_batch_columns_mappings.append(normalized_column_name_mapping)

    return None if verify_only else normalized_batch_columns_mappings


def parse_value_set(value_set):
    parsed_value_set = [
        parse(value) if isinstance(value, str) else value for value in value_set
    ]
    return parsed_value_set


def get_dialect_like_pattern_expression(  # noqa: C901, PLR0912
    column, dialect, like_pattern, positive=True
):
    dialect_supported: bool = False

    try:
        # Bigquery
        if hasattr(dialect, "BigQueryDialect"):
            dialect_supported = True
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    if hasattr(dialect, "dialect"):
        if issubclass(
            dialect.dialect,
            (
                sa.dialects.sqlite.dialect,
                sa.dialects.postgresql.dialect,
                sa.dialects.mysql.dialect,
                sa.dialects.mssql.dialect,
            ),
        ):
            dialect_supported = True

    try:
        if hasattr(dialect, "RedshiftDialect"):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass

    # noinspection PyUnresolvedReferences
    if aws.redshiftdialect and isinstance(dialect, aws.redshiftdialect.RedshiftDialect):
        dialect_supported = True
    else:
        pass

    try:
        # noinspection PyUnresolvedReferences
        if hasattr(dialect, "TrinoDialect") or (
            trino.trinodialect and isinstance(dialect, trino.trinodialect.TrinoDialect)
        ):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass

    try:
        # noinspection PyUnresolvedReferences
        if hasattr(dialect, "ClickhouseDialect") or (
            trino.trinodrivers
            and isinstance(dialect, trino.trinodrivers.base.ClickhouseDialect)
        ):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass
    try:
        if hasattr(dialect, "SnowflakeDialect"):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass

    try:
        if hasattr(dialect, "DremioDialect"):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass

    try:
        if issubclass(dialect.dialect, teradatasqlalchemy.dialect.TeradataDialect):
            dialect_supported = True
    except (AttributeError, TypeError):
        pass

    if dialect_supported:
        try:
            if positive:
                return column.like(sqlalchemy.literal(like_pattern))
            else:
                return sa.not_(column.like(sqlalchemy.literal(like_pattern)))
        except AttributeError:
            pass

    return None


def validate_distribution_parameters(  # noqa: C901, PLR0912, PLR0915
    distribution, params
):
    """Ensures that necessary parameters for a distribution are present and that all parameters are sensical.

       If parameters necessary to construct a distribution are missing or invalid, this function raises ValueError\
       with an informative description. Note that 'loc' and 'scale' are optional arguments, and that 'scale'\
       must be positive.

       Args:
           distribution (string): \
               The scipy distribution name, e.g. normal distribution is 'norm'.
           params (dict or list): \
               The distribution shape parameters in a named dictionary or positional list form following the scipy \
               cdf argument scheme.

               params={'mean': 40, 'std_dev': 5} or params=[40, 5]

       Exceptions:
           ValueError: \
               With an informative description, usually when necessary parameters are omitted or are invalid.

    """

    norm_msg = (
        "norm distributions require 0 parameters and optionally 'mean', 'std_dev'."
    )
    beta_msg = "beta distributions require 2 positive parameters 'alpha', 'beta' and optionally 'loc', 'scale'."
    gamma_msg = "gamma distributions require 1 positive parameter 'alpha' and optionally 'loc','scale'."
    # poisson_msg = "poisson distributions require 1 positive parameter 'lambda' and optionally 'loc'."
    uniform_msg = (
        "uniform distributions require 0 parameters and optionally 'loc', 'scale'."
    )
    chi2_msg = "chi2 distributions require 1 positive parameter 'df' and optionally 'loc', 'scale'."
    expon_msg = (
        "expon distributions require 0 parameters and optionally 'loc', 'scale'."
    )

    if distribution not in [
        "norm",
        "beta",
        "gamma",
        "poisson",
        "uniform",
        "chi2",
        "expon",
    ]:
        raise AttributeError(f"Unsupported  distribution provided: {distribution}")

    if isinstance(params, dict):
        # `params` is a dictionary
        if params.get("std_dev", 1) <= 0 or params.get("scale", 1) <= 0:
            raise ValueError("std_dev and scale must be positive.")

        # alpha and beta are required and positive
        if distribution == "beta" and (
            params.get("alpha", -1) <= 0 or params.get("beta", -1) <= 0
        ):
            raise ValueError(f"Invalid parameters: {beta_msg}")

        # alpha is required and positive
        elif distribution == "gamma" and params.get("alpha", -1) <= 0:
            raise ValueError(f"Invalid parameters: {gamma_msg}")

        # lambda is a required and positive
        # elif distribution == 'poisson' and params.get('lambda', -1) <= 0:
        #    raise ValueError("Invalid parameters: %s" %poisson_msg)

        # df is necessary and required to be positive
        elif distribution == "chi2" and params.get("df", -1) <= 0:
            raise ValueError(f"Invalid parameters: {chi2_msg}:")

    elif isinstance(params, tuple) or isinstance(params, list):  # noqa: PLR1701
        scale = None

        # `params` is a tuple or a list
        if distribution == "beta":
            if len(params) < 2:  # noqa: PLR2004
                raise ValueError(f"Missing required parameters: {beta_msg}")
            if params[0] <= 0 or params[1] <= 0:
                raise ValueError(f"Invalid parameters: {beta_msg}")
            if len(params) == 4:  # noqa: PLR2004
                scale = params[3]
            elif len(params) > 4:  # noqa: PLR2004
                raise ValueError(f"Too many parameters provided: {beta_msg}")

        elif distribution == "norm":
            if len(params) > 2:  # noqa: PLR2004
                raise ValueError(f"Too many parameters provided: {norm_msg}")
            if len(params) == 2:  # noqa: PLR2004
                scale = params[1]

        elif distribution == "gamma":
            if len(params) < 1:
                raise ValueError(f"Missing required parameters: {gamma_msg}")
            if len(params) == 3:  # noqa: PLR2004
                scale = params[2]
            if len(params) > 3:  # noqa: PLR2004
                raise ValueError(f"Too many parameters provided: {gamma_msg}")
            elif params[0] <= 0:
                raise ValueError(f"Invalid parameters: {gamma_msg}")

        # elif distribution == 'poisson':
        #    if len(params) < 1:
        #        raise ValueError("Missing required parameters: %s" %poisson_msg)
        #   if len(params) > 2:
        #        raise ValueError("Too many parameters provided: %s" %poisson_msg)
        #    elif params[0] <= 0:
        #        raise ValueError("Invalid parameters: %s" %poisson_msg)

        elif distribution == "uniform":
            if len(params) == 2:  # noqa: PLR2004
                scale = params[1]
            if len(params) > 2:  # noqa: PLR2004
                raise ValueError(f"Too many arguments provided: {uniform_msg}")

        elif distribution == "chi2":
            if len(params) < 1:
                raise ValueError(f"Missing required parameters: {chi2_msg}")
            elif len(params) == 3:  # noqa: PLR2004
                scale = params[2]
            elif len(params) > 3:  # noqa: PLR2004
                raise ValueError(f"Too many arguments provided: {chi2_msg}")
            if params[0] <= 0:
                raise ValueError(f"Invalid parameters: {chi2_msg}")

        elif distribution == "expon":
            if len(params) == 2:  # noqa: PLR2004
                scale = params[1]
            if len(params) > 2:  # noqa: PLR2004
                raise ValueError(f"Too many arguments provided: {expon_msg}")

        if scale is not None and scale <= 0:
            raise ValueError("std_dev and scale must be positive.")

    else:
        raise ValueError(
            "params must be a dict or list, or use great_expectations.dataset.util.infer_distribution_parameters(data, distribution)"
        )


def _scipy_distribution_positional_args_from_dict(distribution, params):
    """Helper function that returns positional arguments for a scipy distribution using a dict of parameters.

       See the `cdf()` function here https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.beta.html#Methods\
       to see an example of scipy's positional arguments. This function returns the arguments specified by the \
       scipy.stat.distribution.cdf() for that distribution.

       Args:
           distribution (string): \
               The scipy distribution name.
           params (dict): \
               A dict of named parameters.

       Raises:
           AttributeError: \
               If an unsupported distribution is provided.
    """

    params["loc"] = params.get("loc", 0)
    if "scale" not in params:
        params["scale"] = 1

    if distribution == "norm":
        return params["mean"], params["std_dev"]
    elif distribution == "beta":
        return params["alpha"], params["beta"], params["loc"], params["scale"]
    elif distribution == "gamma":
        return params["alpha"], params["loc"], params["scale"]
    # elif distribution == 'poisson':
    #    return params['lambda'], params['loc']
    elif distribution == "uniform":
        return params["min"], params["max"]
    elif distribution == "chi2":
        return params["df"], params["loc"], params["scale"]
    elif distribution == "expon":
        return params["loc"], params["scale"]


def is_valid_continuous_partition_object(partition_object):
    """Tests whether a given object is a valid continuous partition object. See :ref:`partition_object`.

    :param partition_object: The partition_object to evaluate
    :return: Boolean
    """
    if (
        (partition_object is None)
        or ("weights" not in partition_object)
        or ("bins" not in partition_object)
    ):
        return False

    if "tail_weights" in partition_object:
        if len(partition_object["tail_weights"]) != 2:  # noqa: PLR2004
            return False
        comb_weights = partition_object["tail_weights"] + partition_object["weights"]
    else:
        comb_weights = partition_object["weights"]

    ## TODO: Consider adding this check to migrate to the tail_weights structure of partition objects
    # if (partition_object['bins'][0] == -np.inf) or (partition_object['bins'][-1] == np.inf):
    #     return False

    # Expect one more bin edge than weight; all bin edges should be monotonically increasing; weights should sum to one
    return (
        (len(partition_object["bins"]) == (len(partition_object["weights"]) + 1))
        and np.all(np.diff(partition_object["bins"]) > 0)
        and np.allclose(np.sum(comb_weights), 1.0)
    )


def sql_statement_with_post_compile_to_string(
    engine: SqlAlchemyExecutionEngine, select_statement: sqlalchemy.Select
) -> str:
    """
    Util method to compile SQL select statement with post-compile parameters into a string. Logic lifted directly
    from sqlalchemy documentation.

    https://docs.sqlalchemy.org/en/14/faq/sqlexpressions.html#rendering-postcompile-parameters-as-bound-parameters

    Used by _sqlalchemy_map_condition_index() in map_metric_provider to build query that will allow you to
    return unexpected_index_values.

    Args:
        engine (sqlalchemy.engine.Engine): Sqlalchemy engine used to do the compilation.
        select_statement (sqlalchemy.sql.Select): Select statement to compile into string.
    Returns:
        String representation of select_statement

    """
    sqlalchemy_connection: sa.engine.base.Connection = engine.engine
    compiled = select_statement.compile(
        sqlalchemy_connection,
        compile_kwargs={"render_postcompile": True},
        dialect=engine.dialect,
    )
    dialect_name: str = engine.dialect_name

    if dialect_name in ["sqlite", "trino", "mssql"]:
        params = (repr(compiled.params[name]) for name in compiled.positiontup)
        query_as_string = re.sub(r"\?", lambda m: next(params), str(compiled))

    else:
        params = (repr(compiled.params[name]) for name in list(compiled.params.keys()))
        query_as_string = re.sub(r"%\(.*?\)s", lambda m: next(params), str(compiled))

    query_as_string += ";"
    return query_as_string


def get_sqlalchemy_source_table_and_schema(
    engine: SqlAlchemyExecutionEngine,
) -> sa.Table:
    """
    Util method to return table name that is associated with current batch.

    This is used by `_sqlalchemy_map_condition_query()` which returns a query that allows users to return
    unexpected_index_values.

    Args:
        engine (SqlAlchemyExecutionEngine): Engine that is currently being used to calculate the Metrics
    Returns:
        SqlAlchemy Table that is the source table and schema.
    """
    assert isinstance(
        engine.batch_manager.active_batch_data, SqlAlchemyBatchData
    ), "`active_batch_data` not SqlAlchemyBatchData"

    schema_name = engine.batch_manager.active_batch_data.source_schema_name
    table_name = engine.batch_manager.active_batch_data.source_table_name
    if table_name:
        return sa.Table(
            table_name,
            sa.MetaData(),
            schema=schema_name,
        )
    else:
        return engine.batch_manager.active_batch_data.selectable


def get_unexpected_indices_for_multiple_pandas_named_indices(
    domain_records_df: pd.DataFrame,
    unexpected_index_column_names: List[str],
    expectation_domain_column_list: List[str],
) -> List[Dict[str, Any]]:
    """
    Builds unexpected_index list for Pandas Dataframe in situation where the named
    columns is also a named index. This method handles the case when there are multiple named indices.
    Args:
        domain_records_df: reference to Pandas dataframe
        unexpected_index_column_names: column_names for indices, either named index or unexpected_index_columns
        expectation_domain_column_list: list of columns that Expectation is being run on.

    Returns:
        List of Dicts that contain ID/PK values
    """
    if not expectation_domain_column_list:
        raise gx_exceptions.MetricResolutionError(
            message="Error: The list of domain columns is currently empty. Please check your configuration.",
            failed_metrics=["unexpected_index_list"],
        )

    domain_records_df_index_names: List[str] = domain_records_df.index.names
    unexpected_indices: List[tuple[int | str, ...]] = list(domain_records_df.index)

    tuple_index: Dict[str, int] = dict()
    for column_name in unexpected_index_column_names:
        if column_name not in domain_records_df_index_names:
            raise gx_exceptions.MetricResolutionError(
                message=f"Error: The column {column_name} does not exist in the named indices. "
                f"Please check your configuration.",
                failed_metrics=["unexpected_index_list"],
            )
        else:
            tuple_index[column_name] = domain_records_df_index_names.index(
                column_name, 0
            )

    unexpected_index_list: List[Dict[str, Any]] = list()

    for index in unexpected_indices:
        primary_key_dict: Dict[str, Any] = dict()
        for domain_column_name in expectation_domain_column_list:
            primary_key_dict[domain_column_name] = domain_records_df.at[
                index, domain_column_name
            ]
            for column_name in unexpected_index_column_names:
                primary_key_dict[column_name] = index[tuple_index[column_name]]
        unexpected_index_list.append(primary_key_dict)
    return unexpected_index_list


def get_unexpected_indices_for_single_pandas_named_index(
    domain_records_df: pd.DataFrame,
    unexpected_index_column_names: List[str],
    expectation_domain_column_list: List[str],
) -> List[Dict[str, Any]]:
    """
    Builds unexpected_index list for Pandas Dataframe in situation where the named
    columns is also a named index. This method handles the case when there is a single named index.
    Args:
        domain_records_df: reference to Pandas dataframe
        unexpected_index_column_names: column_names for indices, either named index or unexpected_index_columns
        expectation_domain_column_list: list of columns that Expectation is being run on.

    Returns:
        List of Dicts that contain ID/PK values

    """
    if not expectation_domain_column_list:
        return []
    unexpected_index_values_by_named_index: List[int | str] = list(
        domain_records_df.index
    )
    unexpected_index_list: List[Dict[str, Any]] = list()
    if not (
        len(unexpected_index_column_names) == 1
        and unexpected_index_column_names[0] == domain_records_df.index.name
    ):
        raise gx_exceptions.MetricResolutionError(
            message=f"Error: The column {unexpected_index_column_names[0] if unexpected_index_column_names else '<no column specified>'} does not exist in the named indices. Please check your configuration",
            failed_metrics=["unexpected_index_list"],
        )
    for index in unexpected_index_values_by_named_index:
        primary_key_dict: Dict[str, Any] = dict()
        for domain_column in expectation_domain_column_list:
            primary_key_dict[domain_column] = domain_records_df.at[index, domain_column]
        column_name: str = unexpected_index_column_names[0]
        primary_key_dict[column_name] = index
        unexpected_index_list.append(primary_key_dict)
    return unexpected_index_list


def compute_unexpected_pandas_indices(
    domain_records_df: pd.DataFrame,
    expectation_domain_column_list: List[str],
    result_format: Dict[str, Any],
    execution_engine: PandasExecutionEngine,
    metrics: Dict[str, Any],
) -> List[int] | List[Dict[str, Any]]:
    """
    Helper method to compute unexpected_index_list for PandasExecutionEngine. Handles logic needed for named indices.

    Args:
        domain_records_df: DataFrame of data we are currently running Expectation on.
        expectation_domain_column_list: list of columns that we are running Expectation on. It can be one column.
        result_format: configuration that contains `unexpected_index_column_names`
                expectation_domain_column_list: list of columns that we are running Expectation on. It can be one column.
        execution_engine: PandasExecutionEngine
        metrics: dict of currently available metrics

    Returns:
        list of unexpected_index_list values. It can either be a list of dicts or a list of numbers (if using default index).

    """
    unexpected_index_column_names: List[str]
    unexpected_index_list: List[Dict[str, Any]]
    if domain_records_df.index.name is not None:
        unexpected_index_column_names = result_format.get(
            "unexpected_index_column_names", [domain_records_df.index.name]
        )
        unexpected_index_list = get_unexpected_indices_for_single_pandas_named_index(
            domain_records_df=domain_records_df,
            unexpected_index_column_names=unexpected_index_column_names,
            expectation_domain_column_list=expectation_domain_column_list,
        )
    # multiple named indices
    elif domain_records_df.index.names[0] is not None:
        unexpected_index_column_names = result_format.get(
            "unexpected_index_column_names", list(domain_records_df.index.names)
        )
        unexpected_index_list = (
            get_unexpected_indices_for_multiple_pandas_named_indices(
                domain_records_df=domain_records_df,
                unexpected_index_column_names=unexpected_index_column_names,
                expectation_domain_column_list=expectation_domain_column_list,
            )
        )
    # named columns
    elif result_format.get("unexpected_index_column_names"):
        unexpected_index_column_names = result_format["unexpected_index_column_names"]
        unexpected_index_list = []
        unexpected_indices: List[int | str] = list(domain_records_df.index)
        for index in unexpected_indices:
            primary_key_dict: Dict[str, Any] = dict()
            assert (
                expectation_domain_column_list
            ), "`expectation_domain_column_list` was not provided"
            for domain_column_name in expectation_domain_column_list:
                primary_key_dict[domain_column_name] = domain_records_df.at[
                    index, domain_column_name
                ]
                for column_name in unexpected_index_column_names:
                    column_name = get_dbms_compatible_column_names(  # noqa: PLW2901
                        column_names=column_name,
                        batch_columns_list=metrics["table.columns"],
                        error_message_template='Error: The unexpected_index_column "{column_name:s}" does not exist in Dataframe. Please check your configuration and try again.',
                    )
                    primary_key_dict[column_name] = domain_records_df.at[
                        index, column_name
                    ]
            unexpected_index_list.append(primary_key_dict)
    else:
        unexpected_index_list = list(domain_records_df.index)

    return unexpected_index_list
