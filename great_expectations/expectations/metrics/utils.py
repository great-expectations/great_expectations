import logging
from typing import Dict, List

import snowflake
import sqlalchemy as sa

# import sqlalchemy_redshift
from dateutil.parser import parse
from sqlalchemy.dialects import registry
from sqlalchemy.engine import reflection
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import BinaryExpression, TextClause, literal
from sqlalchemy.sql.operators import custom_op

try:
    import psycopg2
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None

logger = logging.getLogger(__name__)

try:
    import pybigquery.sqlalchemy_bigquery

    # Sometimes "pybigquery.sqlalchemy_bigquery" fails to self-register in certain environments, so we do it explicitly.
    # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
    registry.register("bigquery", "pybigquery.sqlalchemy_bigquery", "BigQueryDialect")
    try:
        getattr(pybigquery.sqlalchemy_bigquery, "INTEGER")
        bigquery_types_tuple = None
    except AttributeError:
        # In older versions of the pybigquery driver, types were not exported, so we use a hack
        logger.warning(
            "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
        )
        from collections import namedtuple

        BigQueryTypes = namedtuple(
            "BigQueryTypes", sorted(pybigquery.sqlalchemy_bigquery._type_map)
        )
        bigquery_types_tuple = BigQueryTypes(**pybigquery.sqlalchemy_bigquery._type_map)
except ImportError:
    bigquery_types_tuple = None
    pybigquery = None
from great_expectations.execution_engine.util import check_sql_engine_dialect


def _get_dialect_regex_expression(dialect, column, regex, positive=True):
    try:
        # postgres
        if isinstance(dialect, sa.dialects.postgresql.dialect):
            if positive:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("~")
                )
            else:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("!~")
                )
    except AttributeError:
        pass

    try:
        # redshift
        if isinstance(dialect, sqlalchemy_redshift.dialect.RedshiftDialect):
            if positive:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("~")
                )
            else:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("!~")
                )
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    try:
        # MySQL
        if isinstance(dialect, sa.dialects.mysql.dialect):
            if positive:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("REGEXP")
                )
            else:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("NOT REGEXP")
                )
    except AttributeError:
        pass

    try:
        # Snowflake
        if isinstance(dialect, snowflake.sqlalchemy.snowdialect.SnowflakeDialect,):
            if positive:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("RLIKE")
                )
            else:
                return BinaryExpression(
                    sa.column(column), literal(regex), custom_op("NOT RLIKE")
                )
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    try:
        # Bigquery
        if isinstance(dialect, pybigquery.sqlalchemy_bigquery.BigQueryDialect):
            if positive:
                return sa.func.REGEXP_CONTAINS(sa.column(column), literal(regex))
            else:
                return sa.not_(
                    sa.func.REGEXP_CONTAINS(sa.column(column), literal(regex))
                )
    except (
        AttributeError,
        TypeError,
    ):  # TypeError can occur if the driver was not installed and so is None
        pass

    return None


def _get_dialect_type_module(dialect=None):
    if dialect is None:
        logger.warning(
            "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
        )
        return sa
    try:
        # Redshift does not (yet) export types to top level; only recognize base SA types
        if isinstance(dialect, sqlalchemy_redshift.dialect.RedshiftDialect):
            return dialect.sa
    except (TypeError, AttributeError):
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if (
            isinstance(dialect, pybigquery.sqlalchemy_bigquery.BigQueryDialect,)
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    return dialect


def attempt_allowing_relative_error(dialect):
    detected_redshift: bool = (
        sqlalchemy_redshift is not None
        and check_sql_engine_dialect(
            actual_sql_engine_dialect=dialect,
            candidate_sql_engine_dialect=sqlalchemy_redshift.dialect.RedshiftDialect,
        )
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


def column_reflection_fallback(table, dialect, sqlalchemy_engine):
    """If we can't reflect the table, use a query to at least get column names."""
    col_info_dict_list: List[Dict]
    if dialect.name.lower() == "mssql":
        type_module = _get_dialect_type_module(dialect)
        # Get column names and types from the database
        # StackOverflow to the rescue: https://stackoverflow.com/a/38634368
        col_info_query: TextClause = sa.text(
            f"""
SELECT
cols.NAME, ty.NAME
FROM
tempdb.sys.columns AS cols
JOIN
sys.types AS ty
ON
cols.user_type_id = ty.user_type_id
WHERE
object_id = OBJECT_ID('tempdb..{table}')
            """
        )
        col_info_tuples_list = sqlalchemy_engine.execute(col_info_query).fetchall()
        col_info_dict_list = [
            {"name": col_name, "type": getattr(type_module, col_type.upper())()}
            for col_name, col_type in col_info_tuples_list
        ]
    else:
        query: Select = sa.select([sa.text("*")]).select_from(table).limit(1)
        col_names: list = sqlalchemy_engine.execute(query).keys()
        col_info_dict_list = [{"name": col_name} for col_name in col_names]
    return col_info_dict_list


def _parse_value_set(value_set):
    parsed_value_set = [
        parse(value) if isinstance(value, str) else value for value in value_set
    ]
    return parsed_value_set


def filter_pair_metric_nulls(column_A, column_B, ignore_row_if):
    if ignore_row_if == "both_values_are_missing":
        boolean_mapped_null_values = column_A.isnull() & column_B.isnull()
    elif ignore_row_if == "either_value_is_missing":
        boolean_mapped_null_values = column_A.isnull() | column_B.isnull()
    elif ignore_row_if == "never":
        boolean_mapped_null_values = column_A.map(lambda x: False)
    else:
        raise ValueError("Unknown value of ignore_row_if: %s", (ignore_row_if,))

    assert len(column_A) == len(column_B), "Series A and B must be the same length"

    return (
        column_A[boolean_mapped_null_values == False],
        column_B[boolean_mapped_null_values == False],
    )
