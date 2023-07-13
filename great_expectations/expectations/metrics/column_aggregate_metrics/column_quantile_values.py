import ast
import itertools
import logging
import traceback
from collections.abc import Iterable
from typing import Any, Dict, List

import numpy as np

from great_expectations.compatibility import sqlalchemy, trino
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.execution_engine.util import get_approximate_percentile_disc_sql
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.util import attempt_allowing_relative_error

logger = logging.getLogger(__name__)


if sqlalchemy.Row:
    Row = sqlalchemy.Row
else:
    Row = sqlalchemy.RowProxy


class ColumnQuantileValues(ColumnAggregateMetricProvider):
    metric_name = "column.quantile_values"
    value_keys = ("quantiles", "allow_relative_error")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, quantiles, allow_relative_error, **kwargs):
        """Quantile Function"""
        interpolation_options = ("linear", "lower", "higher", "midpoint", "nearest")

        if not allow_relative_error:
            allow_relative_error = "nearest"

        if allow_relative_error not in interpolation_options:
            raise ValueError(
                f"If specified for pandas, allow_relative_error must be one an allowed value for the 'interpolation'"
                f"parameter of .quantile() (one of {interpolation_options})"
            )

        return column.quantile(quantiles, interpolation=allow_relative_error).tolist()

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0911, PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        dialect_name = execution_engine.dialect_name
        quantiles = metric_value_kwargs["quantiles"]
        allow_relative_error = metric_value_kwargs.get("allow_relative_error", False)
        table_row_count = metrics.get("table.row_count")
        if dialect_name == GXSqlDialect.MSSQL:
            return _get_column_quantiles_mssql(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name == GXSqlDialect.BIGQUERY:
            return _get_column_quantiles_bigquery(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name == GXSqlDialect.MYSQL:
            return _get_column_quantiles_mysql(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name.lower() == GXSqlDialect.CLICKHOUSE:
            return _get_column_quantiles_clickhouse(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name == GXSqlDialect.TRINO:
            return _get_column_quantiles_trino(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name == GXSqlDialect.SNOWFLAKE:
            # NOTE: 20201216 - JPC - snowflake has a representation/precision limitation
            # in its percentile_disc implementation that causes an error when we do
            # not round. It is unclear to me *how* the call to round affects the behavior --
            # the binary representation should be identical before and after, and I do
            # not observe a type difference. However, the issue is replicable in the
            # snowflake console and directly observable in side-by-side comparisons with
            # and without the call to round()
            quantiles = [round(x, 10) for x in quantiles]
            return _get_column_quantiles_generic_sqlalchemy(
                column=column,
                quantiles=quantiles,
                allow_relative_error=allow_relative_error,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        elif dialect_name == GXSqlDialect.SQLITE:
            return _get_column_quantiles_sqlite(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
                table_row_count=table_row_count,
            )
        elif dialect_name == GXSqlDialect.AWSATHENA:
            return _get_column_quantiles_athena(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
            )
        else:
            return _get_column_quantiles_generic_sqlalchemy(
                column=column,
                quantiles=quantiles,
                allow_relative_error=allow_relative_error,
                selectable=selectable,
                execution_engine=execution_engine,
            )

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        quantiles = metric_value_kwargs["quantiles"]
        column = accessor_domain_kwargs["column"]

        allow_relative_error = metric_value_kwargs.get("allow_relative_error", False)
        if not allow_relative_error:
            allow_relative_error = 0.0

        if (
            not isinstance(allow_relative_error, float)
            or allow_relative_error < 0.0  # noqa: PLR2004
            or allow_relative_error > 1.0  # noqa: PLR2004
        ):
            raise ValueError(
                "SparkDFExecutionEngine requires relative error to be False or to be a float between 0 and 1."
            )

        return df.approxQuantile(column, list(quantiles), allow_relative_error)


def _get_column_quantiles_mssql(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # mssql requires over(), so we add an empty over() clause
    selects: List[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(quantile).within_group(column.asc()).over()
        for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query
        ).fetchone()
        return list(quantiles_results)
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_bigquery(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # BigQuery does not support "WITHIN", so we need a special case for it
    selects: List[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(column, quantile).over() for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query
        ).fetchone()
        return list(quantiles_results)
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_mysql(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # MySQL does not support "percentile_disc", so we implement it as a compound query.
    # Please see https://stackoverflow.com/questions/19770026/calculate-percentile-value-using-mysql for reference.
    percent_rank_query: sqlalchemy.CTE = (
        sa.select(
            column,
            sa.cast(
                sa.func.percent_rank().over(order_by=column.asc()),
                sa.dialects.mysql.DECIMAL(18, 15),
            ).label("p"),
        )
        .order_by(sa.column("p").asc())
        .select_from(selectable)
        .cte("t")
    )

    selects: List[sqlalchemy.WithinGroup] = []
    for idx, quantile in enumerate(quantiles):
        # pymysql cannot handle conversion of numpy float64 to float; convert just in case
        if np.issubdtype(type(quantile), np.float_):
            quantile = float(quantile)  # noqa: PLW2901
        quantile_column: sqlalchemy.Label = (
            sa.func.first_value(column)
            .over(
                order_by=sa.case(
                    (
                        percent_rank_query.columns.p
                        <= sa.cast(quantile, sa.dialects.mysql.DECIMAL(18, 15)),
                        percent_rank_query.columns.p,
                    ),
                    else_=None,
                ).desc()
            )
            .label(f"q_{idx}")
        )
        selects.append(quantile_column)
    quantiles_query: sqlalchemy.Select = (
        sa.select(*selects).distinct().order_by(percent_rank_query.columns.p.desc())
    )

    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query
        ).fetchone()
        return list(quantiles_results)
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_trino(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # Trino does not have the percentile_disc func, but instead has approx_percentile
    sql_approx: str = f"approx_percentile({column}, ARRAY{list(quantiles)})"
    selects_approx: List[sqlalchemy.TextClause] = [sa.text(sql_approx)]
    quantiles_query: sqlalchemy.Select = sa.select(*selects_approx).select_from(
        selectable
    )

    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query
        ).fetchone()
        return list(quantiles_results)[0]
    except (sqlalchemy.ProgrammingError, trino.trinoexceptions.TrinoUserError) as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_clickhouse(
    column: str, quantiles: Iterable, selectable, execution_engine
) -> list:
    quantiles_list = list(quantiles)
    sql_approx: str = (
        f"quantilesExact({', '.join([str(x) for x in quantiles_list])})({column})"
    )
    selects_approx: List[sqlalchemy.TextClause] = [sa.text(sql_approx)]
    quantiles_query: sqlalchemy.Select = sa.select(selects_approx).select_from(
        selectable
    )
    try:
        quantiles_results = execution_engine.execute(quantiles_query).fetchone()[0]
        return quantiles_results

    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_sqlite(
    column,
    quantiles: Iterable,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
    table_row_count,
) -> list:
    """
    The present implementation is somewhat inefficient, because it requires as many calls to
    "execution_engine.execute_query()" as the number of partitions in the "quantiles" parameter (albeit, typically,
    only a few).  However, this is the only mechanism available for SQLite at the present time (11/17/2021), because
    the analytical processing is not a very strongly represented capability of the SQLite database management system.
    """
    offsets: List[int] = [quantile * table_row_count - 1 for quantile in quantiles]
    quantile_queries: List[sqlalchemy.Select] = [
        sa.select(column)
        .order_by(column.asc())
        .offset(offset)
        .limit(1)
        .select_from(selectable)
        for offset in offsets
    ]

    quantile_result: Row
    quantile_query: sqlalchemy.Select
    try:
        quantiles_results: List[Row] = [
            execution_engine.execute_query(quantile_query).fetchone()
            for quantile_query in quantile_queries
        ]
        return list(
            itertools.chain.from_iterable(
                [list(quantile_result) for quantile_result in quantiles_results]
            )
        )
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


def _get_column_quantiles_athena(
    column,
    quantiles: Iterable,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
) -> list:
    approx_percentiles = f"approx_percentile({column}, ARRAY{list(quantiles)})"
    selects_approx: List[sqlalchemy.TextClause] = [sa.text(approx_percentiles)]
    quantiles_query_approx: sqlalchemy.Select = sa.select(*selects_approx).select_from(
        selectable
    )
    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query_approx
        ).fetchone()
        # the ast literal eval is needed because the method is returning a json string and not a dict
        results = ast.literal_eval(quantiles_results[0])
        return results
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe


# Support for computing the quantiles column for PostGreSQL and Redshift is included in the same method as that for
# the generic sqlalchemy compatible DBMS engine, because users often use the postgresql driver to connect to Redshift
# The key functional difference is that Redshift does not support the aggregate function
# "percentile_disc", but does support the approximate percentile_disc or percentile_cont function version instead.```
def _get_column_quantiles_generic_sqlalchemy(
    column,
    quantiles: Iterable,
    allow_relative_error: bool,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
) -> list:
    selects: List[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(quantile).within_group(column.asc())
        for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results: Row = execution_engine.execute_query(
            quantiles_query
        ).fetchone()
        return list(quantiles_results)
    except sqlalchemy.ProgrammingError:
        # ProgrammingError: (psycopg2.errors.SyntaxError) Aggregate function "percentile_disc" is not supported;
        # use approximate percentile_disc or percentile_cont instead.
        if attempt_allowing_relative_error(execution_engine.dialect):
            # Redshift does not have a percentile_disc method, but does support an approximate version.
            sql_approx: str = get_approximate_percentile_disc_sql(
                selects=selects, sql_engine_dialect=execution_engine.dialect
            )
            selects_approx: List[sqlalchemy.TextClause] = [sa.text(sql_approx)]
            quantiles_query_approx: sqlalchemy.Select = sa.select(
                *selects_approx
            ).select_from(selectable)
            if allow_relative_error or execution_engine.engine.driver == "psycopg2":
                try:
                    quantiles_results: Row = execution_engine.execute_query(
                        quantiles_query_approx
                    ).fetchone()
                    return list(quantiles_results)
                except sqlalchemy.ProgrammingError as pe:
                    exception_message: str = "An SQL syntax Exception occurred."
                    exception_traceback: str = traceback.format_exc()
                    exception_message += f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
                    logger.error(exception_message)
                    raise pe
            else:
                raise ValueError(
                    f'The SQL engine dialect "{str(execution_engine.dialect)}" does not support computing quantiles '
                    "without approximation error; set allow_relative_error to True to allow approximate quantiles."
                )
        else:
            raise ValueError(
                f'The SQL engine dialect "{str(execution_engine.dialect)}" does not support computing quantiles with '
                "approximation error; set allow_relative_error to False to disable approximate quantiles."
            )
