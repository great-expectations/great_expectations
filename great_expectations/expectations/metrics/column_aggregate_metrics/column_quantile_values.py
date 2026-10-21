from __future__ import annotations

import ast
import itertools
import logging
import math
import traceback
from collections.abc import Iterable
from fractions import Fraction
from typing import TYPE_CHECKING, Any, Optional

import numpy as np

from great_expectations.compatibility import sqlalchemy, trino
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
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
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )

logger = logging.getLogger(__name__)


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
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                f"If specified for pandas, allow_relative_error must be one an allowed value for the 'interpolation'"  # noqa: E501 # FIXME CoP
                f"parameter of .quantile() (one of {interpolation_options})"
            )

        return column.quantile(quantiles, interpolation=allow_relative_error).tolist()

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: C901, PLR0911 # FIXME CoP
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ):
        (
            selectable,
            _compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)  # type: ignore[var-annotated] # FIXME CoP
        dialect_name = execution_engine.dialect_name
        quantiles = metric_value_kwargs["quantiles"]
        allow_relative_error = metric_value_kwargs.get("allow_relative_error", False)
        if dialect_name == GXSqlDialect.SQL_SERVER:
            return _get_column_quantiles_sql_server(
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
                column=column,  # type: ignore[arg-type] # FIXME CoP
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
            # Subscript rather than "get", so a missing dependency raises instead of being read as
            # an empty column and silently returning NaN.
            nonnull_count = metrics["column_values.nonnull.count"]
            if not nonnull_count:
                return [np.nan] * len(quantiles)
            return _get_column_quantiles_sqlite(
                column=column,
                quantiles=quantiles,
                selectable=selectable,
                execution_engine=execution_engine,
                nonnull_count=nonnull_count,
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
    def _spark(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ):
        (
            df,
            _compute_domain_kwargs,
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
            or allow_relative_error < 0.0
            or allow_relative_error > 1.0
        ):
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "SparkDFExecutionEngine requires relative error to be False or to be a float between 0 and 1."  # noqa: E501 # FIXME CoP
            )

        quantiles = list(quantiles)
        quantile_values = df.approxQuantile(column, quantiles, allow_relative_error)  # type: ignore[attr-defined] # FIXME CoP

        # "approxQuantile" returns an empty list for a column with no non-null values, where the
        # other backends return one null per requested quantile. Keep the metric the same shape
        # on every backend, so that consumers can index it by quantile.
        if not quantile_values:
            return [None] * len(quantiles)

        return quantile_values

    @classmethod
    @override
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """The SQLite implementation ranks over the non-null values, so it needs their count."""
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        # Only SQLite reads this, so the other dialects are not charged an extra aggregate.
        if (
            isinstance(execution_engine, SqlAlchemyExecutionEngine)
            and execution_engine.dialect_name == GXSqlDialect.SQLITE
        ):
            dependencies["column_values.nonnull.count"] = MetricConfiguration(
                metric_name="column_values.nonnull.count",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )

        return dependencies


def _get_column_quantiles_sql_server(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # SQL Server requires over(), so we add an empty over() clause
    selects: list[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(quantile).within_group(column.asc()).over()  # type: ignore[misc] # FIXME CoP
        for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()
        return list(quantiles_results)  # type: ignore[arg-type] # FIXME CoP
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_bigquery(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # BigQuery does not support "WITHIN", so we need a special case for it
    selects: list[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(column, quantile).over()  # type: ignore[misc] # FIXME CoP
        for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()
        return list(quantiles_results)  # type: ignore[arg-type] # FIXME CoP
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_mysql(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # MySQL does not support "percentile_disc", so we implement it as a compound query.
    # Please see https://stackoverflow.com/questions/19770026/calculate-percentile-value-using-mysql for reference.  # noqa: E501 # FIXME CoP
    percent_rank_query: sqlalchemy.CTE = (
        sa.select(
            column,
            sa.cast(
                sa.func.percent_rank().over(order_by=column.asc()),
                sa.dialects.mysql.DECIMAL(18, 15),
            ).label("p"),
        )
        .where(column != None)  # noqa: E711 # FIXME CoP
        .order_by(sa.column("p").asc())
        .select_from(selectable)
        .cte("t")
    )

    selects: list[sqlalchemy.WithinGroup] = []
    for idx, quantile in enumerate(quantiles):
        # pymysql cannot handle conversion of numpy float64 to float; convert just in case
        if np.issubdtype(type(quantile), np.double):
            quantile = float(quantile)  # noqa: PLW2901 # FIXME CoP
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
        selects.append(quantile_column)  # type: ignore[arg-type] # FIXME CoP
    quantiles_query: sqlalchemy.Select = (
        sa.select(*selects).distinct().order_by(percent_rank_query.columns.p.desc())
    )

    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()
        # Filtering the nulls out of the CTE leaves it empty for a column with no non-null values,
        # and the query then returns no row at all. Report one absent quantile per requested
        # quantile, which is what the engines with a native "percentile_disc" return.
        if quantiles_results is None:
            return [None] * len(selects)
        return list(quantiles_results)
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_trino(
    column, quantiles: Iterable, selectable, execution_engine: SqlAlchemyExecutionEngine
) -> list:
    # Trino does not have the percentile_disc func, but instead has approx_percentile
    sql_approx: str = f"approx_percentile({column}, ARRAY{list(quantiles)})"
    selects_approx: list[sqlalchemy.TextClause] = [sa.text(sql_approx)]
    quantiles_query: sqlalchemy.Select = sa.select(*selects_approx).select_from(selectable)

    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()
        return list(quantiles_results)[0]  # type: ignore[arg-type] # FIXME CoP
    except (sqlalchemy.ProgrammingError, trino.trinoexceptions.TrinoUserError) as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_clickhouse(
    column: str, quantiles: Iterable, selectable, execution_engine
) -> list:
    quantiles_list = list(quantiles)
    sql_approx: str = f"quantilesExact({', '.join([str(x) for x in quantiles_list])})({column})"
    selects_approx: list[sqlalchemy.TextClause] = [sa.text(sql_approx)]
    quantiles_query: sqlalchemy.Select = sa.select(*selects_approx).select_from(selectable)
    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()[0]
        return quantiles_results

    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_sqlite(
    column,
    quantiles: Iterable,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
    nonnull_count: int,
) -> list:
    """
    The present implementation is somewhat inefficient, because it requires as many calls to
    "execution_engine.execute_query()" as the number of partitions in the "quantiles" parameter (albeit, typically,
    only a few).  However, this is the only mechanism available for SQLite at the present time (11/17/2021), because
    the analytical processing is not a very strongly represented capability of the SQLite database management system.

    Ranks are taken over the non-null values only and follow "percentile_disc", which returns the
    first value whose cumulative distribution reaches the quantile.
    """  # noqa: E501 # FIXME CoP
    # The rank is "ceil(quantile * count)" evaluated on the quantile as written, not on its binary
    # approximation. 0.56 is not representable in binary, so 0.56 * 25 is 14.000000000000002 and
    # ceiling that selects rank 15. Rank 14 is the correct answer, because 14/25 is exactly 0.56
    # and so already reaches the quantile.
    #
    # Note that this deliberately differs from the SQL engines that implement "percentile_disc" in
    # double precision: PostgreSQL returns the 15th value here. The divergence is confined to
    # quantiles whose product with the count is a whole number in decimal but not in binary.
    ranks: list[int] = [
        max(math.ceil(Fraction(str(quantile)) * nonnull_count), 1) for quantile in quantiles
    ]
    quantile_queries: list[sqlalchemy.Select] = [
        sa.select(column)
        .where(column != None)  # noqa: E711 # FIXME CoP
        .order_by(column.asc())
        .offset(rank - 1)
        .limit(1)
        .select_from(selectable)
        for rank in ranks
    ]

    try:
        quantiles_results = [
            execution_engine.execute_query(quantile_query).fetchone()
            for quantile_query in quantile_queries
        ]
        return list(
            itertools.chain.from_iterable(
                [list(quantile_result) for quantile_result in quantiles_results]  # type: ignore[arg-type] # FIXME CoP
            )
        )
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


def _get_column_quantiles_athena(
    column,
    quantiles: Iterable,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
) -> list:
    approx_percentiles = f"approx_percentile({column}, ARRAY{list(quantiles)})"
    selects_approx: list[sqlalchemy.TextClause] = [sa.text(approx_percentiles)]
    quantiles_query_approx: sqlalchemy.Select = sa.select(*selects_approx).select_from(selectable)
    try:
        quantiles_results = execution_engine.execute_query(quantiles_query_approx).fetchone()
        # the ast literal eval is needed because the method is returning a json string and not a dict  # noqa: E501 # FIXME CoP
        results = ast.literal_eval(quantiles_results[0])  # type: ignore[index] # FIXME CoP
        return results
    except sqlalchemy.ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
        logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
        raise pe  # noqa: TRY201 # FIXME CoP


# Support for computing the quantiles column for PostGreSQL and Redshift is included in the same method as that for  # noqa: E501 # FIXME CoP
# the generic sqlalchemy compatible DBMS engine, because users often use the postgresql driver to connect to Redshift  # noqa: E501 # FIXME CoP
# The key functional difference is that Redshift does not support the aggregate function
# "percentile_disc", but does support the approximate percentile_disc or percentile_cont function version instead.```  # noqa: E501 # FIXME CoP
def _get_column_quantiles_generic_sqlalchemy(
    column,
    quantiles: Iterable,
    allow_relative_error: bool,
    selectable,
    execution_engine: SqlAlchemyExecutionEngine,
) -> list:
    selects: list[sqlalchemy.WithinGroup] = [
        sa.func.percentile_disc(quantile).within_group(column.asc()) for quantile in quantiles
    ]
    quantiles_query: sqlalchemy.Select = sa.select(*selects).select_from(selectable)

    try:
        quantiles_results = execution_engine.execute_query(quantiles_query).fetchone()
        return list(quantiles_results)  # type: ignore[arg-type] # FIXME CoP
    except sqlalchemy.ProgrammingError:
        # ProgrammingError: (psycopg2.errors.SyntaxError) Aggregate function "percentile_disc" is not supported;  # noqa: E501 # FIXME CoP
        # use approximate percentile_disc or percentile_cont instead.
        if attempt_allowing_relative_error(execution_engine.dialect):
            # Redshift does not have a percentile_disc method, but does support an approximate version.  # noqa: E501 # FIXME CoP
            sql_approx: str = get_approximate_percentile_disc_sql(
                selects=selects, sql_engine_dialect=execution_engine.dialect
            )
            selects_approx: list[sqlalchemy.TextClause] = [sa.text(sql_approx)]
            quantiles_query_approx: sqlalchemy.Select = sa.select(*selects_approx).select_from(
                selectable
            )
            if allow_relative_error or execution_engine.engine.driver in ("psycopg2", "psycopg"):
                try:
                    quantiles_results = execution_engine.execute_query(
                        quantiles_query_approx
                    ).fetchone()
                    return list(quantiles_results)  # type: ignore[arg-type] # FIXME CoP
                except sqlalchemy.ProgrammingError as pe:
                    exception_message: str = "An SQL syntax Exception occurred."
                    exception_traceback: str = traceback.format_exc()
                    exception_message += (
                        f'{type(pe).__name__}: "{pe!s}".  Traceback: "{exception_traceback}".'
                    )
                    logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
                    raise pe  # noqa: TRY201 # FIXME CoP
            else:
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    f'The SQL engine dialect "{execution_engine.dialect!s}" does not support computing quantiles '  # noqa: E501 # FIXME CoP
                    "without approximation error; set allow_relative_error to True to allow approximate quantiles."  # noqa: E501 # FIXME CoP
                )
        else:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                f'The SQL engine dialect "{execution_engine.dialect!s}" does not support computing quantiles with '  # noqa: E501 # FIXME CoP
                "approximation error; set allow_relative_error to False to disable approximate quantiles."  # noqa: E501 # FIXME CoP
            )
