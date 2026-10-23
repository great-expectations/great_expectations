from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (  # noqa: E501 # FIXME CoP
    multicolumn_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_function_partial import (  # noqa: E501 # FIXME CoP
    multicolumn_function_partial,
)
from great_expectations.validator.validation_graph import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )


def _dialect_name(dialect) -> str:
    """Return the name of a SQLAlchemy dialect or engine, or "" if it has none."""
    for candidate in (getattr(dialect, "dialect", None), dialect):
        name = getattr(candidate, "name", None)
        if isinstance(name, str):
            return name
    return ""


class CompoundColumnsUnique(MulticolumnMapMetricProvider):
    """
    While the support for "PandasExecutionEngine" and "SparkDFExecutionEngine" is accomplished using a compact
    implementation, which combines the "map" and "condition" parts in a single step, the support for
    "SqlAlchemyExecutionEngine" is more detailed.  Thus, the "map" and "condition" parts for "SqlAlchemyExecutionEngine"
    are handled separately, with the "condition" part relying on the "map" part as a metric dependency.
    """  # noqa: E501 # FIXME CoP

    function_metric_name = "compound_columns.count"  # pre-requisite "map" style metric
    condition_metric_name = (
        "compound_columns.unique"  # "condition" style metric required to be implemented by provider
    )
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        row_wise_cond = ~column_list.duplicated(keep=False)
        return row_wise_cond

    @multicolumn_function_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(self, column_list, **kwargs):
        """
        Computes the "map" between the specified "column_list" (treated as a group so as to model the "compound" aspect)
        and the number of occurrences of every permutation of the values of "column_list" as the grouped subset of all
        rows of the table.  In the present context, the term "compound" refers to having to treat the specified columns
        as unique together (e.g., as a multi-column primary key).  For example, suppose that in the example below, all
        three columns ("A", "B", and "C") of the table are included as part of the "compound" columns list (i.e.,
        column_list = ["A", "B", "C"]):

            A B C _num_rows
            1 1 2 2
            1 2 3 1
            1 1 2 2
            2 2 2 1
            3 2 3 1

        The fourth column, "_num_rows", holds the value of the "map" function -- the number of rows the group occurs in.
        """  # noqa: E501 # FIXME CoP

        # Needed as keys (hence, string valued) to access "ColumnElement" objects contained within the "FROM" clauses.  # noqa: E501 # FIXME CoP
        column_names = kwargs.get("_column_names")

        # Need all columns of the table for the purposes of reporting entire rows satisfying unexpected condition logic.  # noqa: E501 # FIXME CoP
        table_columns = kwargs.get("_table_columns")

        table = kwargs.get(
            "_table"
        )  # Note that here, "table" is of the "sqlalchemy.sql.selectable.Subquery" type.

        table_columns_selector = [sa.column(column_name) for column_name in table_columns]

        # BigQuery cannot PARTITION BY a FLOAT64 expression (an integer column holding NULLs is
        # FLOAT64 there), but it can GROUP BY one. Join the per-key GROUP BY counts back to the
        # table with IS NOT DISTINCT FROM, so keys with NULLs in the same columns still match and
        # count as duplicates; an equality join would drop every key containing a NULL.
        if _dialect_name(kwargs.get("_dialect")) == "bigquery":
            original_table_clause = (
                sa.select(*table_columns_selector).select_from(table).alias("original_table_clause")
            )
            key_columns = [original_table_clause.c[name] for name in column_names]
            group_count_query = (
                sa.select(*key_columns, sa.func.count().label("_num_rows"))
                .group_by(*key_columns)
                .alias("group_counts_subquery")
            )
            conditions = sa.and_(
                *(
                    group_count_query.c[name].is_not_distinct_from(original_table_clause.c[name])
                    for name in column_names
                )
            )
            return (
                sa.select(original_table_clause, group_count_query.c._num_rows)
                .select_from(original_table_clause.join(group_count_query, onclause=conditions))
                .alias("records_with_grouped_column_counts_subquery")
            )

        # Elsewhere, count each key with a window function instead of joining GROUP BY counts back
        # to the table. PARTITION BY treats NULLs as equal, so keys with NULLs in the same columns
        # count as duplicates, as in pandas' duplicated(). It also reads the table only once, which
        # MySQL requires for temporary tables (#6286).
        num_rows = (
            sa.func.count()
            .over(partition_by=[sa.column(column_name) for column_name in column_names])
            .label("_num_rows")
        )
        compound_columns_count_query = (
            sa.select(*table_columns_selector, num_rows)
            .select_from(table)
            .alias("records_with_grouped_column_counts_subquery")
        )

        # The returned SQLAlchemy "FromClause" "compound_columns_count_query" object realizes the "map" metric function.  # noqa: E501 # FIXME CoP
        return compound_columns_count_query

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_condition(cls, column_list, **kwargs):
        """
        Retrieve the specified "map" metric dependency value as the "FromClause" "compound_columns_count_query" object
        and extract from it -- using the supported SQLAlchemy column access method -- the "_num_rows" columns.  The
        uniqueness of "compound" columns (as a group) is expressed by the "BinaryExpression" "row_wise_cond" returned.

        Importantly, since the "compound_columns_count_query" is the "FromClause" object that incorporates all columns
        of the original table, no additional "FromClause" objects ("select_from") must augment this "condition" metric.
        Other than boolean operations, column access, argument of filtering, and limiting the size of the result set,
        this "row_wise_cond", serving as the main component of the unexpected condition logic, carries along with it
        the entire object hierarchy, making any encapsulating query ready for execution against the database engine.
        """  # noqa: E501 # FIXME CoP

        metrics = kwargs.get("_metrics")
        compound_columns_count_query, _, _ = metrics[
            f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]

        # noinspection PyProtectedMember
        row_wise_cond = compound_columns_count_query.c._num_rows < 2  # noqa: PLR2004 # FIXME CoP

        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        column_names = column_list.columns
        row_wise_cond = (
            F.count(F.lit(1)).over(pyspark.Window.partitionBy(F.struct(*column_names))) <= 1
        )
        return row_wise_cond

    @classmethod
    @override
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """
        Returns a dictionary of given metric names and their corresponding configuration, specifying the metric types
        and their respective domains.
        """  # noqa: E501 # FIXME CoP

        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            if (
                metric.metric_name
                == f"compound_columns.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
            ):
                dependencies[
                    f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}"
                ] = MetricConfiguration(
                    metric_name=f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs=None,
                )

        return dependencies
