from typing import Optional

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core import ExpectationConfiguration
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
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (
    multicolumn_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_function_partial import (
    multicolumn_function_partial,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class CompoundColumnsUnique(MulticolumnMapMetricProvider):
    """
    While the support for "PandasExecutionEngine" and "SparkDFExecutionEngine" is accomplished using a compact
    implementation, which combines the "map" and "condition" parts in a single step, the support for
    "SqlAlchemyExecutionEngine" is more detailed.  Thus, the "map" and "condition" parts for "SqlAlchemyExecutionEngine"
    are handled separately, with the "condition" part relying on the "map" part as a metric dependency.
    """

    function_metric_name = "compound_columns.count"  # pre-requisite "map" style metric
    condition_metric_name = "compound_columns.unique"  # "condition" style metric required to be implemented by provider
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
        """

        # Needed as keys (hence, string valued) to access "ColumnElement" objects contained within the "FROM" clauses.
        column_names = kwargs.get("_column_names")

        # Need all columns of the table for the purposes of reporting entire rows satisfying unexpected condition logic.
        table_columns = kwargs.get("_table_columns")

        table = kwargs.get(
            "_table"
        )  # Note that here, "table" is of the "sqlalchemy.sql.selectable.Subquery" type.

        # Step-1: Obtain the SQLAlchemy "FromClause" version of the original "table" for the purposes of gaining the
        # "FromClause.c" attribute, which is a namespace of all the columns contained within the "FROM" clause (these
        # elements are themselves subclasses of the SQLAlchemy "ColumnElement" class).
        table_columns_selector = [
            sa.column(column_name) for column_name in table_columns
        ]
        original_table_clause = (
            sa.select(*table_columns_selector)
            .select_from(table)
            .alias("original_table_clause")
        )

        # Step-2: "SELECT FROM" the original table, represented by the "FromClause" object, querying all columns of the
        # table and the count of occurrences of distinct "compound" (i.e., group, as specified by "column_list") values.
        # Give this aggregated group count a distinctive label.
        # Give the resulting sub-query a unique alias in order to disambiguate column names in subsequent queries.
        count_selector = column_list + [sa.func.count().label("_num_rows")]
        group_count_query = (
            sa.select(*count_selector)
            .group_by(*column_list)
            .select_from(original_table_clause)
            .alias("group_counts_subquery")
        )

        # The above "group_count_query", if executed, will produce the result set containing the number of rows that
        # equals the number of distinct values of the group -- unique grouping (e.g., as in a multi-column primary key).
        # Hence, in order for the "_num_rows" column values to provide an entry for each row of the original table, the
        # "SELECT FROM" of "group_count_query" must undergo an "INNER JOIN" operation with the "original_table_clause"
        # object, whereby all table columns in the two "FromClause" objects must match, respectively, as the conditions.
        conditions = sa.and_(
            *(
                group_count_query.c[name] == original_table_clause.c[name]
                for name in column_names
            )
        )
        # noinspection PyProtectedMember
        compound_columns_count_query = (
            sa.select(
                original_table_clause,
                group_count_query.c._num_rows.label("_num_rows"),
            )
            .select_from(
                original_table_clause.join(
                    right=group_count_query, onclause=conditions, isouter=False
                )
            )
            .alias("records_with_grouped_column_counts_subquery")
        )

        # The returned SQLAlchemy "FromClause" "compound_columns_count_query" object realizes the "map" metric function.
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
        """

        metrics = kwargs.get("_metrics")
        compound_columns_count_query, _, _ = metrics[
            f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]

        # noinspection PyProtectedMember
        row_wise_cond = compound_columns_count_query.c._num_rows < 2  # noqa: PLR2004

        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        column_names = column_list.columns
        row_wise_cond = (
            F.count(F.lit(1)).over(pyspark.Window.partitionBy(F.struct(*column_names)))
            <= 1
        )
        return row_wise_cond

    @classmethod
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
        """

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
