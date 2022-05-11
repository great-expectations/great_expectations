from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, Window, sa
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
    multicolumn_function_partial,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class CompoundColumnsUnique(MulticolumnMapMetricProvider):
    '\n    While the support for "PandasExecutionEngine" and "SparkDFExecutionEngine" is accomplished using a compact\n    implementation, which combines the "map" and "condition" parts in a single step, the support for\n    "SqlAlchemyExecutionEngine" is more detailed.  Thus, the "map" and "condition" parts for "SqlAlchemyExecutionEngine"\n    are handled separately, with the "condition" part relying on the "map" part as a metric dependency.\n'
    function_metric_name = "compound_columns.count"
    condition_metric_name = "compound_columns.unique"
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        row_wise_cond = ~column_list.duplicated(keep=False)
        return row_wise_cond

    @multicolumn_function_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(self, column_list, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Computes the "map" between the specified "column_list" (treated as a group so as to model the "compound" aspect)\n        and the number of occurrences of every permutation of the values of "column_list" as the grouped subset of all\n        rows of the table.  In the present context, the term "compound" refers to having to treat the specified columns\n        as unique together (e.g., as a multi-column primary key).  For example, suppose that in the example below, all\n        three columns ("A", "B", and "C") of the table are included as part of the "compound" columns list (i.e.,\n        column_list = ["A", "B", "C"]):\n\n            A B C _num_rows\n            1 1 2 2\n            1 2 3 1\n            1 1 2 2\n            2 2 2 1\n            3 2 3 1\n\n        The fourth column, "_num_rows", holds the value of the "map" function -- the number of rows the group occurs in.\n        '
        column_names = kwargs.get("_column_names")
        table_columns = kwargs.get("_table_columns")
        table = kwargs.get("_table")
        table_columns_selector = [
            sa.column(column_name) for column_name in table_columns
        ]
        original_table_clause = (
            sa.select(table_columns_selector)
            .select_from(table)
            .alias("original_table_clause")
        )
        count_selector = column_list + [sa.func.count().label("_num_rows")]
        group_count_query = (
            sa.select(count_selector)
            .group_by(*column_list)
            .select_from(original_table_clause)
            .alias("group_counts_subquery")
        )
        conditions = sa.and_(
            *(
                (group_count_query.c[name] == original_table_clause.c[name])
                for name in column_names
            )
        )
        compound_columns_count_query = (
            sa.select(
                [
                    original_table_clause,
                    group_count_query.c._num_rows.label("_num_rows"),
                ]
            )
            .select_from(
                original_table_clause.join(
                    right=group_count_query, onclause=conditions, isouter=False
                )
            )
            .alias("records_with_grouped_column_counts_subquery")
        )
        return compound_columns_count_query

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_condition(cls, column_list, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Retrieve the specified "map" metric dependency value as the "FromClause" "compound_columns_count_query" object\n        and extract from it -- using the supported SQLAlchemy column access method -- the "_num_rows" columns.  The\n        uniqueness of "compound" columns (as a group) is expressed by the "BinaryExpression" "row_wise_cond" returned.\n\n        Importantly, since the "compound_columns_count_query" is the "FromClause" object that incorporates all columns\n        of the original table, no additional "FromClause" objects ("select_from") must augment this "condition" metric.\n        Other than boolean operations, column access, argument of filtering, and limiting the size of the result set,\n        this "row_wise_cond", serving as the main component of the unexpected condition logic, carries along with it\n        the entire object hierarchy, making any encapsulating query ready for execution against the database engine.\n        '
        metrics = kwargs.get("_metrics")
        (compound_columns_count_query, _, _) = metrics["compound_columns.count.map"]
        row_wise_cond = compound_columns_count_query.c._num_rows < 2
        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_names = column_list.columns
        row_wise_cond = (
            F.count(F.lit(1)).over(Window.partitionBy(F.struct(*column_names))) <= 1
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns a dictionary of given metric names and their corresponding configuration, specifying the metric types\n        and their respective domains.\n        "
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            if metric.metric_name == "compound_columns.unique.condition":
                dependencies["compound_columns.count.map"] = MetricConfiguration(
                    metric_name="compound_columns.count.map",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
        return dependencies
