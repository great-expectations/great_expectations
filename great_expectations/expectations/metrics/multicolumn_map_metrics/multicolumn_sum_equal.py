from great_expectations.compatibility.pyspark import functions as F
from great_expectations.execution_engine import (
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


class MulticolumnSumEqual(MulticolumnMapMetricProvider):
    condition_metric_name = "multicolumn_sum.equal"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ("sum_total",)

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = column_list.sum(axis=1, skipna=False) == sum_total
        return row_wise_cond

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = sum(column_list) == sum_total
        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        expression = "+".join(
            [f"COALESCE({column_name}, 0)" for column_name in column_list.columns]
        )
        row_wise_cond = F.expr(expression) == F.lit(sum_total)
        return row_wise_cond
