from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
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

    # TODO: <Alex>ALEX -- temporarily only Pandas and SQLAlchemy implementations are provided (Spark to follow).</Alex>
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = column_list.sum(axis=1, skipna=False) == sum_total
        return row_wise_cond

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = sa.case((sum(column_list) == sum_total, True), else_=False)
        return row_wise_cond
