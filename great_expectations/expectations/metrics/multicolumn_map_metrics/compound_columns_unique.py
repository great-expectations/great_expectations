from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


class CompoundColumnsUnique(MulticolumnMapMetricProvider):
    condition_metric_name = "compound_columns.unique"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )

    # TODO: <Alex>ALEX -- temporarily only a Pandas implementation is provided (others to follow).</Alex>
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        row_wise_cond = ~column_list.duplicated(keep=False)
        return row_wise_cond
