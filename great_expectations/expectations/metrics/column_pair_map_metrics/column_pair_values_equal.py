from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


class ColumnPairValuesEqual(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.equal"
    condition_value_keys = ("ignore_row_if",)
    domain_keys = ("batch_id", "table", "column_A", "column_B")
    default_kwarg_values = {"ignore_row_if": "both_values_are_missing"}

    # TODO: <Alex>ALEX -- temporarily only a Pandas implementation is provided (others to follow).</Alex>
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        ignore_row_if = kwargs.get("ignore_row_if")
        if not ignore_row_if:
            ignore_row_if = "both_values_are_missing"

        return column_A == column_B
