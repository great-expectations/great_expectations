from great_expectations.execution_engine import PandasExecutionEngine
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

    # TODO: <Alex>ALEX -- temporarily only a Pandas implementation is provided (others to follow).</Alex>
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        # TODO: <Alex>ALEX</Alex>
        print(f'\n[ALEX_TEST] WOUTPUT_SUM_TOTAL_PARAM: {sum_total} ; TYPE: {str(type(sum_total))}')
        a = column_list.sum(axis=1, skipna=False)
        print(f'\n[ALEX_TEST] D_FRAME:\n{column_list} ; TYPE: {str(type(column_list))}')
        print(f'\n[ALEX_TEST] WOUTPUT-DATA_SUM:\n{a} ; TYPE: {str(type(a))}')
        b = column_list.sum(axis=1, skipna=False)
        print(f'\n[ALEX_TEST] WOUTPUT-ROW_WISE_DATA:\n{b} ; TYPE: {str(type(b))}')
        c = column_list.sum(axis=1, skipna=False) == sum_total
        print(f'\n[ALEX_TEST] WOUTPUT-ROW_WISE_COND:\n{c} ; TYPE: {str(type(c))}')
        # TODO: <Alex>ALEX</Alex>
        row_wise_cond = column_list.sum(axis=1, skipna=False) == sum_total
        return row_wise_cond
