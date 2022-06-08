from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)


class VolumeDataAssistantResult(DataAssistantResult):
    # A mapping is defined for which metrics to plot and their associated expectations
    METRIC_EXPECTATION_MAP = {
        "table.row_count": "expect_table_row_count_to_be_between",
        "column.distinct_values.count": "expect_column_unique_value_count_to_be_between",
    }
