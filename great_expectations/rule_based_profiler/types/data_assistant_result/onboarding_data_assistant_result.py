from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)


class OnboardingDataAssistantResult(DataAssistantResult):
    # A mapping is defined for which metrics to plot and their associated expectations
    EXPECTATION_METRIC_MAP = {
        "expect_table_row_count_to_be_between": "table.row_count",
        "expect_table_columns_to_match_ordered_list": "table.columns",
        "expect_column_unique_value_count_to_be_between": "column.distinct_values.count",
    }
