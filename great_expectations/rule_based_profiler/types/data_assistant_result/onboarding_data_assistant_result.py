from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)


class OnboardingDataAssistantResult(DataAssistantResult):
    # A mapping is defined for which metrics to plot and their associated expectations
    EXPECTATION_METRIC_MAP = {
        "expect_table_row_count_to_be_between": "table.row_count",
        "expect_column_unique_value_count_to_be_between": "column.distinct_values.count",
        "expect_column_min_to_be_between": "column.min",
        "expect_column_max_to_be_between": "column.max",
        # "expect_column_values_to_be_between": "column_values.between",
        "expect_column_mean_to_be_between": "column.mean",
        "expect_column_median_to_be_between": "column.median",
        # "expect_column_quantile_values_to_be_between": "column.quantile_values",
        "expect_column_stdev_to_be_between": "column.standard_deviation",
    }
