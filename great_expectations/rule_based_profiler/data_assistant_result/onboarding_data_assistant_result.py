from typing import Dict, Tuple, Union

from great_expectations.rule_based_profiler.altair import AltairDataTypes
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


class OnboardingDataAssistantResult(DataAssistantResult):
    @property
    def metric_expectation_map(self) -> Dict[Union[str, Tuple[str, ...]], str]:
        """
        A mapping is defined for which metrics to plot and their associated expectations.
        """
        return {
            "table.columns": "expect_table_columns_to_match_set",
            "table.row_count": "expect_table_row_count_to_be_between",
            "column.distinct_values.count": "expect_column_unique_value_count_to_be_between",
            "column.min": "expect_column_min_to_be_between",
            "column.max": "expect_column_max_to_be_between",
            "column.mean": "expect_column_mean_to_be_between",
            "column.median": "expect_column_median_to_be_between",
            "column.standard_deviation": "expect_column_stdev_to_be_between",
            "column.quantile_values": "expect_column_quantile_values_to_be_between",
            ("column.min", "column.max"): "expect_column_values_to_be_between",
        }

    @property
    def metric_types(self) -> Dict[str, AltairDataTypes]:
        """
        A mapping is defined for the Altair data type associated with each metric.
        """
        # Altair data types can be one of:
        #     - Nominal: Metric is a discrete unordered category
        #     - Ordinal: Metric is a discrete ordered quantity
        #     - Quantitative: Metric is a continuous real-valued quantity
        #     - Temporal: Metric is a time or date value
        return {
            "table.columns": AltairDataTypes.NOMINAL,
            "table.row_count": AltairDataTypes.QUANTITATIVE,
            "column.distinct_values.count": AltairDataTypes.QUANTITATIVE,
            "column.min": AltairDataTypes.QUANTITATIVE,
            "column.max": AltairDataTypes.QUANTITATIVE,
            "column.mean": AltairDataTypes.QUANTITATIVE,
            "column.median": AltairDataTypes.QUANTITATIVE,
            "column.standard_deviation": AltairDataTypes.QUANTITATIVE,
            "column.quantile_values": AltairDataTypes.QUANTITATIVE,
        }
