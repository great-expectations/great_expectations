from typing import Dict, Tuple, Union

from great_expectations.rule_based_profiler.altair import AltairDataTypes
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


class ColumnValueMissingDataAssistantResult(DataAssistantResult):
    @property
    def metric_expectation_map(self) -> Dict[Union[str, Tuple[str, ...]], str]:
        """
        A mapping is defined for which metrics to plot and their associated expectations.
        """
        return {
            "column_values.nonnull.unexpected_count": "expect_column_values_to_not_be_null",
            "column_values.null.unexpected_count": "expect_column_values_to_be_null",
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
            "column_values.nonnull.unexpected_count": AltairDataTypes.QUANTITATIVE,
            "column_values.null.unexpected_count": AltairDataTypes.QUANTITATIVE,
        }
