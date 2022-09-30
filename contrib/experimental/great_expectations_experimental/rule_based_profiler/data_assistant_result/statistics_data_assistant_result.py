from typing import Dict, Tuple, Union

from great_expectations.rule_based_profiler.altair import AltairDataTypes
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


class StatisticsDataAssistantResult(DataAssistantResult):
    """
    Note (9/30/2022): Plotting functionality is experimental.
    """

    @property
    def metric_expectation_map(self) -> Dict[Union[str, Tuple[str]], str]:
        """
        A mapping is defined for which metrics to plot and their associated expectations.
        """
        return {}

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
