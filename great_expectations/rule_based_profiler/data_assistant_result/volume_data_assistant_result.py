from dataclasses import dataclass
from typing import Dict, Tuple, Union

from great_expectations.rule_based_profiler.altair import AltairDataTypes
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.helpers.util import default_field


@dataclass
class VolumeDataAssistantResult(DataAssistantResult):
    # A mapping is defined for which metrics to plot and their associated expectations
    METRIC_EXPECTATION_MAP: Dict[Union[str, Tuple[str]], str] = default_field(
        {
            "table.row_count": "expect_table_row_count_to_be_between",
            "column.distinct_values.count": "expect_column_unique_value_count_to_be_between",
        }
    )

    # A mapping is defined for the Altair data type associated with each metric
    # Altair data types can be one of:
    #     - Nominal: Metric is a discrete unordered category
    #     - Ordinal: Metric is a discrete ordered quantity
    #     - Quantitative: Metric is a continuous real-valued quantity
    #     - Temporal: Metric is a time or date value
    METRIC_TYPES: Dict[str, AltairDataTypes] = default_field(
        {
            "table.row_count": AltairDataTypes.QUANTITATIVE,
            "column.distinct_values.count": AltairDataTypes.QUANTITATIVE,
        }
    )
