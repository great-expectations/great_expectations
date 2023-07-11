from typing import List

import pytest

from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
    ColumnValueMissingDataAssistantResult,
)
from great_expectations.rule_based_profiler.data_assistant_result.plot_result import (
    PlotResult,
)


@pytest.mark.unit
def test_column_value_missing_data_assistant_result_plot_expectations_and_metrics_correctly_handle_empty_plot_data() -> (
    None
):
    data_assistant_result: DataAssistantResult = ColumnValueMissingDataAssistantResult()

    include_column_names: List[str] = [
        "congestion_surcharge",
    ]
    plot_result: PlotResult = data_assistant_result.plot_expectations_and_metrics(
        include_column_names=include_column_names
    )

    # This test passes only if absense of any metrics and expectations to plot does not cause exceptions to be raised.
    column_domain_charts: List[dict] = [p.to_dict() for p in plot_result.charts[2:]]
    assert len(column_domain_charts) == 0
