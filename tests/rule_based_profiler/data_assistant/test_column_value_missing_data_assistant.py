import pathlib
from typing import List

import pytest
import pandas as pd

from great_expectations.rule_based_profiler.data_assistant import (
    ColumnValueMissingDataAssistant,
)
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


@pytest.mark.integration
@pytest.mark.slow
def test_single_batch_multiple_columns(ephemeral_context_with_defaults):
    context = ephemeral_context_with_defaults
    datasource = context.sources.add_or_update_pandas("my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    df = pd.DataFrame(
        {
            "non-null": [i for i in range(100)],
            "null": [None for _ in range(100)],
            "low-null": [None for _ in range(38)] + [i for i in range(62)],
        }
    )
    batch_request = asset.build_batch_request(dataframe=df)

    context.assistants._register("my_data_assistant", ColumnValueMissingDataAssistant)
    result = context.assistants.my_data_assistant.run(batch_request=batch_request)

    assert len(result.expectation_configurations) == 3

    expected_results = {
        "null": {"mostly": 1.0, "expectation": "expect_column_values_to_be_null"},
        "non-null": {
            "mostly": 1.0,
            "expectation": "expect_column_values_to_not_be_null",
        },
        "low-null": {
            "mostly": 0.6,
            "expectation": "expect_column_values_to_not_be_null",
        },
    }

    for expectation in result.expectation_configurations:
        assert expectation.expectation_type in [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_null",
        ]
        column = expectation.kwargs["column"]
        assert expectation.kwargs["mostly"] == expected_results[column]["mostly"]
        assert expectation.expectation_type == expected_results[column]["expectation"]
