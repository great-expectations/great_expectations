import pathlib
from typing import List

import pytest
import pandas as pd

from great_expectations import DataContext
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
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
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.validator.validator import Validator


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
@pytest.mark.slow  # 1.67s
def test_column_value_missing_data_assistant_plot_expectations_and_metrics_correctly_handle_empty_plot_data(
    bobby_columnar_table_multi_batch_probabilistic_data_context: DataContext,
) -> None:
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    include_column_names: List[str] = [
        "congestion_surcharge",
    ]

    # TODO: <Alex>ALEX: Delete section below before "missingness" DataAssistant is released.</Alex>
    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="missingness_data_assistant",
        persist=False,
    )
    assert len(validator.batches) == 3

    data_assistant_name: str = "test_missingness_data_assistant"

    data_assistant = ColumnValueMissingDataAssistant(
        name=data_assistant_name,
        validator=validator,
    )
    data_assistant_result: DataAssistantResult = data_assistant.run()
    # TODO: <Alex>ALEX: Delete section above before "missingness" DataAssistant is released.</Alex>
    # TODO: <Alex>ALEX: Uncomment following method call before "missingness" DataAssistant is released.</Alex>
    # data_assistant_result: DataAssistantResult = context.assistants.missingness.run(
    #     batch_request=batch_request,
    #     include_column_names=include_column_names,
    # )
    # TODO: <Alex>ALEX</Alex>
    plot_result: PlotResult = data_assistant_result.plot_expectations_and_metrics(
        include_column_names=include_column_names
    )

    # This test passes only if absense of any metrics and expectations to plot does not cause exceptions to be raised.
    column_domain_charts: List[dict] = [p.to_dict() for p in plot_result.charts[2:]]
    assert len(column_domain_charts) == 0


@pytest.mark.integration
@pytest.mark.slow
def test_single_batch_multiple_columns(empty_data_context):
    context = empty_data_context
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
