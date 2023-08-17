from typing import List
from unittest.mock import patch

import pandas as pd
import pytest

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.rule_based_profiler.data_assistant import (
    ColumnValueMissingDataAssistant,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    ColumnValueMissingDataAssistantResult,
    DataAssistantResult,
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


@pytest.mark.big
@pytest.mark.slow
def test_single_batch_multiple_columns(ephemeral_context_with_defaults):
    context = ephemeral_context_with_defaults
    datasource = context.sources.add_or_update_pandas("my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    # noinspection PyTypeChecker
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


@pytest.mark.unit
def test_column_value_missing_data_assistant_uses_multi_batch_mode_for_multi_batch(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    with patch(
        "great_expectations.rule_based_profiler.data_assistant.data_assistant.run_profiler_on_data"
    ) as mock_run_profiler_on_data:
        context.assistants.missingness.run(batch_request=batch_request)

    call_args = mock_run_profiler_on_data.call_args[1]
    profiler = call_args["profiler"]
    rules = profiler.config.rules
    expectation_configuration_builders = rules["column_value_missing_rule"][
        "expectation_configuration_builders"
    ]
    for builder in expectation_configuration_builders:
        validation_parameter_builder_configs = builder[
            "validation_parameter_builder_configs"
        ]
        assert len(validation_parameter_builder_configs) == 1
        assert validation_parameter_builder_configs[0]["mode"] == "multi_batch"


@pytest.mark.unit
def test_column_value_missing_data_assistant_uses_single_batch_mode_for_single_batch(
    ephemeral_context_with_defaults,
):
    context = ephemeral_context_with_defaults
    datasource = context.sources.add_or_update_pandas("my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    # noinspection PyTypeChecker
    df = pd.DataFrame(
        {
            "non-null": [i for i in range(100)],
            "null": [None for _ in range(100)],
            "low-null": [None for _ in range(38)] + [i for i in range(62)],
        }
    )
    batch_request = asset.build_batch_request(dataframe=df)

    with patch(
        "great_expectations.rule_based_profiler.data_assistant.data_assistant.run_profiler_on_data"
    ) as mock_run_profiler_on_data:
        context.assistants.missingness.run(batch_request=batch_request)

    call_args = mock_run_profiler_on_data.call_args[1]
    profiler = call_args["profiler"]
    rules = profiler.config.rules
    expectation_configuration_builders = rules["column_value_missing_rule"][
        "expectation_configuration_builders"
    ]
    for builder in expectation_configuration_builders:
        validation_parameter_builder_configs = builder[
            "validation_parameter_builder_configs"
        ]
        assert len(validation_parameter_builder_configs) == 1
        assert validation_parameter_builder_configs[0]["mode"] == "single_batch"


@pytest.mark.big
@pytest.mark.spark
def test_missingness_data_assistant_numeric_column_containing_dot_spark(
    spark_session,
    ephemeral_context_with_defaults,
):
    """What does this test and why?

    Spark identifiers are less restrictive than ANSI SQL identifiers. This test ensures that we can use identifiers
    compliant with: https://spark.apache.org/docs/latest/sql-ref-identifier.html, specifically the dot case e.g. `a.b`.
    """

    columns = ["snake_case", "kebab-case", "dot.case"]
    values = [(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)]

    df = spark_session.createDataFrame(data=values, schema=columns)

    context = ephemeral_context_with_defaults
    datasource = context.sources.add_or_update_spark("my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    data_assistant_result: DataAssistantResult = context.assistants.missingness.run(
        batch_request=batch_request, exclude_column_names=["snake_case", "kebab-case"]
    )

    # Histogram metric cannot be computed when using columns containing `.` with the current metric implementation.
    # Other metrics should pass.
    assert list(data_assistant_result.rule_exception_tracebacks.keys()) == [
        "column_value_missing_rule"
    ]
    assert (
        data_assistant_result.rule_exception_tracebacks["column_value_missing_rule"][
            "exception_message"
        ]
        == "Column names cannot contain '.' when computing parameters for unexpected count statistics."
    )
