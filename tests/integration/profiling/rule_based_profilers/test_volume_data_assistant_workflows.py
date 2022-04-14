import pytest

from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.data_assistant import (
    DataAssistant,
    VolumeDataAssistant,
)
from great_expectations.rule_based_profiler.types import DataAssistantResult


def test_volume_data_assistant_plot_missing_result_error(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    data_assistant.build()

    with pytest.raises(TypeError) as e:
        data_assistant.plot()

    assert (
        e.value.args[0]
        == "plot() missing 1 required positional argument: 'data_assistant_result'"
    )


def test_volume_data_assistant_plot(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="test_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    data_assistant.build()

    expectation_suite_name: str = "test_suite"
    result: DataAssistantResult = data_assistant.run(
        expectation_suite_name=expectation_suite_name,
    )

    data_assistant.plot(data_assistant_result=result)
