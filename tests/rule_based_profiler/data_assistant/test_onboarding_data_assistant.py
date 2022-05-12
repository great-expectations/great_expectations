import pytest

from great_expectations import DataContext
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
    OnboardingDataAssistantResult,
)


@pytest.fixture
def onboarding_data_assistant_result(
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> OnboardingDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    return context.assistants.onboarding.run(batch_request=batch_request)


def test_onboarding_data_assistant_result_serialization(
    onboarding_data_assistant_result: OnboardingDataAssistantResult,
) -> None:
    onboarding_data_assistant_result_as_dict: dict = (
        onboarding_data_assistant_result.to_dict()
    )
    assert (
        set(onboarding_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        onboarding_data_assistant_result.to_json_dict()
        == onboarding_data_assistant_result_as_dict
    )
    assert len(onboarding_data_assistant_result.profiler_config.rules) == 2
