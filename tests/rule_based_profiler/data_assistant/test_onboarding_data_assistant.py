from typing import Dict, Optional, cast

import pytest
from freezegun import freeze_time

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    Domain,
    ParameterNode,
)
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
    OnboardingDataAssistantResult,
)


@pytest.fixture
def bobby_onboarding_data_assistant_result(
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> OnboardingDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
        batch_request=batch_request
    )

    return cast(OnboardingDataAssistantResult, data_assistant_result)


@pytest.fixture
def quentin_implicit_invocation_result_actual_time(
    quentin_columnar_table_multi_batch_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
        batch_request=batch_request
    )

    return cast(OnboardingDataAssistantResult, data_assistant_result)


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def quentin_implicit_invocation_result_frozen_time(
    quentin_columnar_table_multi_batch_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
        batch_request=batch_request
    )

    return cast(OnboardingDataAssistantResult, data_assistant_result)


def test_onboarding_data_assistant_result_serialization(
    bobby_onboarding_data_assistant_result: OnboardingDataAssistantResult,
) -> None:
    onboarding_data_assistant_result_as_dict: dict = (
        bobby_onboarding_data_assistant_result.to_dict()
    )
    assert (
        set(onboarding_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        bobby_onboarding_data_assistant_result.to_json_dict()
        == onboarding_data_assistant_result_as_dict
    )
    assert len(bobby_onboarding_data_assistant_result.profiler_config.rules) == 7


def test_onboarding_data_assistant_metrics_count(
    bobby_onboarding_data_assistant_result: OnboardingDataAssistantResult,
) -> None:
    domain: Domain
    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    num_metrics: int

    domain_key: Domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
    )

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_onboarding_data_assistant_result.metrics_by_domain.items():
        if domain.is_superset(domain_key):
            num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 2

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_onboarding_data_assistant_result.metrics_by_domain.items():
        num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 178


def test_onboarding_data_assistant_result_batch_id_to_batch_identifier_display_name_map_coverage(
    bobby_onboarding_data_assistant_result: OnboardingDataAssistantResult,
):
    metrics_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ] = bobby_onboarding_data_assistant_result.metrics_by_domain

    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    parameter_node: ParameterNode
    batch_id: str
    assert all(
        bobby_onboarding_data_assistant_result.batch_id_to_batch_identifier_display_name_map[
            batch_id
        ]
        is not None
        for parameter_values_for_fully_qualified_parameter_names in metrics_by_domain.values()
        for parameter_node in parameter_values_for_fully_qualified_parameter_names.values()
        for batch_id in parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
        ].keys()
    )
