from typing import Dict, List, Optional, cast
from unittest import mock

import pytest
from freezegun import freeze_time

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant import (
    GrowthNumericDataAssistant,
)
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant_result import (
    GrowthNumericDataAssistantResult,
)
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    ParameterNode,
)

# noinspection PyUnresolvedReferences
from tests.conftest import (
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_columnar_table_multi_batch_probabilistic_data_context,
    quentin_columnar_table_multi_batch_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
)


@pytest.fixture
def bobby_growth_numeric_data_assistant_result_usage_stats_enabled(
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
def bobby_growth_numeric_data_assistant_result(
    bobby_columnar_table_multi_batch_probabilistic_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
def quentin_implicit_invocation_result_actual_time(
    quentin_columnar_table_multi_batch_data_context: DataContext,
) -> GrowthNumericDataAssistantResult:
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.fixture(scope="module")
@freeze_time("09/26/2019 13:42:41")
def quentin_implicit_invocation_result_frozen_time(
    quentin_columnar_table_multi_batch_data_context: DataContext,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(GrowthNumericDataAssistantResult, data_assistant_result)


@pytest.mark.integration
@pytest.mark.slow  # 6.90s
def test_growth_numeric_data_assistant_result_serialization(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
) -> None:
    growth_numeric_data_assistant_result_as_dict: dict = (
        bobby_growth_numeric_data_assistant_result.to_dict()
    )
    assert (
        set(growth_numeric_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        bobby_growth_numeric_data_assistant_result.to_json_dict()
        == growth_numeric_data_assistant_result_as_dict
    )
    assert len(bobby_growth_numeric_data_assistant_result.profiler_config.rules) == 4


@pytest.mark.integration
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.slow  # 7.34s
def test_growth_numeric_data_assistant_result_get_expectation_suite(
    mock_emit,
    bobby_growth_numeric_data_assistant_result_usage_stats_enabled: GrowthNumericDataAssistantResult,
):
    expectation_suite_name: str = "my_suite"

    suite: ExpectationSuite = bobby_growth_numeric_data_assistant_result_usage_stats_enabled.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    assert suite is not None and len(suite.expectations) > 0

    assert mock_emit.call_count == 1

    # noinspection PyUnresolvedReferences
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert (
        actual_events[-1][0][0]["event"]
        == UsageStatsEvents.DATA_ASSISTANT_RESULT_GET_EXPECTATION_SUITE.value
    )


@pytest.mark.integration
def test_growth_numeric_data_assistant_metrics_count(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
) -> None:
    domain: Domain
    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    num_metrics: int

    domain_key = Domain(
        domain_type=MetricDomainTypes.TABLE,
    )

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_growth_numeric_data_assistant_result.metrics_by_domain.items():
        if domain.is_superset(domain_key):
            num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 2

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_growth_numeric_data_assistant_result.metrics_by_domain.items():
        num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 121


@pytest.mark.integration
def test_growth_numeric_data_assistant_result_batch_id_to_batch_identifier_display_name_map_coverage(
    bobby_growth_numeric_data_assistant_result: GrowthNumericDataAssistantResult,
):
    metrics_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ] = bobby_growth_numeric_data_assistant_result.metrics_by_domain

    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    parameter_node: ParameterNode
    batch_id: str
    assert all(
        bobby_growth_numeric_data_assistant_result._batch_id_to_batch_identifier_display_name_map[
            batch_id
        ]
        is not None
        for parameter_values_for_fully_qualified_parameter_names in metrics_by_domain.values()
        for parameter_node in parameter_values_for_fully_qualified_parameter_names.values()
        for batch_id in (
            parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY]
            if FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY in parameter_node
            else {}
        ).keys()
    )


@pytest.mark.integration
@pytest.mark.slow  # 39.26s
def test_growth_numeric_data_assistant_get_metrics_and_expectations_using_implicit_invocation_with_variables_directives(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
        estimation="flag_outliers",
        numeric_columns_rule={
            "round_decimals": 15,
            "false_positive_rate": 0.1,
            "random_seed": 43792,
        },
        categorical_columns_rule={
            "false_positive_rate": 0.1,
            # "round_decimals": 4,
        },
    )
    assert (
        data_assistant_result.profiler_config.rules["numeric_columns_rule"][
            "variables"
        ]["round_decimals"]
        == 15
    )
    assert (
        data_assistant_result.profiler_config.rules["numeric_columns_rule"][
            "variables"
        ]["false_positive_rate"]
        == 1.0e-1
    )
    assert (
        data_assistant_result.profiler_config.rules["categorical_columns_rule"][
            "variables"
        ]["false_positive_rate"]
        == 1.0e-1
    )


@pytest.mark.integration
@pytest.mark.slow  # 38.26s
def test_growth_numeric_data_assistant_get_metrics_and_expectations_using_implicit_invocation_with_estimation_directive(
    quentin_columnar_table_multi_batch_data_context,
):
    context: DataContext = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.growth_numeric.run(
        batch_request=batch_request,
    )

    rule_config: dict
    assert all(
        [
            rule_config["variables"]["estimator"] == "exact"
            if "estimator" in rule_config["variables"]
            else True
            for rule_config in data_assistant_result.profiler_config.rules.values()
        ]
    )
