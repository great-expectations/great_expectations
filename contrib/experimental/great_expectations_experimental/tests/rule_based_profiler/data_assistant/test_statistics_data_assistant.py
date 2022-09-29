import math
from typing import Any, Dict, Optional, cast

import numpy as np
import pytest

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant import (
    StatisticsDataAssistant,
)
from contrib.experimental.great_expectations_experimental.rule_based_profiler.data_assistant_result import (
    StatisticsDataAssistantResult,
)
from great_expectations import DataContext
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.helpers.util import (
    convert_metric_values_to_float_dtype_best_effort,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    ParameterNode,
)

# noinspection PyUnresolvedReferences
from tests.conftest import (
    bobby_columnar_table_multi_batch_deterministic_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
)


@pytest.fixture()
def bobby_statistics_data_assistant_result(
    monkeypatch,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
    bobby_columnar_table_multi_batch_deterministic_data_context: DataContext,
) -> StatisticsDataAssistantResult:
    context: DataContext = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    data_assistant_result: DataAssistantResult = context.assistants.statistics.run(
        batch_request=batch_request,
        estimation="flag_outliers",
    )

    return cast(StatisticsDataAssistantResult, data_assistant_result)


@pytest.mark.slow  # 6.90s
def test_statistics_data_assistant_result_serialization(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
) -> None:
    statistics_data_assistant_result_as_dict: dict = (
        bobby_statistics_data_assistant_result.to_dict()
    )
    assert (
        set(statistics_data_assistant_result_as_dict.keys())
        == DataAssistantResult.ALLOWED_KEYS
    )
    assert (
        bobby_statistics_data_assistant_result.to_json_dict()
        == statistics_data_assistant_result_as_dict
    )
    assert len(bobby_statistics_data_assistant_result.profiler_config.rules) == 5


@pytest.mark.integration
def test_statistics_data_assistant_metrics_count(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
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
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        if domain.is_superset(domain_key):
            num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 0

    num_metrics = 0
    for (
        domain,
        parameter_values_for_fully_qualified_parameter_names,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        num_metrics += len(parameter_values_for_fully_qualified_parameter_names)

    assert num_metrics == 153


@pytest.mark.integration
def test_statistics_data_assistant_result_batch_id_to_batch_identifier_display_name_map_coverage(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
):
    metrics_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ] = bobby_statistics_data_assistant_result.metrics_by_domain

    parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
    parameter_node: ParameterNode
    batch_id: str
    assert all(
        bobby_statistics_data_assistant_result._batch_id_to_batch_identifier_display_name_map[
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
def test_statistics_data_assistant_result_normalized_metrics_vector_output(
    bobby_statistics_data_assistant_result: StatisticsDataAssistantResult,
):
    domain: Domain
    metrics: Dict[str, ParameterNode]
    parameter_name: str
    parameter_node: ParameterNode
    parameter_value: Any
    ndarray_is_datetime_type: bool
    parameter_value_magnitude: float
    metrics_magnitude: float = 0.0
    num_elements: int = 0
    for (
        domain,
        metrics,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        for parameter_name, parameter_node in metrics.items():
            parameter_value = np.asarray(parameter_node.value)
            if parameter_value.ndim == 0:
                parameter_value = np.asarray([parameter_node.value])

            (
                ndarray_is_datetime_type,
                parameter_value,
            ) = convert_metric_values_to_float_dtype_best_effort(
                metric_values=parameter_value
            )
            parameter_value_magnitude = np.linalg.norm(parameter_value)
            metrics_magnitude += parameter_value_magnitude * parameter_value_magnitude
            num_elements += 1

    metrics_magnitude = math.sqrt(metrics_magnitude)

    assert np.allclose(metrics_magnitude, 3.331205802908463e3)
    assert (
        num_elements == 153
    )  # This quantity must be equal to the total number of metrics.

    normalized_metrics_vector: MetricValues = []
    for (
        domain,
        metrics,
    ) in bobby_statistics_data_assistant_result.metrics_by_domain.items():
        for parameter_name, parameter_node in metrics.items():
            parameter_value = np.asarray(parameter_node.value)
            if parameter_value.ndim == 0:
                parameter_value = np.asarray([parameter_node.value])

            (
                ndarray_is_datetime_type,
                parameter_value,
            ) = convert_metric_values_to_float_dtype_best_effort(
                metric_values=parameter_value
            )
            parameter_value = parameter_value / metrics_magnitude
            normalized_metrics_vector.append(parameter_value)

    normalized_metrics_vector = np.asarray(normalized_metrics_vector)

    assert normalized_metrics_vector.ndim == 1

    normalized_metrics_vector_magnitude: float = 0.0
    for parameter_value in normalized_metrics_vector:
        parameter_value_magnitude = np.linalg.norm(parameter_value)
        normalized_metrics_vector_magnitude += (
            parameter_value_magnitude * parameter_value_magnitude
        )

    assert np.allclose(normalized_metrics_vector_magnitude, 1.0)
