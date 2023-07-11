from typing import Dict, Optional

import pytest

from great_expectations import DataContext
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    ValueCountsSingleBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
)

# module level markers
pytestmark = [pytest.mark.integration]


def test_instantiation_value_counts_single_batch_parameter_builder(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    # noinspection PyUnusedLocal
    parameter_builder_0: ParameterBuilder = ValueCountsSingleBatchParameterBuilder(
        name="my_name_0",
        data_context=data_context,
    )


def test_value_counts_single_batch_parameter_builder_alice(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = ValueCountsSingleBatchParameterBuilder(
        name="my_name",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_type"}
    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = None
    parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
        runtime_configuration=None,
    )

    expected_parameter_node_as_dict: dict = {
        "value": {
            "values": [19, 22, 73],
            "weights": [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
        },
        "details": {
            "metric_configuration": {
                "metric_name": "column.value_counts",
                "domain_kwargs": {"column": "event_type"},
                "metric_value_kwargs": {"sort": "value"},
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.json_serialized_fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_node_as_dict
