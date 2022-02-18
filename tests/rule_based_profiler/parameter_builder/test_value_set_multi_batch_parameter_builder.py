# TODO AJB 20220216 Add tests for the following conditions:
# Test instantiation
# Test error states
# Integration tests:
# Test single batch (alice)
# Test single batch (alice) exceeds cardinality limit
# Test multi batch (bobby)
# Test multi batch (bobby) exceeds cardinality limit
from typing import List

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder.value_set_multi_batch_parameter_builder import (
    ValueSetMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)


def test_value_set_multi_batch_parameter_builder_alice_single_batch(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context
    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_agent"}

    value_set_multi_batch_parameter_builder: ValueSetMultiBatchParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_user_agent_value_set",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
    )

    assert parameter_container.parameter_nodes is None

    value_set_multi_batch_parameter_builder._build_parameters(
        parameter_container=parameter_container,
        domain=domain,
    )

    assert len(parameter_container.parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.my_user_agent_value_set"
    expected_value: dict = {
        "value_set": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        ],
    }

    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters={domain.id: parameter_container},
        )
        == expected_value
    )


def test_value_set_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    metric_domain_kwargs: dict = {"column": "pickup_location_id"}

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    value_set_multi_batch_parameter_builder: ValueSetMultiBatchParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_pickup_location_id_value_set",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    value_set_multi_batch_parameter_builder._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    assert len(parameter_container.parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_pickup_location_id_value_set"
    )
    expected_value_set: List[str] = ["please", "fill", "with", "data"]
    expected_value: dict = {
        "value_set": expected_value_set,
    }

    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters={domain.id: parameter_container},
        )
        == expected_value
    )
