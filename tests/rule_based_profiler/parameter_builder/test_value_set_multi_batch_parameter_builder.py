from typing import Set

import pytest

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder.value_set_multi_batch_parameter_builder import (
    ValueSetMultiBatchParameterBuilder,
    _get_unique_values_from_nested_collection_of_sets,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)


def test_instantiation_value_set_multi_batch_parameter_builder():
    _: ValueSetMultiBatchParameterBuilder = ValueSetMultiBatchParameterBuilder(
        name="my_name",
    )


def test_instantiation_value_set_multi_batch_parameter_builder_no_name():
    with pytest.raises(TypeError) as excinfo:
        _: ValueSetMultiBatchParameterBuilder = ValueSetMultiBatchParameterBuilder()
    assert "__init__() missing 1 required positional argument: 'name'" in str(
        excinfo.value
    )


def test_value_set_multi_batch_parameter_builder_alice_single_batch_numeric(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context
    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "event_type"}

    value_set_multi_batch_parameter_builder: ValueSetMultiBatchParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_event_type_value_set",
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

    fully_qualified_parameter_name_for_value: str = "$parameter.my_event_type_value_set"
    expected_value_set: Set[int] = {73, 19, 22}
    expected_parameter_value: dict = {
        "value": expected_value_set,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "event_type"},
                "metric_name": "column.distinct_values",
            },
            "num_batches": 1,
        },
    }

    assert (
        get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_parameter_name_for_value,
            expected_return_type=dict,
            parameters={domain.id: parameter_container},
        )
        == expected_parameter_value
    )


def test_value_set_multi_batch_parameter_builder_alice_single_batch_string(
    alice_columnar_table_single_batch_context,
):
    """
    What does this test and why?
    This tests that non-numeric columns are handled appropriately,
    """
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
    expected_value_set: Set[str] = {
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
    }
    expected_parameter_value: dict = {
        "value": expected_value_set,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "user_agent"},
                "metric_name": "column.distinct_values",
            },
            "num_batches": 1,
        },
    }

    assert (
        get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_parameter_name_for_value,
            expected_return_type=dict,
            parameters={domain.id: parameter_container},
        )
        == expected_parameter_value
    )


def test_value_set_multi_batch_parameter_builder_bobby_numeric(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs_for_parameter_builder: str = "$domain.domain_kwargs"
    value_set_multi_batch_parameter_builder: ValueSetMultiBatchParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_passenger_count_value_set",
            metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)

    metric_domain_kwargs: dict = {"column": "passenger_count"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    value_set_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
    )

    assert len(parameter_container.parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_passenger_count_value_set"
    )
    expected_value_set: Set[int] = {0, 1, 2, 3, 4, 5, 6}
    expected_parameter_value: dict = {
        "value": expected_value_set,
        "details": {
            "metric_configuration": {
                "metric_name": "column.distinct_values",
                "domain_kwargs": {"column": "passenger_count"},
            },
            "num_batches": 3,
        },
    }

    assert (
        get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_parameter_name_for_value,
            expected_return_type=dict,
            parameters={domain.id: parameter_container},
        )
        == expected_parameter_value
    )


def test_value_set_multi_batch_parameter_builder_bobby_string(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs_for_parameter_builder: str = "$domain.domain_kwargs"
    value_set_multi_batch_parameter_builder: ValueSetMultiBatchParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_store_and_fwd_flag_value_set",
            metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)

    metric_domain_kwargs: dict = {"column": "store_and_fwd_flag"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    value_set_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
    )

    assert len(parameter_container.parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_store_and_fwd_flag_value_set"
    )
    expected_value_set: Set[str] = {"N", "Y"}
    expected_parameter_value: dict = {
        "value": expected_value_set,
        "details": {
            "metric_configuration": {
                "metric_name": "column.distinct_values",
                "domain_kwargs": {"column": "store_and_fwd_flag"},
            },
            "num_batches": 3,
        },
    }

    assert (
        get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_parameter_name_for_value,
            expected_return_type=dict,
            parameters={domain.id: parameter_container},
        )
        == expected_parameter_value
    )


@pytest.mark.parametrize(
    "test_input,expected",
    [
        [[[{1, 2, 3}, {1, 4, 5}]], {1, 2, 3, 4, 5}],
        [[[{1}, {2, 3}]], {1, 2, 3}],
        [[[{1}, {1, 2}]], {1, 2}],
        [[[{1}, {1}]], {1}],
        [
            [[{1, 2, 3}]],
            {1, 2, 3},
        ],
        [[[{"1", "2", "3"}, {"1", "4", "5"}]], {"1", "2", "3", "4", "5"}],
        [[[{"1"}, {"2", "3"}]], {"1", "2", "3"}],
        [[[{"1"}, {"1", "2"}]], {"1", "2"}],
        [[[{"1"}, {"1"}]], {"1"}],
        [
            [[{"1", "2", "3"}]],
            {"1", "2", "3"},
        ],
    ],
)
def test__get_unique_values_from_nested_collection_of_sets(test_input, expected):
    """
    What does this test and why?
    Tests that all types of string / int inputs are processed appropriately
    by _get_unique_values_from_nested_collection_of_sets
    """
    assert _get_unique_values_from_nested_collection_of_sets(test_input) == expected
