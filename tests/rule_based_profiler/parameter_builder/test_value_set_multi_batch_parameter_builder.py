from typing import Any, Collection, Dict, List, Optional, Set, cast

import pytest

from great_expectations import DataContext
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_builder.value_set_multi_batch_parameter_builder import (
    ValueSetMultiBatchParameterBuilder,
    _get_unique_values_from_nested_collection_of_sets,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
)


@pytest.mark.integration
@pytest.mark.slow  # 1.08s
def test_instantiation_value_set_multi_batch_parameter_builder(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    # noinspection PyUnusedLocal
    parameter_builder: ParameterBuilder = ValueSetMultiBatchParameterBuilder(
        name="my_name",
        data_context=data_context,
    )


@pytest.mark.integration
@pytest.mark.slow  # 1.07s
def test_instantiation_value_set_multi_batch_parameter_builder_no_name(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    with pytest.raises(TypeError) as excinfo:
        # noinspection PyUnusedLocal,PyArgumentList
        parameter_builder: ParameterBuilder = ValueSetMultiBatchParameterBuilder(
            data_context=data_context,
        )
    assert "__init__() missing 1 required positional argument: 'name'" in str(
        excinfo.value
    )


@pytest.mark.integration
@pytest.mark.slow  # 1.19s
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
    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    value_set_multi_batch_parameter_builder: ParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_event_type_value_set",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
        )
    )

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = None
    value_set_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    assert (
        parameter_container.parameter_nodes is None
        or len(parameter_container.parameter_nodes) == 1
    )

    expected_value_set: List[int] = [19, 22, 73]
    expected_parameter_node_as_dict: dict = {
        "value": expected_value_set,
        "details": {
            "parse_strings_as_datetimes": False,
            "metric_configuration": {
                "domain_kwargs": {"column": "event_type"},
                "metric_name": "column.distinct_values",
                "metric_value_kwargs": None,
            },
            "num_batches": 1,
        },
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_event_type_value_set"
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=fully_qualified_parameter_name_for_value,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert sorted(parameter_node.value) == expected_parameter_node_as_dict["value"]
    assert parameter_node.details == expected_parameter_node_as_dict["details"]


@pytest.mark.integration
@pytest.mark.slow  # 1.20s
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
    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    value_set_multi_batch_parameter_builder: ParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_user_agent_value_set",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
        )
    )

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = None
    value_set_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    assert (
        parameter_container.parameter_nodes is None
        or len(parameter_container.parameter_nodes) == 1
    )

    expected_value_set: List[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    ]
    expected_parameter_node_as_dict: dict = {
        "value": expected_value_set,
        "details": {
            "parse_strings_as_datetimes": False,
            "metric_configuration": {
                "domain_kwargs": {"column": "user_agent"},
                "metric_name": "column.distinct_values",
                "metric_value_kwargs": None,
            },
            "num_batches": 1,
        },
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_user_agent_value_set"
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=fully_qualified_parameter_name_for_value,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert sorted(parameter_node.value) == expected_parameter_node_as_dict["value"]
    assert parameter_node.details == expected_parameter_node_as_dict["details"]


@pytest.mark.integration
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
    value_set_multi_batch_parameter_builder: ParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_passenger_count_value_set",
            metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
            data_context=data_context,
        )
    )

    metric_domain_kwargs: dict = {"column": "passenger_count"}
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
    value_set_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    assert (
        parameter_container.parameter_nodes is None
        or len(parameter_container.parameter_nodes) == 1
    )

    expected_value_set: List[int] = [0, 1, 2, 3, 4, 5, 6]
    expected_parameter_node_as_dict: dict = {
        "value": expected_value_set,
        "details": {
            "parse_strings_as_datetimes": False,
            "metric_configuration": {
                "metric_name": "column.distinct_values",
                "domain_kwargs": {"column": "passenger_count"},
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_passenger_count_value_set"
    )
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=fully_qualified_parameter_name_for_value,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert sorted(parameter_node.value) == expected_parameter_node_as_dict["value"]
    assert parameter_node.details == expected_parameter_node_as_dict["details"]


@pytest.mark.integration
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
    value_set_multi_batch_parameter_builder: ParameterBuilder = (
        ValueSetMultiBatchParameterBuilder(
            name="my_store_and_fwd_flag_value_set",
            metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
            data_context=data_context,
        )
    )

    metric_domain_kwargs: dict = {"column": "store_and_fwd_flag"}
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
    value_set_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    assert (
        parameter_container.parameter_nodes is None
        or len(parameter_container.parameter_nodes) == 1
    )

    expected_value_set: List[str] = ["N", "Y"]
    expected_parameter_node_as_dict: dict = {
        "value": expected_value_set,
        "details": {
            "parse_strings_as_datetimes": False,
            "metric_configuration": {
                "metric_name": "column.distinct_values",
                "domain_kwargs": {"column": "store_and_fwd_flag"},
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_store_and_fwd_flag_value_set"
    )
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=fully_qualified_parameter_name_for_value,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert sorted(parameter_node.value) == expected_parameter_node_as_dict["value"]
    assert parameter_node.details == expected_parameter_node_as_dict["details"]


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
@pytest.mark.unit
def test__get_unique_values_from_nested_collection_of_sets(test_input, expected):
    """
    What does this test and why?
    Tests that all types of string / int inputs are processed appropriately
    by _get_unique_values_from_nested_collection_of_sets
    """
    test_input = cast(Collection[Collection[Set[Any]]], test_input)
    assert (
        _get_unique_values_from_nested_collection_of_sets(collection=test_input)
        == expected
    )
