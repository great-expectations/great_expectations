from typing import Dict, Optional

import pytest

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    VARIABLES_KEY,
    ParameterContainer,
    ParameterNode,
    build_parameter_container_for_variables,
    get_parameter_value_by_fully_qualified_parameter_name,
)


@pytest.mark.integration
def test_metric_multi_batch_parameter_builder_bobby_single_batch_default(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    # Omitting "single_batch_mode" argument in order to exercise default (False) behavior.
    metric_multi_batch_parameter_builder = MetricMultiBatchParameterBuilder(
        name="row_count_range",
        metric_name="table.row_count",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        enforce_numeric_metric=True,
        replace_nan_with_zero=True,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = None

    metric_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "attributed_value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    parameter_node["value"] = None
    parameter_node["attributed_value"] = None

    assert parameter_node == expected_value_dict


@pytest.mark.integration
def test_metric_multi_batch_parameter_builder_bobby_single_batch_no(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_multi_batch_parameter_builder = MetricMultiBatchParameterBuilder(
        name="row_count_range",
        metric_name="table.row_count",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        single_batch_mode=f"{VARIABLES_KEY}single_batch_mode",
        enforce_numeric_metric=True,
        replace_nan_with_zero=True,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = build_parameter_container_for_variables(
        variables_configs={
            "single_batch_mode": False,
        }
    )

    metric_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "attributed_value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    parameter_node["value"] = None
    parameter_node["attributed_value"] = None

    assert parameter_node == expected_value_dict


@pytest.mark.integration
def test_metric_multi_batch_parameter_builder_bobby_single_batch_yes(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_multi_batch_parameter_builder = MetricMultiBatchParameterBuilder(
        name="row_count_range",
        metric_name="table.row_count",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        single_batch_mode=f"{VARIABLES_KEY}single_batch_mode",
        enforce_numeric_metric=True,
        replace_nan_with_zero=True,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = build_parameter_container_for_variables(
        variables_configs={
            "single_batch_mode": True,
        }
    )

    metric_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "attributed_value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    parameter_node["value"] = None
    parameter_node["attributed_value"] = None

    assert parameter_node == expected_value_dict
