from typing import Dict, List, Optional

import pytest
from unittest import mock

from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.types.attributes import Attributes
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
)


class DummyParameterBuilder(ParameterBuilder):
    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: 1,
                FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: {"batch_id_0": 1},
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: {"environment": "test"},
            }
        )


@pytest.mark.unit
@mock.patch("test_parameter_builder.DummyParameterBuilder._build_parameters")
def test_parameter_builder_dependencies_evaluated_separately(
    mock_parameter_builder__build_parameters: mock.MagicMock,
):
    my_evaluation_dependency_0_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_0",
            evaluation_parameter_builder_configs=None,
            data_context=None,
        )
    )
    my_evaluation_dependency_1_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_1",
            evaluation_parameter_builder_configs=None,
            data_context=None,
        )
    )

    my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_dependent_parameter_builder",
        evaluation_parameter_builder_configs=None,
        data_context=None,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        rule_name="my_rule",
    )

    variables: Optional[ParameterContainer] = None

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    my_evaluation_dependency_0_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert mock_parameter_builder__build_parameters.call_count == 1

    my_evaluation_dependency_1_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert mock_parameter_builder__build_parameters.call_count == 2

    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert mock_parameter_builder__build_parameters.call_count == 3

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=my_dependent_parameter_builder.json_serialized_fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )
    assert parameter_node == "mock.Mock"


@pytest.mark.unit
@mock.patch("test_parameter_builder.DummyParameterBuilder._build_parameters")
def test_parameter_builder_dependencies_evaluated_in_parameter_builder(
    mock_parameter_builder__build_parameters: mock.MagicMock,
):
    my_evaluation_dependency_0_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_0",
            evaluation_parameter_builder_configs=None,
            data_context=None,
        )
    )
    my_evaluation_dependency_1_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_1",
            evaluation_parameter_builder_configs=None,
            data_context=None,
        )
    )

    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **my_evaluation_dependency_0_parameter_builder.to_json_dict()
        ),
        ParameterBuilderConfig(
            **my_evaluation_dependency_1_parameter_builder.to_json_dict(),
        ),
    ]
    my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_dependent_parameter_builder",
        evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        data_context=None,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        rule_name="my_rule",
    )

    variables: Optional[ParameterContainer] = None

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert mock_parameter_builder__build_parameters.call_count == 3

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=my_dependent_parameter_builder.json_serialized_fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )
    assert parameter_node == "mock.Mock"
