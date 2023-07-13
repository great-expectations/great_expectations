from typing import Dict, List, Optional

import pytest
from unittest import mock

from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.types.attributes import Attributes
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
)


class DummyDomain(Domain):
    def __init__(self, domain_type=MetricDomainTypes.TABLE):
        super().__init__(domain_type=domain_type)

    @property
    def id(
        self,
    ) -> str:
        return "my_id"


class DummyParameterBuilder(ParameterBuilder):
    def __init__(
        self,
        name: str,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
    ) -> None:
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=None,
        )

        self._call_count: int = 0

    @property
    def call_count(self) -> int:
        return self._call_count

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
        self._call_count += 1

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
        )
    )
    my_evaluation_dependency_1_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_1",
            evaluation_parameter_builder_configs=None,
        )
    )

    my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_dependent_parameter_builder",
        evaluation_parameter_builder_configs=None,
    )

    with mock.patch(
        "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
        return_value="my_json_string",
    ):
        domain = DummyDomain()

        parameter_container = ParameterContainer(parameter_nodes=None)
        parameters: Dict[str, ParameterContainer] = {
            domain.id: parameter_container,
        }

        my_evaluation_dependency_0_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 1

        my_evaluation_dependency_1_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 2

        my_dependent_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 3


@pytest.mark.unit
@mock.patch("test_parameter_builder.DummyParameterBuilder._build_parameters")
def test_parameter_builder_dependencies_evaluated_in_parameter_builder(
    mock_parameter_builder__build_parameters: mock.MagicMock,
):
    my_evaluation_dependency_0_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_0",
            evaluation_parameter_builder_configs=None,
        )
    )
    my_evaluation_dependency_1_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_1",
            evaluation_parameter_builder_configs=None,
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
    )

    with mock.patch(
        "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
        return_value="my_json_string",
    ):
        domain = DummyDomain()

        parameter_container = ParameterContainer(parameter_nodes=None)
        parameters: Dict[str, ParameterContainer] = {
            domain.id: parameter_container,
        }

        my_dependent_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 3


@pytest.mark.unit
@mock.patch("test_parameter_builder.DummyParameterBuilder._build_parameters")
def test_parameter_builder_dependencies_evaluated_mixed(
    mock_parameter_builder__build_parameters: mock.MagicMock,
):
    my_evaluation_dependency_0_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_0",
            evaluation_parameter_builder_configs=None,
        )
    )
    my_evaluation_dependency_1_parameter_builder: ParameterBuilder = (
        DummyParameterBuilder(
            name="my_evaluation_dependency_parameter_name_1",
            evaluation_parameter_builder_configs=None,
        )
    )

    with mock.patch(
        "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
        return_value="my_json_string",
    ):
        domain = DummyDomain()

        parameter_container = ParameterContainer(parameter_nodes=None)
        parameters: Dict[str, ParameterContainer] = {
            domain.id: parameter_container,
        }

        my_evaluation_dependency_0_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 1

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            ParameterBuilderConfig(
                **my_evaluation_dependency_1_parameter_builder.to_json_dict(),
            ),
        ]
        my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
            name="my_dependent_parameter_builder",
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
        )

        my_dependent_parameter_builder.build_parameters(
            domain=domain,
            variables=None,
            parameters=parameters,
            batch_request=None,
            runtime_configuration=None,
        )
        assert mock_parameter_builder__build_parameters.call_count == 3
