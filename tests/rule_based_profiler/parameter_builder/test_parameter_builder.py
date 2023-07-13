from typing import Dict, List, Optional, ClassVar, Set

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
    """
    To set up execution of "ParameterBuilder.build_parameters()" only "id" property of "Domain" is required.
    """

    def __init__(self, domain_type=MetricDomainTypes.TABLE):
        super().__init__(domain_type=domain_type)

    @property
    def id(
        self,
    ) -> str:
        return "my_id"


class DummyParameterBuilder(ParameterBuilder):
    """
    Since goal of these tests is to ensure that private implementation method, "ParameterBuilder._build_parameters()",
    of public interface method, "ParameterBuilder.build_parameters()", is called (as proof that
    "evaluation_parameter_builder" dependencies of given "ParameterBuilder" are executed), all production functionality
    is "mocked", and "call_count" property is introduced and incremented in relevant method for assertions in tests.
    """

    exclude_field_names: ClassVar[Set[str]] = ParameterBuilder.exclude_field_names | {
        "call_count",
    }

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
        Only "_call_count" is incremented; otherwise, arbitrary test value is returned.
        """
        self._call_count += 1

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: 1,
                FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: {"batch_id_0": 1},
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: {"environment": "test"},
            }
        )


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_dependencies_evaluated_separately(
    mock_convert_to_json_serializable: mock.MagicMock,
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
    assert my_evaluation_dependency_0_parameter_builder.call_count == 1

    my_evaluation_dependency_1_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_evaluation_dependency_1_parameter_builder.call_count == 1

    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_dependent_parameter_builder.call_count == 1


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_dependencies_evaluated_in_parameter_builder(
    mock_convert_to_json_serializable: mock.MagicMock,
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
    my_evaluation_dependency_0_parameter_builder = (
        my_dependent_parameter_builder._evaluation_parameter_builders[0]
    )
    my_evaluation_dependency_1_parameter_builder = (
        my_dependent_parameter_builder._evaluation_parameter_builders[1]
    )

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
    assert my_evaluation_dependency_0_parameter_builder.call_count == 1
    assert my_evaluation_dependency_1_parameter_builder.call_count == 1
    assert my_dependent_parameter_builder.call_count == 1


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_dependencies_evaluated_mixed(
    mock_convert_to_json_serializable: mock.MagicMock,
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
    assert my_evaluation_dependency_0_parameter_builder.call_count == 1

    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        ParameterBuilderConfig(
            **my_evaluation_dependency_1_parameter_builder.to_json_dict(),
        ),
    ]
    my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_dependent_parameter_builder",
        evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
    )
    my_evaluation_dependency_1_parameter_builder = (
        my_dependent_parameter_builder._evaluation_parameter_builders[0]
    )

    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_evaluation_dependency_1_parameter_builder.call_count == 1
    assert my_dependent_parameter_builder.call_count == 1
