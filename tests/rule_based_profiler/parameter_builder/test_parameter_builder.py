from typing import Dict, List, Optional, ClassVar, Set, Union

import pytest
from unittest import mock

from great_expectations.core.domain import Domain
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
    get_fully_qualified_parameter_names,
    RAW_PARAMETER_KEY,
    PARAMETER_KEY,
)


"""
Tests in this module focus on behavior aspects of "ParameterBuilder.build_parameters()" -- this public method assesses
dependencies on evaluation "ParameterBuilder" objects (if any specified), and resolves these dependencies, prior to
calling its own interface method, "ParameterBuilder.build_parameters()".

Method "ParameterBuilder.resolve_evaluation_dependencies()" is used inside "ParameterBuilder.build_parameters()"; its
execution affects contents of shared memory object, "parameters: Dict[str, ParameterContainer]", whose
"ParameterContainer" stores computation results of every "ParameterBuilder", evaluated within scope of given "Domain"
object.  When one "ParameterBuilder" specifies dependencies on other "ParameterBuilder" objects for its evaluation, then
"ParameterBuilder.resolve_evaluation_dependencies()" will process these dependencies recursively.  Hence, tests are
provided for behavior of this method.  Utility method "get_fully_qualified_parameter_names()", whose thorough tests
are elsewhere, is used to query shared memory.
"""


class DummyParameterBuilder(ParameterBuilder):
    """
    One goal of these tests is to ensure that private implementation method, "ParameterBuilder._build_parameters()",
    of public interface method, "ParameterBuilder.build_parameters()", is called (as proof that
    "evaluation_parameter_builder" dependencies of given "ParameterBuilder" are executed).  All production functionality
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


def test_resolve_evaluation_dependencies_no_parameter_builder_dependencies_specified(
    empty_rule_state: Dict[str, Union[Domain, Dict[str, ParameterContainer]]],
):
    domain = empty_rule_state["domain"]
    parameters = empty_rule_state["parameters"]

    my_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_parameter_builder",
        evaluation_parameter_builder_configs=None,
    )

    my_parameter_builder.resolve_evaluation_dependencies(
        domain=domain,
        variables=None,
        parameters=parameters,
        fully_qualified_parameter_names=None,
        runtime_configuration=None,
    )

    all_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    This assertion shows that shared memory is empty: no "ParameterBuilder" dependencies given to compute values of.
    """
    assert not all_fully_qualified_parameter_names


def test_resolve_evaluation_dependencies_two_parameter_builder_dependencies_specified(
    empty_rule_state: Dict[str, Union[Domain, Dict[str, ParameterContainer]]],
):
    domain = empty_rule_state["domain"]
    parameters = empty_rule_state["parameters"]

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

    my_dependent_parameter_builder.resolve_evaluation_dependencies(
        domain=domain,
        variables=None,
        parameters=parameters,
        fully_qualified_parameter_names=None,
        runtime_configuration=None,
    )

    all_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    This assertion shows that because "ParameterBuilder" dependencies were specified as argument to instantiation of
    dependent "ParameterBuilder", both "ParameterBuilder" dependencies computed their values (this was accomplished by
    "my_dependent_parameter_builder.resolve_evaluation_dependencies()", which ran "build_parameters()" method of each
    dependency "ParameterBuilder" object).  Consequently, shared memory namespace became filled with dependency results.
    """
    assert all_fully_qualified_parameter_names == [
        f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
        f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
        f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
        f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
    ]


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_should_not_recompute_evaluation_parameter_builders_if_precomputed(
    mock_convert_to_json_serializable: mock.MagicMock,
    empty_rule_state: Dict[str, Union[Domain, Dict[str, ParameterContainer]]],
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

    domain = empty_rule_state["domain"]
    parameters = empty_rule_state["parameters"]

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

    """
    Instead of defining constant list of known fully qualified parameter names, here,
    "get_fully_qualified_parameter_names()" is used as utility in order to faithfully track actual contents of shared
    memory as ParameterBuilders are executed; otherwise, initial state of shared memory would not be known exactly.
    """
    dependencies_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    These assertions show that both "ParameterBuilder" dependencies computed their values (and stored in shared memory).
    """
    # First evaluation "ParameterBuilder" dependency computed its value, and it was stored in shared memory.
    assert (
        my_evaluation_dependency_0_parameter_builder.raw_fully_qualified_parameter_name
        in dependencies_fully_qualified_parameter_names
        and my_evaluation_dependency_0_parameter_builder.json_serialized_fully_qualified_parameter_name
        in dependencies_fully_qualified_parameter_names
    )
    # Second evaluation "ParameterBuilder" dependency computed its value, and it was stored in shared memory.
    assert (
        my_evaluation_dependency_1_parameter_builder.raw_fully_qualified_parameter_name
        in dependencies_fully_qualified_parameter_names
        and my_evaluation_dependency_1_parameter_builder.json_serialized_fully_qualified_parameter_name
        in dependencies_fully_qualified_parameter_names
    )

    # Now execute dependent "ParameterBuilder".
    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_dependent_parameter_builder.call_count == 1

    # Assert that precomuted evaluation parameters are not recomputed (call_count for each should remain at 1).
    assert my_evaluation_dependency_0_parameter_builder.call_count == 1
    assert my_evaluation_dependency_1_parameter_builder.call_count == 1

    all_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    These assertions show that dependent "ParameterBuilder" computed its values.
    """
    assert (
        my_dependent_parameter_builder.raw_fully_qualified_parameter_name
        in all_fully_qualified_parameter_names
        and my_dependent_parameter_builder.json_serialized_fully_qualified_parameter_name
        in all_fully_qualified_parameter_names
    )


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_dependencies_evaluated_in_parameter_builder_if_not_precomputed(
    mock_convert_to_json_serializable: mock.MagicMock,
    empty_rule_state: Dict[str, Union[Domain, Dict[str, ParameterContainer]]],
):
    domain = empty_rule_state["domain"]
    parameters = empty_rule_state["parameters"]

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

    # Now execute dependent "ParameterBuilder".
    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_dependent_parameter_builder.call_count == 1

    all_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    This assertion shows that both "ParameterBuilder" dependencies computed their values, and that dependent
    "ParameterBuilder" also computed its value (as all names are in list).
    """
    assert sorted(all_fully_qualified_parameter_names) == sorted(
        [
            f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
            f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
            f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
            f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
            f"{RAW_PARAMETER_KEY}my_dependent_parameter_builder",
            f"{PARAMETER_KEY}my_dependent_parameter_builder",
        ]
    )


# noinspection PyUnresolvedReferences,PyUnusedLocal
@pytest.mark.unit
@mock.patch(
    "great_expectations.rule_based_profiler.parameter_builder.parameter_builder.convert_to_json_serializable",
    return_value="my_json_string",
)
def test_parameter_builder_should_only_evaluate_dependencies_that_are_not_precomputed(
    mock_convert_to_json_serializable: mock.MagicMock,
    empty_rule_state: Dict[str, Union[Domain, Dict[str, ParameterContainer]]],
):
    domain = empty_rule_state["domain"]
    parameters = empty_rule_state["parameters"]

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
            **my_evaluation_dependency_1_parameter_builder.to_json_dict(),
        ),
    ]
    my_dependent_parameter_builder: ParameterBuilder = DummyParameterBuilder(
        name="my_dependent_parameter_builder",
        evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
    )

    my_evaluation_dependency_0_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_evaluation_dependency_0_parameter_builder.call_count == 1

    dependencies_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    This assertion shows that first "ParameterBuilder" dependency computed its value, but second "ParameterBuilder"
    dependency and dependent "ParameterBuilder" have not yet computed their values (as can be seen by parameter names).
    """
    assert dependencies_fully_qualified_parameter_names == [
        f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
        f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_0",
    ]

    my_dependent_parameter_builder.build_parameters(
        domain=domain,
        variables=None,
        parameters=parameters,
        batch_request=None,
        runtime_configuration=None,
    )
    assert my_dependent_parameter_builder.call_count == 1

    all_fully_qualified_parameter_names: List[
        str
    ] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=None,
        parameters=parameters,
    )

    """
    This assertion shows that both "ParameterBuilder" dependencies computed their values, and that dependent
    "ParameterBuilder" also computed its value (as all names are in list).
    """
    assert sorted(all_fully_qualified_parameter_names) == sorted(
        dependencies_fully_qualified_parameter_names
        + [
            f"{RAW_PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
            f"{PARAMETER_KEY}my_evaluation_dependency_parameter_name_1",
            f"{RAW_PARAMETER_KEY}my_dependent_parameter_builder",
            f"{PARAMETER_KEY}my_dependent_parameter_builder",
        ]
    )
