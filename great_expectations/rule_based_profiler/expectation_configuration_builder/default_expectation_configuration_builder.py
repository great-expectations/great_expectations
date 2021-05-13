from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    get_parameter_value,
)


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied as kwargs dictionary).
    """

    def __init__(self, expectation_type: str = None, **kwargs):
        self._expectation_type = expectation_type
        self._parameter_name_to_fully_qualified_parameter_name_dict = kwargs

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs
    ) -> ExpectationConfiguration:
        expectation_kwargs: dict = {}
        parameter_name: str
        fully_qualified_parameter_name: str
        for (
            parameter_name,
            fully_qualified_parameter_name,
        ) in self._parameter_name_to_fully_qualified_parameter_name_dict.items():
            expectation_kwargs[parameter_name] = get_parameter_value(
                fully_qualified_parameter_name=fully_qualified_parameter_name,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        expectation_kwargs.update(kwargs)

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, kwargs=expectation_kwargs
        )
