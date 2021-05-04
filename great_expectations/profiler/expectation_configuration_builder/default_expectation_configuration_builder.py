from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    ParameterContainer,
)
from great_expectations.profiler.util import get_parameter_value


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
        rule_variables: Optional[ParameterContainer] = None,
        rule_domain_parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs
    ) -> ExpectationConfiguration:
        expectation_kwargs: dict = {}
        parameter_name: str
        fully_qualified_parameter_name: str
        for (
            parameter_name,
            fully_qualified_parameter_name,
        ) in self._parameter_name_to_fully_qualified_parameter_name_dict.items():
            if fully_qualified_parameter_name.startswith(
                DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME
            ):
                # TODO: AJB 20210503 is this how to reference the domain? Maybe there should be a method on Domain to return the actual domain name
                expectation_kwargs[parameter_name] = domain.domain_kwargs[
                    domain.domain_type
                ]
            elif fully_qualified_parameter_name.startswith("$"):
                expectation_kwargs[parameter_name] = get_parameter_value(
                    fully_qualified_parameter_name=fully_qualified_parameter_name,
                    domain=domain,
                    rule_variables=rule_variables,
                    rule_domain_parameters=rule_domain_parameters,
                )

        expectation_kwargs.update(kwargs)
        # TODO: AJB 20210503 why is "rule" in these kwargs, is it necessary and how best to
        #  avoid passing it to ExpectationConfiguration?
        expectation_kwargs.pop("rule")

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, kwargs=expectation_kwargs
        )
