from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
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
        print(f'\n[ALEX_TEST] DEFAULTEXPECTATIONCONFIGURATIONBUILDER_INIT: {kwargs} ; TYPE: {str(type(kwargs))}')

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
            print(f'\n[ALEX_TEST] DOMAIN: {domain} ; VARIABLES: {variables} ; PARAMETERS: {parameters}')
            print(f'\n[ALEX_TEST] PARAMETER_NAME: {parameter_name} ; FULLY_QUALIFIED_PARAMETER_NAME: {fully_qualified_parameter_name} ; PARAMETER_VALUE: {expectation_kwargs[parameter_name]}')
            # # TODO: AJB 20210505 this is only valid for columns, we need to check the domain type here
            # if fully_qualified_parameter_name.startswith(
            #     DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME
            # ):
            #     # TODO: AJB 20210503 is this how to reference the domain? Maybe there should be a method on Domain to return the actual domain name
            #     expectation_kwargs[parameter_name] = domain.domain_kwargs[
            #         domain.domain_type
            #     ]
            # elif fully_qualified_parameter_name.startswith("$"):
            #     expectation_kwargs[parameter_name] = get_parameter_value(
            #         fully_qualified_parameter_name=fully_qualified_parameter_name,
            #         domain=domain,
            #         variables=variables,
            #         parameters=parameters,
            #     )

        print(f'\n[ALEX_TEST] PASSED_IN_KWARGS: {kwargs}')
        print(f'\n[ALEX_TEST] FULL_EXPECTATION_KWARGS: {expectation_kwargs}')
        expectation_kwargs.update(kwargs)

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, kwargs=expectation_kwargs
        )
