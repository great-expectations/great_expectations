from typing import Any, Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.rule_based_profiler.util import (
    get_parameter_argument_and_validate_return_type,
)


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary).
    """

    def __init__(
        self,
        expectation_type: str,
        meta: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self._expectation_type = expectation_type
        self._expectation_kwargs = kwargs
        if meta is None:
            meta = {}
        self._meta = meta

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        parameter_name: str
        fully_qualified_parameter_name: str
        expectation_kwargs: Dict[str, Any] = {
            parameter_name: get_parameter_argument_and_validate_return_type(
                domain=domain,
                argument=fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            for parameter_name, fully_qualified_parameter_name in self._expectation_kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_argument_and_validate_return_type(
            domain=domain,
            argument=self._meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        return ExpectationConfiguration(
            expectation_type=self._expectation_type,
            kwargs=expectation_kwargs,
            meta=meta,
        )
