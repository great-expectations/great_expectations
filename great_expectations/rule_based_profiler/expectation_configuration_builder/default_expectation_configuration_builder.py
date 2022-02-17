from typing import Any, Dict, Optional, Set

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary).
    """

    exclude_field_names: Set[str] = {
        "kwargs",
    }

    def __init__(
        self,
        expectation_type: str,
        meta: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(expectation_type=expectation_type, **kwargs)

        self._kwargs = kwargs

        if meta is None:
            meta = {}

        if not isinstance(meta, dict):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{meta}" in "{self.__class__.__name__}" must be of type "dictionary" \
(value of type "{str(type())}" was encountered).
"""
            )

        self._meta = meta

    @property
    def expectation_type(self) -> str:
        return self._expectation_type

    @property
    def kwargs(self) -> dict:
        return self._kwargs

    @property
    def meta(self) -> dict:
        return self._meta

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        parameter_name: str
        fully_qualified_parameter_name: str
        expectation_kwargs: Dict[str, Any] = {
            parameter_name: get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            for parameter_name, fully_qualified_parameter_name in self._kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        return ExpectationConfiguration(
            expectation_type=self.expectation_type,
            kwargs=expectation_kwargs,
            meta=meta,
        )
