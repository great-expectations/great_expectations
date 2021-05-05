from abc import ABC, abstractmethod
from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)


class ExpectationConfigurationBuilder(ABC):
    def build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs
    ) -> ExpectationConfiguration:
        return self._build_expectation_configuration(
            domain=domain, variables=variables, parameters=parameters, **kwargs
        )

    @abstractmethod
    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        **kwargs
    ) -> ExpectationConfiguration:
        pass
