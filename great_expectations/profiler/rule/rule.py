import copy
from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.domain_builder.domain_builder import DomainBuilder
from great_expectations.profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.validator.validator import Validator


class Rule:
    def __init__(
        self,
        name: str,
        domain_builder: Optional[DomainBuilder] = None,
        parameter_builders: Optional[List[ParameterBuilder]] = None,
        expectation_configuration_builders: Optional[
            List[ExpectationConfigurationBuilder]
        ] = None,
        variables: Optional[ParameterContainer] = None,
    ):
        """
        Sets Profiler rule name, domain builders, parameters builders, configuration builders,
        and other necessary instance data (variables)
        :param name: A string representing the name of the ProfilerRule
        :param domain_builder: A Domain Builder object used to build rule data domain
        :param parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters for
        every configuration
        :param expectation_configuration_builders: A list of Expectation Configuration Builders
        :param variables: Any instance data required to verify a rule
        """
        self._name = name
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._expectation_configuration_builders = expectation_configuration_builders
        self._variables = variables

        self._domain_parameters = {}

    def evaluate(
        self, validator: Validator, batch_ids: Optional[List[str]]
    ) -> List[ExpectationConfiguration]:
        """
        Builds a list of Expectation Configurations, returning a single Expectation Configuration entry for every
        ConfigurationBuilder available based on the instantiation.

        :param validator: A Validator object utilized to obtain domain
        :param batch_ids: Batch Identifiers used to specify evaluated batches of data
        :return: List of Corresponding Expectation Configurations representing every configured rule
        """
        expectation_configurations: List[ExpectationConfiguration] = []

        domains: List[Domain] = self._domain_builder.get_domains(
            validator=validator, batch_ids=batch_ids
        )

        domain: Domain
        for domain in domains:
            parameter_builder: ParameterBuilder
            for parameter_builder in self._parameter_builders:
                parameter_container: ParameterContainer = (
                    parameter_builder.build_parameters(batch_ids=batch_ids)
                )
                self._domain_parameters[domain.id] = parameter_container

            expectation_configuration_builder: ExpectationConfigurationBuilder
            for (
                expectation_configuration_builder
            ) in self._expectation_configuration_builders:
                expectation_configurations.append(
                    expectation_configuration_builder.build_expectation_configuration(
                        rule=self, domain=domain
                    )
                )

        return expectation_configurations

    @property
    def variables(self) -> ParameterContainer:
        return self._variables

    @property
    def domain_parameters(self) -> Dict[str, ParameterContainer]:
        # Returning a copy of the "self._domain_parameters" state variable in order to prevent write-before-read hazard.
        return copy.deepcopy(self._domain_parameters)
