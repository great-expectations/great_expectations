from typing import List, Optional

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
from great_expectations.profiler.rule.rule_state import RuleState
from great_expectations.validator.validator import Validator


class Rule:
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilder,
        parameter_builders: List[ParameterBuilder],
        expectation_configuration_builders: List[ExpectationConfigurationBuilder],
        variables: Optional[ParameterContainer] = None,
    ):
        """
        Sets Profiler rule name, domain builders, parameters builders, configuration builders,
        and other necessary instance data (variables)
        :param name: A string representing the name of the ProfilerRule
        :param domain_builder: A Domain Builder object used to build rule data domain
        :param parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters for
        every configuration
        :param expectation_configuration_builders: A list of Expectation Configuration Builders initializing state configurations (utilizes the info in
        a RuleState object)
        :param variables: Any instance data required to verify a rule
        """
        self._name = name
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._expectation_configuration_builders = expectation_configuration_builders
        self._variables = variables

    def evaluate(
        self, validator: Validator, batch_ids: Optional[List[str]]
    ) -> List[ExpectationConfiguration]:
        """
        Builds a RuleState object for configured information (domain_builder, parameter_builder,
        and configuration_builder which are defined at __init__) and proceeds to use this
        RuleState to build a list of Expectation Configurations, returning a single Expectation
        Configuration entry for every ConfigurationBuilder given).

        :param validator: A Validator object utilized to obtain domain
        :param batch_ids: Batch Identifiers used to specify evaluated batches of data
        :return: List of Corresponding Expectation Configurations representing every configured rule
        """
        rule_state: RuleState = RuleState(variables=self._variables)
        expectation_configurations: List[ExpectationConfiguration] = []

        rule_state.domains = self._domain_builder.get_domains(
            validator=validator, batch_ids=batch_ids
        )

        domain: Domain

        for domain in rule_state.domains:
            rule_state.active_domain = domain
            domain_id: str = rule_state.active_domain.id
            parameter_builder: ParameterBuilder
            for parameter_builder in self._parameter_builders:
                parameter_name: str = parameter_builder.parameter_name
                parameter_container: ParameterContainer = (
                    parameter_builder.build_parameters(
                        rule_state=rule_state, validator=validator, batch_ids=batch_ids
                    )
                )
                # TODO: <Alex>ALEX -- this mechanism needs to be discussed.</Alex>
                rule_state.parameters[domain_id][
                    parameter_name
                ] = parameter_container.attributes

            expectation_configuration_builder: ExpectationConfigurationBuilder
            for (
                expectation_configuration_builder
            ) in self._expectation_configuration_builders:
                expectation_configurations.append(
                    expectation_configuration_builder.build_expectation_configuration(
                        rule_state=rule_state
                    )
                )

        return expectation_configurations
