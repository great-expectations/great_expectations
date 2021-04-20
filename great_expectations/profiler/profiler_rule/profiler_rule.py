from typing import Union

from ..core import ExpectationConfiguration
from .configuration_builder.configuration_builder import ConfigurationBuilder
from .domain_builder.domain_builder import DomainBuilder
from .parameter_builder.parameter_builder import ParameterBuilder
from .rule_state import RuleState


class ProfilerRule:
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilder,
        parameter_builders: [ParameterBuilder],
        configuration_builders: [ConfigurationBuilder],
        variables=None,
    ):
        """
        Sets Profiler rule name, domain builders, parameters builders, configuration builders,
        and other necessary instance data (variables)
        :param name: A string representing the name of the ProfilerRule
        :param domain_builder: A Domain Builder object used to build rule data domain
        :param parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters for
        every configuration
        :param configuration_builders: A list of Configuration Builders initializing state configurations (utilizes the info in
        a RuleState object)
        :param variables: Any instance data required to verify a rule
        """
        self._name = name
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._configuration_builders = configuration_builders
        if variables is None:
            variables = dict()
        self._variables = variables

    def evaluate(self, validator, batch_ids=None) -> [ExpectationConfiguration]:
        """
        Builds a RuleState object for configured information (domain_builder, parameter_builder,
        and configuration_builder which are defined at __init__) and proceeds to use this
        RuleState to build a list of Expectation Configurations, returning a single Expectation
        Configuration entry for every ConfigurationBuilder given).

        :param validator: A Validator object utilized to obtain domain
        :param batch_ids: Batch Identifiers used to specify evaluated batches of data
        :return: List of Corresponding Expectation Configurations representing every configured rule
        """
        rule_state = RuleState(variables=self._variables)
        configurations = []

        rule_state.domains = self._domain_builder.get_domains(
            validator=validator, batch_ids=batch_ids
        )
        for domain in rule_state.domains:
            rule_state.active_domain = domain
            domain_id = rule_state.get_active_domain_id()
            for parameter_builder in self._parameter_builders:
                id = parameter_builder.parameter_id
                parameter_result = parameter_builder.build_parameters(
                    rule_state=rule_state, validator=validator, batch_ids=batch_ids
                )
                rule_state.parameters[domain_id][id] = parameter_result["parameters"]
            for configuration_builder in self._configuration_builders:
                configurations.append(
                    configuration_builder.build_configuration(rule_state=rule_state)
                )

        return configurations
