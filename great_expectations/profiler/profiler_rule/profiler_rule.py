from typing import Any, Dict, List, Optional, Union

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.profiler.configuration_builder.configuration_builder import (
    ConfigurationBuilder,
)
from great_expectations.profiler.domain_builder.domain_builder import DomainBuilder
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_tree_container_node import (
    ParameterTreeContainerNode,
)
from great_expectations.profiler.profiler_rule.rule_state import RuleState
from great_expectations.validator.validator import Validator


class ProfilerRule:
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilder,
        parameter_builders: List[ParameterBuilder],
        configuration_builders: List[ConfigurationBuilder],
        variables: Optional[ParameterTreeContainerNode] = None,
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
        configurations: List[ExpectationConfiguration] = []

        rule_state.domains = self._domain_builder.get_domains(
            validator=validator, batch_ids=batch_ids
        )

        domain: Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]

        for domain in rule_state.domains:
            rule_state.active_domain = domain
            domain_id: str = rule_state.active_domain_id
            parameter_builder: ParameterBuilder
            for parameter_builder in self._parameter_builders:
                parameter_name: str = parameter_builder.parameter_name
                parameter_tree_container_node: ParameterTreeContainerNode = (
                    parameter_builder.build_parameters(
                        rule_state=rule_state, validator=validator, batch_ids=batch_ids
                    )
                )
                rule_state.parameters[domain_id][
                    parameter_name
                ] = parameter_tree_container_node.parameters
            for configuration_builder in self._configuration_builders:
                configurations.append(
                    configuration_builder.build_configuration(rule_state=rule_state)
                )

        return configurations
