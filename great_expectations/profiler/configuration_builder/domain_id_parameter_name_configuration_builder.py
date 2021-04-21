from typing import Any

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.configuration_builder.configuration_builder import (
    ConfigurationBuilder,
)
from great_expectations.profiler.profiler_rule.rule_state import RuleState


class DomainIdParameterNameConfigurationBuilder(ConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and domain_id-parameter_name pairs as
    attribute directives (supplied in kwargs).
    """

    def __init__(self, expectation_type: str = None, **kwargs):
        self._expectation_type = expectation_type
        self._domain_id_parameter_name_dict = kwargs

    def _build_configuration(
        self, rule_state: RuleState, **kwargs
    ) -> ExpectationConfiguration:
        """
        Utilizes RuleState object to obtain parameter names out of the parameter names initialized in the constructor,
        returning an ExpectationConfiguration, initialized with the classes' Expectation type ass well as the dictionary
        of all the additional flexible attributes (passed as kwargs to this method).

        :param rule_state: An object keeping track of the state information necessary for rule validation, such as
            domain, metric parameters, and necessary variables.
        :return: ExpectationConfiguration
        """
        rule_expectation_kwargs: dict = {}
        domain_id: str
        parameter_name: Any
        for domain_id, parameter_name in self._domain_id_parameter_name_dict.items():
            rule_expectation_kwargs[domain_id] = rule_state.get_parameter_value(
                parameter_name=parameter_name
            )

        rule_expectation_kwargs.update(kwargs)
        return ExpectationConfiguration(
            expectation_type=self._expectation_type, **kwargs
        )
