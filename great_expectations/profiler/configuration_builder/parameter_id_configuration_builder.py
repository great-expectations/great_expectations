from typing import Any

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.configuration_builder.configuration_builder import (
    ConfigurationBuilder,
)
from great_expectations.profiler.profiler_rule.rule_state import RuleState


class ParameterIdConfigurationBuilder(ConfigurationBuilder):
    """Class which creates Expectation configuration out of a given
    Expectation type and a parameter-name config"""

    def __init__(self, expectation: str = None, **kwargs):
        self._expectation_type = expectation
        self._config = kwargs

    def _build_configuration(
        self, rule_state: RuleState, **kwargs
    ) -> ExpectationConfiguration:
        """
        Utilizes RuleState object to obtain parameter names out of the parameter ids initialized in the constructor,
        returning an Expectation configuration initialized with the classes' Expectation type and a dict of all
        necessary parameters (as kwargs).

        :param rule_state: An object keeping track of the state information necessary for rule validation, such as
            domain, metric parameters, and necessary variables
        :return: Expectation config initialized with Expectation type and parameters that have been specified by
            the class' config and obtained from the Rule State.
        """
        rule_expectation_kwargs: dict = {}
        attribute_name: str
        attribute_value: Any
        for attribute_name, attribute_value in self._config.items():
            rule_expectation_kwargs[attribute_name] = rule_state.get_parameter_value(
                parameter_name=attribute_value
            )

        kwargs.update(rule_expectation_kwargs)
        return ExpectationConfiguration(
            expectation_type=self._expectation_type, **kwargs
        )
