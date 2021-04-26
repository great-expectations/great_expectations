from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.profiler.rule.rule_state import RuleState


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied as kwargs dictionary).
    """

    def __init__(self, expectation_type: str = None, **kwargs):
        self._expectation_type = expectation_type
        self._parameter_name_to_fully_qualified_parameter_name_dict = kwargs

    def _build_expectation_configuration(
        self, rule_state: RuleState, **kwargs
    ) -> ExpectationConfiguration:
        """
        Utilizes RuleState object to obtain parameter values out of the fully qualified parameter names initialized in
        the constructor, returning an ExpectationConfiguration, instantiated with the classes' Expectation type as well
        as the dictionary of all the additional flexible attributes (passed as kwargs to this method).

        :param rule_state: An object keeping track of the state information necessary for rule validation, such as
            domain, metric parameters, and necessary variables.
        :return: ExpectationConfiguration
        """
        expectation_kwargs: dict = {}
        parameter_name: str
        fully_qualified_parameter_name: str
        for (
            parameter_name,
            fully_qualified_parameter_name,
        ) in self._parameter_name_to_fully_qualified_parameter_name_dict.items():
            expectation_kwargs[parameter_name] = rule_state.get_parameter_value(
                fully_qualified_parameter_name=fully_qualified_parameter_name
            )

        expectation_kwargs.update(kwargs)

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, **expectation_kwargs
        )
