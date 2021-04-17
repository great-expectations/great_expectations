from great_expectations.core.expectation_configuration import ExpectationConfiguration

from ..rule_state import RuleState
from .configuration_builder import ConfigurationBuilder


class ParameterIdConfigurationBuilder(ConfigurationBuilder):
    """Class which creates Expectation configuration out of a given
    Expectation type and a parameter-name config"""

    def __init__(self, expectation: str = None, **kwargs):
        self._expectation_type = expectation
        self._config = kwargs

    def _build_configuration(self, rule_state: RuleState, **kwargs):
        """
        Utilizes RuleState object to obtain parameter names out of the parameter ids initialized in the constructor,
        returning an Expectation configuration initialized with the classes' Expectation type and a dict of all necessary
        parameters (as kwargs).

        :param rule_state: An object keeping track of the state information necessary for rule validation, such as domain,
                metric parameters, and necessary variables
        :return: Expectation config initialized with Expectation type and specified parameters that have been specified by
        the classes' config and obtained in the Rule State.
        """
        kwargs = dict()
        for parameter_name, parameter_id in enumerate(self._config):
            kwargs[parameter_name] = rule_state.get_value(parameter_id)

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, kwargs=kwargs
        )
