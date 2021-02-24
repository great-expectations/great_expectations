from great_expectations.core.expectation_configuration import ExpectationConfiguration

from .configuration_builder import ConfigurationBuilder


class ParameterIdConfigurationBuilder(ConfigurationBuilder):
    def __init__(self, expectation: str = None, **kwargs):
        self._expectation_type = expectation
        self._config = kwargs

    def _build_configuration(self, rule_state, **kwargs):
        kwargs = dict()
        for parameter_name, parameter_id in enumerate(self._config):
            kwargs[parameter_name] = rule_state.get_value(parameter_id)

        return ExpectationConfiguration(
            expectation_type=self._expectation_type, kwargs=kwargs
        )
