from abc import ABC, abstractmethod

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.rule.rule_state import RuleState


class ExpectationConfigurationBuilder(ABC):
    def build_configuration(
        self, rule_state: RuleState, **kwargs
    ) -> ExpectationConfiguration:
        """
        Calls the implemented build_configuration method of a concrete subclass
        args
            :param rule_state: An object keeping track of the state information necessary for rule validation, such as domain,
                metric parameters, and necessary variables
        :return: Built Configuration
        """
        return self._build_expectation_configuration(rule_state=rule_state, **kwargs)

    @abstractmethod
    def _build_expectation_configuration(
        self, rule_state: RuleState, **kwargs
    ) -> ExpectationConfiguration:
        pass
