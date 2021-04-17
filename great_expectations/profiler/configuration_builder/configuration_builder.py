from abc import ABC, abstractmethod
from typing import Optional

from great_expectations.profiler.rule_state import RuleState
from great_expectations.validator.validator import Validator


class ConfigurationBuilder(ABC):
    def build_configuration(self, rule_state: RuleState, **kwargs):
        """
        Calls the implemented build_configuration method of a concrete subclass
        args
            :param rule_state: An object keeping track of the state information necessary for rule validation, such as domain,
                metric parameters, and necessary variables
        :return: Built Configuration
        """
        return self._build_configuration(rule_state, **kwargs)

    @abstractmethod
    def _build_configuration(self, rule_state: RuleState, **kwargs):
        pass
