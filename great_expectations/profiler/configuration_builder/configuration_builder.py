from abc import ABC, abstractmethod
from typing import Optional

from great_expectations.validator.validator import Validator


class ConfigurationBuilder(ABC):
    def build_configuration(self, rule_state, **kwargs):
        return self._build_configuration(rule_state, **kwargs)

    @abstractmethod
    def _build_configuration(self, rule_state, **kwargs):
        pass
