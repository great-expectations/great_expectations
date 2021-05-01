from abc import ABC, abstractmethod

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.rule.rule import Rule


class ExpectationConfigurationBuilder(ABC):
    def build_expectation_configuration(
        self, rule: Rule, domain: Domain, **kwargs
    ) -> ExpectationConfiguration:
        return self._build_expectation_configuration(rule=rule, domain=domain, **kwargs)

    @abstractmethod
    def _build_expectation_configuration(
        self, rule: Rule, domain: Domain, **kwargs
    ) -> ExpectationConfiguration:
        pass
