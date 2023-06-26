from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)

if TYPE_CHECKING:
    from great_expectations.rule_based_profiler.rule.rule import Rule


class RuleState:
    """
    RuleState maintains state information, resulting from executing "Rule.run()" method by combining passed "Batch" data
    with currently loaded configuration of "Rule" components ("DomainBuilder" object, "ParameterBuilder" objects, and
    "ExpectationConfigurationBuilder" objects).  Using "RuleState" with correponding flags is sufficient for generating
    outputs for different purposes (in raw and aggregated form) from available "Domain" objects and computed parameters.
    """

    def __init__(
        self,
        rule: Optional[Rule] = None,
        domains: Optional[List[Domain]] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> None:
        """
        Args:
            rule: Rule object for which present RuleState object corresponds (needed for various Rule properties).
            domains: List of Domain objects, which DomainBuilder of associated Rule generated.
            variables: attribute name/value pairs (part of state, relevant for associated Rule).
            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.
        """
        self._rule = rule

        if domains is None:
            domains = []

        self._domains = domains

        self._variables = variables

        if parameters is None:
            parameters = {}

        self._parameters = parameters

        self._rule_domain_builder_execution_time = 0.0
        self._rule_execution_time = 0.0

    @property
    def rule(self) -> Optional[Rule]:
        return self._rule

    @rule.setter
    def rule(self, value: Rule) -> None:
        self._rule = value

    @property
    def domains(self) -> List[Domain]:
        return self._domains

    @domains.setter
    def domains(self, value: List[Domain]) -> None:
        self._domains = value

    @property
    def variables(self) -> Optional[ParameterContainer]:
        return self._variables

    @variables.setter
    def variables(self, value: Optional[ParameterContainer]) -> None:
        self._variables = value

    @property
    def parameters(self) -> Dict[str, ParameterContainer]:
        return self._parameters

    @parameters.setter
    def parameters(self, value: Dict[str, ParameterContainer]) -> None:
        self._parameters = value

    @property
    def rule_domain_builder_execution_time(self) -> float:
        return self._rule_domain_builder_execution_time

    @rule_domain_builder_execution_time.setter
    def rule_domain_builder_execution_time(self, value: float) -> None:
        self._rule_domain_builder_execution_time = value

    @property
    def rule_execution_time(self) -> float:
        return self._rule_execution_time

    @rule_execution_time.setter
    def rule_execution_time(self, value: float) -> None:
        self._rule_execution_time = value

    def reset(self) -> None:
        self.reset_domains()
        self.reset_parameter_containers()

    def reset_domains(self) -> None:
        self.domains = []

    def reset_parameter_containers(self) -> None:
        self.parameters = {}

    def add_domain(
        self,
        domain: Domain,
        allow_duplicates: bool = False,
    ) -> None:
        domain_cursor: Domain
        if not allow_duplicates and domain.id in [
            domain_cursor.id for domain_cursor in self.domains
        ]:
            raise gx_exceptions.ProfilerConfigurationError(
                f"""Error: Domain\n{domain}\nalready exists.  In order to add it, either pass "allow_duplicates=True" \
or call "RuleState.remove_domain_if_exists()" with Domain having ID equal to "{domain.id}" as argument first.
"""
            )

        self.domains.append(domain)

    def remove_domain_if_exists(self, domain: Domain) -> None:
        domain_cursor: Domain
        if domain.id in [domain_cursor.id for domain_cursor in self.domains]:
            self.domains.remove(domain)
            self.remove_domain_if_exists(domain=domain)

    def get_domains_as_dict(self) -> Dict[str, Domain]:
        domain: Domain
        return {domain.id: domain for domain in self.domains}

    def initialize_parameter_container_for_domain(
        self,
        domain: Domain,
        overwrite: bool = True,
    ) -> None:
        if not overwrite and domain.id in self.parameters:
            raise gx_exceptions.ProfilerConfigurationError(
                f"""Error: ParameterContainer for Domain\n{domain}\nalready exists.  In order to overwrite it, either \
pass "overwrite=True" or call "RuleState.remove_parameter_container_from_domain()" with Domain having ID equal to \
"{domain.id}" as argument first.
"""
            )

        parameter_container = ParameterContainer(parameter_nodes=None)
        self._parameters[domain.id] = parameter_container

    def remove_parameter_container_from_domain_if_exists(self, domain: Domain) -> None:
        self.parameters.pop(domain.id, None)
