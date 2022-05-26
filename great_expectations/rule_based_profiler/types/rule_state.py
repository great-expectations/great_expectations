from typing import Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer


class RuleState:
    """
    RuleState maintains state information, resulting from executing "Rule.run()" method by combining passed "Batch" data
    with currently loaded configuration of "Rule" components ("DomainBuilder" object, "ParameterBuilder" objects, and
    "ExpectationConfigurationBuilder" objects).  Using "RuleState" with correponding flags is sufficient for generating
    outputs for different purposes (in raw and aggregated form) from available "Domain" objects and computed parameters.
    """

    def __init__(
        self,
        rule: "Rule",  # noqa: F821
        variables: Optional[ParameterContainer] = None,
        domains: Optional[List[Domain]] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> None:
        """
        Args:
            rule: Rule object for which present RuleState object corresponds (needed for various Rule properties).
            variables: attribute name/value pairs (part of state, relevant for associated Rule).
            domains: List of Domain objects, which DomainBuilder of associated Rule generated.
            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.
        """
        self._rule = rule

        self._variables = variables

        if domains is None:
            domains = []

        self._domains = domains

        if parameters is None:
            parameters = {}

        self._parameters = parameters

    @property
    def rule(self) -> "Rule":  # noqa: F821:
        return self._rule

    @property
    def variables(self) -> Optional[ParameterContainer]:
        return self._variables

    @property
    def domains(self) -> List[Domain]:
        return self._domains

    @domains.setter
    def domains(self, value: Optional[List[Domain]]) -> None:
        self._domains = value

    @property
    def parameters(self) -> Dict[str, ParameterContainer]:
        return self._parameters

    @parameters.setter
    def parameters(self, value: Optional[Dict[str, ParameterContainer]]) -> None:
        self._parameters = value

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
            raise ge_exceptions.ProfilerConfigurationError(
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
            raise ge_exceptions.ProfilerConfigurationError(
                f"""Error: ParameterContainer for Domain\n{domain}\nalready exists.  In order to overwrite it, either \
pass "overwrite=True" or call "RuleState.remove_parameter_container_from_domain()" with Domain having ID equal to \
"{domain.id}" as argument first.
"""
            )

        parameter_container: ParameterContainer = ParameterContainer(
            parameter_nodes=None
        )
        self._parameters[domain.id] = parameter_container

    def remove_parameter_container_from_domain_if_exists(self, domain: Domain) -> None:
        self.parameters.pop(domain.id, None)
