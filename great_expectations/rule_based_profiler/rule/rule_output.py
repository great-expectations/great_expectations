from typing import Any, Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    RuleState,
    get_fully_qualified_parameter_names,
    get_parameter_values_for_fully_qualified_parameter_names,
)


class RuleOutput:
    """
    RuleOutput provides methods for extracting useful information from RuleState using directives and application logic.
    """

    def __init__(
        self,
        rule_state: RuleState,
    ):
        """
        Args:
            rule_state: RuleState object represented by "Domain" objects and parameters,.computed for one Rule object.
        """
        self._rule_state = rule_state

    @property
    def rule_state(self) -> RuleState:
        return self._rule_state

    def get_expectation_configurations(self) -> List[ExpectationConfiguration]:
        expectation_configurations: List[ExpectationConfiguration] = []

        domains: List[Domain] = self.rule_state.domains

        domain: Domain
        expectation_configuration_builder: ExpectationConfigurationBuilder
        for domain in domains:
            for (
                expectation_configuration_builder
            ) in self.rule_state.rule.expectation_configuration_builders:
                expectation_configurations.append(
                    expectation_configuration_builder.build_expectation_configuration(
                        domain=domain,
                        variables=self.rule_state.variables,
                        parameters=self.rule_state.parameters,
                    )
                )

        return expectation_configurations

    def get_fully_qualified_parameter_names_by_domain(self) -> Dict[Domain, List[str]]:
        domain: Domain
        return {
            domain: self.get_fully_qualified_parameter_names_for_domain(domain=domain)
            for domain in self.rule_state.domains
        }

    def get_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> List[str]:
        domains_dict: Dict[str, Domain] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        fully_qualified_parameter_names: List[
            str
        ] = self.get_fully_qualified_parameter_names_for_domain(domain=domain)
        return fully_qualified_parameter_names

    def get_fully_qualified_parameter_names_for_domain(
        self,
        domain: Optional[Domain] = None,
    ) -> List[str]:
        fully_qualified_parameter_names: List[
            str
        ] = get_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_by_domain(
        self,
    ) -> Dict[Domain, Dict[str, Any]]:
        domain: Domain
        return {
            domain: self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
                domain=domain
            )
            for domain in self.rule_state.domains
        }

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> Dict[str, Any]:
        domains_dict: Dict[str, Domain] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        parameter_values_for_fully_qualified_parameter_names: Dict[
            str, Any
        ] = self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
            domain=domain
        )
        return parameter_values_for_fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain(
        self,
        domain: Optional[Domain] = None,
    ) -> Dict[str, Any]:
        parameter_values_for_fully_qualified_parameter_names: Dict[
            str, Any
        ] = get_parameter_values_for_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return parameter_values_for_fully_qualified_parameter_names
