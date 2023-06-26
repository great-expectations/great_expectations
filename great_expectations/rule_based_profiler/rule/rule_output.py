from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.domain import Domain
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterNode,
    get_fully_qualified_parameter_names,
    get_parameter_values_for_fully_qualified_parameter_names,
)
from great_expectations.rule_based_profiler.rule.rule_state import (
    RuleState,
)


class RuleOutput:
    """
    RuleOutput provides methods for extracting useful information from RuleState using directives and application logic.
    """

    def __init__(
        self,
        rule_state: RuleState,
    ) -> None:
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
            expectation_configuration_builders: List[ExpectationConfigurationBuilder]
            if (
                self.rule_state.rule
                and self.rule_state.rule.expectation_configuration_builders
            ):
                expectation_configuration_builders = (
                    self.rule_state.rule.expectation_configuration_builders
                )
            else:
                expectation_configuration_builders = []

            for expectation_configuration_builder in expectation_configuration_builders:
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
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        domain: Domain
        return {
            domain: self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
                domain=domain
            )
            for domain in self.rule_state.domains
        }

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> Dict[str, ParameterNode]:
        domains_dict: Dict[str, Domain] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        parameter_values_for_fully_qualified_parameter_names: Dict[
            str, ParameterNode
        ] = self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
            domain=domain
        )
        return parameter_values_for_fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain(
        self,
        domain: Optional[Domain] = None,
    ) -> Dict[str, ParameterNode]:
        parameter_values_for_fully_qualified_parameter_names: Dict[
            str, ParameterNode
        ] = get_parameter_values_for_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return parameter_values_for_fully_qualified_parameter_names
