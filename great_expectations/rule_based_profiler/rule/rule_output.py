from __future__ import annotations

from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.domain import Domain
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterNode,
    get_fully_qualified_parameter_names,
    get_parameter_values_for_fully_qualified_parameter_names,
)
from great_expectations.rule_based_profiler.rule.rule_state import RuleState


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

    def get_expectation_configurations(self) -> list[ExpectationConfiguration]:
        expectation_configurations: list[ExpectationConfiguration] = []

        domains: list[Domain] = self.rule_state.domains

        domain: Domain
        expectation_configuration_builder: ExpectationConfigurationBuilder
        for domain in domains:
            expectation_configuration_builders: list[ExpectationConfigurationBuilder]
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

    def get_fully_qualified_parameter_names_by_domain(self) -> dict[Domain, list[str]]:
        domain: Domain
        return {
            domain: self.get_fully_qualified_parameter_names_for_domain(domain=domain)
            for domain in self.rule_state.domains
        }

    def get_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> list[str]:
        domains_dict: dict[str, Domain] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        fully_qualified_parameter_names: list[
            str
        ] = self.get_fully_qualified_parameter_names_for_domain(domain=domain)
        return fully_qualified_parameter_names

    def get_fully_qualified_parameter_names_for_domain(
        self,
        domain: Optional[Domain] = None,
    ) -> list[str]:
        fully_qualified_parameter_names: list[
            str
        ] = get_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_by_domain(
        self,
    ) -> dict[Domain, dict[str, ParameterNode]]:
        domain: Domain
        return {
            domain: self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
                domain=domain
            )
            for domain in self.rule_state.domains
        }

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> dict[str, ParameterNode]:
        domains_dict: dict[str, Domain] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        parameter_values_for_fully_qualified_parameter_names: dict[
            str, ParameterNode
        ] = self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
            domain=domain
        )
        return parameter_values_for_fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain(
        self,
        domain: Optional[Domain] = None,
    ) -> dict[str, ParameterNode]:
        parameter_values_for_fully_qualified_parameter_names: dict[
            str, ParameterNode
        ] = get_parameter_values_for_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return parameter_values_for_fully_qualified_parameter_names
