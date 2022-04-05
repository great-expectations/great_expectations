from typing import List, Optional, Union

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, RuleState


class RuleOutput:
    def __init__(
        self,
        rule_state: RuleState,
    ):
        self._rule_state = rule_state

    @property
    def rule_state(self) -> RuleState:
        return self._rule_state

    def expectation_configurations(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        force_batch_data: bool = False,
    ) -> List[ExpectationConfiguration]:
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
                        batch_list=batch_list,
                        batch_request=batch_request,
                        force_batch_data=force_batch_data,
                    )
                )

        return expectation_configurations
