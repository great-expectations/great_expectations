from typing import Any, Dict, Optional

from pyparsing import Suppress, Word, alphanums, alphas, nums, oneOf

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)


class ConditionalExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """ """

    def __init__(
        self,
        expectation_type: str,
        condition: str,
    ):
        super().__init__(expectation_type=expectation_type)

        self._condition = condition

    def _parse_condition(self):
        var = Word(alphas + "._", alphanums + "._")
        text = Suppress("'") + Word(alphas, alphanums) + Suppress("'")
        integer = Word(nums).setParseAction(lambda t: int(t[0]))
        operator = oneOf(">= <= != > < ==")
        comparison = (var + operator + (integer | text)).setParseAction(
            lambda t: self.operands_map[t[1]](t[0], t[2])
        )
        comparison

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        parameter_name: str
        fully_qualified_parameter_name: str
        expectation_kwargs: Dict[str, Any] = {
            parameter_name: get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            # for parameter_name, fully_qualified_parameter_name in self._expectation_kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        return ExpectationConfiguration(
            expectation_type=self._expectation_type,
            kwargs=expectation_kwargs,
            meta=meta,
            success_on_last_run=self._success_on_last_run,
        )

    @property
    def condition(self) -> str:
        return self._condition
