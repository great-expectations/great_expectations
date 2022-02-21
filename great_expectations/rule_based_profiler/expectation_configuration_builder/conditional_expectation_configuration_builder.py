from typing import Any, Dict, Optional, Set

import numpy as np
from pyparsing import (
    And,
    Literal,
    Or,
    ParseException,
    ParseResults,
    Suppress,
    Word,
    ZeroOrMore,
    alphanums,
    alphas,
    infixNotation,
    nums,
    oneOf,
    opAssoc,
    operatorPrecedence,
)

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)


def operands_map(n1, n2):
    return 10, 20


var = Literal("$") + Word(alphas + "._", alphanums + "._") + Literal("[0]")
text = Suppress("'") + Word(alphas, alphanums) + Suppress("'")
integer = Word(nums).setParseAction(lambda t: int(t[0]))
operator = oneOf(">= <= != > < ==")
# comparison = (var + operator + (integer | text)).setParseAction(lambda t: operands_map[t[1]](t[0], t[2]))
# comparison = (var + operator + (integer | text)).setParseAction(lambda t: [t[0], t[1], t[2]])

# expr = operatorPrecedence(infixNotation, [
#         ("|", 2, opAssoc.LEFT, lambda t: Or(t)),
#         ("&", 2, opAssoc.LEFT, lambda t: And(t))
#     ]
# )

condition_parser = var + operator + integer

# condition_parser = Word(alphas, alphanums + "_.") + ZeroOrMore(
#     (
#         (
#             Suppress(Literal('["'))
#             + Word(alphas, alphanums + "_.")
#             + Suppress(Literal('"]'))
#         )
#         ^ (
#             Suppress(Literal("['"))
#             + Word(alphas, alphanums + "_.")
#             + Suppress(Literal("']"))
#         )
#     )
#     ^ (
#         Suppress(Literal("["))
#         + Word(nums).setParseAction(lambda s, l, t: [int(t[0])])
#         + Suppress(Literal("]"))
#     )
# )


class ExpectationConfigurationConditionParserError(
    ge_exceptions.GreatExpectationsError
):
    pass


class ConditionalExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary)
    if, and only if, the supplied condition is met.
    """

    include_field_names: Set[str] = {
        "expectation_type",
    }

    def __init__(
        self,
        expectation_type: str,
        condition: str,
        meta: Optional[Dict[str, Any]] = None,
        success_on_last_run: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(expectation_type=expectation_type)

        self._expectation_kwargs = kwargs

        if meta is None:
            meta = {}
        if not isinstance(meta, dict):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{meta}" in "{self.__class__.__name__}" must be of type "dictionary" \
(value of type "{str(type(meta))}" was encountered).
"""
            )

        if not isinstance(condition, str):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{condition}" in "{self.__class__.__name__}" must be of type "string" \
(value of type "{str(type(condition))}" was encountered).
"""
            )

        self._condition = condition
        self._meta = meta
        self._success_on_last_run = success_on_last_run

    def _parse_condition(self) -> ParseResults:
        """
        Using the grammer defined by "condition", provides the parsing of collection (list, dictionary) access syntax:
        List: variable[index: int]
        Dictionary: variable[key: str]
        Nested List/Dictionary: variable[index_0: int][key_0: str][index_1: int][key_1: str][key_2: str][index_2: int]...

        Applicability: To be used as part of configuration (e.g., YAML-based files or text strings).
        Extendability: Readily extensible to include "slice" and other standard accessors (as long as no dynamic elements).
        """

        try:
            return condition_parser.parseString(self._condition)
        except ParseException as e:
            print(str(e.value))
            raise ExpectationConfigurationConditionParserError(
                f'Unable to parse Expectation Configuration Condition: "{self._condition}".'
            )

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
            for parameter_name, fully_qualified_parameter_name in self._expectation_kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )
        parsed_condition: ParseResults = self._parse_condition()

        parameter: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=parsed_condition[0]
            + parsed_condition[1]
            + parsed_condition[2],
            expected_return_type=np.int64,
            variables=variables,
            parameters=parameters,
        )

        condition = False
        if parsed_condition[3] == ">":
            condition = parameter > parsed_condition[4]

        if condition:
            return ExpectationConfiguration(
                expectation_type=self._expectation_type,
                kwargs=expectation_kwargs,
                meta=meta,
                success_on_last_run=self._success_on_last_run,
            )
        else:
            return None

    @property
    def condition(self) -> str:
        return self._condition
